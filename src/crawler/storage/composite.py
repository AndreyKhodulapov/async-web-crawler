"""Storage that keeps every page in several storages at once."""

import asyncio
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

from crawler.exceptions import StorageError
from crawler.models import PageRecord
from crawler.storage.base import DataStorage


class CompositeStorage(DataStorage):
    """Saves every page to each of the `storages`, e.g. to a file and a database::

        storage = CompositeStorage(JSONStorage("pages.jsonl"), SQLiteStorage("crawler.db"))
        crawler = AsyncCrawler(storage=storage)

    Each storage keeps its own buffer, batch size and retries; this one
    only passes the calls on, to all of them at once. A storage that fails
    does not keep the others from saving: `open`, `save`, `flush` and
    `close` reach every storage and then raise one `StorageError` naming
    those that failed.

    A page counts as `written` once every storage has written it, and as
    `pending` while any of them still buffers it; `write_failed` is that
    of any of them. A page is reported once every storage has written or
    dropped it: to `on_dropped` if any of them dropped it, to `on_settled`
    otherwise. `read` gives the records of the first storage.
    """

    def __init__(self, *storages: DataStorage) -> None:
        if not storages:
            raise ValueError("CompositeStorage needs at least one storage")
        super().__init__()
        self.storages = storages
        self._settled: Counter[str] = Counter()  # by URL, the storages that have written or dropped its record
        self._dropped: set[str] = set()  # the URLs of those settled so far whose record one of them dropped
        for storage in storages:
            storage.on_settled = self._settle_in_one
            storage.on_dropped = self._drop_in_one

    @property
    def pending(self) -> int:
        return max(storage.pending for storage in self.storages)

    @property
    def written(self) -> int:
        return min(storage.written for storage in self.storages)

    @property
    def write_failed(self) -> bool:
        return any(storage.write_failed for storage in self.storages)

    async def open(self) -> None:
        await self._for_each(lambda storage: storage.open(), "open")

    async def save(self, record: PageRecord) -> None:
        await self._for_each(lambda storage: storage.save(record), "save the page to")

    async def flush(self) -> None:
        await self._for_each(lambda storage: storage.flush(), "flush")

    async def close(self) -> None:
        await self._for_each(lambda storage: storage.close(), "close")

    async def _drop_in_one(self, urls: list[str]) -> None:
        self._dropped.update(urls)
        await self._settle_in_one(urls)

    async def _settle_in_one(self, urls: list[str]) -> None:
        """A storage has written or dropped these records: those settled by all of them are reported."""
        self._settled.update(urls)
        written, dropped = [], []
        for url in urls:
            if self._settled[url] < len(self.storages):
                continue
            del self._settled[url]
            if url in self._dropped:
                self._dropped.discard(url)
                dropped.append(url)
            else:
                written.append(url)
        if written and self.on_settled is not None:
            await self.on_settled(written)
        if dropped and self.on_dropped is not None:
            await self.on_dropped(dropped)

    async def _for_each(self, call: Callable[[DataStorage], Awaitable[None]], action: str) -> None:
        outcomes = await asyncio.gather(*(call(storage) for storage in self.storages), return_exceptions=True)
        failures = [
            (storage, outcome)
            for storage, outcome in zip(self.storages, outcomes, strict=True)
            if isinstance(outcome, BaseException)
        ]
        if not failures:
            return
        for _, error in failures:
            if not isinstance(error, Exception):
                raise error  # a cancellation is not a failure of the storage
        details = "; ".join(f"{type(storage).__name__}: {error}" for storage, error in failures)
        raise StorageError(
            f"failed to {action} {len(failures)} of {len(self.storages)} storages: {details}"
        ) from failures[0][1]

    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        # Not reached: the storages buffer and write the records themselves.
        raise NotImplementedError

    def _read(self) -> AsyncIterator[PageRecord]:
        return self.storages[0].read()

    async def _close(self) -> None:
        # Not reached either: `close` closes the storages.
        raise NotImplementedError
