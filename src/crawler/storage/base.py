"""Base class of the storages that keep crawled pages."""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from types import TracebackType
from typing import ClassVar, Self

from crawler.exceptions import StorageError
from crawler.models import PageRecord
from crawler.retry import RetryStrategy

logger = logging.getLogger(__name__)


class DataStorage(ABC):
    """Keeps crawled pages; a subclass writes them to a file or a database.

    Can be used as an async context manager, or closed explicitly::

        async with JSONStorage("pages.jsonl") as storage:
            await storage.save(record)

    `save` puts a record into a buffer, and the buffer is written out once
    it holds `batch_size` records, on `flush` and on `close`: one write per
    batch costs much less than one per record. Several tasks may save at
    once; their writes go one after another.

    A failed write is retried as `retry_strategy` says: by default the
    errors in `WRITE_ERRORS`, up to 3 times with exponential backoff from
    0.1 s. When the retries run out, `StorageError` is raised and the
    records stay in the buffer, so the next write takes them along.

    A subclass implements `_write_batch`, `_read` and `_close`, and lists in
    `WRITE_ERRORS` the exceptions its writes fail with.
    """

    WRITE_ERRORS: ClassVar[tuple[type[Exception], ...]] = (OSError,)

    def __init__(self, batch_size: int = 100, *, retry_strategy: RetryStrategy | None = None) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        self.batch_size = batch_size
        self.retry_strategy = retry_strategy or RetryStrategy(retry_on=self.WRITE_ERRORS, base_delay=0.1)
        self._buffer: list[PageRecord] = []
        # Keeps the batches in order and a record out of two writes at once.
        self._lock = asyncio.Lock()
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def save(self, record: PageRecord) -> None:
        """Add a page to the storage; it is written with the rest of its batch.

        Raises:
            StorageError: the storage is closed, or the batch this record
                completed could not be written. The record is kept either
                way, unless the storage is closed.
        """
        async with self._lock:
            if self._closed:
                raise StorageError(f"{type(self).__name__} is closed")
            self._buffer.append(record)
            if len(self._buffer) >= self.batch_size:
                await self._flush_buffer()

    async def flush(self) -> None:
        """Write out the buffered records.

        Raises:
            StorageError: the write failed, retries included.
        """
        async with self._lock:
            await self._flush_buffer()

    async def read(self) -> AsyncIterator[PageRecord]:
        """Iterate over the saved records, oldest first; the buffered ones are written out before."""
        await self.flush()
        async for record in self._read():
            yield record

    async def close(self) -> None:
        """Write out the buffered records and release the file or the connection.

        Safe to call more than once.

        Raises:
            StorageError: the buffered records could not be written and are
                lost; the storage is closed all the same.
        """
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                await self._flush_buffer()
            finally:
                await self._close()

    async def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        try:
            await self.retry_strategy.run(
                lambda: self._write_batch(batch),
                target=f"write of {len(batch)} records to {type(self).__name__}",
            )
        except self.WRITE_ERRORS as error:
            raise StorageError(f"failed to write {len(batch)} records: {error}") from error
        self._buffer = []
        logger.debug("Wrote %d records to %s", len(batch), type(self).__name__)

    @abstractmethod
    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        """Write the records in one go: a failed write is repeated with the same records."""

    @abstractmethod
    def _read(self) -> AsyncIterator[PageRecord]:
        """Iterate over the records written so far, oldest first."""

    @abstractmethod
    async def _close(self) -> None:
        """Release the file or the connection."""
