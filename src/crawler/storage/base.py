"""Base class of the storages that keep crawled pages."""

import asyncio
import contextlib
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
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
    records stay in the buffer, so the next write takes them along. For
    `cooldown` seconds after that `save` only buffers the records, so that
    a storage that is down does not make every save wait for the retries;
    `flush` and `close` write at once all the same. Any
    other error is raised as it is and its batch is dropped: no retry cures
    it, and kept in the buffer the batch would fail every later write.

    `open` opens the file or the connection ahead of the first write and
    checks that it can be written to: a storage that cannot be is found
    out before anything is crawled, not a batch of pages later. Without
    it, the first write does the same.

    A subclass implements `_write_batch`, `_read` and `_close`, and lists in
    `WRITE_ERRORS` the exceptions its writes fail with; `_open_storage`
    does what the first write would otherwise do to open the storage.
    """

    WRITE_ERRORS: ClassVar[tuple[type[Exception], ...]] = (OSError,)

    def __init__(
        self,
        batch_size: int = 100,
        *,
        retry_strategy: RetryStrategy | None = None,
        cooldown: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if cooldown < 0:
            raise ValueError(f"cooldown must be >= 0, got {cooldown}")
        self.batch_size = batch_size
        self.cooldown = cooldown
        self._clock = clock
        self._paused_until = 0.0
        self.retry_strategy = retry_strategy or RetryStrategy(retry_on=self.WRITE_ERRORS, base_delay=0.1)
        self._buffer: list[PageRecord] = []
        # Keeps the batches in order and a record out of two writes at once.
        self._lock = asyncio.Lock()
        self._closed = False
        self._written = 0

    @property
    def pending(self) -> int:
        """The number of records saved but not written out yet."""
        return len(self._buffer)

    @property
    def written(self) -> int:
        """The number of records written out since the storage was created."""
        return self._written

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def open(self) -> None:
        """Open the file or the connection and check that it can be written to.

        Does up front what the first write would do otherwise: the file is
        opened and checked, the database connected and its table created.
        Nothing is written. Safe to call more than once.

        Raises:
            StorageError: the storage cannot be opened (the file is of
                another layout or cannot be written, the database cannot
                be reached), or it is closed.
        """
        async with self._lock:
            if self._closed:
                raise StorageError(f"{type(self).__name__} is closed")
            try:
                await self._open_storage()
            except StorageError:
                raise
            except Exception as error:
                raise StorageError(f"{type(self).__name__} cannot be opened: {error}") from error

    async def save(self, record: PageRecord) -> None:
        """Add a page to the storage; it is written with the rest of its batch.

        Raises:
            StorageError: the storage is closed, or the batch this record
                completed could not be written. The record is kept either
                way, unless the storage is closed. During the `cooldown`
                after a failed write nothing is written and nothing raised.
            Exception: the batch failed with an error outside `WRITE_ERRORS`
                and is dropped, this record included.
        """
        async with self._lock:
            if self._closed:
                raise StorageError(f"{type(self).__name__} is closed")
            self._buffer.append(record)
            if len(self._buffer) >= self.batch_size and self._clock() >= self._paused_until:
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
        async with contextlib.aclosing(self._read()) as records:
            async for record in records:
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
                # Whatever is still here is lost: a later flush must not
                # reopen what `_close` releases.
                self._buffer = []
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
            self._paused_until = self._clock() + self.cooldown
            raise StorageError(f"failed to write {len(batch)} records: {error}") from error
        except Exception:
            # No retry cures this error, so the next write of the same batch
            # would fail too, and every one after it.
            self._buffer = []
            logger.error("Dropped %d records that %s cannot write", len(batch), type(self).__name__)
            raise
        self._buffer = []
        self._paused_until = 0.0
        self._written += len(batch)
        logger.debug("Wrote %d records to %s", len(batch), type(self).__name__)

    async def _open_storage(self) -> None:
        """Open the file or the connection, unless it is open; what the first write does otherwise. Nothing by default."""

    @abstractmethod
    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        """Write the records in one go: a failed write is repeated with the same records."""

    @abstractmethod
    def _read(self) -> AsyncIterator[PageRecord]:
        """Iterate over the records written so far, oldest first."""

    @abstractmethod
    async def _close(self) -> None:
        """Release the file or the connection."""


def warn_adding_to_file(path: Path, size: int) -> None:
    """Log that a file storage adds pages to a file that is not empty, such as one of an earlier run."""
    logger.warning(
        "%s already has %d bytes: adding the pages to it; overwrite (storage.overwrite, --overwrite) starts it anew",
        path,
        size,
    )
