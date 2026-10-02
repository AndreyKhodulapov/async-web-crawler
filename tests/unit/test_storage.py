"""Unit tests for DataStorage: buffering, batches, retries and closing."""

import asyncio
import logging

import pytest
from helpers import MemoryStorage, make_record

from crawler import DataStorage, RetryStrategy, StorageError


async def save_pages(storage: DataStorage, *names: str) -> None:
    for name in names:
        await storage.save(make_record(name))


class TestBuffering:
    async def test_records_wait_for_a_full_batch(self):
        storage = MemoryStorage(batch_size=3)

        await save_pages(storage, "a", "b")
        assert storage.batches == []

        await save_pages(storage, "c", "d")
        assert storage.urls == [["a", "b", "c"]]

    async def test_flush_writes_an_incomplete_batch(self):
        storage = MemoryStorage(batch_size=3)
        await save_pages(storage, "a", "b")

        await storage.flush()

        assert storage.urls == [["a", "b"]]

    async def test_flush_of_an_empty_buffer_writes_nothing(self):
        storage = MemoryStorage()

        await storage.flush()

        assert storage.attempts == 0

    async def test_batch_size_of_one_writes_every_record(self):
        storage = MemoryStorage(batch_size=1)

        await save_pages(storage, "a", "b")

        assert storage.urls == [["a"], ["b"]]

    async def test_record_is_written_as_saved(self):
        storage = MemoryStorage(batch_size=1)
        record = make_record(title="Café", links=[])

        await storage.save(record)

        assert storage.batches == [[record]]

    async def test_concurrent_saves_lose_and_repeat_nothing(self):
        storage = MemoryStorage(batch_size=4)
        names = [f"page-{number}" for number in range(10)]

        await asyncio.gather(*(storage.save(make_record(name)) for name in names))
        await storage.close()

        assert [len(batch) for batch in storage.batches] == [4, 4, 2]
        assert sorted(url for batch in storage.urls for url in batch) == names

    async def test_read_includes_buffered_records(self):
        storage = MemoryStorage(batch_size=2)
        await save_pages(storage, "a", "b", "c")

        assert [record["url"] async for record in storage.read()] == ["a", "b", "c"]

    async def test_pending_and_written_count_the_records(self):
        storage = MemoryStorage(batch_size=3)

        await save_pages(storage, "a", "b", "c", "d")
        assert (storage.pending, storage.written) == (1, 3)

        await storage.close()
        assert (storage.pending, storage.written) == (0, 4)

    async def test_records_of_a_failed_write_stay_pending(self):
        storage = MemoryStorage(batch_size=2, failures=[OSError("disk full")] * 4)

        with pytest.raises(StorageError):
            await save_pages(storage, "a", "b")

        assert (storage.pending, storage.written) == (2, 0)

    @pytest.mark.parametrize("batch_size", [0, -1])
    def test_batch_size_must_be_positive(self, batch_size):
        with pytest.raises(ValueError, match="batch_size"):
            MemoryStorage(batch_size=batch_size)


class TestClosing:
    async def test_close_writes_the_buffer_and_releases_the_storage(self):
        storage = MemoryStorage()
        await save_pages(storage, "a")

        await storage.close()

        assert storage.urls == [["a"]]
        assert storage.released == 1

    async def test_close_twice_releases_once(self):
        storage = MemoryStorage()

        await storage.close()
        await storage.close()

        assert storage.released == 1

    async def test_context_manager_closes(self):
        async with MemoryStorage() as storage:
            await save_pages(storage, "a")

        assert storage.urls == [["a"]]
        assert storage.released == 1

    async def test_save_after_close_fails(self):
        storage = MemoryStorage(batch_size=1)
        await storage.close()

        with pytest.raises(StorageError, match="MemoryStorage is closed"):
            await storage.save(make_record())

        assert storage.batches == []

    async def test_storage_is_released_even_if_the_last_write_fails(self):
        storage = MemoryStorage(failures=[OSError("disk full")] * 4)
        await save_pages(storage, "a")

        with pytest.raises(StorageError):
            await storage.close()

        assert storage.released == 1


class TestWriteErrors:
    async def test_failed_write_is_retried(self, caplog):
        storage = MemoryStorage(batch_size=2, failures=[OSError("disk busy")])

        with caplog.at_level(logging.WARNING, logger="crawler.retry"):
            await save_pages(storage, "a", "b")

        assert storage.attempts == 2
        assert storage.urls == [["a", "b"]]
        assert "write of 2 records to MemoryStorage failed: OSError: disk busy; retrying" in caplog.text

    async def test_storage_error_once_retries_run_out(self, caplog):
        storage = MemoryStorage(batch_size=1, failures=[OSError("disk full")] * 4)

        with caplog.at_level(logging.WARNING, logger="crawler.retry"), pytest.raises(StorageError) as raised:
            await storage.save(make_record())

        assert storage.attempts == 4  # the write and 3 retries
        assert "failed to write 1 records: disk full" in str(raised.value)
        assert isinstance(raised.value.__cause__, OSError)
        assert "Failed write of 1 records to MemoryStorage on attempt 4/4" in caplog.text

    async def test_records_of_a_failed_write_go_with_the_next_one(self):
        storage = MemoryStorage(batch_size=2, failures=[OSError("disk full")] * 4)
        with pytest.raises(StorageError):
            await save_pages(storage, "a", "b")

        await save_pages(storage, "c")

        assert storage.urls == [["a", "b", "c"]]

    async def test_other_errors_are_not_retried(self):
        storage = MemoryStorage(batch_size=1, failures=[TypeError("not serializable")])

        with pytest.raises(TypeError):
            await storage.save(make_record())

        assert storage.attempts == 1

    async def test_retries_can_be_turned_off(self):
        storage = MemoryStorage(batch_size=1, failures=[OSError("disk full")], retry_strategy=RetryStrategy(0))

        with pytest.raises(StorageError):
            await storage.save(make_record())

        assert storage.attempts == 1

    def test_default_strategy_retries_the_write_errors_of_the_storage(self):
        class Storage(MemoryStorage):
            WRITE_ERRORS = (OSError, KeyError)

        strategy = Storage(retry_strategy=None).retry_strategy

        assert strategy.retry_on == (OSError, KeyError)
        assert strategy.max_retries == 3
