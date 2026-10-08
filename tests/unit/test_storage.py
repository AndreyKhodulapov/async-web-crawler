"""Unit tests for DataStorage: buffering, batches, retries and closing."""

import asyncio
import logging

import pytest
from helpers import FakeClock, MemoryStorage, make_record

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

    async def test_flush_after_a_failed_close_writes_nothing(self):
        storage = MemoryStorage(failures=[OSError("disk full")] * 4)
        await save_pages(storage, "a")
        with pytest.raises(StorageError):
            await storage.close()

        await storage.flush()

        assert storage.attempts == 4
        assert storage.pending == 0


class TestOpening:
    async def test_open_writes_nothing(self):
        storage = MemoryStorage(batch_size=1)

        await storage.open()
        await storage.open()

        assert (storage.batches, storage.written, storage.released) == ([], 0, 0)
        await save_pages(storage, "a")
        assert storage.urls == [["a"]]

    async def test_open_after_close_fails(self):
        storage = MemoryStorage()
        await storage.close()

        with pytest.raises(StorageError, match="MemoryStorage is closed"):
            await storage.open()

    @pytest.mark.parametrize(
        "error", [OSError("read-only file system"), TypeError("not a path")], ids=["write error", "other"]
    )
    async def test_a_storage_that_cannot_be_opened_is_a_storage_error(self, error):
        class Unopenable(MemoryStorage):
            async def _open_storage(self) -> None:
                raise error

        storage = Unopenable()

        with pytest.raises(StorageError, match=f"Unopenable cannot be opened: {error}") as raised:
            await storage.open()

        assert raised.value.__cause__ is error

    async def test_a_storage_error_of_opening_is_passed_on(self):
        class Unopenable(MemoryStorage):
            async def _open_storage(self) -> None:
                raise StorageError("pages.jsonl is not JSON Lines")

        with pytest.raises(StorageError, match="^pages.jsonl is not JSON Lines$"):
            await Unopenable().open()


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

    async def test_only_the_record_that_cannot_be_written_is_dropped(self, caplog):
        names = [f"page-{number}" for number in range(10)]
        storage = MemoryStorage(batch_size=10, refused={"page-3"})

        with caplog.at_level(logging.ERROR, logger="crawler.storage"), pytest.raises(ValueError, match="page-3"):
            await save_pages(storage, *names)

        # The batch is written again one record at a time.
        assert storage.urls == [[name] for name in names if name != "page-3"]
        assert (storage.pending, storage.written) == (0, 9)
        assert "Dropped the record of page-3: MemoryStorage cannot write it: cannot write page-3" in caplog.text

    async def test_storage_goes_on_after_a_record_is_dropped(self):
        storage = MemoryStorage(batch_size=2, refused={"a"})
        with pytest.raises(ValueError):
            await save_pages(storage, "a", "b")

        await save_pages(storage, "c", "d")

        assert storage.urls == [["b"], ["c", "d"]]
        assert (storage.pending, storage.written) == (0, 3)

    async def test_batch_of_one_record_that_cannot_be_written_is_dropped(self, caplog):
        storage = MemoryStorage(batch_size=1, refused={"a"})

        with caplog.at_level(logging.ERROR, logger="crawler.storage"), pytest.raises(ValueError):
            await save_pages(storage, "a")

        assert storage.attempts == 1  # not written again on its own
        assert (storage.pending, storage.written) == (0, 0)
        assert "Dropped the record of a: MemoryStorage cannot write it" in caplog.text

    async def test_batch_that_fails_only_whole_is_written_one_by_one(self, caplog):
        storage = MemoryStorage(batch_size=2, failures=[TypeError("not serializable")])

        with caplog.at_level(logging.WARNING, logger="crawler.storage"):
            await save_pages(storage, "a", "b")

        # Nothing is lost, so nothing is raised.
        assert storage.urls == [["a"], ["b"]]
        assert (storage.pending, storage.written) == (0, 2)
        assert "wrote its records one by one" in caplog.text

    async def test_storage_error_of_a_write_is_not_retried_and_drops_the_records(self):
        # Such as a file of another layout, found out when the first write opens it.
        storage = MemoryStorage(batch_size=2, failures=[StorageError("not JSON Lines")] * 3)

        with pytest.raises(StorageError, match="^not JSON Lines$"):
            await save_pages(storage, "a", "b")

        assert storage.attempts == 3  # the batch, then each record
        assert (storage.pending, storage.written) == (0, 0)

    async def test_write_error_while_writing_one_by_one_keeps_the_rest(self):
        class DiskFillsUp(MemoryStorage):
            async def _write_batch(self, records):
                if self.batches:
                    raise OSError("disk full")
                await super()._write_batch(records)

        storage = DiskFillsUp(batch_size=3, refused={"a"})

        # a is dropped, b written, c fails with a write error and its retries.
        with pytest.raises(StorageError, match="disk full"):
            await save_pages(storage, "a", "b", "c")

        assert storage.urls == [["b"]]
        assert (storage.pending, storage.written) == (1, 1)
        assert [record["url"] for record in storage._buffer] == ["c"]

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


class TestCooldown:
    """After a write that ran out of retries `save` only buffers for `cooldown` seconds."""

    @staticmethod
    async def failed_storage(clock: FakeClock, failures: int = 4) -> MemoryStorage:
        storage = MemoryStorage(batch_size=1, failures=[OSError("disk full")] * failures, cooldown=5, clock=clock)
        with pytest.raises(StorageError):
            await storage.save(make_record("a"))
        return storage

    async def test_save_does_not_write_during_the_cooldown(self):
        clock = FakeClock()
        storage = await self.failed_storage(clock)
        clock.now += 4.9

        await save_pages(storage, "b", "c")

        assert storage.attempts == 4
        assert (storage.pending, storage.written) == (3, 0)

    async def test_save_writes_again_after_the_cooldown(self):
        clock = FakeClock()
        storage = await self.failed_storage(clock)
        await save_pages(storage, "b")
        clock.now += 5

        await save_pages(storage, "c")

        assert storage.urls == [["a", "b", "c"]]

    async def test_flush_writes_during_the_cooldown(self):
        storage = await self.failed_storage(FakeClock())

        await storage.flush()

        assert storage.urls == [["a"]]

    async def test_successful_write_ends_the_cooldown(self):
        storage = await self.failed_storage(FakeClock())
        await storage.flush()

        await save_pages(storage, "b")

        assert storage.urls == [["a"], ["b"]]

    async def test_another_failure_starts_the_cooldown_anew(self):
        clock = FakeClock()
        storage = await self.failed_storage(clock, failures=8)
        clock.now += 5
        with pytest.raises(StorageError):
            await storage.save(make_record("b"))
        clock.now += 4.9

        await save_pages(storage, "c")

        assert storage.attempts == 8

    async def test_write_failed_tells_of_records_a_write_could_not_take(self):
        storage = MemoryStorage(batch_size=1, failures=[OSError("disk full")] * 4)
        assert not storage.write_failed

        with pytest.raises(StorageError):
            await save_pages(storage, "a")
        assert storage.write_failed

        await storage.flush()
        assert not storage.write_failed
        assert storage.urls == [["a"]]

    async def test_write_failed_is_cleared_once_the_records_are_gone(self):
        # The record left by the failed write is then one no write can take: dropped.
        storage = MemoryStorage(batch_size=1, failures=[OSError("disk full")] * 4 + [TypeError("not serializable")])
        with pytest.raises(StorageError):
            await save_pages(storage, "a")

        with pytest.raises(TypeError):
            await storage.flush()

        assert not storage.write_failed
        assert storage.pending == 0

    async def test_record_that_cannot_be_written_is_no_failed_write(self):
        storage = MemoryStorage(batch_size=2, refused={"a"})

        with pytest.raises(ValueError):
            await save_pages(storage, "a", "b")

        assert not storage.write_failed

    def test_negative_cooldown_is_refused(self):
        with pytest.raises(ValueError, match="cooldown"):
            MemoryStorage(cooldown=-1)


class TestSettled:
    @staticmethod
    def listened(storage: DataStorage) -> list[list[str]]:
        """The URLs `on_settled` is called with, a list per call, and those of `on_dropped` marked "dropped"."""
        calls: list[list[str]] = []

        async def on_settled(urls: list[str]) -> None:
            calls.append(urls)

        async def on_dropped(urls: list[str]) -> None:
            calls.append(["dropped", *urls])

        storage.on_settled, storage.on_dropped = on_settled, on_dropped
        return calls

    async def test_urls_of_a_written_batch_are_reported(self):
        storage = MemoryStorage(batch_size=2)
        calls = self.listened(storage)

        await save_pages(storage, "a")
        assert calls == []
        await save_pages(storage, "b")

        assert calls == [["a", "b"]]

    async def test_records_of_a_failed_write_are_reported_once_written(self):
        storage = MemoryStorage(batch_size=2, failures=[OSError("disk full")] * 4)
        calls = self.listened(storage)
        with pytest.raises(StorageError):
            await save_pages(storage, "a", "b")
        assert calls == []

        await storage.flush()

        assert calls == [["a", "b"]]

    async def test_records_dropped_are_reported_apart_from_those_written(self):
        names = [f"page-{number}" for number in range(4)]
        storage = MemoryStorage(batch_size=4, refused={"page-1"})
        calls = self.listened(storage)

        with pytest.raises(ValueError):
            await save_pages(storage, *names)

        # Written one by one: each is reported once written or dropped.
        assert calls == [["page-0"], ["dropped", "page-1"], ["page-2"], ["page-3"]]

    async def test_batch_of_one_record_dropped_is_reported(self):
        storage = MemoryStorage(batch_size=1, refused={"a"})
        calls = self.listened(storage)

        with pytest.raises(ValueError):
            await save_pages(storage, "a")

        assert calls == [["dropped", "a"]]

    async def test_records_lost_at_close_are_not_reported(self):
        storage = MemoryStorage(batch_size=2, failures=[OSError("disk full")] * 4)
        calls = self.listened(storage)
        await save_pages(storage, "a")

        with pytest.raises(StorageError):
            await storage.close()

        assert calls == []

    async def test_error_of_the_listener_is_logged_and_the_write_goes_on(self, caplog):
        storage = MemoryStorage(batch_size=1)

        async def on_settled(urls: list[str]) -> None:
            raise RuntimeError("frontier is down")

        storage.on_settled = on_settled
        with caplog.at_level(logging.ERROR, logger="crawler.storage"):
            await save_pages(storage, "a", "b")

        assert storage.urls == [["a"], ["b"]]
        assert "Failed to report 1 records settled by MemoryStorage" in caplog.text

    async def test_error_of_the_listener_of_drops_is_logged(self, caplog):
        storage = MemoryStorage(batch_size=1, refused={"a"})

        async def on_dropped(urls: list[str]) -> None:
            raise RuntimeError("frontier is down")

        storage.on_dropped = on_dropped
        with caplog.at_level(logging.ERROR, logger="crawler.storage"), pytest.raises(ValueError):
            await save_pages(storage, "a")

        assert storage.pending == 0
        assert "Failed to report 1 records dropped by MemoryStorage" in caplog.text
