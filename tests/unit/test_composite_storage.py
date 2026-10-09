"""Unit tests for CompositeStorage: every page goes to several storages."""

import asyncio

import pytest
from helpers import MemoryStorage, make_record

from crawler import CompositeStorage, StorageError

DISK_FULL = OSError("disk full")
# More failures than any test has writes: the storage never recovers.
ALWAYS = 1000


async def save_pages(storage: CompositeStorage, *names: str) -> None:
    for name in names:
        await storage.save(make_record(name))


class TestSaving:
    async def test_record_goes_to_every_storage(self):
        first, second = MemoryStorage(batch_size=1), MemoryStorage(batch_size=1)
        storage = CompositeStorage(first, second)

        await save_pages(storage, "a", "b")

        assert first.urls == second.urls == [["a"], ["b"]]

    async def test_each_storage_keeps_its_batch_size(self):
        first, second = MemoryStorage(batch_size=1), MemoryStorage(batch_size=3)
        storage = CompositeStorage(first, second)

        await save_pages(storage, "a", "b")
        assert (first.urls, second.urls) == ([["a"], ["b"]], [])

        await storage.flush()
        assert second.urls == [["a", "b"]]

    async def test_close_writes_the_buffers_and_closes_every_storage(self):
        first, second = MemoryStorage(), MemoryStorage()

        async with CompositeStorage(first, second) as storage:
            await save_pages(storage, "a")

        assert first.urls == second.urls == [["a"]]
        assert (first.released, second.released) == (1, 1)
        await storage.close()
        assert (first.released, second.released) == (1, 1)

    async def test_read_gives_the_records_of_the_first_storage(self):
        first, second = MemoryStorage(), MemoryStorage()
        storage = CompositeStorage(first, second)
        await save_pages(storage, "a", "b")

        assert [record["url"] async for record in storage.read()] == ["a", "b"]
        # Reading writes out the buffers of all the storages, as of any storage.
        assert second.urls == [["a", "b"]]

    async def test_concurrent_saves_lose_and_repeat_nothing(self):
        first, second = MemoryStorage(batch_size=3), MemoryStorage(batch_size=4)
        storage = CompositeStorage(first, second)
        names = [f"page-{number}" for number in range(10)]

        await asyncio.gather(*(storage.save(make_record(name)) for name in names))
        await storage.close()

        for part in (first, second):
            assert sorted(url for batch in part.urls for url in batch) == names

    def test_at_least_one_storage_is_required(self):
        with pytest.raises(ValueError, match="at least one storage"):
            CompositeStorage()


class TestFailures:
    async def test_failed_storage_does_not_keep_the_others_from_saving(self):
        broken = MemoryStorage(batch_size=1, failures=[DISK_FULL] * ALWAYS)
        working = MemoryStorage(batch_size=1)
        storage = CompositeStorage(broken, working)

        with pytest.raises(StorageError) as raised:
            await storage.save(make_record("a"))

        assert working.urls == [["a"]]
        message = str(raised.value)
        assert message.startswith(
            "failed to save the page to 1 of 2 storages: MemoryStorage: failed to write 1 records"
        )
        assert isinstance(raised.value.__cause__, StorageError)

    async def test_every_failed_storage_is_named(self):
        class OtherStorage(MemoryStorage):
            pass

        storage = CompositeStorage(
            MemoryStorage(batch_size=1, failures=[DISK_FULL] * ALWAYS),
            OtherStorage(batch_size=1, failures=[TypeError("not serializable")]),
        )

        with pytest.raises(StorageError) as raised:
            await storage.save(make_record())

        message = str(raised.value)
        assert "2 of 2 storages" in message
        assert "MemoryStorage: failed to write 1 records: disk full" in message
        assert "OtherStorage: not serializable" in message

    async def test_open_opens_every_storage_and_names_the_one_that_cannot_be(self):
        class Unopenable(MemoryStorage):
            async def _open_storage(self) -> None:
                raise OSError("read-only file system")

        opened = []

        class Openable(MemoryStorage):
            async def _open_storage(self) -> None:
                opened.append(self)

        working = Openable()
        storage = CompositeStorage(Unopenable(), working)

        with pytest.raises(StorageError) as raised:
            await storage.open()

        assert opened == [working]
        assert str(raised.value).startswith(
            "failed to open 1 of 2 storages: Unopenable: Unopenable cannot be opened: read-only file system"
        )

    async def test_close_closes_the_others_when_one_fails(self):
        broken = MemoryStorage(failures=[DISK_FULL] * ALWAYS)
        working = MemoryStorage()
        storage = CompositeStorage(broken, working)
        await storage.save(make_record("a"))

        with pytest.raises(StorageError, match="failed to close 1 of 2 storages"):
            await storage.close()

        assert working.urls == [["a"]]
        assert (broken.released, working.released) == (1, 1)

    async def test_save_after_close_fails(self):
        storage = CompositeStorage(MemoryStorage(), MemoryStorage())
        await storage.close()

        with pytest.raises(StorageError, match="2 of 2 storages: MemoryStorage: MemoryStorage is closed"):
            await storage.save(make_record())

    async def test_cancellation_is_not_reported_as_a_failure(self):
        class StuckStorage(MemoryStorage):
            async def _write_batch(self, records) -> None:
                await asyncio.Event().wait()

        storage = CompositeStorage(StuckStorage(batch_size=1), MemoryStorage(batch_size=1))
        task = asyncio.create_task(storage.save(make_record()))
        await asyncio.sleep(0.01)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task


class TestCounters:
    async def test_page_is_written_once_every_storage_has_written_it(self):
        first, second = MemoryStorage(batch_size=1), MemoryStorage(batch_size=3)
        storage = CompositeStorage(first, second)

        await save_pages(storage, "a", "b")
        assert (storage.pending, storage.written) == (2, 0)

        await save_pages(storage, "c", "d")
        assert (storage.pending, storage.written) == (1, 3)

        await storage.flush()
        assert (storage.pending, storage.written) == (0, 4)

    async def test_page_a_storage_cannot_write_stays_pending(self):
        broken = MemoryStorage(batch_size=1, failures=[DISK_FULL] * ALWAYS)
        storage = CompositeStorage(MemoryStorage(batch_size=1), broken)

        with pytest.raises(StorageError):
            await storage.save(make_record())

        assert (storage.pending, storage.written) == (1, 0)
        assert storage.write_failed

        broken.failures = []
        await storage.flush()
        assert not storage.write_failed


class TestSettled:
    async def test_record_is_reported_once_every_storage_has_settled_it(self):
        first, second = MemoryStorage(batch_size=1), MemoryStorage(batch_size=3, refused={"b"})
        storage = CompositeStorage(first, second)
        calls: list[list[str]] = []

        async def on_settled(urls: list[str]) -> None:
            calls.append(urls)

        async def on_dropped(urls: list[str]) -> None:
            calls.append(["dropped", *urls])

        storage.on_settled, storage.on_dropped = on_settled, on_dropped
        await save_pages(storage, "a", "b")
        assert calls == []

        # The second storage writes a and drops b, which the first has written.
        with pytest.raises(StorageError):
            await storage.flush()

        assert calls == [["a"], ["dropped", "b"]]

    async def test_record_dropped_before_the_others_write_it_is_reported_dropped(self):
        first, second = MemoryStorage(batch_size=1, refused={"a"}), MemoryStorage(batch_size=3)
        storage = CompositeStorage(first, second)
        calls: list[list[str]] = []

        async def on_dropped(urls: list[str]) -> None:
            calls.append(urls)

        storage.on_dropped = on_dropped
        with pytest.raises(StorageError):
            await save_pages(storage, "a")
        assert calls == []

        await storage.flush()

        assert calls == [["a"]]
        assert second.urls == [["a"]]
