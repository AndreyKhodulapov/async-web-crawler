"""Unit tests for JSONStorage: both file layouts, appending, overwriting, reading back."""

import json
import logging

import pytest
from helpers import make_record

from crawler import JSONStorage, RetryStrategy, StorageError

FAST_RETRIES = RetryStrategy(retry_on=(OSError,), base_delay=0.001, max_delay=0.001)
LAYOUTS = pytest.mark.parametrize("indent", [None, 2], ids=["lines", "array"])


def make_records(count: int) -> list:
    return [make_record(f"https://site/page-{number}", title=f"Page {number}") for number in range(count)]


def as_json(record: dict) -> dict:
    return record | {"crawled_at": record["crawled_at"].isoformat()}


async def read_all(storage: JSONStorage) -> list:
    return [record async for record in storage.read()]


class TestJSONLines:
    async def test_every_record_is_a_line(self, tmp_path):
        path = tmp_path / "pages.jsonl"
        records = make_records(3)

        async with JSONStorage(path, batch_size=2) as storage:
            for record in records:
                await storage.save(record)

        lines = path.read_text(encoding="utf-8").splitlines()
        assert [json.loads(line) for line in lines] == [as_json(record) for record in records]

    async def test_text_is_not_escaped(self, tmp_path):
        path = tmp_path / "pages.jsonl"

        async with JSONStorage(path) as storage:
            await storage.save(make_record(title="Crème brûlée 日本語"))

        assert "Crème brûlée 日本語" in path.read_text(encoding="utf-8")


class TestJSONArray:
    async def test_file_is_an_indented_array(self, tmp_path):
        path = tmp_path / "pages.json"
        records = make_records(5)

        async with JSONStorage(path, indent=2, batch_size=2) as storage:
            for record in records:
                await storage.save(record)

        expected = json.dumps([as_json(record) for record in records], indent=2, ensure_ascii=False)
        assert path.read_text(encoding="utf-8") == expected + "\n"

    async def test_file_is_valid_after_every_write(self, tmp_path):
        path = tmp_path / "pages.json"
        storage = JSONStorage(path, indent=4, batch_size=1)

        for count, record in enumerate(make_records(3), start=1):
            await storage.save(record)
            assert len(json.loads(path.read_text(encoding="utf-8"))) == count

        await storage.close()

    async def test_array_of_another_program_is_left_alone(self, tmp_path):
        path = tmp_path / "pages.json"
        path.write_text('[{"url": "https://site/a"}]', encoding="utf-8")

        storage = JSONStorage(path, indent=2, batch_size=1)
        with pytest.raises(StorageError, match="it was not written"):
            await storage.save(make_record())

        assert path.read_text(encoding="utf-8") == '[{"url": "https://site/a"}]'


@LAYOUTS
class TestBothLayouts:
    async def test_records_are_read_back_as_saved(self, tmp_path, indent):
        records = [
            make_record("https://site/a", title='Quotes " and \\ and\nnew lines', text="Crème brûlée 日本語 🙂"),
            make_record("https://site/b", title="", links=[], metadata={}, content_type=""),
            make_record("https://site/c", metadata={"nested": {"deep": [1, 2.5, None, True]}, "braces": "}{]["}),
        ]

        async with JSONStorage(tmp_path / "pages", indent=indent) as storage:
            for record in records:
                await storage.save(record)

            assert await read_all(storage) == records

    async def test_records_are_added_to_an_existing_file(self, tmp_path, indent):
        path = tmp_path / "pages"
        first, second = make_records(4)[:2], make_records(4)[2:]
        async with JSONStorage(path, indent=indent) as storage:
            for record in first:
                await storage.save(record)

        async with JSONStorage(path, indent=indent) as storage:
            for record in second:
                await storage.save(record)

            assert await read_all(storage) == first + second
        if indent is not None:
            assert len(json.loads(path.read_text(encoding="utf-8"))) == 4

    async def test_reading_in_small_pieces(self, tmp_path, indent, monkeypatch):
        monkeypatch.setattr(JSONStorage, "READ_CHUNK", 7)
        records = [make_record(f"https://site/{name}", text="Crème brûlée 日本語") for name in "abc"]

        async with JSONStorage(tmp_path / "pages", indent=indent) as storage:
            for record in records:
                await storage.save(record)

            assert await read_all(storage) == records

    async def test_many_records(self, tmp_path, indent):
        records = make_records(3000)

        async with JSONStorage(tmp_path / "pages", indent=indent, batch_size=500) as storage:
            for record in records:
                await storage.save(record)

            assert await read_all(storage) == records

    async def test_no_file_until_a_record_is_written(self, tmp_path, indent):
        path = tmp_path / "pages"

        async with JSONStorage(path, indent=indent) as storage:
            assert await read_all(storage) == []

        assert not path.exists()

    async def test_retried_write_does_not_repeat_records(self, tmp_path, indent):
        records = make_records(4)
        storage = JSONStorage(tmp_path / "pages", indent=indent, batch_size=2, retry_strategy=FAST_RETRIES)
        for record in records[:2]:
            await storage.save(record)
        flush = storage._file.flush
        failures = [OSError("disk busy")]

        async def flush_failing_once() -> None:
            await flush()
            if failures:
                raise failures.pop()

        storage._file.flush = flush_failing_once
        for record in records[2:]:
            await storage.save(record)

        assert failures == []
        assert await read_all(storage) == records
        await storage.close()

    async def test_file_of_the_other_layout_is_left_alone(self, tmp_path, indent):
        path = tmp_path / "pages"
        other = 2 if indent is None else None
        async with JSONStorage(path, indent=other) as storage:
            await storage.save(make_record())
        written = path.read_bytes()

        storage = JSONStorage(path, indent=indent, batch_size=1)
        with pytest.raises(StorageError, match="it was not written"):
            await storage.save(make_record())

        assert path.read_bytes() == written

    async def test_truncated_file_is_reported(self, tmp_path, indent):
        path = tmp_path / "pages"
        async with JSONStorage(path, indent=indent) as storage:
            for record in make_records(2):
                await storage.save(record)
        whole = path.read_bytes()
        path.write_bytes(whole[: len(whole) - 40])

        reader = JSONStorage(path, indent=indent)
        with pytest.raises(StorageError, match="ends with a broken record"):
            await read_all(reader)

    async def test_nothing_is_added_to_a_truncated_file(self, tmp_path, indent):
        path = tmp_path / "pages"
        async with JSONStorage(path, indent=indent) as storage:
            for record in make_records(2):
                await storage.save(record)
        whole = path.read_bytes()
        path.write_bytes(whole[: len(whole) - 40])

        storage = JSONStorage(path, indent=indent, batch_size=1)
        with pytest.raises(StorageError, match="line break|it was not written"):
            await storage.save(make_record())

        assert path.read_bytes() == whole[: len(whole) - 40]

    async def test_record_that_is_not_a_page_is_reported(self, tmp_path, indent):
        path = tmp_path / "pages"
        path.write_text('{"url": "https://site/a"}\n' if indent is None else '[\n  {"url": "https://site/a"}\n]\n')

        with pytest.raises(StorageError, match="has a record that is not a page"):
            await read_all(JSONStorage(path, indent=indent))

    async def test_file_that_is_not_utf_8_is_reported(self, tmp_path, indent):
        path = tmp_path / "pages"
        path.write_bytes(b'{"title": "caf\xe9"}\n')

        with pytest.raises(StorageError, match="is not UTF-8"):
            await read_all(JSONStorage(path, indent=indent))

    async def test_unwritable_path_is_a_storage_error(self, tmp_path, indent):
        storage = JSONStorage(tmp_path / "missing" / "pages", indent=indent, retry_strategy=FAST_RETRIES)
        await storage.save(make_record())

        with pytest.raises(StorageError, match="failed to write 1 records"):
            await storage.flush()

    async def test_record_that_is_not_json_is_refused(self, tmp_path, indent):
        storage = JSONStorage(tmp_path / "pages", indent=indent, batch_size=1)

        with pytest.raises(TypeError, match="set is not JSON serializable"):
            await storage.save(make_record(metadata={"tags": {"a"}}))


@LAYOUTS
class TestOpening:
    async def test_open_makes_the_file_and_writes_nothing(self, tmp_path, indent):
        path = tmp_path / "pages"

        async with JSONStorage(path, indent=indent) as storage:
            await storage.open()
            assert path.read_bytes() == b""
            assert storage.written == 0
            await storage.save(make_record())

        assert await read_all(JSONStorage(path, indent=indent)) == [make_record()]

    async def test_open_leaves_a_file_of_this_storage_as_it_is(self, tmp_path, indent):
        path = tmp_path / "pages"
        async with JSONStorage(path, indent=indent) as storage:
            await storage.save(make_record())
        before = path.read_bytes()

        async with JSONStorage(path, indent=indent) as storage:
            await storage.open()
            assert path.read_bytes() == before
            await storage.save(make_record("https://site/b"))

        assert [record["url"] for record in await read_all(JSONStorage(path, indent=indent))] == [
            "https://site/page",
            "https://site/b",
        ]

    async def test_open_refuses_a_file_of_the_other_layout(self, tmp_path, indent):
        path = tmp_path / "pages"
        other = JSONStorage(path, indent=None if indent is not None else 2)
        async with other as storage:
            await storage.save(make_record())
        before = path.read_bytes()

        async with JSONStorage(path, indent=indent) as storage:
            with pytest.raises(StorageError, match="is not"):
                await storage.open()

        assert path.read_bytes() == before

    async def test_open_refuses_a_path_that_cannot_be_written(self, tmp_path, indent):
        storage = JSONStorage(tmp_path / "missing" / "pages", indent=indent)

        with pytest.raises(StorageError, match="JSONStorage cannot be opened: .*missing"):
            await storage.open()


@LAYOUTS
class TestOverwrite:
    async def test_file_of_an_earlier_run_is_replaced(self, tmp_path, indent):
        path = tmp_path / "pages"
        first, second = make_records(4)[:2], make_records(4)[2:]
        async with JSONStorage(path, indent=indent) as storage:
            for record in first:
                await storage.save(record)

        async with JSONStorage(path, indent=indent, overwrite=True, batch_size=1) as storage:
            for record in second:
                await storage.save(record)

            assert await read_all(storage) == second
        if indent is not None:
            assert len(json.loads(path.read_text(encoding="utf-8"))) == 2

    async def test_file_that_cannot_be_added_to_is_replaced(self, tmp_path, indent):
        path = tmp_path / "pages"
        path.write_text("not this storage's file")
        record = make_record()

        async with JSONStorage(path, indent=indent, overwrite=True) as storage:
            await storage.save(record)

            assert await read_all(storage) == [record]

    async def test_file_is_kept_until_the_first_write(self, tmp_path, indent):
        path = tmp_path / "pages"
        path.write_text("kept")

        async with JSONStorage(path, indent=indent, overwrite=True) as storage:
            await storage.open()
            await storage.flush()

        assert path.read_text() == "kept"

    async def test_a_longer_file_of_an_earlier_run_is_cut(self, tmp_path, indent):
        path = tmp_path / "pages"
        async with JSONStorage(path, indent=indent) as storage:
            for record in make_records(20):
                await storage.save(record)
        record = make_record("https://site/new", title="New")

        async with JSONStorage(path, indent=indent, overwrite=True) as storage:
            await storage.open()
            await storage.save(record)

        assert await read_all(JSONStorage(path, indent=indent)) == [record]
        if indent is None:
            assert path.read_text(encoding="utf-8").count("\n") == 1
        else:
            assert json.loads(path.read_text(encoding="utf-8")) == [as_json(record)]

    async def test_adding_to_a_file_is_logged(self, tmp_path, indent, caplog):
        caplog.set_level(logging.WARNING, logger="crawler.storage")
        path = tmp_path / "pages"
        async with JSONStorage(path, indent=indent) as storage:
            await storage.save(make_record("https://site/a"))
        async with JSONStorage(path, indent=indent, overwrite=True) as storage:
            await storage.save(make_record("https://site/b"))
        assert caplog.records == []
        size = path.stat().st_size

        async with JSONStorage(path, indent=indent) as storage:
            await storage.save(make_record("https://site/c"))

        [warning] = caplog.records
        assert warning.levelno == logging.WARNING
        assert warning.getMessage().startswith(f"{path} already has {size} bytes: adding the pages to it;")
