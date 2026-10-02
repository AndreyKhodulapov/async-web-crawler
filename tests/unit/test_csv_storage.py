"""Unit tests for CSVStorage: header, quoting, encodings, appending, reading back."""

import codecs
import csv
import io
import json

import pytest
from helpers import make_record

from crawler import CSVStorage, RetryStrategy, StorageError

FIELDS = ["url", "title", "text", "links", "metadata", "crawled_at", "status_code", "content_type"]


def make_records(count: int) -> list:
    return [make_record(f"https://site/page-{number}", title=f"Page {number}") for number in range(count)]


async def save_all(storage: CSVStorage, records: list) -> None:
    for record in records:
        await storage.save(record)


async def read_all(storage: CSVStorage) -> list:
    return [record async for record in storage.read()]


def parse_csv(path, encoding: str = "utf-8") -> list[list[str]]:
    return list(csv.reader(io.StringIO(path.read_text(encoding=encoding), newline="")))


class TestHeader:
    async def test_header_comes_from_the_first_record(self, tmp_path):
        path = tmp_path / "pages.csv"

        async with CSVStorage(path, batch_size=2) as storage:
            await save_all(storage, make_records(5))

        rows = parse_csv(path)
        assert rows[0] == FIELDS
        assert [row[0] for row in rows[1:]] == [f"https://site/page-{number}" for number in range(5)]

    async def test_header_follows_the_fields_of_the_record(self, tmp_path):
        path = tmp_path / "prices.csv"

        async with CSVStorage(path) as storage:
            await storage.save({"sku": "A-1", "price": 9.5})
            await storage.save({"price": 12, "sku": "B-2"})

        assert parse_csv(path) == [["sku", "price"], ["A-1", "9.5"], ["B-2", "12"]]

    async def test_existing_file_keeps_its_header(self, tmp_path):
        path = tmp_path / "pages.csv"
        path.write_text("status_code,url\r\n404,https://site/old\r\n", encoding="utf-8")

        async with CSVStorage(path) as storage:
            await storage.save({"url": "https://site/new", "status_code": 200})

            assert await read_all(storage) == [
                {"url": "https://site/old", "status_code": 404},
                {"url": "https://site/new", "status_code": 200},
            ]
        assert parse_csv(path) == [["status_code", "url"], ["404", "https://site/old"], ["200", "https://site/new"]]

    async def test_records_are_added_to_an_existing_file(self, tmp_path):
        path = tmp_path / "pages.csv"
        records = make_records(4)
        async with CSVStorage(path) as storage:
            await save_all(storage, records[:2])

        async with CSVStorage(path) as storage:
            await save_all(storage, records[2:])

            assert await read_all(storage) == records
        assert [row[0] for row in parse_csv(path)].count("url") == 1

    async def test_empty_existing_file_gets_a_header(self, tmp_path):
        path = tmp_path / "pages.csv"
        path.touch()

        async with CSVStorage(path) as storage:
            await storage.save(make_record())

        assert parse_csv(path)[0] == FIELDS

    async def test_record_with_an_unknown_field_is_refused(self, tmp_path):
        storage = CSVStorage(tmp_path / "pages.csv", batch_size=1)
        await storage.save({"url": "https://site/a"})

        with pytest.raises(ValueError, match="depth"):
            await storage.save({"url": "https://site/b", "depth": 2})

    async def test_no_file_until_a_record_is_written(self, tmp_path):
        path = tmp_path / "pages.csv"

        async with CSVStorage(path) as storage:
            assert await read_all(storage) == []

        assert not path.exists()


class TestValues:
    async def test_special_characters_survive(self, tmp_path):
        records = [
            make_record("https://site/a?x=1,2", title='He said "hi", twice', text="line one\nline two\r\nline three"),
            make_record("https://site/b", title=' "quoted" ', text='a lone " quote\nand ""two""; semicolon\ttab'),
            make_record("https://site/c", title="", text="", links=[], metadata={}, content_type=""),
            make_record("https://site/d", text="Crème brûlée 日本語 🙂", links=['https://site/?q="a,b"']),
            make_record("https://site/e", metadata={"description": 'commas, "quotes"\nand lines', "depth": 3}),
        ]

        async with CSVStorage(tmp_path / "pages.csv", batch_size=2) as storage:
            await save_all(storage, records)

            assert await read_all(storage) == records

    async def test_lists_and_dicts_are_json_cells(self, tmp_path):
        path = tmp_path / "pages.csv"
        record = make_record()

        async with CSVStorage(path) as storage:
            await storage.save(record)

        row = dict(zip(*parse_csv(path), strict=True))
        assert json.loads(row["links"]) == record["links"]
        assert json.loads(row["metadata"]) == record["metadata"]
        assert row["crawled_at"] == "2025-03-14T15:09:26.535897+00:00"

    async def test_value_longer_than_the_default_field_limit(self, tmp_path):
        record = make_record(text="word " * 40_000)

        async with CSVStorage(tmp_path / "pages.csv") as storage:
            await storage.save(record)

            assert await read_all(storage) == [record]

    async def test_many_records(self, tmp_path):
        records = make_records(3000)

        async with CSVStorage(tmp_path / "pages.csv", batch_size=500) as storage:
            await save_all(storage, records)

            assert await read_all(storage) == records


class TestEncodings:
    @pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16", "cp1252", "latin-1"])
    async def test_file_is_written_in_the_encoding(self, tmp_path, encoding):
        path = tmp_path / "pages.csv"
        records = [make_record(f"https://site/{name}", title="Crème brûlée, à la carte") for name in "abc"]

        async with CSVStorage(path, encoding=encoding, batch_size=2) as storage:
            await save_all(storage, records)

            assert await read_all(storage) == records
        assert [row[1] for row in parse_csv(path, encoding)[1:]] == ["Crème brûlée, à la carte"] * 3

    async def test_byte_order_mark_is_written_once(self, tmp_path):
        path = tmp_path / "pages.csv"
        async with CSVStorage(path, encoding="utf-8-sig", batch_size=1) as storage:
            await save_all(storage, make_records(2))

        async with CSVStorage(path, encoding="utf-8-sig") as storage:
            await save_all(storage, make_records(1))

        written = path.read_bytes()
        assert written.startswith(codecs.BOM_UTF8)
        assert written.count(codecs.BOM_UTF8) == 1

    async def test_character_missing_from_the_encoding_is_replaced(self, tmp_path):
        path = tmp_path / "pages.csv"

        async with CSVStorage(path, encoding="cp1252") as storage:
            await storage.save(make_record(title="Café 日本"))

        assert parse_csv(path, "cp1252")[1][1] == "Café ??"

    def test_unknown_encoding_is_refused(self, tmp_path):
        with pytest.raises(LookupError, match="utf-99"):
            CSVStorage(tmp_path / "pages.csv", encoding="utf-99")


class TestWriteErrors:
    async def test_retried_write_does_not_repeat_rows(self, tmp_path):
        records = make_records(4)
        fast_retries = RetryStrategy(retry_on=(OSError,), base_delay=0.001, max_delay=0.001)
        storage = CSVStorage(tmp_path / "pages.csv", batch_size=2, retry_strategy=fast_retries)
        await save_all(storage, records[:2])
        flush = storage._file.flush
        failures = [OSError("disk busy")]

        async def flush_failing_once() -> None:
            await flush()
            if failures:
                raise failures.pop()

        storage._file.flush = flush_failing_once
        await save_all(storage, records[2:])

        assert failures == []
        assert await read_all(storage) == records
        await storage.close()

    async def test_header_is_written_once_when_the_first_write_fails(self, tmp_path):
        path = tmp_path / "missing" / "pages.csv"
        storage = CSVStorage(path, batch_size=1, retry_strategy=RetryStrategy(max_retries=0))
        with pytest.raises(StorageError, match="failed to write"):
            await storage.save(make_record("https://site/a"))

        path.parent.mkdir()
        await storage.save(make_record("https://site/b"))
        await storage.close()

        assert [row[0] for row in parse_csv(path)] == ["url", "https://site/a", "https://site/b"]
