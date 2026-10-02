"""Unit tests for DatabaseStorage: what it asks of a driver."""

from collections.abc import AsyncIterator, Sequence
from datetime import datetime

import pytest
from helpers import make_record

from crawler import DatabaseDriver, DatabaseStorage, RetryStrategy, StorageError


class RecordingDriver(DatabaseDriver):
    """Keeps the calls instead of running them; a call fails with the next of `failures`, if any."""

    ID_COLUMN = "SERIAL PRIMARY KEY"
    JSON_TYPE = "JSONB"
    TIMESTAMP_TYPE = "TIMESTAMPTZ"

    def __init__(self, failures: Sequence[Exception] = ()) -> None:
        self.failures = list(failures)
        self.connections = 0
        self.closed = 0
        self.statements: list[str] = []
        self.batches: list[tuple[str, list]] = []
        self.queries: list[tuple[str, tuple]] = []
        self.rows: list[tuple] = []

    def _fail_if_told(self) -> None:
        if self.failures:
            raise self.failures.pop(0)

    async def connect(self) -> None:
        self.connections += 1

    async def execute(self, statement: str) -> None:
        self._fail_if_told()
        self.statements.append(statement)

    async def execute_many(self, statement: str, rows: Sequence[Sequence[object]]) -> None:
        self._fail_if_told()
        self.batches.append((statement, list(rows)))

    async def fetch(self, query: str, *parameters: object) -> AsyncIterator[Sequence[object]]:
        self.queries.append((query, parameters))
        for row in self.rows:
            yield row

    async def close(self) -> None:
        self.closed += 1

    def placeholder(self, position: int) -> str:
        return f"${position}"

    def to_timestamp(self, moment: datetime) -> object:
        return ("stored", moment)

    def from_timestamp(self, stored: object) -> datetime:
        return stored[1]


class Storage(DatabaseStorage):
    WRITE_ERRORS = (ConnectionError,)


def make_storage(driver: RecordingDriver, **options) -> Storage:
    options.setdefault("retry_strategy", RetryStrategy(retry_on=(ConnectionError,), base_delay=0.001, max_delay=0.001))
    return Storage(driver, **options)


class TestInitDb:
    async def test_table_and_indexes_use_the_types_of_the_driver(self):
        driver = RecordingDriver()

        await make_storage(driver).init_db()

        table, *indexes = driver.statements
        assert table.startswith("CREATE TABLE IF NOT EXISTS pages (id SERIAL PRIMARY KEY, url TEXT NOT NULL UNIQUE,")
        assert "links JSONB NOT NULL, metadata JSONB NOT NULL, crawled_at TIMESTAMPTZ NOT NULL," in table
        assert indexes == [
            "CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages (crawled_at)",
            "CREATE INDEX IF NOT EXISTS idx_pages_status_code ON pages (status_code)",
        ]

    async def test_first_write_initializes_the_database_once(self):
        driver = RecordingDriver()
        storage = make_storage(driver, batch_size=1)

        await storage.save(make_record("a"))
        await storage.save(make_record("b"))

        assert driver.connections == 1
        assert len(driver.statements) == 3

    async def test_explicit_init_is_not_repeated_by_a_write(self):
        driver = RecordingDriver()
        storage = make_storage(driver, batch_size=1)
        await storage.init_db()

        await storage.save(make_record())

        assert len(driver.statements) == 3

    async def test_failed_init_is_retried_on_the_same_connection(self):
        driver = RecordingDriver(failures=[ConnectionError("server is starting")])
        storage = make_storage(driver, batch_size=1)

        await storage.save(make_record())

        assert driver.connections == 1
        assert len(driver.batches) == 1


class TestWriting:
    async def test_batch_is_one_call_with_a_row_per_record(self):
        driver = RecordingDriver()
        storage = make_storage(driver, batch_size=3)
        record = make_record("a")

        for url in "abc":
            await storage.save(make_record(url))

        ((statement, rows),) = driver.batches
        assert statement == (
            "INSERT INTO pages (url, title, text, links, metadata, crawled_at, status_code, content_type) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
            "ON CONFLICT (url) DO UPDATE SET title = excluded.title, text = excluded.text, "
            "links = excluded.links, metadata = excluded.metadata, crawled_at = excluded.crawled_at, "
            "status_code = excluded.status_code, content_type = excluded.content_type"
        )
        assert [row[0] for row in rows] == ["a", "b", "c"]
        assert rows[0] == (
            "a",
            "Page",
            "Some text",
            '["https://site/a", "https://site/b"]',
            '{"description": "A page", "keywords": ["one", "two"], "language": "en", "depth": 1}',
            ("stored", record["crawled_at"]),
            200,
            "text/html",
        )

    async def test_failed_batch_is_retried_whole(self):
        driver = RecordingDriver()
        storage = make_storage(driver, batch_size=2)
        await storage.init_db()
        driver.failures = [ConnectionError("connection reset")]

        await storage.save(make_record("a"))
        await storage.save(make_record("b"))

        assert [[row[0] for row in rows] for _, rows in driver.batches] == [["a", "b"]]

    async def test_storage_error_when_the_database_stays_down(self):
        driver = RecordingDriver()
        storage = make_storage(driver, batch_size=1)
        await storage.init_db()
        driver.failures = [ConnectionError("connection refused")] * 4

        with pytest.raises(StorageError, match="failed to write 1 records: connection refused"):
            await storage.save(make_record())


class TestReading:
    async def test_rows_become_records(self):
        driver = RecordingDriver()
        storage = make_storage(driver)
        record = make_record()
        driver.rows = [
            (
                record["url"],
                "Page",
                "Some text",
                '["https://site/a", "https://site/b"]',
                '{"description": "A page", "keywords": ["one", "two"], "language": "en", "depth": 1}',
                ("stored", record["crawled_at"]),
                200,
                "text/html",
            )
        ]

        assert [saved async for saved in storage.read()] == [record]
        assert await storage.get(record["url"]) == record
        assert driver.queries[-1] == (
            "SELECT url, title, text, links, metadata, crawled_at, status_code, content_type FROM pages WHERE url = $1",
            (record["url"],),
        )

    async def test_buffered_records_are_written_before_a_query(self):
        driver = RecordingDriver()
        storage = make_storage(driver)
        driver.rows = [(1,)]
        await storage.save(make_record())

        await storage.count()

        assert len(driver.batches) == 1


class TestClosing:
    async def test_close_closes_the_connection(self):
        driver = RecordingDriver()
        storage = make_storage(driver)
        await storage.save(make_record())

        await storage.close()

        assert len(driver.batches) == 1
        assert driver.closed == 1

    async def test_unused_storage_has_no_connection_to_close(self):
        driver = RecordingDriver()

        await make_storage(driver).close()

        assert driver.connections == 0
        assert driver.closed == 0

    @pytest.mark.parametrize("read", ["count", "status_counts", "get", "read"])
    async def test_closed_storage_refuses_to_read(self, read):
        driver = RecordingDriver()
        storage = make_storage(driver)
        await storage.save(make_record())
        await storage.close()

        with pytest.raises(StorageError, match="is closed"):
            if read == "read":
                _ = [record async for record in storage.read()]
            elif read == "get":
                await storage.get("https://site/page")
            else:
                await getattr(storage, read)()

        assert driver.connections == 1
        assert driver.queries == []
