"""Integration tests for the database storages: the same checks for every database."""

import asyncio
import contextlib
import sqlite3
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime, timedelta, timezone

import asyncpg
import pytest
from helpers import POSTGRES_DSN, make_record

from crawler import DatabaseStorage, PostgresStorage, RetryStrategy, SQLiteStorage, StorageError, storage_from_env

StorageFactory = Callable[..., DatabaseStorage]


async def run_in_postgres(query: str) -> list[asyncpg.Record]:
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        return await connection.fetch(query)
    finally:
        await connection.close()


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)])
async def open_storage(request, tmp_path) -> AsyncGenerator[StorageFactory, None]:
    """Opens storages on one empty database; closes them after the test."""
    opened = []
    if request.param == "postgres":
        await run_in_postgres("DROP TABLE IF EXISTS pages")

    def open_storage(**options) -> DatabaseStorage:
        if request.param == "postgres":
            storage = PostgresStorage(POSTGRES_DSN, **options)
        else:
            storage = SQLiteStorage(tmp_path / "crawler.db", **options)
        opened.append(storage)
        return storage

    yield open_storage
    for storage in opened:
        # A test may leave records that no database accepts.
        with contextlib.suppress(Exception):
            await storage.close()


async def save_all(storage: DatabaseStorage, records: list) -> None:
    for record in records:
        await storage.save(record)


async def read_all(storage: DatabaseStorage) -> list:
    return [record async for record in storage.read()]


class TestInitDb:
    async def test_creates_an_empty_table(self, open_storage):
        storage = open_storage()

        await storage.init_db()

        assert await storage.count() == 0
        assert await read_all(storage) == []

    async def test_can_be_repeated_and_keeps_the_pages(self, open_storage):
        storage = open_storage()
        await storage.init_db()
        await storage.save(make_record())
        await storage.flush()

        await storage.init_db()
        other = open_storage()
        await other.init_db()

        assert await storage.count() == 1
        assert await other.count() == 1

    async def test_is_not_required_before_saving(self, open_storage):
        storage = open_storage()

        await storage.save(make_record())

        assert await storage.count() == 1


class TestSavingAndReading:
    async def test_records_are_read_back_as_saved(self, open_storage):
        records = [
            make_record("https://site/a", title="Quotes \" and ' and\nnew lines", text="Crème brûlée 日本語 🙂"),
            make_record("https://site/b", title="", text="", links=[], metadata={}, content_type=""),
            make_record("https://site/c", metadata={"nested": {"deep": [1, 2.5, None, True]}, "sql": "'; DROP --"}),
            make_record("https://site/d", status_code=404, text="word " * 50_000),
        ]
        storage = open_storage(batch_size=3)

        await save_all(storage, records)

        assert await read_all(storage) == records

    async def test_time_zone_does_not_change_the_moment(self, open_storage):
        moment = datetime(2025, 3, 14, 18, 9, 26, tzinfo=timezone(timedelta(hours=3)))
        storage = open_storage()

        await storage.save(make_record(crawled_at=moment))

        (saved,) = await read_all(storage)
        assert saved["crawled_at"] == moment
        assert saved["crawled_at"].utcoffset() == timedelta(0)

    async def test_pages_outlive_the_storage(self, open_storage):
        records = [make_record(f"https://site/{name}") for name in "abc"]
        async with open_storage() as storage:
            await save_all(storage, records)

        assert await read_all(open_storage()) == records

    async def test_saving_a_url_again_replaces_its_row(self, open_storage):
        storage = open_storage(batch_size=2)
        await save_all(storage, [make_record("https://site/a"), make_record("https://site/b")])

        later = datetime(2025, 4, 1, tzinfo=UTC)
        updated = make_record("https://site/a", title="New title", status_code=410, links=[], crawled_at=later)
        await storage.save(updated)

        assert await storage.count() == 2
        assert await read_all(storage) == [updated, make_record("https://site/b")]

    async def test_url_saved_twice_in_one_batch_keeps_the_last(self, open_storage):
        storage = open_storage()

        await save_all(storage, [make_record(title="First"), make_record(title="Second")])

        assert [record["title"] for record in await read_all(storage)] == ["Second"]

    async def test_batch_is_written_whole_or_not_at_all(self, open_storage):
        # A failed batch is written again one record at a time: no record of it may be in the table.
        storage = open_storage()
        records = [make_record("https://site/a"), make_record("https://site/b"), make_record("https://site/c")]
        records[2]["title"] = None  # the column does not allow it

        with pytest.raises(Exception, match="(?i)null"):
            await storage._write_batch(records)

        assert await open_storage().count() == 0

    async def test_only_the_record_the_database_refuses_is_dropped(self, open_storage):
        storage = open_storage(batch_size=3)
        records = [make_record("https://site/a"), make_record("https://site/b"), make_record("https://site/c")]
        records[1]["title"] = None  # the column does not allow it

        with pytest.raises(Exception, match="(?i)null"):
            await save_all(storage, records)

        assert [record["url"] for record in await read_all(open_storage())] == ["https://site/a", "https://site/c"]
        assert (storage.pending, storage.written) == (0, 2)

    async def test_text_with_a_lone_surrogate_is_dropped_alone(self, open_storage):
        # Not valid UTF-8: no database takes it.
        storage = open_storage(batch_size=10)
        records = [make_record(f"https://site/{number}") for number in range(10)]
        records[4]["text"] = "broken \ud83d emoji"

        with pytest.raises(Exception, match="(?i)surrogate"):
            await save_all(storage, records)

        assert await open_storage().count() == 9
        assert await open_storage().get("https://site/4") is None

    async def test_many_records_in_batches(self, open_storage):
        records = [make_record(f"https://site/page-{number}") for number in range(2000)]
        storage = open_storage(batch_size=500)

        await save_all(storage, records)

        assert await storage.count() == 2000
        assert await read_all(storage) == records

    async def test_concurrent_saves(self, open_storage):
        storage = open_storage(batch_size=7)
        records = [make_record(f"https://site/page-{number}") for number in range(50)]

        await asyncio.gather(*(storage.save(record) for record in records))

        assert await storage.count() == 50

    async def test_reader_may_stop_early(self, open_storage):
        storage = open_storage()
        await save_all(storage, [make_record(f"https://site/{name}") for name in "abc"])

        async for record in storage.read():
            assert record["url"] == "https://site/a"
            break

        assert await storage.count() == 3


class TestQueries:
    async def test_get_finds_a_page_by_url(self, open_storage):
        storage = open_storage()
        await save_all(storage, [make_record("https://site/a"), make_record("https://site/b", title="B")])

        assert await storage.get("https://site/b") == make_record("https://site/b", title="B")
        assert await storage.get("https://site/missing") is None

    async def test_status_counts(self, open_storage):
        storage = open_storage()
        statuses = [200, 404, 200, 301, 200]
        await save_all(
            storage, [make_record(f"https://site/{n}", status_code=status) for n, status in enumerate(statuses)]
        )

        assert await storage.status_counts() == {200: 3, 301: 1, 404: 1}

    async def test_buffered_records_are_counted(self, open_storage):
        storage = open_storage(batch_size=100)

        await storage.save(make_record())

        assert await storage.count() == 1


class TestSQLite:
    async def test_table_has_its_indexes(self, tmp_path):
        path = tmp_path / "crawler.db"
        async with SQLiteStorage(path) as storage:
            await storage.init_db()

        with sqlite3.connect(path) as connection:
            indexes = {name for (name,) in connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
            plan = connection.execute("EXPLAIN QUERY PLAN SELECT * FROM pages WHERE status_code = 404").fetchall()
        connection.close()

        assert {"idx_pages_crawled_at", "idx_pages_status_code"} <= indexes
        assert "idx_pages_status_code" in plan[0][-1]

    async def test_moments_are_stored_in_utc(self, tmp_path):
        path = tmp_path / "crawler.db"
        moment = datetime(2025, 3, 14, 18, 9, 26, tzinfo=timezone(timedelta(hours=3)))
        async with SQLiteStorage(path) as storage:
            await storage.save(make_record(crawled_at=moment))

        with sqlite3.connect(path) as connection:
            stored = connection.execute("SELECT crawled_at FROM pages").fetchone()
        connection.close()

        assert stored == ("2025-03-14T15:09:26+00:00",)

    async def test_file_is_in_wal_mode_and_a_lock_is_waited_for(self, tmp_path):
        path = tmp_path / "crawler.db"
        async with SQLiteStorage(path) as storage:
            await storage.init_db()
            async with storage._driver._connection.execute("PRAGMA busy_timeout") as cursor:
                assert await cursor.fetchone() == (30000,)

        with sqlite3.connect(path) as connection:
            assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        connection.close()

    async def test_reader_does_not_hold_the_writes_up(self, tmp_path):
        path = tmp_path / "crawler.db"
        async with SQLiteStorage(path, batch_size=1) as storage:
            await storage.save(make_record("https://site/first"))
            # A reader in the middle of its query, as one looking at the file while the crawl goes on.
            reader = sqlite3.connect(path)
            reader.execute("BEGIN")
            assert reader.execute("SELECT COUNT(*) FROM pages").fetchone() == (1,)

            await storage.save(make_record("https://site/second"))

            assert await storage.count() == 2
            reader.close()

    async def test_write_is_retried_when_the_database_is_locked(self, tmp_path):
        path = tmp_path / "crawler.db"
        locker = sqlite3.connect(path)
        errors = []

        async def unlock(error: Exception, delay: float) -> None:
            errors.append(str(error))
            locker.rollback()

        retries = RetryStrategy(retry_on=(sqlite3.OperationalError,), wait=unlock)
        storage = SQLiteStorage(path, batch_size=1, retry_strategy=retries)
        await storage.init_db()
        # No waiting inside SQLite: the lock is reported at once.
        await storage._driver._connection.execute("PRAGMA busy_timeout = 0")
        locker.execute("BEGIN EXCLUSIVE")

        await storage.save(make_record())

        assert errors == ["database is locked"]
        assert await storage.count() == 1
        locker.close()
        await storage.close()

    async def test_database_that_cannot_be_opened_is_a_storage_error(self, tmp_path):
        retries = RetryStrategy(retry_on=(sqlite3.OperationalError,), base_delay=0.001, max_delay=0.001)
        storage = SQLiteStorage(tmp_path / "missing" / "crawler.db", batch_size=1, retry_strategy=retries)

        with pytest.raises(StorageError, match="unable to open database file"):
            await storage.save(make_record())


class TestPostgres:
    @pytest.mark.postgres
    async def test_table_has_its_indexes_and_native_types(self):
        await run_in_postgres("DROP TABLE IF EXISTS pages")
        async with PostgresStorage(POSTGRES_DSN) as storage:
            await storage.init_db()

        indexes = await run_in_postgres("SELECT indexname FROM pg_indexes WHERE tablename = 'pages'")
        columns = await run_in_postgres(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'pages'"
        )

        assert {"idx_pages_crawled_at", "idx_pages_status_code", "pages_url_key"} <= {name for (name,) in indexes}
        types = dict(columns)
        assert (types["links"], types["metadata"]) == ("jsonb", "jsonb")
        assert types["crawled_at"] == "timestamp with time zone"

    @pytest.mark.postgres
    async def test_storages_starting_at_once_create_the_table_once(self):
        """As the workers of a crawl job that save to one database do."""
        await run_in_postgres("DROP TABLE IF EXISTS pages")
        storages = [PostgresStorage(POSTGRES_DSN) for _ in range(8)]
        try:
            await asyncio.gather(*(storage.init_db() for storage in storages))
        finally:
            for storage in storages:
                await storage.close()

        indexes = await run_in_postgres("SELECT indexname FROM pg_indexes WHERE tablename = 'pages'")
        assert {"idx_pages_crawled_at", "idx_pages_status_code"} <= {name for (name,) in indexes}

    @pytest.mark.postgres
    async def test_writes_wait_for_the_disk(self):
        """Unlike those of a frontier: a page is marked saved once its record is written for good."""
        async with PostgresStorage(POSTGRES_DSN) as storage:
            await storage.init_db()
            assert await storage._driver._pool.fetchval("SHOW synchronous_commit") == "on"

    @pytest.mark.postgres
    async def test_json_is_queryable_in_the_database(self):
        await run_in_postgres("DROP TABLE IF EXISTS pages")
        async with PostgresStorage(POSTGRES_DSN) as storage:
            await storage.save(make_record(metadata={"language": "fr", "depth": 2}))

        rows = await run_in_postgres("SELECT url FROM pages WHERE metadata ->> 'language' = 'fr'")

        assert [url for (url,) in rows] == ["https://site/page"]

    @pytest.mark.postgres
    async def test_text_with_a_nul_character_is_dropped_alone(self):
        # SQLite keeps it; a text column of PostgreSQL cannot.
        await run_in_postgres("DROP TABLE IF EXISTS pages")
        records = [make_record(f"https://site/{number}") for number in range(10)]
        records[4]["text"] = "before\x00after"

        async with PostgresStorage(POSTGRES_DSN, batch_size=10) as storage:
            with pytest.raises(asyncpg.CharacterNotInRepertoireError):
                await save_all(storage, records)

            assert await storage.count() == 9

    @pytest.mark.postgres
    async def test_storage_chosen_by_the_url_reaches_the_server(self):
        await run_in_postgres("DROP TABLE IF EXISTS pages")

        async with storage_from_env({"CRAWLER_DATABASE_URL": POSTGRES_DSN}) as storage:
            await storage.save(make_record())

            assert type(storage) is PostgresStorage
            assert await storage.count() == 1

    async def test_server_that_cannot_be_reached_is_a_storage_error(self, closed_port_url):
        port = closed_port_url.rstrip("/").rpartition(":")[2]
        waits = []

        async def no_wait(error: Exception, delay: float) -> None:
            waits.append(type(error))

        retries = RetryStrategy(max_retries=2, retry_on=PostgresStorage.WRITE_ERRORS, wait=no_wait)
        storage = PostgresStorage(f"postgresql://crawler:crawler@127.0.0.1:{port}/crawler", retry_strategy=retries)
        await storage.save(make_record())

        with pytest.raises(StorageError, match="failed to write 1 records"):
            await storage.flush()

        assert len(waits) == 2
        assert all(issubclass(error, OSError) for error in waits)
