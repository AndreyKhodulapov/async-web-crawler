"""Unit tests for the choice of a database storage by a URL."""

from pathlib import Path

import pytest
from helpers import make_record
from test_database_storage import RecordingDriver

from crawler import (
    DatabaseStorage,
    PostgresStorage,
    RetryStrategy,
    SQLiteStorage,
    register_database,
    storage_from_env,
    storage_from_url,
)
from crawler.storage import DATABASE_URL_VARIABLE, DEFAULT_DATABASE_URL, factory


@pytest.fixture(autouse=True)
def registry(monkeypatch) -> None:
    """Gives a test a registry of its own, so that what it registers does not outlive it."""
    monkeypatch.setattr(factory, "_builders", dict(factory._builders))


class TestStorageFromUrl:
    @pytest.mark.parametrize(
        ("url", "path"),
        [
            ("sqlite:///crawler.db", "crawler.db"),
            ("sqlite:///data/pages.sqlite3", "data/pages.sqlite3"),
            ("sqlite:////var/data/crawler.db", "/var/data/crawler.db"),
            ("SQLite:///crawler.db", "crawler.db"),
            ("sqlite:///what is this?.db", "what is this?.db"),
        ],
    )
    def test_sqlite_url_names_the_file(self, url, path):
        storage = storage_from_url(url)

        assert type(storage) is SQLiteStorage
        assert storage.path == Path(path)

    @pytest.mark.parametrize("scheme", ["postgresql", "postgres", "PostgreSQL"])
    def test_postgres_url_is_passed_whole(self, scheme):
        url = f"{scheme}://crawler:secret@db.example:5433/pages?sslmode=require"

        storage = storage_from_url(url)

        assert type(storage) is PostgresStorage
        assert storage._driver._dsn == url

    @pytest.mark.parametrize("url", ["sqlite:///crawler.db", "postgresql://crawler@localhost/crawler"])
    def test_options_go_to_the_storage(self, url):
        retries = RetryStrategy(max_retries=1)

        storage = storage_from_url(url, batch_size=7, retry_strategy=retries)

        assert storage.batch_size == 7
        assert storage.retry_strategy is retries

    def test_nothing_is_opened_until_the_first_use(self, tmp_path):
        path = tmp_path / "crawler.db"

        storage_from_url(f"sqlite:///{path}")

        assert not path.exists()

    async def test_storage_saves_and_reads(self, tmp_path):
        path = tmp_path / "crawler.db"
        records = [make_record("https://site/a"), make_record("https://site/b")]

        async with storage_from_url(f"sqlite:///{path}") as storage:
            for record in records:
                await storage.save(record)
        async with storage_from_url(f"sqlite:///{path}") as storage:
            saved = [record async for record in storage.read()]

        assert saved == records

    def test_unknown_scheme_names_the_known_ones(self):
        with pytest.raises(ValueError, match="unknown scheme") as raised:
            storage_from_url("mysql://crawler:secret@localhost/crawler")

        message = str(raised.value)
        assert '"mysql"' in message
        assert "postgres://, postgresql://, sqlite://" in message
        assert "secret" not in message

    @pytest.mark.parametrize("url", ["", "crawler.db", "/var/data/crawler.db", "sqlite:crawler.db"])
    def test_url_without_a_scheme_is_rejected(self, url):
        with pytest.raises(ValueError, match="it has no scheme"):
            storage_from_url(url)

    @pytest.mark.parametrize("url", ["sqlite://", "sqlite:///", "sqlite://crawler.db", "sqlite://host/crawler.db"])
    def test_sqlite_url_without_a_file_is_rejected(self, url):
        with pytest.raises(ValueError, match="sqlite:///path/to/file.db"):
            storage_from_url(url)


class TestStorageFromEnv:
    def test_variable_chooses_the_database(self, monkeypatch):
        monkeypatch.setenv(DATABASE_URL_VARIABLE, "postgresql://crawler@localhost/crawler")

        assert type(storage_from_env()) is PostgresStorage

    def test_variable_is_named_crawler_database_url(self, monkeypatch):
        monkeypatch.setenv("CRAWLER_DATABASE_URL", "sqlite:///from-env.db")

        assert storage_from_env().path == Path("from-env.db")

    @pytest.mark.parametrize("environ", [{}, {"CRAWLER_DATABASE_URL": ""}])
    def test_sqlite_file_by_default(self, environ):
        storage = storage_from_env(environ)

        assert DEFAULT_DATABASE_URL == "sqlite:///crawler.db"
        assert type(storage) is SQLiteStorage
        assert storage.path == Path("crawler.db")

    def test_unset_variable_gives_the_default(self, monkeypatch):
        monkeypatch.delenv(DATABASE_URL_VARIABLE, raising=False)

        assert storage_from_env().path == Path("crawler.db")

    def test_given_environment_replaces_the_real_one(self, monkeypatch):
        monkeypatch.setenv(DATABASE_URL_VARIABLE, "postgresql://crawler@localhost/crawler")

        storage = storage_from_env({DATABASE_URL_VARIABLE: "sqlite:///given.db"}, batch_size=3)

        assert storage.path == Path("given.db")
        assert storage.batch_size == 3

    def test_unknown_scheme_is_rejected(self):
        with pytest.raises(ValueError, match='unknown scheme "oracle"'):
            storage_from_env({DATABASE_URL_VARIABLE: "oracle://localhost/crawler"})


class TestRegisterDatabase:
    async def test_another_database_is_chosen_by_its_scheme(self):
        drivers = {}

        class RecordingStorage(DatabaseStorage):
            def __init__(self, url: str, **options) -> None:
                drivers[url] = RecordingDriver()
                super().__init__(drivers[url], **options)

        register_database("recording", RecordingStorage)

        storage = storage_from_url("recording://host/pages", batch_size=1)
        await storage.save(make_record())

        assert type(storage) is RecordingStorage
        assert len(drivers["recording://host/pages"].batches) == 1
        assert type(storage_from_env({DATABASE_URL_VARIABLE: "Recording://other"})) is RecordingStorage

    def test_registered_scheme_can_be_replaced(self):
        calls = []

        def build(url: str, **options) -> DatabaseStorage:
            calls.append((url, options))
            return SQLiteStorage("replaced.db", **options)

        register_database("SQLite", build)

        storage = storage_from_url("sqlite:///crawler.db", batch_size=2)

        assert calls == [("sqlite:///crawler.db", {"batch_size": 2})]
        assert storage.path == Path("replaced.db")

    def test_registration_does_not_outlive_a_test(self):
        with pytest.raises(ValueError, match="unknown scheme"):
            storage_from_url("recording://host/pages")
