"""Storage of crawled pages in an SQLite database."""

import sqlite3
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from crawler.retry import RetryStrategy
from crawler.storage.database import DatabaseDriver, DatabaseStorage


class SQLiteDriver(DatabaseDriver):
    """One aiosqlite connection: SQLite runs in a thread of its own, off the event loop."""

    ID_COLUMN = "INTEGER PRIMARY KEY AUTOINCREMENT"
    # SQLite has neither type. A moment is kept as ISO 8601 text in UTC,
    # which sorts in the order of time.
    JSON_TYPE = "TEXT"
    TIMESTAMP_TYPE = "TEXT"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._connection: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._connection = await aiosqlite.connect(self.path)

    async def execute(self, statement: str) -> None:
        await self._connection.execute(statement)
        await self._connection.commit()

    async def execute_many(self, statement: str, rows: Sequence[Sequence[object]]) -> None:
        try:
            await self._connection.executemany(statement, rows)
            await self._connection.commit()
        except BaseException:
            await self._connection.rollback()
            raise

    async def fetch(self, query: str, *parameters: object) -> AsyncIterator[Sequence[object]]:
        async with self._connection.execute(query, parameters) as cursor:
            async for row in cursor:
                yield row

    async def close(self) -> None:
        await self._connection.close()

    def placeholder(self, position: int) -> str:
        return "?"

    def to_timestamp(self, moment: datetime) -> str:
        return moment.astimezone(UTC).isoformat()

    def from_timestamp(self, stored: str) -> datetime:
        return datetime.fromisoformat(stored)


class SQLiteStorage(DatabaseStorage):
    """Keeps pages in an SQLite database file, created on the first use.

    See `DatabaseStorage`. A write is retried when the database is locked by
    another connection or the file cannot be opened or written.
    """

    WRITE_ERRORS = (sqlite3.OperationalError,)

    def __init__(
        self,
        path: str | Path,
        *,
        batch_size: int = 100,
        retry_strategy: RetryStrategy | None = None,
    ) -> None:
        super().__init__(SQLiteDriver(path), batch_size=batch_size, retry_strategy=retry_strategy)
        self.path = Path(path)
