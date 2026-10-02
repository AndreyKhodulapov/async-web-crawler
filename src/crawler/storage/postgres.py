"""Storage of crawled pages in a PostgreSQL database."""

from collections.abc import AsyncIterator, Sequence

import asyncpg

from crawler.retry import RetryStrategy
from crawler.storage.database import DatabaseDriver, DatabaseStorage


class PostgresDriver(DatabaseDriver):
    """A small asyncpg connection pool: a reader gets a connection of its own, apart from the writes."""

    ID_COLUMN = "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"
    JSON_TYPE = "JSONB"
    TIMESTAMP_TYPE = "TIMESTAMPTZ"
    MAX_CONNECTIONS = 4

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=self.MAX_CONNECTIONS)

    async def execute(self, statement: str) -> None:
        await self._pool.execute(statement)

    async def execute_many(self, statement: str, rows: Sequence[Sequence[object]]) -> None:
        # asyncpg runs it in a transaction of its own: all rows or none.
        await self._pool.executemany(statement, rows)

    async def fetch(self, query: str, *parameters: object) -> AsyncIterator[Sequence[object]]:
        # A cursor lives only inside a transaction.
        async with self._pool.acquire() as connection, connection.transaction():
            async for row in connection.cursor(query, *parameters):
                yield tuple(row)

    async def close(self) -> None:
        await self._pool.close()

    def placeholder(self, position: int) -> str:
        return f"${position}"


class PostgresStorage(DatabaseStorage):
    """Keeps pages in a PostgreSQL database.

    `dsn` is a connection URL: "postgresql://user:password@host:5432/database".
    See `DatabaseStorage`. A write is retried when the server cannot be
    reached, drops the connection, has no free connections or picks the
    transaction as the victim of a deadlock.
    """

    WRITE_ERRORS = (
        OSError,
        asyncpg.PostgresConnectionError,
        asyncpg.CannotConnectNowError,
        asyncpg.TooManyConnectionsError,
        asyncpg.DeadlockDetectedError,
    )

    def __init__(self, dsn: str, *, batch_size: int = 100, retry_strategy: RetryStrategy | None = None) -> None:
        super().__init__(PostgresDriver(dsn), batch_size=batch_size, retry_strategy=retry_strategy)
