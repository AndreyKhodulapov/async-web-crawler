"""Storage of crawled pages in a relational database."""

import asyncio
import contextlib
import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import ClassVar

from crawler.exceptions import StorageError
from crawler.models import PageRecord
from crawler.retry import RetryStrategy
from crawler.storage.base import DataStorage


class DatabaseDriver(ABC):
    """What `DatabaseStorage` needs from a database: a connection and its SQL dialect.

    A driver for another database implements the methods and names its
    column types; the storage builds the statements from them.
    """

    # Definition of an auto-incremented primary key column, and the types
    # that hold a JSON document and a moment in time.
    ID_COLUMN: ClassVar[str]
    JSON_TYPE: ClassVar[str]
    TIMESTAMP_TYPE: ClassVar[str]

    @abstractmethod
    async def connect(self) -> None:
        """Open the connection."""

    @abstractmethod
    async def execute(self, statement: str) -> None:
        """Run a statement that takes no parameters and returns no rows."""

    @abstractmethod
    async def execute_many(self, statement: str, rows: Sequence[Sequence[object]]) -> None:
        """Run the statement for every row of parameters, all in one transaction."""

    @abstractmethod
    def fetch(self, query: str, *parameters: object) -> AsyncIterator[Sequence[object]]:
        """Iterate over the rows of a query without loading them all."""

    @abstractmethod
    async def close(self) -> None:
        """Close the connection."""

    @abstractmethod
    def placeholder(self, position: int) -> str:
        """The parameter at `position` (from 1) as the database writes it in a statement."""

    def to_timestamp(self, moment: datetime) -> object:
        """A moment as the driver passes it to a `TIMESTAMP_TYPE` column."""
        return moment

    def from_timestamp(self, stored: object) -> datetime:
        return stored


class DatabaseStorage(DataStorage):
    """Keeps pages in the `pages` table of a database, a row per URL.

    The same code serves every database: what differs between them is in
    the `driver` (see `SQLiteStorage`, `PostgresStorage`). A subclass for
    another database passes its driver and lists in `WRITE_ERRORS` the
    errors of the driver that a retry may cure.

    `init_db` creates the table; it is called by `open` and on the first
    use if it was not called before. A batch is written in one transaction: all of its
    pages are saved or none. Saving a URL again replaces its row. `links`
    and `metadata` are stored as JSON. `crawled_at` and `status_code` are
    indexed, and so is `url`, being unique.
    """

    TABLE = "pages"
    COLUMNS = ("url", "title", "text", "links", "metadata", "crawled_at", "status_code", "content_type")

    def __init__(
        self,
        driver: DatabaseDriver,
        *,
        batch_size: int = 100,
        retry_strategy: RetryStrategy | None = None,
        cooldown: float = 5.0,
    ) -> None:
        super().__init__(batch_size, retry_strategy=retry_strategy, cooldown=cooldown)
        self._driver = driver
        self._connected = False
        self._initialized = False
        # A read and a write may both be the first use: only one connects.
        self._init_lock = asyncio.Lock()

    async def init_db(self) -> None:
        """Connect and create the table of pages with its indexes, unless they exist."""
        async with self._init_lock:
            if not self._connected:
                await self._driver.connect()
                self._connected = True
            driver = self._driver
            await driver.execute(
                f"CREATE TABLE IF NOT EXISTS {self.TABLE} ("
                f"id {driver.ID_COLUMN}, "
                "url TEXT NOT NULL UNIQUE, "
                "title TEXT NOT NULL, "
                "text TEXT NOT NULL, "
                f"links {driver.JSON_TYPE} NOT NULL, "
                f"metadata {driver.JSON_TYPE} NOT NULL, "
                f"crawled_at {driver.TIMESTAMP_TYPE} NOT NULL, "
                "status_code INTEGER NOT NULL, "
                "content_type TEXT NOT NULL)"
            )
            for column in ("crawled_at", "status_code"):
                await driver.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE}_{column} ON {self.TABLE} ({column})")
            self._initialized = True

    async def count(self) -> int:
        """The number of pages saved."""
        (total,) = await self._fetch_one(f"SELECT COUNT(*) FROM {self.TABLE}")
        return total

    async def status_counts(self) -> dict[int, int]:
        """The number of pages by HTTP status code."""
        await self._prepare_to_read()
        query = f"SELECT status_code, COUNT(*) FROM {self.TABLE} GROUP BY status_code ORDER BY status_code"
        return {status: pages async for status, pages in self._driver.fetch(query)}

    async def get(self, url: str) -> PageRecord | None:
        """The page saved for a URL, or None."""
        row = await self._fetch_one(
            f"SELECT {', '.join(self.COLUMNS)} FROM {self.TABLE} WHERE url = {self._driver.placeholder(1)}", url
        )
        return None if row is None else self._to_record(row)

    async def _open_storage(self) -> None:
        if not self._initialized:
            await self.init_db()

    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        if not self._initialized:
            await self.init_db()
        placeholders = ", ".join(self._driver.placeholder(position) for position in range(1, len(self.COLUMNS) + 1))
        updates = ", ".join(f"{column} = excluded.{column}" for column in self.COLUMNS[1:])
        await self._driver.execute_many(
            f"INSERT INTO {self.TABLE} ({', '.join(self.COLUMNS)}) VALUES ({placeholders}) "
            f"ON CONFLICT (url) DO UPDATE SET {updates}",
            [self._to_row(record) for record in records],
        )

    async def _read(self) -> AsyncIterator[PageRecord]:
        await self._prepare_to_read()
        query = f"SELECT {', '.join(self.COLUMNS)} FROM {self.TABLE} ORDER BY id"
        # Closed explicitly, so that a reader that stops early does not leave a cursor open.
        async with contextlib.aclosing(self._driver.fetch(query)) as rows:
            async for row in rows:
                yield self._to_record(row)

    async def _close(self) -> None:
        if self._connected:
            await self._driver.close()
            self._connected = False

    async def _prepare_to_read(self) -> None:
        if self._closed:
            # The connection is gone, and a new one would never be closed.
            raise StorageError(f"{type(self).__name__} is closed")
        await self.flush()
        if not self._initialized:
            await self.init_db()

    async def _fetch_one(self, query: str, *parameters: object) -> Sequence[object] | None:
        await self._prepare_to_read()
        async with contextlib.aclosing(self._driver.fetch(query, *parameters)) as rows:
            async for row in rows:
                return row
        return None

    def _to_row(self, record: PageRecord) -> tuple[object, ...]:
        return (
            record["url"],
            record["title"],
            record["text"],
            json.dumps(record["links"], ensure_ascii=False),
            json.dumps(record["metadata"], ensure_ascii=False),
            self._driver.to_timestamp(record["crawled_at"]),
            record["status_code"],
            record["content_type"],
        )

    def _to_record(self, row: Sequence[object]) -> PageRecord:
        record = dict(zip(self.COLUMNS, row, strict=True))
        record["links"] = json.loads(record["links"])
        record["metadata"] = json.loads(record["metadata"])
        record["crawled_at"] = self._driver.from_timestamp(record["crawled_at"])
        return record
