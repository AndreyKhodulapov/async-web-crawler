"""Storage of crawled pages in a JSON file."""

import json
import os
import re
import textwrap
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from pathlib import Path

import aiofiles
import aiofiles.os
from aiofiles.threadpool.binary import AsyncBufferedReader

from crawler.exceptions import StorageError
from crawler.models import PageRecord
from crawler.retry import RetryStrategy
from crawler.storage.base import DataStorage

# What stands between two records in either layout.


class JSONStorage(DataStorage):
    """Keeps pages in a JSON file, adding to the file if it exists.

    By default the file is JSON Lines: a record per line, which other tools
    can read line by line. With `indent` it is one indented JSON array, easier
    on the eye; it is a complete JSON document after every write, not only
    once the storage is closed.

    Either way records are added without reading the file, and `read` goes
    through it in pieces, so the file may be larger than the memory. The
    file is UTF-8, `crawled_at` is written in ISO 8601.

    Raises (on the first write):
        StorageError: the file exists in the other layout, is an array
            that some other program wrote, or is JSON Lines whose last
            line is not complete.
    Raises (on `read`):
        StorageError: the file is not UTF-8, ends with a broken record, or
            holds something that is not a page.
    """

    READ_CHUNK = 64 * 1024

    def __init__(
        self,
        path: str | Path,
        *,
        indent: int | None = None,
        batch_size: int = 100,
        retry_strategy: RetryStrategy | None = None,
        cooldown: float = 5.0,
    ) -> None:
        super().__init__(batch_size, retry_strategy=retry_strategy, cooldown=cooldown)
        self.path = Path(path)
        self.indent = indent
        # Closes the array; written after the records and overwritten by the next ones.
        self._tail = b"" if indent is None else b"\n]\n"
        self._file: AsyncBufferedReader | None = None
        self._end = 0  # where the next record goes

    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        file = self._file or await self._open()
        if self.indent is None:
            lines = "".join(f"{self._dump(record)}\n" for record in records)
        else:
            items = ",\n".join(textwrap.indent(self._dump(record), " " * self.indent) for record in records)
            lines = ("[\n" if self._end == 0 else ",\n") + items
        encoded = lines.encode()
        # A retry starts where the failed write did, so nothing is written twice.
        await file.seek(self._end)
        await file.write(encoded + self._tail)
        await file.flush()
        self._end += len(encoded)

    async def _open(self) -> AsyncBufferedReader:
        exists = await aiofiles.os.path.exists(self.path)
        file = await aiofiles.open(self.path, "r+b" if exists else "w+b")
        size = await file.seek(0, os.SEEK_END)
        if size:
            self._end = max(size - len(self._tail), 0)
            await file.seek(0)
            first = await file.read(1)
            await file.seek(self._end)
            # The end is checked too: the next records overwrite it.
            if first != (b"{" if self.indent is None else b"[") or await file.read() != self._tail:
                await file.close()
                layout, option = ("JSON Lines", "without") if self.indent is None else ("a JSON array", "with")
                raise StorageError(f"{self.path} is not {layout} of this storage: it was not written {option} indent")
            await file.seek(size - 1)
            if self.indent is None and await file.read() != b"\n":
                # A write that was cut short: the next record would be glued to its last line.
                await file.close()
                raise StorageError(f"{self.path} does not end with a line break: its last record may be broken")
        self._file = file
        return file

    def _dump(self, record: PageRecord) -> str:
        return json.dumps(record, ensure_ascii=False, indent=self.indent, default=_to_json)

    async def _read(self) -> AsyncIterator[PageRecord]:
        if not await aiofiles.os.path.exists(self.path):
            return
        decoder = json.JSONDecoder()
        between_records = re.compile(r"[\s,\[\]]*")
        unread = ""
        try:
            async with aiofiles.open(self.path, encoding="utf-8") as file:
                while chunk := await file.read(self.READ_CHUNK):
                    unread += chunk
                    position = 0
                    while True:
                        start = between_records.match(unread, position).end()
                        try:
                            record, position = decoder.raw_decode(unread, start)
                        except json.JSONDecodeError:
                            # The rest of the record is in the next chunk.
                            break
                        try:
                            record["crawled_at"] = datetime.fromisoformat(record["crawled_at"])
                        except (KeyError, TypeError, ValueError) as error:
                            raise StorageError(
                                f"{self.path} has a record that is not a page: {unread[start:position][:80]!r}"
                            ) from error
                        yield record
                    unread = unread[position:]
        except UnicodeError as error:
            raise StorageError(f"{self.path} is not UTF-8: {error}") from error
        if not between_records.fullmatch(unread):
            raise StorageError(f"{self.path} ends with a broken record: {unread[:80]!r}")

    async def _close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None


def _to_json(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")
