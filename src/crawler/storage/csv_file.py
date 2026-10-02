"""Storage of crawled pages in a CSV file."""

import csv
import io
import json
import os
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import ClassVar

import aiofiles
import aiofiles.os
from aiofiles.threadpool.binary import AsyncBufferedReader

from crawler.exceptions import StorageError
from crawler.models import PageRecord
from crawler.retry import RetryStrategy
from crawler.storage.base import DataStorage


class CSVStorage(DataStorage):
    """Keeps pages in a CSV file, a row per page, adding to the file if it exists.

    The header row is made of the fields of the first record; a file that
    exists keeps the header it has, and its columns decide the order. A
    record with a field the header lacks is refused with `ValueError`.

    A file that is not what this storage writes is left alone and reported
    with `StorageError`: on the first write, if it starts with an empty
    line or does not end with a line break (a write that was cut short);
    on `read`, if a row does not fit the header or the file is not in
    `encoding`.

    Commas, quotes and line breaks in a value are quoted as RFC 4180 says,
    so a row may span several lines. `links` and `metadata` are written as
    JSON, `crawled_at` in ISO 8601.

    `encoding` is that of the file, e.g. "utf-8-sig" for Excel. A character
    the encoding lacks is written as "?".
    """

    PARSERS: ClassVar[Mapping[str, Callable[[str], object]]] = {
        "links": json.loads,
        "metadata": json.loads,
        "crawled_at": datetime.fromisoformat,
        "status_code": int,
    }

    def __init__(
        self,
        path: str | Path,
        *,
        encoding: str = "utf-8",
        batch_size: int = 100,
        retry_strategy: RetryStrategy | None = None,
        cooldown: float = 5.0,
    ) -> None:
        "".encode(encoding)  # an unknown encoding fails here, not on the first write
        super().__init__(batch_size, retry_strategy=retry_strategy, cooldown=cooldown)
        self.path = Path(path)
        self.encoding = encoding
        self._file: AsyncBufferedReader | None = None
        self._header: list[str] | None = None
        self._end = 0  # where the next row goes

    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        file = self._file or await self._open()
        rows = io.StringIO()
        writer = csv.DictWriter(rows, self._header or list(records[0]))
        if self._header is None:
            writer.writeheader()
        writer.writerows({field: _to_cell(value) for field, value in record.items()} for record in records)
        encoded = rows.getvalue().encode(self.encoding, errors="replace")
        if self._end:
            # Encodings such as utf-8-sig and utf-16 start every encoded
            # string with a byte order mark; the file needs only the first.
            encoded = encoded[len("".encode(self.encoding)) :]
        # A retry starts where the failed write did, so nothing is written twice.
        await file.seek(self._end)
        await file.write(encoded)
        await file.flush()
        self._end += len(encoded)
        self._header = list(writer.fieldnames)

    async def _open(self) -> AsyncBufferedReader:
        if await aiofiles.os.path.exists(self.path):
            try:
                async with aiofiles.open(self.path, encoding=self.encoding, newline="") as file:
                    first_line = await file.readline()
            except UnicodeError as error:
                raise StorageError(f"{self.path} is not in {self.encoding}: {error}") from error
            # An empty file has no header yet.
            self._header = next(csv.reader([first_line]), None) or None
            if first_line and self._header is None:
                # Not a file to start anew: there may be rows below.
                raise StorageError(f"{self.path} starts with an empty line, not with a header")
        file = await aiofiles.open(self.path, "w+b" if self._header is None else "r+b")
        self._end = await file.seek(0, os.SEEK_END)
        if self._header is not None:
            line_break = "\n".encode(self.encoding)[len("".encode(self.encoding)) :]
            await file.seek(self._end - len(line_break))
            if await file.read() != line_break:
                # A write that was cut short: the next row would be glued to its last line.
                await file.close()
                raise StorageError(f"{self.path} does not end with a line break: its last row may be broken")
        self._file = file
        return file

    async def _read(self) -> AsyncIterator[PageRecord]:
        if not await aiofiles.os.path.exists(self.path):
            return
        # The default limit of 128 KB is less than the text of a long page.
        # The limit is one for the whole process.
        csv.field_size_limit(2**31 - 1)
        header: list[str] | None = None
        row_lines: list[str] = []
        inside_quotes = False
        try:
            async with aiofiles.open(self.path, encoding=self.encoding, newline="") as file:
                async for line in file:
                    row_lines.append(line)
                    # An odd number of quotes: the line starts or ends a quoted
                    # value that spans several lines.
                    if line.count('"') % 2:
                        inside_quotes = not inside_quotes
                    if inside_quotes:
                        continue
                    row_text = "".join(row_lines)
                    row_lines = []
                    row = next(csv.reader([row_text]))
                    if header is None:
                        header = row
                        continue
                    try:
                        record = dict(zip(header, row, strict=True))
                        for field, parse in self.PARSERS.items():
                            if field in record:
                                record[field] = parse(record[field])
                    except ValueError as error:
                        # Too few or too many values, or one that is not what its column holds.
                        raise StorageError(f"{self.path} has a broken row: {row_text[:80]!r}") from error
                    yield record
        except UnicodeError as error:
            raise StorageError(f"{self.path} is not in {self.encoding}: {error}") from error
        if row_lines:
            raise StorageError(f"{self.path} ends with a broken row: {''.join(row_lines)[:80]!r}")

    async def _close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None


def _to_cell(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list | dict):
        return json.dumps(value, ensure_ascii=False)
    return value
