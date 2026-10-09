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
from crawler.storage.base import DataStorage, warn_adding_to_file


class CSVStorage(DataStorage):
    """Keeps pages in a CSV file, a row per page, adding to the file if it exists.

    With `overwrite` the file is started anew instead: what it held,
    header included, is dropped on the first write, not on `open`, so a
    crawl that saves nothing leaves it as it was. Adding to a file that
    is not empty is logged as a warning, since a second run with the same
    file keeps the pages of the first one too.

    The header row is made of the fields of the first record; a file that
    exists keeps the header it has, and its columns decide the order. A
    record with a field the header lacks is refused with `ValueError`.

    A file that is not what this storage writes is reported with
    `StorageError`: on `open` or the first write (and left alone), unless
    `overwrite`, if it starts with an empty line or does not end with a
    line break (a write that was cut short); on `read`, if a row does not
    fit the header or the file is not in `encoding`.

    Commas, quotes and line breaks in a value are quoted as RFC 4180 says,
    so a row may span several lines. `links` and `metadata` are written as
    JSON, `crawled_at` in ISO 8601.

    `encoding` is that of the file, e.g. "utf-8-sig" for Excel. A character
    the encoding lacks is written as "?".

    With `escape_formulas`, the default, a value that a spreadsheet would
    take for a formula (it starts with "=", "+", "-", "@", a tab or a
    carriage return) is written after an apostrophe, and so is one that
    starts with an apostrophe itself; `read` drops it and returns the
    value as it was. The pages are written by the sites, and a title such
    as "=HYPERLINK(...)" would otherwise run in Excel. An apostrophe that
    is not followed by one of these characters is left as it is, so a file
    written without escaping is read as before.
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
        overwrite: bool = False,
        batch_size: int = 100,
        retry_strategy: RetryStrategy | None = None,
        cooldown: float = 5.0,
        escape_formulas: bool = True,
    ) -> None:
        "".encode(encoding)  # an unknown encoding fails here, not on the first write
        super().__init__(batch_size, retry_strategy=retry_strategy, cooldown=cooldown)
        self.path = Path(path)
        self.encoding = encoding
        self.overwrite = overwrite
        self.escape_formulas = escape_formulas
        self._file: AsyncBufferedReader | None = None
        self._header: list[str] | None = None
        self._end = 0  # where the next row goes
        self._stale = False  # the file holds an earlier run that the first write drops

    async def _open_storage(self) -> None:
        if self._file is None:
            await self._open()

    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        file = self._file or await self._open()
        rows = io.StringIO()
        writer = csv.DictWriter(rows, self._header or list(records[0]))
        if self._header is None:
            writer.writeheader()
        writer.writerows({field: self._to_cell(value) for field, value in record.items()} for record in records)
        encoded = rows.getvalue().encode(self.encoding, errors="replace")
        if self._end:
            # Encodings such as utf-8-sig and utf-16 start every encoded
            # string with a byte order mark; the file needs only the first.
            encoded = encoded[len("".encode(self.encoding)) :]
        # A retry starts where the failed write did, so nothing is written twice.
        await file.seek(self._end)
        await file.write(encoded)
        if self._stale:
            await file.truncate()  # the rest of the earlier run
            self._stale = False
        await file.flush()
        self._end += len(encoded)
        self._header = list(writer.fieldnames)

    async def _open(self) -> AsyncBufferedReader:
        exists = await aiofiles.os.path.exists(self.path)
        if not self.overwrite and exists:
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
        file = await aiofiles.open(self.path, "r+b" if exists else "w+b")
        self._end = await file.seek(0, os.SEEK_END)
        if self.overwrite:
            # Kept until the first write, which starts at the beginning.
            self._stale = self._end > 0
            self._end = 0
            await file.seek(0)
        elif self._header is not None:
            line_break = "\n".encode(self.encoding)[len("".encode(self.encoding)) :]
            await file.seek(self._end - len(line_break))
            if await file.read() != line_break:
                # A write that was cut short: the next row would be glued to its last line.
                await file.close()
                raise StorageError(f"{self.path} does not end with a line break: its last row may be broken")
            warn_adding_to_file(self.path, self._end)
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
                        if self.escape_formulas:
                            row = [unescape_formula(cell) for cell in row]
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

    def _to_cell(self, value: object) -> object:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, list | dict):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, str) and self.escape_formulas:
            return escape_formula(value)
        return value


# What a spreadsheet takes for the start of a formula.
FORMULA_START = ("=", "+", "-", "@", "\t", "\r")


def escape_formula(value: str) -> str:
    """`value` after an apostrophe if a spreadsheet would take it for a formula or it starts with one; else as it is."""
    return f"'{value}" if value.startswith((*FORMULA_START, "'")) else value


def unescape_formula(cell: str) -> str:
    """The value `escape_formula` turned into `cell`."""
    return cell[1:] if cell.startswith("'") and cell[1:].startswith((*FORMULA_START, "'")) else cell
