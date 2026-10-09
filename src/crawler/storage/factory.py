"""Choice of the storage by a file name or by a database URL, such as the one in `CRAWLER_DATABASE_URL`."""

import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from crawler.storage.base import DataStorage
from crawler.storage.csv_file import CSVStorage
from crawler.storage.database import DatabaseStorage
from crawler.storage.json_file import JSONStorage
from crawler.storage.postgres import PostgresStorage
from crawler.storage.sqlite import SQLiteStorage

DATABASE_URL_VARIABLE = "CRAWLER_DATABASE_URL"
DEFAULT_DATABASE_URL = "sqlite:///crawler.db"

# Takes the URL and the options of the storage (`batch_size`, `retry_strategy`, `cooldown`).
StorageBuilder = Callable[..., DatabaseStorage]

_builders: dict[str, StorageBuilder] = {}


def register_database(scheme: str, builder: StorageBuilder) -> None:
    """Make `storage_from_url` build the storages of a URL scheme with `builder`.

    This is how another database is added: `builder` is called with the URL
    and the options given to `storage_from_url`, and returns the storage. A
    scheme registered before is replaced. The scheme is case-insensitive.
    """
    _builders[scheme.lower()] = builder


def storage_from_url(url: str, **options: Any) -> DatabaseStorage:
    """The storage for a database URL; the scheme of the URL chooses the database.

    Out of the box: "sqlite:///crawler.db" (see `SQLiteStorage.from_url`)
    and "postgresql://user:password@host:5432/database" (or "postgres://").
    `options` go to the storage: `batch_size`, `retry_strategy`, `cooldown`.

    Raises:
        ValueError: it is not a URL, or no database is registered for its
            scheme, or the URL is not what the database expects.
    """
    scheme, separator, _ = url.partition("://")
    builder = _builders.get(scheme.lower())
    if not separator or builder is None:
        # Not the URL itself: it may hold a password.
        problem = f'unknown scheme "{scheme}"' if separator else "it has no scheme"
        known = ", ".join(f"{name}://" for name in sorted(_builders))
        raise ValueError(f"Cannot choose a database by the URL: {problem}; expected one of {known}")
    return builder(url, **options)


def storage_from_output(
    output: str | Path,
    *,
    csv_encoding: str = "utf-8",
    csv_escape_formulas: bool = True,
    overwrite: bool = False,
    **options: Any,
) -> DataStorage:
    """The storage for an output: a file, chosen by its extension, or a database URL.

    "pages.jsonl" (or ".ndjson") is JSON Lines, "pages.json" an indented
    JSON array, "pages.csv" CSV in `csv_encoding` (with `csv_escape_formulas`,
    see `CSVStorage`), "pages.db" (or ".sqlite",
    ".sqlite3") an SQLite database; "~" at the start of a path is the home
    directory. A string with "://" is a database URL, see
    `storage_from_url`. `overwrite` starts a file anew instead of adding
    to it; a database is left as it is (it keeps a row per URL anyway).
    `options` go to the storage: `batch_size`, `retry_strategy`, `cooldown`.

    Raises:
        ValueError: the extension is unknown, or as `storage_from_url`.
        LookupError: `csv_encoding` is unknown.
    """
    if isinstance(output, str) and "://" in output:
        return storage_from_url(output, **options)
    path = Path(output).expanduser()
    extension = path.suffix.lower()
    if extension == ".csv":
        return CSVStorage(
            path, encoding=csv_encoding, escape_formulas=csv_escape_formulas, overwrite=overwrite, **options
        )
    if extension == ".json":
        return JSONStorage(path, indent=2, overwrite=overwrite, **options)
    if extension in (".jsonl", ".ndjson"):
        return JSONStorage(path, overwrite=overwrite, **options)
    if extension in (".db", ".sqlite", ".sqlite3"):
        return SQLiteStorage(path, **options)
    problem = f'unknown extension "{path.suffix}"' if path.suffix else "it has no extension"
    raise ValueError(
        f'Cannot choose a storage for "{output}": {problem}; '
        "expected one of .jsonl, .ndjson, .json, .csv, .db, .sqlite, .sqlite3, or a database URL"
    )


def storage_from_env(environ: Mapping[str, str] | None = None, **options: Any) -> DatabaseStorage:
    """The storage for the URL in `CRAWLER_DATABASE_URL`.

    An SQLite database in "crawler.db" of the working directory, if the
    variable is not set or is empty. `environ` replaces `os.environ`.

    Raises:
        ValueError: as `storage_from_url`.
    """
    if environ is None:
        environ = os.environ
    return storage_from_url(environ.get(DATABASE_URL_VARIABLE) or DEFAULT_DATABASE_URL, **options)


register_database("sqlite", SQLiteStorage.from_url)
register_database("postgresql", PostgresStorage)
register_database("postgres", PostgresStorage)
