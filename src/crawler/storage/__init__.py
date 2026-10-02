"""Storages that keep crawled pages in files and databases."""

from crawler.storage.base import DataStorage
from crawler.storage.composite import CompositeStorage
from crawler.storage.csv_file import CSVStorage
from crawler.storage.database import DatabaseDriver, DatabaseStorage
from crawler.storage.factory import (
    DATABASE_URL_VARIABLE,
    DEFAULT_DATABASE_URL,
    register_database,
    storage_from_env,
    storage_from_url,
)
from crawler.storage.json_file import JSONStorage
from crawler.storage.postgres import PostgresDriver, PostgresStorage
from crawler.storage.sqlite import SQLiteDriver, SQLiteStorage

__all__ = [
    "DATABASE_URL_VARIABLE",
    "DEFAULT_DATABASE_URL",
    "CSVStorage",
    "CompositeStorage",
    "DataStorage",
    "DatabaseDriver",
    "DatabaseStorage",
    "JSONStorage",
    "PostgresDriver",
    "PostgresStorage",
    "SQLiteDriver",
    "SQLiteStorage",
    "register_database",
    "storage_from_env",
    "storage_from_url",
]
