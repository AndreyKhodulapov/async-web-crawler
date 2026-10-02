"""Storages that keep crawled pages in files and databases."""

from crawler.storage.base import DataStorage
from crawler.storage.csv_file import CSVStorage
from crawler.storage.database import DatabaseDriver, DatabaseStorage
from crawler.storage.json_file import JSONStorage
from crawler.storage.sqlite import SQLiteDriver, SQLiteStorage

__all__ = [
    "CSVStorage",
    "DataStorage",
    "DatabaseDriver",
    "DatabaseStorage",
    "JSONStorage",
    "SQLiteDriver",
    "SQLiteStorage",
]
