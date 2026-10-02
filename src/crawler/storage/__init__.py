"""Storages that keep crawled pages in files and databases."""

from crawler.storage.base import DataStorage
from crawler.storage.csv_file import CSVStorage
from crawler.storage.json_file import JSONStorage

__all__ = ["CSVStorage", "DataStorage", "JSONStorage"]
