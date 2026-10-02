"""Storages that keep crawled pages in files and databases."""

from crawler.storage.base import DataStorage
from crawler.storage.json_file import JSONStorage

__all__ = ["DataStorage", "JSONStorage"]
