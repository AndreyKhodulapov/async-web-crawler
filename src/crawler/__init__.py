"""Asynchronous web crawler built on asyncio and aiohttp."""

from crawler.client import AsyncCrawler
from crawler.exceptions import (
    CrawlerClosedError,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    UnexpectedError,
)
from crawler.models import FetchResult

__all__ = [
    "AsyncCrawler",
    "CrawlerClosedError",
    "FetchError",
    "FetchResult",
    "FetchTimeoutError",
    "HTTPStatusError",
    "InvalidURLError",
    "NetworkError",
    "UnexpectedError",
]
