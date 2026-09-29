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
from crawler.models import FetchResult, ParsedPage
from crawler.parser import HTMLParser
from crawler.urls import is_same_host

__all__ = [
    "AsyncCrawler",
    "CrawlerClosedError",
    "FetchError",
    "FetchResult",
    "FetchTimeoutError",
    "HTMLParser",
    "HTTPStatusError",
    "InvalidURLError",
    "NetworkError",
    "ParsedPage",
    "UnexpectedError",
    "is_same_host",
]
