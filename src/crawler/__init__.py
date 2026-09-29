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
from crawler.filters import UrlFilter
from crawler.models import CrawlStats, FetchResult, ParsedPage
from crawler.parser import HTMLParser
from crawler.queue import CrawlerQueue
from crawler.semaphores import SemaphoreManager
from crawler.urls import get_host, is_same_host, is_valid_http_url

__all__ = [
    "AsyncCrawler",
    "CrawlStats",
    "CrawlerClosedError",
    "CrawlerQueue",
    "FetchError",
    "FetchResult",
    "FetchTimeoutError",
    "HTMLParser",
    "HTTPStatusError",
    "InvalidURLError",
    "NetworkError",
    "ParsedPage",
    "SemaphoreManager",
    "UnexpectedError",
    "UrlFilter",
    "get_host",
    "is_same_host",
    "is_valid_http_url",
]
