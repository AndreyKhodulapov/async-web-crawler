"""Asynchronous web crawler built on asyncio and aiohttp."""

from crawler.client import AsyncCrawler
from crawler.exceptions import (
    CertificateError,
    CrawlerClosedError,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    RobotsDisallowedError,
    RobotsUnreachableError,
    TooManyRedirectsError,
    UnexpectedError,
)
from crawler.filters import UrlFilter
from crawler.models import CrawlStats, DomainRate, FetchResult, ParsedPage, RateStats
from crawler.parser import HTMLParser
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryPolicy
from crawler.robots import RobotsParser, RobotsRules, product_token
from crawler.semaphores import SemaphoreManager
from crawler.urls import get_host, is_same_host, is_valid_http_url

__all__ = [
    "AsyncCrawler",
    "CertificateError",
    "CrawlStats",
    "CrawlerClosedError",
    "CrawlerQueue",
    "DomainRate",
    "FetchError",
    "FetchResult",
    "FetchTimeoutError",
    "HTMLParser",
    "HTTPStatusError",
    "InvalidURLError",
    "NetworkError",
    "ParsedPage",
    "RateLimiter",
    "RateStats",
    "RetryPolicy",
    "RobotsDisallowedError",
    "RobotsParser",
    "RobotsRules",
    "RobotsUnreachableError",
    "SemaphoreManager",
    "TooManyRedirectsError",
    "UnexpectedError",
    "UrlFilter",
    "get_host",
    "is_same_host",
    "is_valid_http_url",
    "product_token",
]
