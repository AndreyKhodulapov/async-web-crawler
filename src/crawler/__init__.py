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
    ParseError,
    PermanentError,
    PermanentHTTPError,
    RobotsDisallowedError,
    RobotsUnreachableError,
    TooManyRedirectsError,
    TransientError,
    TransientHTTPError,
    UnexpectedError,
)
from crawler.filters import UrlFilter
from crawler.models import CrawlStats, DomainRate, FetchResult, ParsedPage, RateStats
from crawler.parser import HTMLParser
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryRule, RetryStrategy
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
    "ParseError",
    "ParsedPage",
    "PermanentError",
    "PermanentHTTPError",
    "RateLimiter",
    "RateStats",
    "RetryRule",
    "RetryStrategy",
    "RobotsDisallowedError",
    "RobotsParser",
    "RobotsRules",
    "RobotsUnreachableError",
    "SemaphoreManager",
    "TooManyRedirectsError",
    "TransientError",
    "TransientHTTPError",
    "UnexpectedError",
    "UrlFilter",
    "get_host",
    "is_same_host",
    "is_valid_http_url",
    "product_token",
]
