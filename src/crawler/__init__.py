"""Asynchronous web crawler built on asyncio and aiohttp."""

from crawler.circuit_breaker import BreakerCall, CircuitBreaker, CircuitState
from crawler.client import AsyncCrawler
from crawler.exceptions import (
    CertificateError,
    CircuitOpenError,
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
    StorageError,
    TooManyRedirectsError,
    TransientError,
    TransientHTTPError,
    UnexpectedError,
    error_kind,
)
from crawler.filters import UrlFilter
from crawler.models import (
    CircuitStats,
    CrawlStats,
    DomainRate,
    ErrorStats,
    FetchResult,
    PageRecord,
    ParsedPage,
    RateStats,
)
from crawler.parser import HTMLParser
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryRule, RetryStrategy
from crawler.robots import RobotsParser, RobotsRules, product_token
from crawler.semaphores import SemaphoreManager
from crawler.storage import CSVStorage, DatabaseDriver, DatabaseStorage, DataStorage, JSONStorage, SQLiteStorage
from crawler.urls import get_host, is_same_host, is_valid_http_url

__all__ = [
    "AsyncCrawler",
    "BreakerCall",
    "CSVStorage",
    "CertificateError",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "CircuitStats",
    "CrawlStats",
    "CrawlerClosedError",
    "CrawlerQueue",
    "DataStorage",
    "DatabaseDriver",
    "DatabaseStorage",
    "DomainRate",
    "ErrorStats",
    "FetchError",
    "FetchResult",
    "FetchTimeoutError",
    "HTMLParser",
    "HTTPStatusError",
    "InvalidURLError",
    "JSONStorage",
    "NetworkError",
    "PageRecord",
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
    "SQLiteStorage",
    "SemaphoreManager",
    "StorageError",
    "TooManyRedirectsError",
    "TransientError",
    "TransientHTTPError",
    "UnexpectedError",
    "UrlFilter",
    "error_kind",
    "get_host",
    "is_same_host",
    "is_valid_http_url",
    "product_token",
]
