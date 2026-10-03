"""Asynchronous web crawler built on asyncio and aiohttp."""

from crawler.advanced import AdvancedCrawler
from crawler.circuit_breaker import BreakerCall, CircuitBreaker, CircuitState
from crawler.client import AsyncCrawler
from crawler.config import CrawlerConfig, load_config
from crawler.exceptions import (
    CertificateError,
    CircuitOpenError,
    ConfigError,
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
    SitemapError,
    StorageError,
    TooManyRedirectsError,
    TransientError,
    TransientHTTPError,
    UnexpectedError,
    error_kind,
)
from crawler.filters import UrlFilter
from crawler.logging_setup import configure_logging
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
from crawler.progress import Progress, ProgressTracker, format_progress, show_progress
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryRule, RetryStrategy
from crawler.robots import RobotsParser, RobotsRules, product_token
from crawler.semaphores import SemaphoreManager
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage import (
    CompositeStorage,
    CSVStorage,
    DatabaseDriver,
    DatabaseStorage,
    DataStorage,
    JSONStorage,
    PostgresStorage,
    SQLiteStorage,
    register_database,
    storage_from_env,
    storage_from_output,
    storage_from_url,
)
from crawler.urls import get_host, is_same_host, is_valid_http_url

__all__ = [
    "AdvancedCrawler",
    "AsyncCrawler",
    "BreakerCall",
    "CSVStorage",
    "CertificateError",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "CircuitStats",
    "CompositeStorage",
    "ConfigError",
    "CrawlStats",
    "CrawlerClosedError",
    "CrawlerConfig",
    "CrawlerQueue",
    "CrawlerStats",
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
    "PostgresStorage",
    "Progress",
    "ProgressTracker",
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
    "SitemapError",
    "SitemapParser",
    "StorageError",
    "TooManyRedirectsError",
    "TransientError",
    "TransientHTTPError",
    "UnexpectedError",
    "UrlFilter",
    "configure_logging",
    "error_kind",
    "format_progress",
    "get_host",
    "is_same_host",
    "is_valid_http_url",
    "load_config",
    "product_token",
    "register_database",
    "show_progress",
    "storage_from_env",
    "storage_from_output",
    "storage_from_url",
]
