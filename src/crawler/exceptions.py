"""Exceptions raised by the crawler.

Low-level aiohttp/asyncio errors are translated into a small, stable hierarchy
so that callers never have to depend on transport-specific exception types.
"""


class FetchError(Exception):
    """Base class for every error that happens while fetching a URL."""

    def __init__(self, url: str, message: str) -> None:
        super().__init__(f"{url}: {message}")
        self.url = url
        self.message = message


class HTTPStatusError(FetchError):
    """The server responded with a 4xx or 5xx status code."""

    def __init__(self, url: str, status: int, reason: str) -> None:
        super().__init__(url, f"HTTP {status} {reason}")
        self.status = status


class NetworkError(FetchError):
    """The request failed at the network level (DNS, connection, payload)."""


class FetchTimeoutError(FetchError):
    """The request did not complete within the configured timeouts."""


class CrawlerClosedError(FetchError):
    """The crawler was closed before the request could start."""
