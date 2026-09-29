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
    """The server responded with a 4xx or 5xx status code.

    `retry_after` is the number of seconds the server asked to wait in a
    Retry-After header, if it sent one.
    """

    def __init__(self, url: str, status: int, reason: str, *, retry_after: float | None = None) -> None:
        super().__init__(url, f"HTTP {status} {reason}")
        self.status = status
        self.retry_after = retry_after


class NetworkError(FetchError):
    """The request failed at the network level (DNS, connection, payload)."""


class FetchTimeoutError(FetchError):
    """The request did not complete within the configured timeouts."""


class InvalidURLError(FetchError):
    """The URL is malformed or does not use the http(s) scheme."""


class CrawlerClosedError(FetchError):
    """The crawler was closed before the request could start."""


class RobotsDisallowedError(FetchError):
    """robots.txt of the site does not allow this crawler to fetch the URL."""


class UnexpectedError(FetchError):
    """An unforeseen exception (most likely a bug); the traceback is logged."""
