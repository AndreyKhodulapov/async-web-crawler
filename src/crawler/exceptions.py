"""Exceptions raised by the crawler.

Low-level aiohttp/asyncio errors are translated into a small, stable hierarchy
so that callers never have to depend on transport-specific exception types.

Most errors fall into one of four kinds that decide whether a retry can help:

- `TransientError`: the server or the path to it is overloaded for now
  (a timeout, HTTP 429, 503); the same request may succeed later.
  `RenderTimeoutError` is the timeout of the browser on a page already
  downloaded: it is retried as one, and not held against the host.
- `NetworkError`: the request did not reach the server (DNS, a refused or
  reset connection); worth retrying too. `DNSError` is the one of them
  that is mostly for good: a host name that does not exist. A resolver
  that fails for now (EAI_AGAIN) gives a plain `NetworkError`.
  `ProxyNetworkError` is the one where a proxy failed, not the site: its
  retry goes through another proxy.
- `PermanentError`: the request is wrong or forbidden (HTTP 404, 403, a bad
  certificate, a page over the size limit, a sitemap that is not one); every
  attempt would fail the same way.
- `ParseError`: the page was downloaded but is not an HTML document.

`CrawlerClosedError`, `RobotsUnreachableError`, `CircuitOpenError`,
`HostHeldBackError`, `NoProxyError`, `RenderError` and `UnexpectedError`
belong to none of them: they are not about the request itself, and none is
retried.

`ProxyError` is the base of the errors of proxies, `ProxyNetworkError` and
`NoProxyError`: they say nothing about the site.

`StorageError` is not about a URL at all: it reports a failure to save the
pages already crawled. Nor is `JobError`, about a crawl job of distributed
workers.
"""

from collections.abc import Sequence
from typing import ClassVar, Self


class FetchError(Exception):
    """Base class for every error the crawler reports for a URL."""

    def __init__(self, url: str, message: str) -> None:
        super().__init__(f"{url}: {message}")
        self.url = url
        self.message = message


class TransientError(FetchError):
    """A failure that may go away on its own, such as a timeout or HTTP 503."""


class PermanentError(FetchError):
    """A failure that would repeat on every attempt, such as HTTP 404."""


class NetworkError(FetchError):
    """The request failed at the network level (DNS, connection, payload)."""


class DNSError(NetworkError):
    """The resolver says the host name has no address: mostly a name that does not exist.

    A lookup that failed for now, such as a resolver that cannot be
    reached, is a plain `NetworkError`. That takes the codes of the system
    resolver: with aiodns installed, aiohttp uses it instead, and every
    failed lookup is a `DNSError`.
    """


class ParseError(FetchError):
    """The response is not an HTML document that can be parsed."""


class HTTPStatusError(FetchError):
    """The server responded with a 4xx or 5xx status code.

    `retry_after` is the number of seconds the server asked to wait in a
    Retry-After header, if it sent one.

    Creating an HTTPStatusError gives a `TransientHTTPError` for the
    statuses in `TRANSIENT_STATUSES` and a `PermanentHTTPError` for any
    other, so an HTTP error always has a kind.
    """

    # Request Timeout, Too Many Requests and server errors that usually pass,
    # including Cloudflare's 520-524: its origin server is down or too slow.
    # 501 Not Implemented or 505 HTTP Version Not Supported never do.
    TRANSIENT_STATUSES: ClassVar[frozenset[int]] = frozenset({408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524})

    def __new__(cls, url: str, status: int, reason: str, *, retry_after: float | None = None) -> Self:
        kind = cls
        if cls is HTTPStatusError:
            kind = TransientHTTPError if status in cls.TRANSIENT_STATUSES else PermanentHTTPError
        return super().__new__(kind)

    def __init__(self, url: str, status: int, reason: str, *, retry_after: float | None = None) -> None:
        super().__init__(url, f"HTTP {status} {reason}")
        self.status = status
        self.retry_after = retry_after


class TransientHTTPError(HTTPStatusError, TransientError):
    """HTTP 408, 429, 500, 502, 503, 504 or 520-524."""


class PermanentHTTPError(HTTPStatusError, PermanentError):
    """Any other 4xx or 5xx status, such as 401, 403 or 404."""


class TooManyRedirectsError(PermanentError):
    """The redirects did not end within the limit, e.g. a redirect loop."""


class CertificateError(PermanentError):
    """The server's TLS certificate failed verification."""


class FetchTimeoutError(TransientError):
    """The request did not complete within the configured timeouts."""


class InvalidURLError(PermanentError):
    """The URL is malformed or does not use the http(s) scheme."""


class CrawlerClosedError(FetchError):
    """The crawler was closed before the request could start."""


class RobotsDisallowedError(PermanentError):
    """robots.txt of the site does not allow this crawler to fetch the URL."""


class RobotsUnreachableError(FetchError):
    """robots.txt of the site could not be read, so no URL of the site may be fetched for now."""


class CircuitOpenError(FetchError):
    """The circuit breaker of the host is open: the request was not sent."""


class HostHeldBackError(FetchError):
    """The host is held back for longer than the request was to wait: the request was not sent.

    Only a crawl of several workers asks for it (see `Fetcher.fetch`): its
    page goes back to the queue, and the worker takes another one meanwhile.
    `seconds` is how much longer the host is held back.
    """

    def __init__(self, url: str, message: str, *, seconds: float) -> None:
        super().__init__(url, message)
        self.seconds = seconds


class ProxyError(FetchError):
    """The request could not go through a proxy; the site is not to blame."""


class ProxyNetworkError(ProxyError, NetworkError):
    """A proxy failed: it could not be reached, or it refused the request (HTTP 407)."""


class NoProxyError(ProxyError):
    """Every proxy for the URL is out of rotation: the request was not sent."""


class RenderError(FetchError):
    """The headless browser failed to render the page: it is not installed, could not start or crashed.

    The site is not to blame: the circuit breaker of its host does not count it.
    """


class RenderTimeoutError(FetchTimeoutError):
    """The headless browser took longer than the rendering timeout for the page.

    The document was downloaded in time, so the page is to blame, not its
    host: the circuit breaker does not count it, and the other requests to
    the host do not wait for its retry.
    """


class PageTooLargeError(PermanentError):
    """The body of the page is over the size limit; the rest of it was not downloaded."""


class SitemapError(PermanentError):
    """The sitemap was downloaded but cannot be read: it is not XML, not a sitemap, or too large."""


class UnexpectedError(FetchError):
    """An unforeseen exception (most likely a bug); the traceback is logged."""


class StorageError(Exception):
    """Crawled pages could not be written to a storage, or the storage is closed."""


class JobError(Exception):
    """A crawl job cannot be created, resumed or joined: its name is taken, there is no such job, or its configuration differs."""


class ConfigError(ValueError):
    """A configuration cannot be read, or has unknown keys or invalid values.

    `problems` lists them all, each starting with the path of its key, such
    as "crawler.max_pages", or with the file and line of a list of URLs.
    The message lists the first `shown` of them under `summary`, "N problems"
    if no summary is given.
    """

    shown = 20  # a file of the wrong kind can have thousands of problems

    def __init__(self, problems: Sequence[str], source: str | None = None, *, summary: str | None = None) -> None:
        self.problems = list(problems)
        self.source = source
        prefix = f"{source}: " if source else ""
        if len(self.problems) == 1 and summary is None:
            super().__init__(f"Invalid configuration: {prefix}{self.problems[0]}")
            return
        lines = "".join(f"\n  - {problem}" for problem in self.problems[: self.shown])
        if len(self.problems) > self.shown:
            lines += f"\n  - ... and {len(self.problems) - self.shown} more"
        super().__init__(f"Invalid configuration: {prefix}{summary or f'{len(self.problems)} problems'}{lines}")


ERROR_KINDS = (TransientError, PermanentError, NetworkError, ParseError)


def error_kind(error: BaseException) -> str:
    """The kind of an error by class name, such as "TransientError"; "other" if it has none."""
    for kind in ERROR_KINDS:
        if isinstance(error, kind):
            return kind.__name__
    return "other"
