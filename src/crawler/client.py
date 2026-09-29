"""Asynchronous HTTP client that downloads many pages concurrently."""

import asyncio
import logging
import ssl
import time
from collections.abc import Iterable
from types import TracebackType
from typing import NamedTuple, Self

import aiohttp
import certifi

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
from crawler.parser import HTMLParser, ParsedPage
from crawler.urls import is_valid_http_url

logger = logging.getLogger(__name__)

# Sites such as Wikipedia ask bots to identify themselves with a contact URL
# and may block generic user agents.
DEFAULT_USER_AGENT = "AsyncWebCrawler/0.1 (+https://github.com/AndreyKhodulapov/async-web-crawler)"


class _Response(NamedTuple):
    status: int
    content: str
    size: int
    final_url: str
    content_type: str | None


class AsyncCrawler:
    """Downloads web pages concurrently over a shared connection pool.

    Can be used as an async context manager, or closed explicitly::

        async with AsyncCrawler(max_concurrent=5) as crawler:
            pages = await crawler.fetch_urls(urls)
            page = await crawler.fetch_and_parse("https://example.com")

    Fetching from a closed crawler fails with `CrawlerClosedError`, reported
    the same way as any other per-URL failure. Closing does not interrupt
    requests that are already in flight: they finish on their own or hit
    `total_timeout`.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        *,
        total_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
        user_agent: str = DEFAULT_USER_AGENT,
        parser: HTMLParser | None = None,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent must be >= 1, got {max_concurrent}")
        for name, value in (
            ("total_timeout", total_timeout),
            ("connect_timeout", connect_timeout),
            ("read_timeout", read_timeout),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")

        self.max_concurrent = max_concurrent
        # `connect` covers DNS resolution and waiting for a pooled connection,
        # unlike `sock_connect`, which is only the TCP handshake.
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=connect_timeout,
            sock_read=read_timeout,
        )
        self._user_agent = user_agent
        self.parser = parser or HTMLParser()
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    async def fetch_url(self, url: str) -> str:
        """Download a single page and return its decoded body.

        Raises:
            FetchError: a subclass describing why the request failed.
        """
        result = await self.fetch_result(url)
        if result.error is not None:
            raise result.error
        assert result.content is not None
        return result.content

    async def fetch_and_parse(self, url: str) -> ParsedPage:
        """Download a page and extract structured data from it.

        Relative links are resolved against the URL reached after redirects.
        Parsing problems never raise: they are logged and listed in the
        result's ``errors``, e.g. for a response that is not HTML.

        Raises:
            FetchError: a subclass describing why the download failed.
        """
        result = await self.fetch_result(url)
        if result.error is not None:
            raise result.error
        assert result.content is not None
        return await self.parser.parse_html(
            result.content,
            url,
            final_url=result.final_url,
            content_type=result.content_type,
        )

    async def fetch_urls(self, urls: Iterable[str]) -> dict[str, str]:
        """Download pages concurrently; return bodies of successful ones only.

        Failures are logged and skipped. Use `fetch_many` to inspect them.
        """
        unique_urls = list(dict.fromkeys(urls))
        results = await self.fetch_many(unique_urls)
        return {result.url: result.content for result in results if result.content is not None}

    async def fetch_many(self, urls: Iterable[str]) -> list[FetchResult]:
        """Download pages concurrently; return one result per URL, in order."""
        # fetch_result() reports every per-URL failure in its result, so one
        # failed URL never cancels its siblings in the TaskGroup.
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(self.fetch_result(url)) for url in urls]
        return [task.result() for task in tasks]

    async def fetch_result(self, url: str) -> FetchResult:
        """Download a single page, reporting failures in the result."""
        async with self._semaphore:
            logger.info("Fetching %s", url)
            started = time.perf_counter()
            try:
                response = await self._request(url)
            except FetchError as error:
                elapsed = time.perf_counter() - started
                logger.warning(
                    "Failed %s after %.2fs: %s: %s",
                    url,
                    elapsed,
                    type(error).__name__,
                    error.message,
                )
                return FetchResult.failure(url, error, elapsed)
            except Exception as exc:
                # Last line of defense: a bug or an error type we did not
                # anticipate fails only this URL instead of cancelling the
                # whole batch. The traceback is logged so it stays visible.
                elapsed = time.perf_counter() - started
                logger.exception("Unexpected error for %s after %.2fs", url, elapsed)
                error = UnexpectedError(url, f"{type(exc).__name__}: {exc}")
                error.__cause__ = exc
                return FetchResult.failure(url, error, elapsed)

            elapsed = time.perf_counter() - started
            logger.info(
                "Fetched %s: status=%d size=%dB elapsed=%.2fs",
                url,
                response.status,
                response.size,
                elapsed,
            )
            return FetchResult(
                url=url,
                elapsed=elapsed,
                status=response.status,
                content=response.content,
                size=response.size,
                final_url=response.final_url,
                content_type=response.content_type,
            )

    async def close(self) -> None:
        """Close the underlying session. Safe to call more than once."""
        self._closed = True
        if self._session is not None:
            await self._session.close()
            self._session = None
            logger.debug("HTTP session closed")

    def _get_session(self) -> aiohttp.ClientSession:
        # The session is created lazily because aiohttp requires a running
        # event loop.
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self) -> aiohttp.ClientSession:
        # certifi's CA bundle is added on top of the system store: TLS then
        # works on Python builds without system certificates, and locally
        # installed CAs (corporate proxies) stay trusted.
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(cafile=certifi.where())
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, ttl_dns_cache=300, ssl=ssl_context)
        return aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent},
        )

    async def _request(self, url: str) -> _Response:
        """Perform the GET request and read the whole body.

        The size is measured after content decoding (gzip, deflate, ...),
        so it may be larger than the number of bytes sent over the network.
        """
        # Checked here rather than in the public methods: close() may run
        # while this task waits for the semaphore, and a per-URL failure
        # keeps the rest of a fetch_many() batch intact.
        if self._closed:
            raise CrawlerClosedError(url, "crawler is closed")
        _validate_url(url)
        session = self._get_session()
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                body = await response.read()
                # aiohttp reports "application/octet-stream" when the header
                # is missing; None lets callers tell the two cases apart.
                has_type = aiohttp.hdrs.CONTENT_TYPE in response.headers
                return _Response(
                    status=response.status,
                    content=_decode(body, response.get_encoding()),
                    size=len(body),
                    final_url=str(response.url),
                    content_type=response.content_type if has_type else None,
                )
        # Order matters: TooManyRedirects is a ClientResponseError, which is a
        # ClientError; aiohttp's ServerTimeoutError is both a ClientError and
        # a TimeoutError; InvalidURL is a ClientError too.
        except aiohttp.TooManyRedirects as exc:
            raise NetworkError(url, f"too many redirects ({len(exc.history)})") from exc
        except aiohttp.ClientResponseError as exc:
            raise HTTPStatusError(url, exc.status, exc.message) from exc
        except TimeoutError as exc:
            raise FetchTimeoutError(url, "request timed out") from exc
        # UnicodeError comes from IDNA encoding of the host, e.g. a domain
        # label longer than 63 characters; aiohttp does not wrap it.
        except (aiohttp.InvalidURL, UnicodeError) as exc:
            raise InvalidURLError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientError as exc:
            raise NetworkError(url, f"{type(exc).__name__}: {exc}") from exc


def _decode(body: bytes, encoding: str) -> str:
    # A wrong charset header should not drop the whole page: undecodable bytes
    # are replaced, and a charset naming a non-text codec (e.g. "base64")
    # falls back to UTF-8.
    try:
        return body.decode(encoding, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def _validate_url(url: str) -> None:
    """Reject URLs without an http(s) scheme or a host before sending them.

    aiohttp does not wrap every malformed URL: "//host" fails on an internal
    assert, so such input is caught here instead.
    """
    if not is_valid_http_url(url):
        raise InvalidURLError(url, "expected an absolute http(s) URL")
