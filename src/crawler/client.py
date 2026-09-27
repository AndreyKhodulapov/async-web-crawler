"""Asynchronous HTTP client that downloads many pages concurrently."""

import asyncio
import logging
import ssl
import time
from collections.abc import Iterable
from types import TracebackType
from typing import Self

import aiohttp
import certifi

from crawler.exceptions import (
    CrawlerClosedError,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    NetworkError,
)
from crawler.models import FetchResult

logger = logging.getLogger(__name__)


class AsyncCrawler:
    """Downloads web pages concurrently over a shared connection pool.

    Can be used as an async context manager, or closed explicitly::

        async with AsyncCrawler(max_concurrent=5) as crawler:
            pages = await crawler.fetch_urls(urls)

    Fetching from a closed crawler fails with `CrawlerClosedError`, reported
    the same way as any other per-URL failure.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        *,
        total_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
        user_agent: str = "AsyncWebCrawler/0.1",
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
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            sock_connect=connect_timeout,
            sock_read=read_timeout,
        )
        self._user_agent = user_agent
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

    async def fetch_urls(self, urls: Iterable[str]) -> dict[str, str]:
        """Download pages concurrently; return bodies of successful ones only.

        Failures are logged and skipped. Use `fetch_many` to inspect them.
        """
        unique_urls = list(dict.fromkeys(urls))
        results = await self.fetch_many(unique_urls)
        return {
            result.url: result.content
            for result in results
            if result.content is not None
        }

    async def fetch_many(self, urls: Iterable[str]) -> list[FetchResult]:
        """Download pages concurrently; return one result per URL, in order."""
        # fetch_result() never raises FetchError, so one failed URL does not
        # cancel its siblings. Unexpected exceptions (bugs) still propagate.
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(self.fetch_result(url)) for url in urls]
        return [task.result() for task in tasks]

    async def fetch_result(self, url: str) -> FetchResult:
        """Download a single page, reporting failures in the result."""
        async with self._semaphore:
            logger.info("Fetching %s", url)
            started = time.perf_counter()
            try:
                status, content, size = await self._request(url)
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

            elapsed = time.perf_counter() - started
            logger.info(
                "Fetched %s: status=%d size=%dB elapsed=%.2fs",
                url,
                status,
                size,
                elapsed,
            )
            return FetchResult(
                url=url, elapsed=elapsed, status=status, content=content, size=size
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
        # event loop. There is no await between the check and the assignment,
        # so concurrent tasks cannot create two sessions.
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self) -> aiohttp.ClientSession:
        # certifi's CA bundle is added on top of the system store: TLS then
        # works on Python builds without system certificates, and locally
        # installed CAs (corporate proxies) stay trusted.
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(cafile=certifi.where())
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent, ttl_dns_cache=300, ssl=ssl_context
        )
        return aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent},
        )

    async def _request(self, url: str) -> tuple[int, str, int]:
        """Perform the GET request; return (status, text, body size in bytes).

        The size is measured after content decoding (gzip, deflate, ...),
        so it may be larger than the number of bytes sent over the network.
        """
        # Checked here rather than in the public methods: close() may run
        # while this task waits for the semaphore, and a per-URL failure
        # keeps the rest of a fetch_many() batch intact.
        if self._closed:
            raise CrawlerClosedError(url, "crawler is closed")
        session = self._get_session()
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                body = await response.read()
                # A wrong charset header should not drop the whole page.
                text = body.decode(response.get_encoding(), errors="replace")
                return response.status, text, len(body)
        # Order matters: TooManyRedirects is a ClientResponseError, which is a
        # ClientError; aiohttp's ServerTimeoutError is both a ClientError and
        # a TimeoutError.
        except aiohttp.TooManyRedirects as exc:
            raise NetworkError(url, f"too many redirects ({len(exc.history)})") from exc
        except aiohttp.ClientResponseError as exc:
            raise HTTPStatusError(url, exc.status, exc.message) from exc
        except TimeoutError as exc:
            raise FetchTimeoutError(url, "request timed out") from exc
        except aiohttp.ClientError as exc:
            raise NetworkError(url, f"{type(exc).__name__}: {exc}") from exc
