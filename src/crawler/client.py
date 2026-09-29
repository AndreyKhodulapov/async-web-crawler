"""Asynchronous HTTP client that downloads many pages concurrently."""

import asyncio
import logging
import ssl
import time
from collections.abc import Iterable, Mapping
from types import TracebackType
from typing import NamedTuple, Self

import aiohttp
import certifi
from bs4.dammit import EncodingDetector

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
from crawler.parser import HTMLParser, is_html_content_type
from crawler.queue import CrawlerQueue
from crawler.semaphores import SemaphoreManager
from crawler.urls import get_host, is_valid_http_url, normalize_url

logger = logging.getLogger(__name__)


class _Response(NamedTuple):
    status: int
    content: str
    size: int
    final_url: str
    content_type: str | None


class AsyncCrawler:
    """Downloads web pages concurrently over a shared connection pool.

    Can be used as an async context manager, or closed explicitly::

        async with AsyncCrawler(max_concurrent=5, max_depth=2) as crawler:
            pages = await crawler.fetch_urls(urls)
            page = await crawler.fetch_and_parse("https://example.com")
            site = await crawler.crawl(["https://example.com"], same_domain_only=True)

    At most `max_concurrent` requests run at once, and at most
    `max_per_domain` to one host (no per-host limit when it is None).

    Fetching from a closed crawler fails with `CrawlerClosedError`, reported
    the same way as any other per-URL failure. Closing does not interrupt
    requests that are already in flight: they finish on their own or hit
    `total_timeout`.
    """

    # Sites such as Wikipedia ask bots to identify themselves with a contact
    # URL and may block generic user agents.
    DEFAULT_USER_AGENT = "AsyncWebCrawler/0.1 (+https://github.com/AndreyKhodulapov/async-web-crawler)"

    def __init__(
        self,
        max_concurrent: int = 10,
        *,
        max_depth: int = 2,
        max_per_domain: int | None = None,
        total_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
        user_agent: str = DEFAULT_USER_AGENT,
        parser: HTMLParser | None = None,
    ) -> None:
        if max_depth < 0:
            raise ValueError(f"max_depth must be >= 0, got {max_depth}")
        for name, value in (
            ("total_timeout", total_timeout),
            ("connect_timeout", connect_timeout),
            ("read_timeout", read_timeout),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")

        # Validates max_concurrent and max_per_domain.
        self._limits = SemaphoreManager(max_concurrent, max_per_domain)
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        # `connect` covers DNS resolution and waiting for a pooled connection,
        # unlike `sock_connect`, which is only the TCP handshake.
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=connect_timeout,
            sock_read=read_timeout,
        )
        self._user_agent = user_agent
        self._parser = parser or HTMLParser()
        self._session: aiohttp.ClientSession | None = None
        self._closed = False
        # State of the latest crawl() call.
        self._queue = CrawlerQueue()
        self.processed_urls: dict[str, ParsedPage] = {}
        self._crawl_started: float | None = None
        self._crawl_finished: float | None = None

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

    @property
    def visited_urls(self) -> set[str]:
        """URLs the latest crawl took for fetching, successful or not. Do not modify."""
        return self._queue.visited

    @property
    def failed_urls(self) -> dict[str, str]:
        """URL -> error description for pages the latest crawl could not fetch. Do not modify."""
        return self._queue.failed

    @property
    def url_depths(self) -> Mapping[str, int]:
        """Depth of every URL the latest crawl accepted: 0 for start URLs."""
        return self._queue.depths

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
        result's `errors`, e.g. for a response that is not HTML. The body of
        such a response is not downloaded at all.

        Raises:
            FetchError: a subclass describing why the download failed.
        """
        result = await self._fetch(url, html_only=True)
        if result.error is not None:
            raise result.error
        assert result.content is not None
        return await self._parser.parse_html(
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
        return await self._fetch(url)

    async def _fetch(self, url: str, *, html_only: bool = False) -> FetchResult:
        async with self._limits.slot(url):
            logger.info("Fetching %s", url)
            started = time.perf_counter()
            try:
                response = await self._request(url, html_only=html_only)
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

    async def crawl(
        self,
        start_urls: Iterable[str],
        max_pages: int = 100,
        *,
        same_domain_only: bool = False,
        include_patterns: Iterable[str] = (),
        exclude_patterns: Iterable[str] = (),
    ) -> dict[str, ParsedPage]:
        """Crawl from the start URLs following links; return pages by normalized URL.

        Pages are fetched by `max_concurrent` workers, breadth-first: a link
        found on a page at depth d gets depth d + 1 and is followed only up
        to `max_depth`. Every URL is fetched at most once. `max_pages` caps
        the number of pages fetched, failed ones included.

        Filters apply to discovered links, not to the start URLs:
        `same_domain_only` keeps links on the hosts of the start URLs (and of
        the pages they redirect to); `include_patterns` and `exclude_patterns`
        are regular expressions, see `UrlFilter`. A link that passes the
        filters but redirects to a URL that does not is dropped: the page is
        not returned and its links are not followed.

        Failed and dropped pages do not stop the crawl: they are listed in
        `failed_urls` with the reason.
        The state of the crawl (`processed_urls`, `visited_urls`,
        `failed_urls`, `url_depths`, `crawl_stats()`) is reset on every call
        and stays available after it returns.

        Raises:
            TypeError: a single string is passed instead of a list of URLs or patterns.
            ValueError: `max_pages` is not positive, a start URL or a pattern is invalid.
            RuntimeError: another crawl is running on this crawler.
        """
        if isinstance(start_urls, str):
            raise TypeError(f"expected a list of start URLs, got a string: {start_urls!r}")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1, got {max_pages}")
        start_urls = list(start_urls)
        invalid = [url for url in start_urls if not is_valid_http_url(url)]
        if invalid:
            raise ValueError(f"invalid start URLs: {', '.join(map(repr, invalid))}")
        if self._crawl_started is not None and self._crawl_finished is None:
            raise RuntimeError("a crawl is already running on this crawler")

        url_filter = UrlFilter(
            allowed_hosts={get_host(url) for url in start_urls} if same_domain_only else None,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )
        self._queue = CrawlerQueue()
        self.processed_urls = {}
        for url in start_urls:
            self._queue.add_url(url, priority=0, depth=0)

        logger.info(
            "Crawl started: %d start URLs, max_depth=%d, max_pages=%d", len(start_urls), self.max_depth, max_pages
        )
        self._crawl_started, self._crawl_finished = time.perf_counter(), None
        try:
            async with asyncio.TaskGroup() as group:
                for _ in range(self.max_concurrent):
                    group.create_task(self._crawl_worker(self._queue, url_filter, max_pages))
        finally:
            self._crawl_finished = time.perf_counter()
        stats = self.crawl_stats()
        logger.info(
            "Crawl finished: %d processed, %d failed, %d left in queue, %.2fs",
            stats.processed,
            stats.failed,
            stats.queued,
            stats.elapsed,
        )
        return self.processed_urls

    def crawl_stats(self) -> CrawlStats:
        """Progress of the running crawl, or the result of the latest one."""
        if self._crawl_started is None:
            return CrawlStats()
        stats = self._queue.get_stats()
        return CrawlStats(
            processed=stats["processed"],
            failed=stats["failed"],
            queued=stats["queued"],
            in_progress=stats["in_progress"],
            active_requests=self._limits.active,
            elapsed=(self._crawl_finished or time.perf_counter()) - self._crawl_started,
        )

    async def _crawl_worker(self, queue: CrawlerQueue, url_filter: UrlFilter, max_pages: int) -> None:
        while (url := await queue.get_next()) is not None:
            if len(queue.visited) >= max_pages:
                # This URL is the last one allowed: the others stop taking new ones.
                queue.close()
            try:
                await self._crawl_page(url, queue, url_filter)
            except Exception as exc:
                # fetch_and_parse() reports expected failures as FetchError,
                # so this is a bug; it must not kill the worker, and the URL
                # must leave the in-progress state, or get_next() would wait forever.
                logger.exception("Unexpected error while crawling %s", url)
                queue.mark_failed(url, f"UnexpectedError: {type(exc).__name__}: {exc}")

    async def _crawl_page(self, url: str, queue: CrawlerQueue, url_filter: UrlFilter) -> None:
        depth = queue.depth(url)
        try:
            page = await self.fetch_and_parse(url)
        except FetchError as error:
            queue.mark_failed(url, f"{type(error).__name__}: {error.message}")
            return

        final_url = normalize_url(page["final_url"]) or page["final_url"]
        if final_url != url:
            # A later link to the redirect target must not fetch the page again.
            queue.mark_seen(final_url)
            final_host = get_host(final_url)
            if depth == 0 and final_host is not None:
                # A start URL that redirects ("example.com" -> "www.example.com")
                # defines the site as much as the URL itself.
                url_filter.allow_host(final_host)
            elif depth > 0 and not url_filter.allows(final_url):
                # aiohttp follows redirects on its own, so a link inside the
                # crawl scope can lead out of it, e.g. to a sign-in page on
                # another domain. Such a page is not part of the site.
                logger.info("Dropped %s: redirected out of scope to %s", url, final_url)
                queue.mark_failed(url, f"redirected out of scope: {final_url}")
                return

        queued = 0
        if depth < self.max_depth:
            for link in page["links"]:
                if url_filter.allows(link) and queue.add_url(link, priority=depth + 1, depth=depth + 1):
                    queued += 1
        self.processed_urls[url] = page
        queue.mark_processed(url)
        logger.info("Crawled %s (depth %d): %d links, %d new queued", url, depth, len(page["links"]), queued)

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
            fallback_charset_resolver=_sniff_charset,
        )

    async def _request(self, url: str, *, html_only: bool = False) -> _Response:
        """Perform the GET request and read the whole body.

        With `html_only`, the body of a response whose Content-Type is not
        HTML is not read: the content is empty and the size is 0.
        The size is measured after content decoding (gzip, deflate, ...),
        so it may be larger than the number of bytes sent over the network.
        """
        # Checked here rather than in the public methods: close() may run
        # while this task waits for a free slot, and a per-URL failure
        # keeps the rest of a fetch_many() batch intact.
        if self._closed:
            raise CrawlerClosedError(url, "crawler is closed")
        _validate_url(url)
        session = self._get_session()
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                # aiohttp reports "application/octet-stream" when the header
                # is missing; None lets callers tell the two cases apart.
                content_type = response.content_type if aiohttp.hdrs.CONTENT_TYPE in response.headers else None
                if html_only and not is_html_content_type(content_type):
                    # A link to an archive or a video must not be downloaded
                    # just to be rejected by the parser. Leaving the block
                    # without reading closes the connection mid-transfer.
                    logger.info("Skipping body of %s: %s is not HTML", url, content_type)
                    return _Response(
                        status=response.status,
                        content="",
                        size=0,
                        final_url=str(response.url),
                        content_type=content_type,
                    )
                body = await response.read()
                return _Response(
                    status=response.status,
                    content=_decode(body, response.get_encoding()),
                    size=len(body),
                    final_url=str(response.url),
                    content_type=content_type,
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


def _sniff_charset(response: aiohttp.ClientResponse, body: bytes) -> str:
    """Pick an encoding when the Content-Type header has no charset.

    aiohttp calls this only in that case and would otherwise assume UTF-8.
    Many pages declare their encoding in the markup instead:
    <meta charset="..."> or <meta http-equiv="Content-Type" content="...">.
    """
    declared = EncodingDetector.find_declared_encoding(body, is_html=True)
    if declared is None:
        return "utf-8"
    if not _is_ascii_compatible(declared):
        # The declaration was found by reading the bytes as ASCII, so they
        # cannot be UTF-16 and the like; the HTML spec says to use UTF-8.
        # This also rejects unknown names and codecs such as "undefined",
        # "idna" or "base64" that cannot decode a page at all.
        logger.debug("Ignoring declared charset %r for %s", declared, response.url)
        return "utf-8"
    return declared


def _is_ascii_compatible(encoding: str) -> bool:
    # A charset that decodes printable ASCII unchanged can read the markup.
    probe = bytes(range(0x20, 0x7F)) + b"\t\n\r"
    try:
        return probe.decode(encoding, errors="replace") == probe.decode("ascii")
    except (LookupError, UnicodeError):
        return False


def _decode(body: bytes, encoding: str) -> str:
    # A byte order mark overrides any declared charset (HTML spec) and is not
    # part of the text. A wrong charset should not drop the whole page:
    # undecodable bytes are replaced, and a charset naming a codec that cannot
    # decode text (e.g. "base64" or "undefined") falls back to UTF-8.
    body, bom_encoding = EncodingDetector.strip_byte_order_mark(body)
    try:
        return body.decode(bom_encoding or encoding, errors="replace")
    except (LookupError, UnicodeError):
        return body.decode("utf-8", errors="replace")


def _validate_url(url: str) -> None:
    """Reject URLs without an http(s) scheme, a host or a valid port before sending them.

    aiohttp does not wrap every malformed URL: "//host" fails on an internal
    assert, so such input is caught here instead.
    """
    if not is_valid_http_url(url):
        raise InvalidURLError(url, "expected an absolute http(s) URL with a valid host and port")
