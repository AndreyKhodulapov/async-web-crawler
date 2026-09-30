"""Asynchronous HTTP client that downloads many pages concurrently."""

import asyncio
import functools
import itertools
import logging
import math
import ssl
import time
from collections.abc import Iterable, Mapping, Sequence
from types import TracebackType
from typing import NamedTuple, Self

import aiohttp
import certifi
from bs4.dammit import EncodingDetector

from crawler.exceptions import (
    CertificateError,
    CrawlerClosedError,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    ParseError,
    RobotsDisallowedError,
    RobotsUnreachableError,
    TooManyRedirectsError,
    UnexpectedError,
)
from crawler.filters import UrlFilter
from crawler.models import CrawlStats, FetchResult, ParsedPage
from crawler.parser import HTMLParser, is_html_content_type
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryStrategy, parse_retry_after
from crawler.robots import RobotsParser, product_token
from crawler.semaphores import SemaphoreManager
from crawler.urls import get_host, is_valid_http_url, normalize_url

logger = logging.getLogger(__name__)


class _Response(NamedTuple):
    status: int
    content: str
    size: int
    final_url: str
    content_type: str | None
    redirected: bool


class AsyncCrawler:
    """Downloads web pages concurrently over a shared connection pool.

    Can be used as an async context manager, or closed explicitly::

        async with AsyncCrawler(max_concurrent=5, max_depth=2) as crawler:
            pages = await crawler.fetch_urls(urls)
            page = await crawler.fetch_and_parse("https://example.com")
            site = await crawler.crawl(["https://example.com"], same_domain_only=True)

    At most `max_concurrent` requests run at once, and at most
    `max_per_domain` to one host (no per-host limit when it is None).

    The crawler is polite by default:

    - Requests to one host start at least `1 / requests_per_second`,
      `min_delay` and the host's robots.txt Crawl-delay seconds apart, plus
      a random `jitter` (see `RateLimiter`). `per_domain_rate=False` applies
      the rate to all hosts together; `requests_per_second=None` removes it.
    - With `respect_robots`, every URL is checked against robots.txt of its
      site first. A disallowed URL is not requested and fails with
      `RobotsDisallowedError`. While robots.txt of a site cannot be read,
      its URLs are not requested either and fail with
      `RobotsUnreachableError` (see `RobotsParser`).
    - Failed requests are retried as `retry_strategy` says: by default
      transient and network errors, such as a timeout or HTTP 503, up to 3
      times with exponential backoff (see `RetryStrategy`). The pause before
      a retry is spent in the rate limiter: the whole host waits with it; so
      it does after a Retry-After header, even when the request is not
      retried.

    Every request has a `connect_timeout` (DNS, TCP and TLS, waiting for a
    pooled connection), a `read_timeout` (for each chunk of the response)
    and a `total_timeout` (the whole request). The n-th retry (from 1)
    multiplies all three by `timeout_growth**n`, at most by
    `MAX_TIMEOUT_GROWTH`: a server that is merely slow gets a chance to
    answer, and a dead one is not waited for forever.

    `user_agent` identifies the crawler, and robots.txt rules are looked up
    by its name ("MyBot/1.0 (+url)" is "mybot"). `user_agents` rotates
    several User-Agent strings between requests; they must all carry that
    same name, so rotation cannot sidestep robots.txt.

    Fetching from a closed crawler fails with `CrawlerClosedError`, reported
    the same way as any other per-URL failure. Closing does not interrupt
    requests that are already in flight: they finish on their own or hit
    `total_timeout`.
    """

    # Sites such as Wikipedia ask bots to identify themselves with a contact
    # URL and may block generic user agents.
    DEFAULT_USER_AGENT = "AsyncWebCrawler/0.1 (+https://github.com/AndreyKhodulapov/async-web-crawler)"
    MAX_TIMEOUT_GROWTH = 4.0

    def __init__(
        self,
        max_concurrent: int = 10,
        *,
        max_depth: int = 2,
        max_per_domain: int | None = None,
        requests_per_second: float | None = 1.0,
        per_domain_rate: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        respect_robots: bool = True,
        retry_strategy: RetryStrategy | None = None,
        total_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
        timeout_growth: float = 1.5,
        user_agent: str = DEFAULT_USER_AGENT,
        user_agents: Sequence[str] = (),
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
        if not (math.isfinite(timeout_growth) and timeout_growth >= 1):
            raise ValueError(f"timeout_growth must be a number >= 1, got {timeout_growth}")
        if isinstance(user_agents, str):
            raise TypeError(f"expected a list of user agents, got a string: {user_agents!r}")
        robots_name = product_token(user_agent)
        for agent in user_agents:
            if product_token(agent) != robots_name:
                raise ValueError(
                    f"rotated user agent {agent!r} must use the robots.txt name {robots_name!r} of user_agent"
                )

        # These validate their own arguments.
        self._limits = SemaphoreManager(max_concurrent, max_per_domain)
        self.rate_limiter = RateLimiter(requests_per_second, per_domain_rate, min_delay=min_delay, jitter=jitter)
        self.retry_strategy = retry_strategy or RetryStrategy()
        self.robots = RobotsParser(self._download_robots) if respect_robots else None
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        # `connect` covers DNS resolution and waiting for a pooled connection,
        # unlike `sock_connect`, which is only the TCP handshake.
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=connect_timeout,
            sock_read=read_timeout,
        )
        self.timeout_growth = timeout_growth
        self._user_agent = user_agent
        self._rotated_agents = itertools.cycle(user_agents) if user_agents else None
        self._parser = parser or HTMLParser()
        self._session: aiohttp.ClientSession | None = None
        self._closed = False
        # State of the latest crawl() call.
        self._queue = CrawlerQueue()
        self.processed_urls: dict[str, ParsedPage] = {}
        self._pages_requested = 0
        self._retries = 0
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
    def skipped_urls(self) -> dict[str, str]:
        """URL -> reason for pages the latest crawl fetched but left out. Do not modify."""
        return self._queue.skipped

    @property
    def blocked_urls(self) -> dict[str, str]:
        """URL -> reason for pages the latest crawl was not allowed to fetch. Do not modify."""
        return self._queue.blocked

    @property
    def unreachable_urls(self) -> dict[str, str]:
        """URL -> reason for pages the latest crawl skipped because robots.txt was unreachable. Do not modify."""
        return self._queue.unreachable

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
        Problems in parts of the page are logged and listed in the result's
        `errors`. A response that is not HTML fails with `ParseError`, and
        its body is not downloaded at all.

        Raises:
            FetchError: a subclass describing why the download or parsing failed.
        """
        result = await self._fetch(url, html_only=True)
        if result.error is not None:
            raise result.error
        return await self._parse(result)

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

    async def _fetch(
        self,
        url: str,
        *,
        html_only: bool = False,
        check_robots: bool = True,
        failure_level: int = logging.WARNING,
    ) -> FetchResult:
        """Download a page, checking robots.txt first and retrying transient failures."""
        # Checked up front as well as in _request(): a closed crawler must
        # report itself even for a URL that robots.txt would block.
        if self._closed:
            return FetchResult.failure(url, CrawlerClosedError(url, "crawler is closed"), 0.0)
        if check_robots and (refusal := await self._check_robots(url)) is not None:
            return FetchResult.failure(url, refusal, 0.0)
        # The strategy needs an attempt that raises; the result of the last
        # one is kept to report a failure with its timing.
        last: FetchResult | None = None
        attempts = 0

        async def attempt() -> FetchResult:
            nonlocal last, attempts
            timeout = self._timeout_for(retries=attempts)
            attempts += 1
            last = await self._fetch_once(url, html_only=html_only, timeout=timeout)
            if last.error is not None:
                raise last.error
            return last

        try:
            return await self.retry_strategy.run(
                attempt, wait=self._wait_before_retry, target=url, failure_level=failure_level
            )
        except FetchError as error:
            assert last is not None and last.error is error
            host = get_host(url)
            # The server asked to wait: the other requests to the host wait
            # even when this one is not retried, up to the longest retry pause.
            if isinstance(error, HTTPStatusError) and error.retry_after and host is not None:
                self.rate_limiter.penalize(host, min(error.retry_after, self.retry_strategy.max_delay))
            return last

    async def _wait_before_retry(self, error: Exception, delay: float) -> None:
        """Hold back the host of the failed request instead of sleeping.

        The retry then waits for its turn in the rate limiter, and so does
        every other request to the host: a timeout or HTTP 429 usually means
        the whole site is overloaded, not one page.
        """
        assert isinstance(error, FetchError)  # _fetch_once() reports every failure as one
        host = get_host(error.url)
        assert host is not None  # an invalid URL fails with an error that is not retried
        self._retries += 1
        self.rate_limiter.penalize(host, delay)

    def _timeout_for(self, retries: int) -> aiohttp.ClientTimeout:
        """Timeouts of a request after `retries` failed attempts."""
        try:
            growth = min(self.timeout_growth**retries, self.MAX_TIMEOUT_GROWTH)
        except OverflowError:
            growth = self.MAX_TIMEOUT_GROWTH
        base = self._timeout
        assert base.total is not None and base.connect is not None and base.sock_read is not None
        return aiohttp.ClientTimeout(
            total=base.total * growth,
            connect=base.connect * growth,
            sock_read=base.sock_read * growth,
        )

    async def _check_robots(self, url: str) -> FetchError | None:
        """Return the error to fail `url` with if robots.txt does not allow it, None if it may be fetched."""
        host = get_host(url)
        if self.robots is None or host is None:
            return None  # an invalid URL fails in _request() with InvalidURLError
        try:
            allowed = await self.robots.is_allowed(url, self._user_agent)
        except CrawlerClosedError as error:
            return error
        crawl_delay = self.robots.get_crawl_delay(url, self._user_agent)
        if crawl_delay:
            self.rate_limiter.set_delay(host, crawl_delay)
        if allowed:
            return None
        unreachable = self.robots.unreachable_reason(url)
        refusal: FetchError
        if unreachable is None:
            refusal = RobotsDisallowedError(url, "disallowed by robots.txt")
        else:
            refusal = RobotsUnreachableError(url, f"robots.txt is unreachable ({unreachable})")
        logger.info("Blocked %s: %s", url, refusal.message)
        return refusal

    async def _download_robots(self, url: str) -> tuple[int, str]:
        """Fetcher for RobotsParser: robots.txt goes through the same limits and retries as a page."""
        # Many sites have no robots.txt; RobotsParser logs the outcomes that matter.
        result = await self._fetch(url, check_robots=False, failure_level=logging.INFO)
        if isinstance(result.error, HTTPStatusError):
            return result.error.status, ""
        if result.error is not None:
            raise result.error
        assert result.status is not None and result.content is not None
        return result.status, result.content

    async def _fetch_once(self, url: str, *, html_only: bool, timeout: aiohttp.ClientTimeout) -> FetchResult:
        host = get_host(url)
        gate = functools.partial(self._limits.slot, url)
        # The rate limit is waited for before taking a concurrency slot, so
        # a request waiting for its host does not hold a slot another host
        # could use; inside the slot the interval is checked once more.
        async with gate() if host is None else self.rate_limiter.slot(host, gate):
            logger.info("Fetching %s", url)
            started = time.perf_counter()
            try:
                response = await self._request(url, html_only=html_only, timeout=timeout)
            except FetchError as error:
                elapsed = time.perf_counter() - started
                # RetryStrategy logs the failure along with what comes next.
                logger.debug(
                    "Request to %s failed after %.2fs: %s: %s", url, elapsed, type(error).__name__, error.message
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
                redirected=response.redirected,
            )

    async def _parse(self, result: FetchResult) -> ParsedPage:
        """Parse a successful fetch result."""
        assert result.content is not None
        return await self._parser.parse_html(
            result.content,
            result.url,
            final_url=result.final_url,
            content_type=result.content_type,
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
        the number of pages requested, failed ones included. Pages that
        robots.txt disallows are not requested: they are listed in
        `blocked_urls` and do not count toward `max_pages`. Neither do pages
        of sites whose robots.txt cannot be read: they are listed in
        `unreachable_urls`.

        Filters apply to discovered links, not to the start URLs:
        `same_domain_only` keeps links on the hosts of the start URLs (and of
        the pages they redirect to); `include_patterns` and `exclude_patterns`
        are regular expressions, see `UrlFilter`. A link that passes the
        filters but redirects to a URL that does not is skipped: the page is
        not returned, its links are not followed, and it is listed in
        `skipped_urls` with the reason.

        Failed pages do not stop the crawl: they are listed in `failed_urls`.
        The state of the crawl (`processed_urls`, `visited_urls`,
        `failed_urls`, `skipped_urls`, `blocked_urls`, `unreachable_urls`, `url_depths`,
        `crawl_stats()`) is reset on every call and stays available after it
        returns. The rate limits and the robots.txt cache carry over.

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
        self._pages_requested = 0
        self._retries = 0
        self.rate_limiter.reset_stats()
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
            "Crawl finished: %d processed, %d failed, %d skipped, %d blocked, %d unreachable, %d left in queue, %.2fs",
            stats.processed,
            stats.failed,
            stats.skipped,
            stats.blocked,
            stats.unreachable,
            stats.queued,
            stats.elapsed,
        )
        return self.processed_urls

    def crawl_stats(self) -> CrawlStats:
        """Progress of the running crawl, or the result of the latest one."""
        if self._crawl_started is None:
            return CrawlStats()
        stats = self._queue.get_stats()
        rate = self.rate_limiter.get_stats()
        return CrawlStats(
            processed=stats["processed"],
            failed=stats["failed"],
            skipped=stats["skipped"],
            blocked=stats["blocked"],
            unreachable=stats["unreachable"],
            queued=stats["queued"],
            in_progress=stats["in_progress"],
            active_requests=self._limits.active,
            elapsed=(self._crawl_finished or time.perf_counter()) - self._crawl_started,
            requests=rate.requests,
            retries=self._retries,
            current_rps=rate.current_rps,
            avg_delay=rate.avg_delay,
            avg_wait=rate.avg_wait,
        )

    async def _crawl_worker(self, queue: CrawlerQueue, url_filter: UrlFilter, max_pages: int) -> None:
        while (url := await queue.get_next()) is not None:
            try:
                # robots.txt is checked before the page counts toward
                # max_pages: a blocked page costs no request.
                refusal = await self._check_robots(url)
                if refusal is not None:
                    if isinstance(refusal, RobotsDisallowedError):
                        queue.mark_blocked(url, refusal.message)
                    elif isinstance(refusal, RobotsUnreachableError):
                        queue.mark_unreachable(url, refusal.message)
                    else:
                        queue.mark_failed(url, f"{type(refusal).__name__}: {refusal.message}")
                    continue
                if self._pages_requested >= max_pages:
                    # Taken while another worker was still checking the page
                    # that reached the limit: it goes back to the queue.
                    queue.requeue(url, priority=queue.depth(url))
                    continue
                self._pages_requested += 1
                if self._pages_requested >= max_pages:
                    # This URL is the last one allowed: the others stop taking new ones.
                    queue.close()
                await self._crawl_page(url, queue, url_filter)
            except Exception as exc:
                # _fetch() reports expected failures in the result, so this
                # is a bug; it must not kill the worker, and the URL
                # must leave the in-progress state, or get_next() would wait forever.
                logger.exception("Unexpected error while crawling %s", url)
                queue.mark_failed(url, f"UnexpectedError: {type(exc).__name__}: {exc}")

    async def _crawl_page(self, url: str, queue: CrawlerQueue, url_filter: UrlFilter) -> None:
        depth = queue.depth(url)
        result = await self._fetch(url, html_only=True, check_robots=False)
        if result.error is not None:
            queue.mark_failed(url, f"{type(result.error).__name__}: {result.error.message}")
            return
        if result.redirected:
            # Normalized like the links, so that patterns see the same form.
            final_url = normalize_url(result.final_url or url)
            assert final_url is not None  # aiohttp has just fetched it
            # A later link to the redirect target must not fetch the page again.
            queue.mark_seen(final_url)
            if depth == 0:
                # A start URL that redirects ("example.com" -> "www.example.com")
                # defines the site as much as the URL itself.
                url_filter.allow_host_of(final_url)
            elif not url_filter.allows(final_url):
                # aiohttp follows redirects on its own, so a link inside the
                # crawl scope can lead out of it, e.g. to a sign-in page on
                # another domain. Such a page is not part of the site.
                logger.info("Skipped %s: redirected out of scope to %s", url, final_url)
                queue.mark_skipped(url, f"redirected out of scope: {final_url}")
                return

        try:
            page = await self._parse(result)
        except ParseError as error:
            logger.warning("Failed to parse %s: %s", url, error.message)
            queue.mark_failed(url, f"ParseError: {error.message}")
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

    async def _request(self, url: str, *, html_only: bool, timeout: aiohttp.ClientTimeout) -> _Response:
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
        headers = None if self._rotated_agents is None else {"User-Agent": next(self._rotated_agents)}
        try:
            async with session.get(url, headers=headers, timeout=timeout) as response:
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
                        redirected=bool(response.history),
                    )
                body = await response.read()
                return _Response(
                    status=response.status,
                    content=_decode(body, response.get_encoding()),
                    size=len(body),
                    final_url=str(response.url),
                    content_type=content_type,
                    redirected=bool(response.history),
                )
        # Order matters: TooManyRedirects is a ClientResponseError, which is a
        # ClientError; aiohttp's ServerTimeoutError is both a ClientError and
        # a TimeoutError; InvalidURL and the certificate error are ClientErrors too.
        except aiohttp.TooManyRedirects as exc:
            raise TooManyRedirectsError(url, f"too many redirects ({len(exc.history)})") from exc
        except aiohttp.ClientResponseError as exc:
            retry_after = parse_retry_after(exc.headers.get(aiohttp.hdrs.RETRY_AFTER) if exc.headers else None)
            raise HTTPStatusError(url, exc.status, exc.message, retry_after=retry_after) from exc
        except TimeoutError as exc:
            raise FetchTimeoutError(url, _describe_timeout(exc, timeout)) from exc
        # UnicodeError comes from IDNA encoding of the host, e.g. a domain
        # label longer than 63 characters; aiohttp does not wrap it.
        except (aiohttp.InvalidURL, UnicodeError) as exc:
            raise InvalidURLError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientConnectorCertificateError as exc:
            raise CertificateError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientError as exc:
            raise NetworkError(url, f"{type(exc).__name__}: {exc}") from exc


def _describe_timeout(exc: TimeoutError, timeout: aiohttp.ClientTimeout) -> str:
    # aiohttp raises its own subclasses for the connect and read timeouts
    # and a plain TimeoutError for the total one.
    if isinstance(exc, aiohttp.ConnectionTimeoutError):
        return f"connect timeout ({timeout.connect:.1f}s)"
    if isinstance(exc, aiohttp.SocketTimeoutError):
        return f"read timeout ({timeout.sock_read:.1f}s)"
    return f"total timeout ({timeout.total:.1f}s)"


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
