"""Asynchronous HTTP client that downloads many pages concurrently."""

import asyncio
import codecs
import contextlib
import dataclasses
import itertools
import logging
import math
import ssl
import time
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import NamedTuple, Self

import aiohttp
import certifi
from bs4.dammit import EncodingDetector

from crawler.circuit_breaker import CircuitBreaker, CircuitState
from crawler.error_stats import ErrorTracker
from crawler.exceptions import (
    CertificateError,
    CircuitOpenError,
    CrawlerClosedError,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    PageTooLargeError,
    ParseError,
    RobotsDisallowedError,
    RobotsUnreachableError,
    SitemapError,
    StorageError,
    TooManyRedirectsError,
    UnexpectedError,
)
from crawler.filters import UrlFilter
from crawler.models import CrawlStats, ErrorStats, FetchResult, PageRecord, ParsedPage
from crawler.parser import HTMLParser, is_html_content_type
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryStrategy, parse_retry_after
from crawler.robots import RobotsParser, product_token
from crawler.semaphores import SemaphoreManager
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage.base import DataStorage
from crawler.urls import get_host, is_valid_http_url, normalize_url, resolve_url

logger = logging.getLogger(__name__)


class _Response(NamedTuple):
    status: int
    content: str
    size: int
    final_url: str
    content_type: str | None
    redirected: bool = False  # a redirect, not followed; `final_url` is its Location header
    body: bytes | None = None  # the bytes as sent, when asked for instead of the text


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
    - Redirects are followed one request at a time, up to
      `MAX_REDIRECTS`: the target of each goes through robots.txt, the
      rate limit, the retries and the circuit breaker of its own host.
    - Failed requests are retried as `retry_strategy` says: by default
      transient and network errors, such as a timeout or HTTP 503, up to 3
      times with exponential backoff (see `RetryStrategy`). The pause before
      a retry is spent in the rate limiter: the whole host waits with it; so
      it does after a Retry-After header, even when the request is not
      retried.
    - A host whose requests keep failing is left alone for a while, as
      `circuit_breaker` says: by default once half of at least 5 requests
      in a minute have failed with a transient or network error, its
      requests fail with `CircuitOpenError` for 30 seconds without being
      sent (see `CircuitBreaker`). A retry the breaker would refuse is not
      made: the request fails with the error of its last attempt.
      robots.txt that cannot be downloaded for this reason is not cached
      as unreachable. In `crawl()`, the pages of such a host wait for it.

    Every request has a `connect_timeout` (DNS, TCP and TLS, waiting for a
    pooled connection), a `read_timeout` (for each chunk of the response)
    and a `total_timeout` (the whole request). The n-th retry (from 1)
    multiplies all three by `timeout_growth**n`, at most by
    `MAX_TIMEOUT_GROWTH`: a server that is merely slow gets a chance to
    answer, and a dead one is not waited for forever.

    A page body over `max_page_size` bytes (None for no limit) fails with
    `PageTooLargeError`; the rest of it is not downloaded. The limit is on
    the unpacked body, so a small gzipped response that unpacks into
    gigabytes fails too.

    `user_agent` identifies the crawler, and robots.txt rules are looked up
    by its name ("MyBot/1.0 (+url)" is "mybot"). `user_agents` rotates
    several User-Agent strings between requests; they must all carry that
    same name, so rotation cannot sidestep robots.txt.

    `error_stats()` counts the errors of page requests and their retries
    (see `ErrorStats`); robots.txt downloads and the URLs it blocks are not
    counted there, and neither are the requests the circuit breaker refused
    (see `circuit_breaker.get_stats()`).

    `stats` counts the pages of the latest `crawl()` by outcome, status code
    and domain, with the speed and the running time of the crawl (see
    `CrawlerStats`); `crawl_stats()` is a snapshot of its progress.

    `crawl()` can take the pages to start from out of sitemaps (see
    `SitemapParser`), which are downloaded like pages: robots.txt, the
    limits, the retries and the circuit breaker apply to them.

    With a `storage`, `crawl()` saves every page it has processed there (see
    `DataStorage`, `PageRecord`). A page that cannot be saved is logged and
    counted in `crawl_stats()`; the crawl goes on. Closing the crawler
    closes the storage too.

    `crawl()` keeps every parsed page in `processed_urls` and returns them,
    so its memory grows with the size of the crawl. `keep_pages=False`
    lets a page go once it is saved and its links are queued:
    `processed_urls` stays empty, the pages are in the storage, the counts
    in `stats` and `crawl_stats()`. That is the setting for a large crawl.

    Fetching from a closed crawler fails with `CrawlerClosedError`, reported
    the same way as any other per-URL failure. Closing does not interrupt
    requests that are already in flight: they finish on their own or hit
    `total_timeout`.
    """

    # Sites such as Wikipedia ask bots to identify themselves with a contact
    # URL and may block generic user agents.
    DEFAULT_USER_AGENT = "AsyncWebCrawler/0.1 (+https://github.com/AndreyKhodulapov/async-web-crawler)"
    DEFAULT_MAX_PAGE_SIZE = 10 * 1024 * 1024
    MAX_TIMEOUT_GROWTH = 4.0
    MAX_REDIRECTS = 10
    REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
    MAX_CIRCUIT_OPENINGS = 3

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
        circuit_breaker: CircuitBreaker | None = None,
        total_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
        timeout_growth: float = 1.5,
        max_page_size: int | None = DEFAULT_MAX_PAGE_SIZE,
        user_agent: str = DEFAULT_USER_AGENT,
        user_agents: Sequence[str] = (),
        parser: HTMLParser | None = None,
        storage: DataStorage | None = None,
        keep_pages: bool = True,
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
        if max_page_size is not None and max_page_size < 1:
            raise ValueError(f"max_page_size must be >= 1 or None, got {max_page_size}")
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
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.robots = RobotsParser(self._download_robots) if respect_robots else None
        self.sitemaps = SitemapParser(self._download_sitemap)
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
        self.max_page_size = max_page_size
        self._user_agent = user_agent
        self._rotated_agents = itertools.cycle(user_agents) if user_agents else None
        self._parser = parser or HTMLParser()
        self.storage = storage
        self.keep_pages = keep_pages
        self._session: aiohttp.ClientSession | None = None
        self._closed = False
        # State of the latest crawl() call.
        self._queue = CrawlerQueue()
        self.processed_urls: dict[str, ParsedPage] = {}
        self._start_urls: set[str] = set()
        self._sitemap_pages_out_of_scope: list[str] = []
        self._redirect_sources: dict[str, str] = {}  # redirect target -> the page that led to it
        self._failed_sitemaps: dict[str, str] = {}
        self.stats = CrawlerStats()
        self._pages_requested = 0
        self._pages_to_save = 0
        self._written_before = 0
        self._pending_before = 0
        self._retries = 0
        self._errors = ErrorTracker()
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
        """URL -> reason for pages the latest crawl fetched but left out, e.g. not HTML. Do not modify."""
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
    def failed_sitemaps(self) -> dict[str, str]:
        """Sitemap URL -> error description for sitemaps the latest crawl could not read. Do not modify."""
        return self._failed_sitemaps

    @property
    def url_depths(self) -> Mapping[str, int]:
        """Depth of every URL the latest crawl accepted: 0 for start URLs and pages listed in sitemaps."""
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
        raw: bool = False,
        truncate_at: int | None = None,
        check_robots: bool = True,
        check_redirect_robots: bool = True,
        follow: Callable[[str], bool] | None = None,
        failure_level: int = logging.WARNING,
        track_errors: bool = True,
    ) -> FetchResult:
        """Download a page, following its redirects one at a time.

        Every request of the chain goes through robots.txt, the circuit
        breaker, the rate limit and the retries of its own host, as a link
        to the target would (see `_fetch_hop`). robots.txt is checked for
        `url` with `check_robots`, for the targets of its redirects with
        `check_redirect_robots`. `follow`, if given, is asked about every
        target, normalized, before it is requested; when it says no, the
        result is that of the redirect itself: no content, `redirected`
        set and `final_url` the target. More than `MAX_REDIRECTS` redirects
        in a row fail with `TooManyRedirectsError`.

        With `raw`, which is how a sitemap is downloaded, the result has the
        `body` as it was sent instead of the decoded `content`, and a body
        over the size limit of a sitemap fails with `SitemapError`. With
        `truncate_at`, which is how robots.txt is downloaded, the body is
        cut to that many bytes instead of failing over `max_page_size`. With
        `track_errors`, the attempts count in `error_stats()`.
        """
        target, elapsed, retried, attempts = url, 0.0, False, 0
        for redirects in range(self.MAX_REDIRECTS + 1):
            if redirects == self.MAX_REDIRECTS:
                result = FetchResult.failure(
                    url, TooManyRedirectsError(url, f"too many redirects ({redirects})"), elapsed
                )
                break
            result, attempts = await self._fetch_hop(
                target,
                html_only=html_only,
                raw=raw,
                truncate_at=truncate_at,
                check_robots=check_robots if redirects == 0 else check_redirect_robots,
                failure_level=failure_level,
                track_errors=track_errors,
            )
            elapsed += result.elapsed
            retried = retried or attempts > 1
            if not result.redirected:
                break
            assert result.final_url is not None
            location = resolve_url(result.final_url, target)
            if location is None:
                error = InvalidURLError(target, f"redirects to an invalid URL: {result.final_url!r}")
                result = FetchResult.failure(url, error, elapsed)
                break
            if follow is not None and not follow(location):
                result = dataclasses.replace(result, final_url=location)
                break
            logger.info("Redirect %s -> %s (%d)", target, location, result.status)
            target = location
        # A request refused before it was sent (robots.txt, the circuit
        # breaker) has no outcome to count.
        if track_errors and attempts:
            self._errors.record_outcome(url, result.error, retried=retried)
        if result.error is None and redirects:
            result = dataclasses.replace(result, redirected=True)
        return dataclasses.replace(result, url=url, elapsed=elapsed)

    async def _fetch_hop(
        self,
        url: str,
        *,
        html_only: bool,
        raw: bool,
        truncate_at: int | None,
        check_robots: bool,
        failure_level: int,
        track_errors: bool,
    ) -> tuple[FetchResult, int]:
        """Make the request for one URL, checking robots.txt first and retrying transient failures.

        A redirect is not followed: it comes back as a result with
        `redirected` set and `final_url` its Location header as sent. Also
        returns the number of attempts made, 0 if the request was refused
        before it was sent.
        """
        # Checked up front as well as in _request(): a closed crawler must
        # report itself even for a URL that robots.txt would block.
        if self._closed:
            return FetchResult.failure(url, CrawlerClosedError(url, "crawler is closed"), 0.0), 0
        if check_robots and (refusal := await self._check_robots(url)) is not None:
            return FetchResult.failure(url, refusal, 0.0), 0
        # Refused at once, without waiting for the turn of the host.
        if (refusal := self._check_circuit(url)) is not None:
            logger.info("Refused %s: %s", url, refusal.message)
            return FetchResult.failure(url, refusal, 0.0), 0
        # The strategy needs an attempt that raises; the result of the last
        # one is kept to report a failure with its timing.
        last: FetchResult | None = None
        attempts = 0
        failed_at: float | None = None  # when the previous attempt failed

        async def attempt() -> FetchResult:
            nonlocal last, attempts, failed_at
            timeout = self._timeout_for(retries=attempts)
            attempts += 1
            result = await self._fetch_once(url, html_only=html_only, raw=raw, truncate_at=truncate_at, timeout=timeout)
            if isinstance(result.error, CircuitOpenError):
                # Not sent. A retry fails as the attempt before it did: the
                # strategy sees the circuit open and stops, and the failure
                # reported is that of a request that was sent.
                last = last or result
                assert last.error is not None
                raise last.error
            last = result
            if attempts > 1:
                self._retries += 1
            if track_errors:
                now = time.perf_counter()
                if failed_at is not None:
                    self._errors.record_retry(now - failed_at)
                if last.error is not None:
                    failed_at = now
                    self._errors.record_error(last.error)
            if last.error is not None:
                raise last.error
            return last

        try:
            result = await self.retry_strategy.run(
                attempt,
                wait=self._wait_before_retry,
                target=url,
                failure_level=failure_level,
                # A retry the circuit breaker would refuse is not waited for.
                veto=lambda error: self.circuit_breaker.refusal(url),
            )
        except FetchError as error:
            assert last is not None and last.error is error
            host = get_host(url)
            # The server asked to wait: the other requests to the host wait
            # even when this one is not retried, up to the longest retry pause.
            if isinstance(error, HTTPStatusError) and error.retry_after and host is not None:
                self.rate_limiter.penalize(host, min(error.retry_after, self.retry_strategy.max_delay))
            return last, attempts
        return result, attempts

    async def _wait_before_retry(self, error: Exception, delay: float) -> None:
        """Hold back the host of the failed request instead of sleeping.

        The retry then waits for its turn in the rate limiter, and so does
        every other request to the host: a timeout or HTTP 429 usually means
        the whole site is overloaded, not one page.
        """
        assert isinstance(error, FetchError)  # _fetch_once() reports every failure as one
        host = get_host(error.url)
        assert host is not None  # an invalid URL fails with an error that is not retried
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
        except (CrawlerClosedError, CircuitOpenError) as error:
            # Raised for the robots.txt URL; the page fails for the same reason under its own.
            return type(error)(url, error.message)
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

    def _check_circuit(self, url: str) -> CircuitOpenError | None:
        """The error to fail `url` with if the circuit breaker of its host refuses it, None if it may be fetched."""
        try:
            self.circuit_breaker.check(url)
        except CircuitOpenError as error:
            return error
        return None

    async def _download_robots(self, url: str) -> tuple[int, str]:
        """Fetcher for RobotsParser: robots.txt goes through the same limits and retries as a page."""
        # Many sites have no robots.txt; RobotsParser logs the outcomes that matter.
        result = await self._fetch(
            url,
            truncate_at=RobotsParser.MAX_SIZE,
            # Its redirects too: their robots.txt may be the one being downloaded.
            check_robots=False,
            check_redirect_robots=False,
            failure_level=logging.INFO,
            track_errors=False,
        )
        if isinstance(result.error, HTTPStatusError):
            return result.error.status, ""
        if result.error is not None:
            raise result.error
        assert result.status is not None and result.content is not None
        return result.status, result.content

    async def _download_sitemap(self, url: str) -> bytes:
        """Fetcher for SitemapParser: a sitemap goes through robots.txt, the limits and retries as a page does."""
        # Not decoded: a sitemap may be gzipped. Whoever asked for the sitemap logs the failure.
        result = await self._fetch(url, raw=True, failure_level=logging.INFO, track_errors=False)
        if result.error is not None:
            raise result.error
        assert result.body is not None
        return result.body

    async def _fetch_once(
        self, url: str, *, html_only: bool, raw: bool, truncate_at: int | None, timeout: aiohttp.ClientTimeout
    ) -> FetchResult:
        """Make one request, unless the circuit breaker of the host refuses it."""
        host = get_host(url)
        call = self.circuit_breaker.call(url)

        @contextlib.asynccontextmanager
        async def gate() -> AsyncGenerator[None, None]:
            # Asked again after the wait for the rate limit, as the circuit
            # may have opened meanwhile; a refused request does not wait for
            # a slot and does not count as sent.
            call.admit()
            async with self._limits.slot(url):
                # And once more with the slot: the request that held it
                # before may have opened the circuit.
                call.admit()
                yield

        try:
            # Admitted before the wait, so that of the requests to a
            # half-open circuit only the probe waits for its turn.
            with call:
                # The rate limit is waited for before taking a concurrency slot,
                # so a request waiting for its host does not hold a slot another
                # host could use; inside the slot the interval is checked once more.
                async with gate() if host is None else self.rate_limiter.slot(host, gate):
                    result = await self._send(
                        url, html_only=html_only, raw=raw, truncate_at=truncate_at, timeout=timeout
                    )
                    call.record(result.error)
                    return result
        except CircuitOpenError as error:
            return FetchResult.failure(url, error, 0.0)

    async def _send(
        self, url: str, *, html_only: bool, raw: bool, truncate_at: int | None, timeout: aiohttp.ClientTimeout
    ) -> FetchResult:
        """Send the request and report its outcome, whatever it is, as a FetchResult."""
        logger.info("Fetching %s", url)
        started = time.perf_counter()
        try:
            response = await self._request(url, html_only=html_only, raw=raw, truncate_at=truncate_at, timeout=timeout)
        except FetchError as error:
            elapsed = time.perf_counter() - started
            # RetryStrategy logs the failure along with what comes next.
            logger.debug("Request to %s failed after %.2fs: %s: %s", url, elapsed, type(error).__name__, error.message)
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
            body=response.body,
        )

    async def _parse(self, result: FetchResult) -> ParsedPage:
        """Parse a successful fetch result; a failure counts in `error_stats()`."""
        assert result.content is not None
        try:
            return await self._parser.parse_html(
                result.content,
                result.url,
                final_url=result.final_url,
                content_type=result.content_type,
            )
        except ParseError as error:
            self._errors.record_error(error)
            raise

    async def crawl(
        self,
        start_urls: Iterable[str],
        max_pages: int = 100,
        *,
        same_domain_only: bool = False,
        include_patterns: Iterable[str] = (),
        exclude_patterns: Iterable[str] = (),
        exclude_extensions: Iterable[str] = (),
        sitemap_urls: Iterable[str] = (),
        robots_sitemaps: bool = False,
    ) -> dict[str, ParsedPage]:
        """Crawl from the start URLs following links; return pages by normalized URL.

        With `keep_pages=False` the pages are not kept and the result is empty.

        Pages are fetched by `max_concurrent` workers, breadth-first: a link
        found on a page at depth d gets depth d + 1 and is followed only up
        to `max_depth`. Every URL is fetched at most once. `max_pages` caps
        the number of pages requested, failed ones included. Pages that
        robots.txt disallows are not requested: they are listed in
        `blocked_urls` and do not count toward `max_pages`. Neither do pages
        of sites whose robots.txt cannot be read: they are listed in
        `unreachable_urls`. Pages that the circuit breaker refuses do not
        count either: they are put off until their host may be probed and
        tried again, and the crawl goes on with other pages meanwhile. Once
        the circuit of a host has opened `MAX_CIRCUIT_OPENINGS` times in
        the crawl, its refused pages go to `failed_urls` with
        `CircuitOpenError`, so a host that stays down holds the crawl for
        about two cooldowns of the breaker.

        Filters apply to discovered links, not to the start URLs:
        `same_domain_only` keeps links on the hosts of the start URLs (and of
        the pages they redirect to); `include_patterns` and `exclude_patterns`
        are regular expressions and `exclude_extensions` file extensions
        such as "pdf", see `UrlFilter`. A link that passes the
        filters but redirects to a URL that does not is skipped, and so is a
        page that redirects to a URL already seen: the target is not
        requested, the page is not returned, and it is listed in
        `skipped_urls` with the reason. A page that redirects to a URL
        robots.txt disallows is listed in `blocked_urls`; having been
        requested, it counts toward `max_pages`. A page whose Content-Type
        is not HTML is skipped too, with its body left undownloaded; it counts
        toward `max_pages` as well.

        The pages listed in the sitemaps `sitemap_urls` are crawled too, and
        with `robots_sitemaps` so are those of the sitemaps that robots.txt
        of the start URLs' sites names. The sitemaps are read before the
        first page is fetched (see `SitemapParser`). A page a sitemap lists
        has depth 0, like a start URL, but must pass the filters, like a
        link; it comes after the start URLs and before the links.
        `same_domain_only` keeps the hosts of `sitemap_urls` as well as
        those of the start URLs and of the pages they redirect to. A sitemap
        that cannot be read does not stop the crawl: it is logged and listed
        in `failed_sitemaps`.

        Failed pages do not stop the crawl: they are listed in `failed_urls`.
        Every processed page is saved to the `storage` of the crawler, if it
        has one, and the storage is flushed before the crawl returns. A page
        that cannot be saved is still returned: the failure is logged and
        counted in `crawl_stats()`.
        The state of the crawl (`processed_urls`, `visited_urls`,
        `failed_urls`, `skipped_urls`, `blocked_urls`, `unreachable_urls`, `failed_sitemaps`, `url_depths`,
        `stats`, `crawl_stats()`, `error_stats()`, the counters of `circuit_breaker.get_stats()`) is reset
        on every call and stays available after it returns. The rate limits, the robots.txt
        cache and the states of the circuit breaker carry over.

        Raises:
            TypeError: a single string is passed instead of a list of URLs, patterns or extensions.
            ValueError: `max_pages` is not positive, a start URL, a sitemap URL, a pattern or an extension
                is invalid, `robots_sitemaps` is asked of a crawler that does not read robots.txt.
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
        if isinstance(sitemap_urls, str):
            raise TypeError(f"expected a list of sitemap URLs, got a string: {sitemap_urls!r}")
        sitemap_urls = list(sitemap_urls)
        invalid = [url for url in sitemap_urls if not is_valid_http_url(url)]
        if invalid:
            raise ValueError(f"invalid sitemap URLs: {', '.join(map(repr, invalid))}")
        if robots_sitemaps and self.robots is None:
            raise ValueError("robots_sitemaps needs a crawler that reads robots.txt (respect_robots=True)")
        if self._crawl_started is not None and self._crawl_finished is None:
            raise RuntimeError("a crawl is already running on this crawler")

        url_filter = UrlFilter(
            allowed_hosts={get_host(url) for url in start_urls + sitemap_urls} if same_domain_only else None,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            exclude_extensions=exclude_extensions,
        )
        self._queue = CrawlerQueue()
        self.processed_urls = {}
        self._failed_sitemaps = {}
        self._sitemap_pages_out_of_scope = []
        self._redirect_sources = {}
        self._pages_requested = 0
        self._pages_to_save = 0
        self._written_before = self.storage.written if self.storage is not None else 0
        self._pending_before = self.storage.pending if self.storage is not None else 0
        self._retries = 0
        self._errors = ErrorTracker()
        self.rate_limiter.reset_stats()
        self.circuit_breaker.reset_stats()
        for url in start_urls:
            self._queue.add_url(url, priority=0, depth=0)
        self._start_urls = set(self._queue.depths)

        logger.info(
            "Crawl started: %d start URLs, max_depth=%d, max_pages=%d", len(start_urls), self.max_depth, max_pages
        )
        self._crawl_started, self._crawl_finished = time.perf_counter(), None
        self.stats.start()
        try:
            if sitemap_urls or robots_sitemaps:
                await self._queue_sitemap_pages(sitemap_urls, start_urls if robots_sitemaps else [], url_filter)
            async with asyncio.TaskGroup() as group:
                for _ in range(self.max_concurrent):
                    group.create_task(self._crawl_worker(self._queue, url_filter, max_pages))
            await self._flush_storage()
        finally:
            self._crawl_finished = time.perf_counter()
            self.stats.finish()
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
        if self.storage is not None:
            logger.info(
                "Saved %d pages to %s, %d not saved", stats.saved, type(self.storage).__name__, stats.save_failed
            )
        return self.processed_urls

    async def _queue_sitemap_pages(self, sitemap_urls: list[str], robots_of: list[str], url_filter: UrlFilter) -> None:
        """Queue the pages listed in `sitemap_urls` and in the sitemaps robots.txt of the sites of `robots_of` names."""
        async with asyncio.TaskGroup() as group:
            named = [group.create_task(self._sitemaps_in_robots(url)) for url in robots_of]
        # Normalized, so a sitemap given twice, or given and named in robots.txt, is read once.
        sitemaps = dict.fromkeys(normalize_url(url) for url in sitemap_urls)
        for task in named:
            sitemaps.update(dict.fromkeys(task.result()))
        async with asyncio.TaskGroup() as group:
            loads = [group.create_task(self._load_sitemap(url)) for url in sitemaps if url is not None]
        listed = queued = 0
        for load in loads:
            for page in load.result():
                listed += 1
                if not url_filter.allows(page):
                    # Hosts join the scope only under `same_domain_only`.
                    if url_filter.allowed_hosts is not None:
                        self._sitemap_pages_out_of_scope.append(page)
                elif self._queue.add_url(page, priority=0, depth=0):
                    queued += 1
        logger.info(
            "Sitemaps: %d read, %d failed, %d pages listed, %d new queued",
            len(loads) - len(self._failed_sitemaps),
            len(self._failed_sitemaps),
            listed,
            queued,
        )

    def _queue_sitemap_pages_in_scope(self, queue: CrawlerQueue, url_filter: UrlFilter) -> None:
        """Queue the sitemap pages that the filter let through once a start URL redirected to their host.

        The sitemaps are read before the first page, when only the hosts of
        the start URLs are known: the pages of "www.example.com" are out of
        scope until "example.com" redirects there.
        """
        out_of_scope = []
        for page in self._sitemap_pages_out_of_scope:
            if url_filter.allows(page):
                queue.add_url(page, priority=0, depth=0)
            else:
                out_of_scope.append(page)
        self._sitemap_pages_out_of_scope = out_of_scope

    async def _sitemaps_in_robots(self, url: str) -> list[str]:
        """The sitemaps that robots.txt of the site of `url` names; none if it cannot be read."""
        assert self.robots is not None
        try:
            return (await self.robots.fetch_robots(url))["sitemaps"]
        except (CrawlerClosedError, CircuitOpenError) as error:
            logger.warning("No sitemaps from robots.txt of %s: %s: %s", url, type(error).__name__, error.message)
            return []

    async def _load_sitemap(self, url: str) -> list[str]:
        """The pages a sitemap lists; a sitemap that cannot be read is logged and lists none."""
        try:
            return await self.sitemaps.fetch_sitemap(url)
        except FetchError as error:
            reason = f"{type(error).__name__}: {error.message}"
            logger.warning("Sitemap %s is left out: %s", url, reason)
            self._failed_sitemaps[url] = reason
            return []

    def crawl_stats(self) -> CrawlStats:
        """Progress of the running crawl, or the result of the latest one."""
        if self._crawl_started is None:
            return CrawlStats()
        stats = self._queue.get_stats()
        rate = self.rate_limiter.get_stats()
        saved = save_failed = 0
        if self.storage is not None:
            # Records left in the buffer by an earlier crawl are written
            # first and are not pages of this one.
            written = self.storage.written - self._written_before - self._pending_before
            saved = min(max(written, 0), self._pages_to_save)
            save_failed = self._pages_to_save - saved
            if self._crawl_finished is None:
                # Buffered pages are yet to be written.
                save_failed = max(save_failed - self.storage.pending, 0)
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
            saved=saved,
            save_failed=save_failed,
        )

    def error_stats(self) -> ErrorStats:
        """Errors of page requests since the latest crawl() started, or since the crawler was created."""
        return self._errors.get_stats()

    async def _crawl_worker(self, queue: CrawlerQueue, url_filter: UrlFilter, max_pages: int) -> None:
        while (url := await queue.get_next()) is not None:
            try:
                # robots.txt and the circuit breaker are checked before the
                # page counts toward max_pages: a refused page costs no request.
                refusal = await self._check_robots(url) or self._check_circuit(url) or self._check_probes_left(url)
                if refusal is not None:
                    if isinstance(refusal, RobotsDisallowedError):
                        queue.mark_blocked(url, refusal.message)
                    elif isinstance(refusal, RobotsUnreachableError):
                        queue.mark_unreachable(url, refusal.message)
                    elif isinstance(refusal, CircuitOpenError):
                        self._defer_or_fail(url, queue, refusal)
                    else:
                        self._fail_page(url, queue, refusal)
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
                self._fail_page(url, queue, UnexpectedError(url, f"{type(exc).__name__}: {exc}"))

    async def _crawl_page(self, url: str, queue: CrawlerQueue, url_filter: UrlFilter) -> None:
        depth = queue.depth(url)
        skip_reason: str | None = None

        def follow(target: str) -> bool:
            nonlocal skip_reason
            skip_reason = self._redirect_refusal(url, target, queue, url_filter)
            return skip_reason is None

        result = await self._fetch(url, html_only=True, check_robots=False, follow=follow)
        if isinstance(result.error, CircuitOpenError):
            # Not sent: the circuit of the host, or of the host a redirect
            # leads to, opened while the page waited for its turn.
            self._pages_requested -= 1
            self._defer_or_fail(url, queue, result.error)
            return
        # The worker has checked robots.txt for the page; these are about the target of its redirect.
        if isinstance(result.error, RobotsDisallowedError):
            queue.mark_blocked(url, f"redirects to {result.error.url}, {result.error.message}")
            return
        if isinstance(result.error, RobotsUnreachableError):
            queue.mark_unreachable(url, f"redirects to {result.error.url}, {result.error.message}")
            return
        if result.error is not None:
            self._fail_page(url, queue, result.error, result)
            return
        if skip_reason is None and not is_html_content_type(result.content_type):
            # A link without a file extension may still lead to a PDF or an
            # image. Not a failure: the page is fine, just not one to parse.
            skip_reason = f"not HTML: {result.content_type}"
        if skip_reason is not None:
            logger.info("Skipped %s: %s", url, skip_reason)
            queue.mark_skipped(url, skip_reason)
            self.stats.record_page(url, status=result.status, elapsed=result.elapsed, skipped=True)
            return

        try:
            page = await self._parse(result)
        except ParseError as error:
            logger.warning("Failed to parse %s: %s", url, error.message)
            self._fail_page(url, queue, error, result)
            return
        queued = 0
        if depth < self.max_depth:
            for link in page["links"]:
                if url_filter.allows(link) and queue.add_url(link, priority=depth + 1, depth=depth + 1):
                    queued += 1
        if self.keep_pages:
            self.processed_urls[url] = page
        queue.mark_processed(url)
        self.stats.record_page(url, status=result.status, elapsed=result.elapsed)
        logger.info("Crawled %s (depth %d): %d links, %d new queued", url, depth, len(page["links"]), queued)
        if self.storage is not None:
            await self._save_page(_page_record(result, page, depth))

    def _redirect_refusal(self, url: str, target: str, queue: CrawlerQueue, url_filter: UrlFilter) -> str | None:
        """Why the page `url` of the crawl must not follow its redirect to `target`; None if it may.

        Asked before the target is requested, so a redirect out of the crawl
        scope costs no request to another site.
        """
        if url in self._start_urls:
            # A start URL that redirects ("example.com" -> "www.example.com")
            # defines the site as much as the URL itself. A page from a
            # sitemap has depth 0 too, but is filtered like a link.
            url_filter.allow_host_of(target)
            self._queue_sitemap_pages_in_scope(queue, url_filter)
        elif not url_filter.allows(target):
            # A link inside the crawl scope can lead out of it, e.g. to a
            # sign-in page on another domain. Such a page is not part of the site.
            return f"redirected out of scope: {target}"
        # A page is crawled under one URL: a later link to the target is not
        # fetched, and a redirect to a page already seen is not followed.
        # The redirects of one page may lead back to it (a cookie check) or
        # loop; they are followed up to MAX_REDIRECTS.
        if target != url and self._redirect_sources.get(target) != url:
            if queue.is_seen(target):
                return f"redirected to a page already seen: {target}"
            queue.mark_seen(target)
            self._redirect_sources[target] = url
        return None

    def _fail_page(self, url: str, queue: CrawlerQueue, error: FetchError, result: FetchResult | None = None) -> None:
        """Finish a page of the crawl as failed; `result` is that of its request, None if none was sent."""
        queue.mark_failed(url, f"{type(error).__name__}: {error.message}")
        self.stats.record_page(
            url,
            status=None if result is None else result.status,
            elapsed=None if result is None else result.elapsed,
            error=type(error).__name__,
        )

    async def _save_page(self, record: PageRecord) -> None:
        """Hand a page to the storage; a failure is logged and does not stop the crawl."""
        assert self.storage is not None
        self._pages_to_save += 1
        try:
            await self.storage.save(record)
        except StorageError as error:
            logger.error("Failed to save %s: %s", record["url"], error)
        except Exception:
            # Not a failed write, which the storage reports as StorageError:
            # a bug in the storage must not stop the crawl either.
            logger.exception("Unexpected error while saving %s", record["url"])

    async def _flush_storage(self) -> None:
        """Write out the pages the storage still buffers; a failure is logged."""
        if self.storage is None:
            return
        try:
            await self.storage.flush()
        except StorageError as error:
            logger.error("Failed to save the last pages of the crawl: %s", error)
        except Exception:
            logger.exception("Unexpected error while saving the last pages of the crawl")

    def _check_probes_left(self, url: str) -> CircuitOpenError | None:
        """In a crawl, a host whose circuit has opened `MAX_CIRCUIT_OPENINGS` times gets no more probes."""
        host = get_host(url)
        if host is None or self.circuit_breaker.state(host) is CircuitState.CLOSED:
            return None
        opened = self.circuit_breaker.times_opened(host)
        if opened < self.MAX_CIRCUIT_OPENINGS:
            return None
        return CircuitOpenError(url, f"circuit breaker of {host} opened {opened} times, no more probes in this crawl")

    def _defer_or_fail(self, url: str, queue: CrawlerQueue, refusal: CircuitOpenError) -> None:
        """Put off a page the circuit breaker refused until its host may be probed, or give up on it.

        The host is that of the refusal: the page may redirect to another one.
        """
        host = get_host(refusal.url)
        assert host is not None  # a URL without a host has no circuit
        opened = self.circuit_breaker.times_opened(host)
        if opened >= self.MAX_CIRCUIT_OPENINGS:
            logger.info("Gave up on %s: circuit breaker of %s opened %d times", url, host, opened)
            self._fail_page(url, queue, refusal)
            return
        # Back when the probe may go; a page refused while the probe is in
        # flight comes back a second later.
        delay = self.circuit_breaker.probe_in(refusal.url) or 1.0
        logger.info("Deferred %s for %.1fs: %s", url, delay, refusal.message)
        queue.defer(url, delay, priority=queue.depth(url))

    async def close(self) -> None:
        """Close the underlying session and the storage. Safe to call more than once.

        A storage that cannot write its last pages is closed all the same;
        the failure is logged.
        """
        self._closed = True
        if self._session is not None:
            await self._session.close()
            self._session = None
            logger.debug("HTTP session closed")
        if self.storage is not None:
            try:
                await self.storage.close()
            except StorageError as error:
                logger.error("Failed to close %s: %s", type(self.storage).__name__, error)
            except Exception:
                logger.exception("Unexpected error while closing %s", type(self.storage).__name__)

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

    async def _request(
        self, url: str, *, html_only: bool, raw: bool, truncate_at: int | None, timeout: aiohttp.ClientTimeout
    ) -> _Response:
        """Perform the GET request and read the body up to its size limit.

        With `html_only`, the body of a response whose Content-Type is not
        HTML is not read: the content is empty and the size is 0. With
        `raw`, the body is returned as bytes and the content is empty; over
        the size limit of a sitemap it fails with `SitemapError`. With
        `truncate_at`, the body is cut to that many bytes. Otherwise a body
        over `max_page_size` fails with `PageTooLargeError`.
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
            # Redirects are followed by _fetch(), one request at a time, so
            # that each one is checked as a link to its target would be.
            async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as response:
                response.raise_for_status()
                # aiohttp reports "application/octet-stream" when the header
                # is missing; None lets callers tell the two cases apart.
                content_type = response.content_type if aiohttp.hdrs.CONTENT_TYPE in response.headers else None
                location = response.headers.get(aiohttp.hdrs.LOCATION)
                if response.status in self.REDIRECT_STATUSES and location is not None:
                    # The body of a redirect is not wanted.
                    return _Response(
                        status=response.status,
                        content="",
                        size=0,
                        final_url=location,
                        content_type=content_type,
                        redirected=True,
                    )
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
                body = await self._read_body(response, url, raw=raw, truncate_at=truncate_at)
                return _Response(
                    status=response.status,
                    content="" if raw else _decode(body, _encoding(response, body)),
                    size=len(body),
                    final_url=str(response.url),
                    content_type=content_type,
                    body=body if raw else None,
                )
        # Order matters: aiohttp's ServerTimeoutError is both a ClientError and
        # a TimeoutError; InvalidURL and the certificate error are ClientErrors too.
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

    async def _read_body(
        self, response: aiohttp.ClientResponse, url: str, *, raw: bool, truncate_at: int | None
    ) -> bytes:
        """Read the body, giving up once it is over its size limit; the rest is not downloaded.

        A response sent with Content-Encoding: gzip is unpacked as it is
        read, so a few hundred kilobytes may turn into gigabytes: the limit
        is on the unpacked body.
        """
        limit = truncate_at or (self.sitemaps.MAX_SIZE if raw else self.max_page_size)
        if limit is None:
            return await response.read()
        too_large = SitemapError if raw else PageTooLargeError
        # Content-Length counts the packed bytes, never more than the unpacked ones.
        if truncate_at is None and response.content_length is not None and response.content_length > limit:
            raise too_large(url, f"larger than {limit} bytes")
        chunks, size = [], 0
        async for chunk in response.content.iter_chunked(64 * 1024):
            if size + len(chunk) > limit:
                if truncate_at is None:
                    raise too_large(url, f"larger than {limit} bytes")
                chunks.append(chunk[: limit - size])
                logger.info("Cut the body of %s to %d bytes", url, limit)
                break
            chunks.append(chunk)
            size += len(chunk)
        return b"".join(chunks)


def _page_record(result: FetchResult, page: ParsedPage, depth: int) -> PageRecord:
    """A crawled page as a storage keeps it: the parsed page with the facts of its response."""
    assert result.status is not None
    # The title has a field of its own.
    metadata = {name: value for name, value in page["metadata"].items() if name != "title"}
    return PageRecord(
        url=page["url"],
        title=page["title"] or "",
        text=page["text"],
        links=page["links"],
        metadata={**metadata, "final_url": page["final_url"], "depth": depth},
        crawled_at=datetime.now(UTC),
        status_code=result.status,
        content_type=result.content_type or "",
    )


def _describe_timeout(exc: TimeoutError, timeout: aiohttp.ClientTimeout) -> str:
    # aiohttp raises its own subclasses for the connect and read timeouts
    # and a plain TimeoutError for the total one.
    if isinstance(exc, aiohttp.ConnectionTimeoutError):
        return f"connect timeout ({timeout.connect:.1f}s)"
    if isinstance(exc, aiohttp.SocketTimeoutError):
        return f"read timeout ({timeout.sock_read:.1f}s)"
    return f"total timeout ({timeout.total:.1f}s)"


def _encoding(response: aiohttp.ClientResponse, body: bytes) -> str:
    """The charset of the Content-Type header if Python knows it, else the one the markup declares.

    `response.get_encoding()` does the same with a resolver, but only for a
    body read whole with `read()`, not in chunks.
    """
    if response.charset:
        with contextlib.suppress(LookupError, ValueError):
            return codecs.lookup(response.charset).name
    return _sniff_charset(response, body)


def _sniff_charset(response: aiohttp.ClientResponse, body: bytes) -> str:
    """Pick an encoding when the Content-Type header has no charset.

    Without one, UTF-8 would be assumed, but many pages declare their
    encoding in the markup instead:
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
