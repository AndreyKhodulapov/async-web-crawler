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
from collections import Counter
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import NamedTuple, Self
from urllib.parse import urlsplit

import aiohttp
import certifi
from bs4.dammit import EncodingDetector

from crawler.circuit_breaker import BreakerCall, CircuitBreaker, CircuitState
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
from crawler.queue import CrawlerQueue, queue_form
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryStrategy, parse_retry_after
from crawler.robots import RobotsParser, product_token, robots_tag_directives
from crawler.semaphores import SemaphoreManager
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage.base import DataStorage
from crawler.urls import get_host, is_valid_http_url, normalize_url, resolve_url, strip_tracking_params

logger = logging.getLogger(__name__)


class _Response(NamedTuple):
    status: int
    content: str
    size: int
    final_url: str
    content_type: str | None
    redirected: bool = False  # a redirect, not followed; `final_url` is its Location header
    body: bytes | None = None  # the bytes as sent, when asked for instead of the text
    robots_tag: tuple[str, ...] = ()  # the X-Robots-Tag headers as sent


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
      site first; `crawl()` also honors `noindex` and `nofollow` of pages
      and links. A disallowed URL is not requested and fails with
      `RobotsDisallowedError`. While robots.txt of a site cannot be read,
      its URLs are not requested either and fail with
      `RobotsUnreachableError` (see `RobotsParser`); in `crawl()`, they
      wait for it to be downloaded again, and so do the pages that
      redirect to them and the sitemaps of the site.
    - Redirects are followed one request at a time, up to
      `MAX_REDIRECTS`: the target of each goes through robots.txt, the
      rate limit, the retries and the circuit breaker of its own host.
    - Failed requests are retried as `retry_strategy` says: by default
      transient and network errors, such as a timeout or HTTP 503, up to 3
      times with exponential backoff (see `RetryStrategy`). After HTTP 429,
      a Retry-After header or a timeout, which say the whole site is
      overloaded, the pause before the retry is spent in the rate limiter:
      the whole host waits with it; so it does for as long as a Retry-After
      header asks, up to `max_retry_after` seconds, even when the request
      is not retried. After any other failure (HTTP 500, a reset
      connection) only the failed request waits: the other pages of the
      host are fetched meanwhile. In `crawl()`, a page whose Retry-After
      was too long to retry comes back once the host may be asked again.
    - A host whose requests keep failing is left alone for a while, as
      `circuit_breaker` says: by default once half of at least 5 requests
      in a minute have failed with a transient or network error, its
      requests fail with `CircuitOpenError` for 30 seconds without being
      sent (see `CircuitBreaker`). A request counts once in the window of
      the breaker, however many attempts it took: a page made good by a
      retry is a success of the host. A retry the breaker would refuse is
      not made: the request fails with the error of its last attempt.
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
    The queue of `crawl()` is bounded by `max_pages` too, see `crawl()`.

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
    # The longest Retry-After a host is held back for, in seconds.
    DEFAULT_MAX_RETRY_AFTER = 600.0
    # In crawl(), a page whose host is held back longer than this is put off.
    MIN_PENALTY_TO_DEFER = 1.0
    # In crawl(), a page waits at most this many times for the robots.txt of
    # its site to be downloaded again, or for a Retry-After too long to retry
    # it, before it is given up.
    MAX_WAITS_PER_PAGE = 3
    # In crawl(), longer links are not followed: they are mostly generated ones.
    MAX_URL_LENGTH = 2048
    # In crawl(), new links are not queued once the pages queued, in progress
    # and requested reach this many times max_pages: most would never be fetched.
    # Likewise a host has at most this many times max_pages_per_host queued.
    FRONTIER_FACTOR = 3

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
        max_retry_after: float = DEFAULT_MAX_RETRY_AFTER,
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
        if max_retry_after <= 0:
            raise ValueError(f"max_retry_after must be positive, got {max_retry_after}")
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
        self.max_retry_after = max_retry_after
        self._user_agent = user_agent
        self._rotated_agents = itertools.cycle(user_agents) if user_agents else None
        # Links marked rel="nofollow" are left out of the pages, as robots.txt is followed;
        # a robots meta tag may name the crawler ("asyncwebcrawler"), as X-Robots-Tag may.
        self._parser = parser or HTMLParser(skip_nofollow=respect_robots, robots_name=robots_name)
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
        self._host_pages: Counter[str] = Counter()  # pages requested by host
        self._over_host_limit = 0  # pages skipped without a request over max_pages_per_host
        self._hosts_warned_held_back: set[str] = set()  # hosts whose long Retry-After was logged by the crawl
        self._page_waits: Counter[str] = Counter()  # times a page waited for robots.txt or a long Retry-After
        self._max_frontier = 0  # pages queued, in progress and requested that crawl() allows
        self._links_dropped = 0
        self._host_queued: Counter[str] = Counter()  # pages ever queued by host
        self._max_host_queued: int | None = None  # pages queued by host that crawl() allows
        self._links_dropped_by_host = 0
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
        """URL -> reason for pages the latest crawl left out, e.g. not HTML. Do not modify.

        All of them were fetched, except those over `max_pages_per_host`.
        """
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
        for redirects in itertools.count():
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
            if redirects == self.MAX_REDIRECTS:
                # Checked before the target is asked about: it is not reached.
                error = TooManyRedirectsError(url, f"too many redirects (more than {self.MAX_REDIRECTS})")
                result = FetchResult.failure(url, error, elapsed)
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
        # Shared by the attempts: the request counts once in the breaker's window.
        call = self.circuit_breaker.call(url)

        async def attempt() -> FetchResult:
            nonlocal last, attempts, failed_at
            timeout = self._timeout_for(retries=attempts)
            attempts += 1
            result = await self._fetch_once(
                url, html_only=html_only, raw=raw, truncate_at=truncate_at, timeout=timeout, call=call
            )
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
            # as long as it asked, even when this one is not retried.
            if isinstance(error, HTTPStatusError) and error.retry_after and host is not None:
                if error.retry_after > self.max_retry_after:
                    logger.warning(
                        "%s asked to wait %gs (Retry-After), waiting %gs", host, error.retry_after, self.max_retry_after
                    )
                self.rate_limiter.penalize(host, min(error.retry_after, self.max_retry_after))
            return last, attempts
        return result, attempts

    async def _wait_before_retry(self, error: Exception, delay: float) -> None:
        """Wait `delay` seconds before the retry; a failure that speaks for the whole host holds the host back.

        HTTP 429, a Retry-After header or a timeout usually means the whole
        site is overloaded: the pause is spent in the rate limiter, so that
        the retry and every other request to the host wait for it. Any
        other failure (HTTP 500, a reset connection) is taken to be about
        the one page: only this request sleeps, and the host is asked for
        its other pages meanwhile.
        """
        assert isinstance(error, FetchError)  # _fetch_once() reports every failure as one
        if not _signals_overload(error):
            await asyncio.sleep(delay)
            return
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
        self,
        url: str,
        *,
        html_only: bool,
        raw: bool,
        truncate_at: int | None,
        timeout: aiohttp.ClientTimeout,
        call: BreakerCall,
    ) -> FetchResult:
        """Make one request under the breaker `call` of `url`, unless the circuit breaker of the host refuses it."""
        host = get_host(url)

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
            robots_tag=robots_tag_directives(response.robots_tag, self._user_agent),
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
        max_pages_per_host: int | None = None,
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
        of sites whose robots.txt cannot be read: they are put off until it
        is downloaded again (see `RobotsParser.UNREACHABLE_TTL`), at most
        `MAX_WAITS_PER_PAGE` times, then listed in `unreachable_urls`; a
        site whose robots.txt failed for a moment is crawled once it is
        back. So does a page that redirects to such a site: it is requested
        again when it comes back, uncounted meanwhile. Pages that the circuit breaker refuses do not
        count either: they are put off until their host may be probed and
        tried again, and the crawl goes on with other pages meanwhile. Once
        the circuit of a host has opened `MAX_CIRCUIT_OPENINGS` times in
        the crawl, its refused pages go to `failed_urls` with
        `CircuitOpenError`, so a host that stays down holds the crawl for
        about two cooldowns of the breaker. A page whose host is held back
        for longer than `MIN_PENALTY_TO_DEFER` seconds, by a Retry-After or
        the pause before the retry of a request that found the host
        overloaded (HTTP 429, a timeout), is put off until the host may be
        asked again, without counting toward `max_pages` before then. A
        Retry-After longer than `max_delay` of the retry strategy is
        logged as a warning once per host, as the crawl may be quiet for
        that long; the page that got it, which the request did not retry,
        comes back with the host too, at most `MAX_WAITS_PER_PAGE` times,
        then goes to `failed_urls`.

        With `respect_robots`, the links of a page whose <meta name="robots">
        (or <meta> with the robots.txt name of the crawler, "asyncwebcrawler")
        or X-Robots-Tag says "nofollow" are not followed, and neither are
        links marked rel="nofollow". A page that says "noindex" is not
        returned or saved but listed in `skipped_urls`; its links are
        followed. "none" means both.

        Filters apply to discovered links, not to the start URLs:
        `same_domain_only` keeps links on the hosts of the start URLs (and of
        the pages they redirect to) and on their subdomains, "www." and the
        apex being one host; `include_patterns` and `exclude_patterns`
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

        Against endless URL spaces (sort orders, filters, session IDs):
        tracking parameters such as "utm_source" are dropped from every URL
        queued (see `CrawlerQueue`); a link longer than `MAX_URL_LENGTH` is
        not followed; a page whose canonical URL differs from its own in the
        query alone and has been seen already is skipped as a duplicate, and
        its links are not followed. `max_pages_per_host` caps the pages
        requested from one host; the others are skipped without a request
        and do not count toward `max_pages`.

        The queue is bounded, so that the memory does not grow with every
        link of a large site: once the pages queued, in progress and
        requested reach `FRONTIER_FACTOR` times `max_pages`, new links and
        sitemap pages are not queued (nor remembered: a page found again
        later may be queued then). The spare room is for pages that do not
        count toward `max_pages`, such as those robots.txt disallows; a
        crawl whose queue is mostly such pages may end before `max_pages`.
        With `max_pages_per_host`, a host has at most `FRONTIER_FACTOR`
        times that many pages queued in the whole crawl, so that one large
        site does not fill the queue with pages it will skip.

        The pages listed in the sitemaps `sitemap_urls` are crawled too, and
        with `robots_sitemaps` so are those of the sitemaps that robots.txt
        of the start URLs' sites names. The sitemaps are read before the
        first page is fetched (see `SitemapParser`). A page a sitemap lists
        has depth 0, like a start URL, but must pass the filters, like a
        link; it comes after the start URLs and before the links.
        `same_domain_only` keeps the hosts of `sitemap_urls` as well as
        those of the start URLs and of the pages they redirect to. A sitemap
        that cannot be read does not stop the crawl: it is logged and listed
        in `failed_sitemaps`. A sitemap of a site whose robots.txt cannot
        be read waits for it as a page does, up to `MAX_WAITS_PER_PAGE`
        times, before the first page is fetched; so do the sitemaps named
        in such a robots.txt.

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
            ValueError: `max_pages` or `max_pages_per_host` is not positive, a start URL, a sitemap URL,
                a pattern or an extension is invalid, `robots_sitemaps` is asked of a crawler that does not
                read robots.txt.
            RuntimeError: another crawl is running on this crawler.
        """
        if isinstance(start_urls, str):
            raise TypeError(f"expected a list of start URLs, got a string: {start_urls!r}")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1, got {max_pages}")
        if max_pages_per_host is not None and max_pages_per_host < 1:
            raise ValueError(f"max_pages_per_host must be >= 1 or None, got {max_pages_per_host}")
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
            max_url_length=self.MAX_URL_LENGTH,
        )
        self._queue = CrawlerQueue()
        self.processed_urls = {}
        self._failed_sitemaps = {}
        self._sitemap_pages_out_of_scope = []
        self._redirect_sources = {}
        self._pages_requested = 0
        self._host_pages = Counter()
        self._over_host_limit = 0
        self._hosts_warned_held_back = set()
        self._page_waits = Counter()
        self._max_frontier = self.FRONTIER_FACTOR * max_pages
        self._links_dropped = 0
        self._max_host_queued = None if max_pages_per_host is None else self.FRONTIER_FACTOR * max_pages_per_host
        self._links_dropped_by_host = 0
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
        self._host_queued = Counter(get_host(url) for url in self._start_urls)

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
                    group.create_task(self._crawl_worker(self._queue, url_filter, max_pages, max_pages_per_host))
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
        if self._links_dropped:
            logger.info("%d links were not queued: the queue was full", self._links_dropped)
        if self._links_dropped_by_host:
            logger.info(
                "%d links were not queued: their host had %d x max_pages_per_host pages queued",
                self._links_dropped_by_host,
                self.FRONTIER_FACTOR,
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
                elif self._queue_found(self._queue, page, depth=0):
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
        the start URLs are known: the pages of "example.com" are out of
        scope until "example.org" redirects there.
        """
        out_of_scope = []
        for page in self._sitemap_pages_out_of_scope:
            if url_filter.allows(page):
                self._queue_found(queue, page, depth=0)
            else:
                out_of_scope.append(page)
        self._sitemap_pages_out_of_scope = out_of_scope

    def _queue_found(self, queue: CrawlerQueue, url: str, *, depth: int) -> bool:
        """Queue a link or a sitemap page unless the queue, or the share of its host, is full; True if it was queued.

        Its priority is its depth: breadth-first.
        """
        host = get_host(url)
        if self._max_host_queued is not None and self._host_queued[host] >= self._max_host_queued:
            if not queue.closed and not queue.is_seen(url):
                self._links_dropped_by_host += 1
            return False
        if not queue.closed and queue.unfinished + self._pages_requested >= self._max_frontier:
            if not queue.is_seen(url):
                if not self._links_dropped:
                    logger.info(
                        "Queue is full: pages queued, in progress and requested reached %d (%d x max_pages); "
                        "new links are not queued until it has room",
                        self._max_frontier,
                        self.FRONTIER_FACTOR,
                    )
                self._links_dropped += 1
            return False
        if not queue.add_url(url, priority=depth, depth=depth):
            return False
        self._host_queued[host] += 1
        if self._host_queued[host] == self._max_host_queued:
            logger.info(
                "Host %s has %d pages queued (%d x max_pages_per_host): its new links are not queued",
                host,
                self._max_host_queued,
                self.FRONTIER_FACTOR,
            )
        return True

    async def _sitemaps_in_robots(self, url: str) -> list[str]:
        """The sitemaps that robots.txt of the site of `url` names; none if it cannot be read.

        While it is unreachable, it is waited for and downloaded again, up
        to `MAX_WAITS_PER_PAGE` times, as the pages of the site wait for it.
        """
        assert self.robots is not None
        waits = 0
        try:
            while True:
                rules = await self.robots.fetch_robots(url)
                if rules["unreachable"] is None:
                    return rules["sitemaps"]
                reason = f"robots.txt is unreachable ({rules['unreachable']})"
                if waits >= self.MAX_WAITS_PER_PAGE:
                    break
                waits += 1
                delay = self._robots_back_in(url)
                logger.info("Sitemaps of %s wait %.1fs: %s", url, delay, reason)
                await asyncio.sleep(delay)
        except (CrawlerClosedError, CircuitOpenError) as error:
            reason = f"{type(error).__name__}: {error.message}"
        logger.warning("No sitemaps from robots.txt of %s: %s", url, reason)
        return []

    async def _load_sitemap(self, url: str) -> list[str]:
        """The pages a sitemap lists; a sitemap that cannot be read is logged and lists none.

        While robots.txt of its site is unreachable, the sitemap waits for
        it to be downloaded again, up to `MAX_WAITS_PER_PAGE` times, as a
        page of the crawl does: a crawl fed by sitemaps alone would
        otherwise end empty after a 503 of a few seconds.
        """
        waits = 0
        while True:
            try:
                return await self.sitemaps.fetch_sitemap(url)
            except FetchError as error:
                if not isinstance(error, RobotsUnreachableError) or waits >= self.MAX_WAITS_PER_PAGE:
                    reason = f"{type(error).__name__}: {error.message}"
                    logger.warning("Sitemap %s is left out: %s", url, reason)
                    self._failed_sitemaps[url] = reason
                    return []
                waits += 1
                delay = self._robots_back_in(error.url)
                logger.info("Sitemap %s waits %.1fs: %s", url, delay, error.message)
                await asyncio.sleep(delay)

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
            over_host_limit=self._over_host_limit,
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

    async def _crawl_worker(
        self, queue: CrawlerQueue, url_filter: UrlFilter, max_pages: int, max_pages_per_host: int | None
    ) -> None:
        while (url := await queue.get_next()) is not None:
            try:
                # A host that asked to wait (Retry-After) or waits out the
                # pause before a retry: the worker takes pages of other hosts
                # meanwhile instead of waiting in the rate limiter.
                if (penalty := self._penalty_left(url)) > self.MIN_PENALTY_TO_DEFER:
                    self._warn_once_held_back(url, penalty)
                    logger.info("Deferred %s for %.1fs: its host is held back", url, penalty)
                    queue.defer(url, penalty, priority=queue.depth(url))
                    continue
                # robots.txt and the circuit breaker are checked before the
                # page counts toward max_pages: a refused page costs no request.
                refusal = await self._check_robots(url) or self._check_circuit(url) or self._check_probes_left(url)
                if refusal is not None:
                    if isinstance(refusal, RobotsDisallowedError):
                        queue.mark_blocked(url, refusal.message)
                    elif isinstance(refusal, RobotsUnreachableError):
                        if not self._wait_for_robots(url, queue, refusal, requested=False):
                            logger.info("Gave up on %s: %s", url, refusal.message)
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
                host = get_host(url)
                assert host is not None  # the queue holds valid URLs only
                if max_pages_per_host is not None and self._host_pages[host] >= max_pages_per_host:
                    # Not requested, so not counted toward max_pages.
                    self._over_host_limit += 1
                    self._skip_page(url, queue, f"max_pages_per_host reached: {max_pages_per_host} pages of {host}")
                    continue
                self._pages_requested += 1
                self._host_pages[host] += 1
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
        sent = False  # a request of the page got an answer: a redirect
        targets: list[str] = []  # the redirects followed, remembered as seen for this page

        def follow(target: str) -> bool:
            nonlocal skip_reason, sent
            sent = True
            skip_reason = self._redirect_refusal(url, target, queue, url_filter)
            if skip_reason is None:
                targets.append(target)
            return skip_reason is None

        result = await self._fetch(url, html_only=True, check_robots=False, follow=follow)
        if isinstance(result.error, CircuitOpenError):
            # The circuit of the host, or of the host a redirect leads to,
            # opened while the request waited for its turn.
            if not sent:
                # Nothing was sent: the page costs nothing of the limits.
                self._uncount_page(url, queue)
            if not self._defer_or_fail(url, queue, result.error):
                self._forget_redirects(url, targets, queue)
            return
        if self._outwaits_retries(result.error) and self._wait_for_host(url, queue, result.error):
            return
        # The worker has checked robots.txt for the page; these are about the target of its redirect.
        if isinstance(result.error, RobotsUnreachableError) and self._wait_for_robots(
            url, queue, result.error, requested=True
        ):
            return
        if result.error is not None:
            # The page is not crawled: its redirect targets are no longer
            # seen, so that a direct link to one of them is still followed.
            self._forget_redirects(url, targets, queue)
        if isinstance(result.error, RobotsDisallowedError):
            queue.mark_blocked(url, f"redirects to {result.error.url}, {result.error.message}")
            return
        if isinstance(result.error, RobotsUnreachableError):
            reason = f"redirects to {result.error.url}, {result.error.message}"
            logger.info("Gave up on %s: %s", url, reason)
            queue.mark_unreachable(url, reason)
            return
        if result.error is not None:
            self._fail_page(url, queue, result.error, result)
            return
        if url in self._start_urls and result.redirected and skip_reason is None:
            # A start URL that redirects ("example.org" -> "example.com")
            # defines the site as much as the URL itself; the hosts the chain
            # only passed through (a consent page) do not. A page from a
            # sitemap has depth 0 too, but is filtered like a link.
            assert result.final_url is not None
            url_filter.allow_host_of(result.final_url)
            self._queue_sitemap_pages_in_scope(queue, url_filter)
        if skip_reason is None and not is_html_content_type(result.content_type):
            # A link without a file extension may still lead to a PDF or an
            # image. Not a failure: the page is fine, just not one to parse.
            skip_reason = f"not HTML: {result.content_type}"
        if skip_reason is not None:
            self._skip_page(url, queue, skip_reason, result)
            return

        try:
            page = await self._parse(result)
        except ParseError as error:
            logger.warning("Failed to parse %s: %s", url, error.message)
            self._fail_page(url, queue, error, result)
            return
        duplicate = self._duplicate_of(url, result.final_url, page, queue)
        if duplicate is not None:
            # A variant of a page already seen ("?sort=price" of "/list"):
            # its links are variants too, so they are not followed.
            self._skip_page(url, queue, f"duplicate of {duplicate}", result)
            return
        noindex, nofollow = self._robots_directives(result, page)
        queued = 0
        if depth < self.max_depth and not nofollow:
            for link in page["links"]:
                if url_filter.allows(link) and self._queue_found(queue, link, depth=depth + 1):
                    queued += 1
        if noindex is not None:
            # The site asks not to keep the page; its links may still be followed.
            self._skip_page(url, queue, noindex, result, links_queued=queued)
            return
        if self.keep_pages:
            self.processed_urls[url] = page
        queue.mark_processed(url)
        self.stats.record_page(url, status=result.status, elapsed=result.elapsed)
        logger.info("Crawled %s (depth %d): %d links, %d new queued", url, depth, len(page["links"]), queued)
        if self.storage is not None:
            await self._save_page(_page_record(result, page, depth))

    @staticmethod
    def _duplicate_of(url: str, final_url: str | None, page: ParsedPage, queue: CrawlerQueue) -> str | None:
        """The canonical URL of the page `url` if it is another page of the crawl it is a variant of; else None.

        `final_url` is where the page was served from after its redirects.
        Only a canonical URL that differs from it in the query alone counts:
        a page with "?sort=price" or "?sessionid=1" whose canonical URL is
        the plain one. A canonical URL elsewhere is not trusted, as a site
        that gets it wrong (every page pointing to the home page) would lose
        all its pages. The canonical page must be processed or still to be
        crawled: if it failed or was left out, the variant is all there is.
        URLs are compared in the form the queue keeps them in.
        """
        canonical = page["metadata"]["canonical"]
        target = None if canonical is None else queue_form(canonical)
        served = queue_form(final_url or url)
        if target is None or served is None or target in (url, served):
            # The page names itself, under its own URL or the one it was redirected to.
            return None
        target_parts, served_parts = urlsplit(target), urlsplit(served)
        if (target_parts.netloc, target_parts.path) != (served_parts.netloc, served_parts.path):
            return None
        return canonical if queue.is_pending_or_processed(target) else None

    def _robots_directives(self, result: FetchResult, page: ParsedPage) -> tuple[str | None, bool]:
        """Whether the page asks not to be kept, and why (None if it does not); whether not to follow its links.

        Read from X-Robots-Tag and the robots meta tags (<meta name="robots">
        and the one with the crawler's name), only while robots.txt is followed.
        """
        if self.robots is None:
            return None, False
        sources = (("X-Robots-Tag", result.robots_tag), ("a robots meta tag", page["metadata"]["robots"]))
        noindex = next((f"noindex in {name}" for name, found in sources if {"noindex", "none"} & set(found)), None)
        nofollow = any({"nofollow", "none"} & set(found) for _, found in sources)
        return noindex, nofollow

    def _redirect_refusal(self, url: str, target: str, queue: CrawlerQueue, url_filter: UrlFilter) -> str | None:
        """Why the page `url` of the crawl must not follow its redirect to `target`; None if it may.

        Asked before the target is requested, so a redirect out of the crawl
        scope costs no request to another site.
        """
        # A link inside the crawl scope can lead out of it, e.g. to a sign-in
        # page on another domain. Such a page is not part of the site. A start
        # URL may lead anywhere: the host it ends on joins the crawl scope
        # once the chain is over (see _crawl_page).
        if url not in self._start_urls and not url_filter.allows(target):
            return f"redirected out of scope: {target}"
        # A page is crawled under one URL: a later link to the target is not
        # fetched, and a redirect to a page already seen is not followed.
        # The redirects of one page may lead back to it (a cookie check) or
        # loop; they are followed up to MAX_REDIRECTS.
        # Compared without tracking parameters, as the queue keeps URLs.
        page = strip_tracking_params(target)
        if page != url and self._redirect_sources.get(page) != url:
            if queue.is_seen(page):
                return f"redirected to a page already seen: {target}"
            queue.mark_seen(page)
            self._redirect_sources[page] = url
        return None

    def _forget_redirects(self, url: str, targets: Iterable[str], queue: CrawlerQueue) -> None:
        """Let the redirect targets of the page `url` be queued again: the page failed, so they were not crawled."""
        for target in targets:
            page = strip_tracking_params(target)
            if self._redirect_sources.get(page) == url:
                del self._redirect_sources[page]
                queue.forget(page)

    def _fail_page(self, url: str, queue: CrawlerQueue, error: FetchError, result: FetchResult | None = None) -> None:
        """Finish a page of the crawl as failed; `result` is that of its request, None if none was sent."""
        queue.mark_failed(url, f"{type(error).__name__}: {error.message}")
        self.stats.record_page(
            url,
            status=None if result is None else result.status,
            elapsed=None if result is None else result.elapsed,
            error=type(error).__name__,
        )

    def _skip_page(
        self,
        url: str,
        queue: CrawlerQueue,
        reason: str,
        result: FetchResult | None = None,
        *,
        links_queued: int | None = None,
    ) -> None:
        """Finish a page of the crawl as skipped; `result` is that of its request, None if none was sent.

        `links_queued` is how many new links of the page were queued, for the log; None if they were not followed.
        """
        if links_queued is None:
            logger.info("Skipped %s: %s", url, reason)
        else:
            logger.info("Skipped %s: %s; %d new links queued", url, reason, links_queued)
        queue.mark_skipped(url, reason)
        self.stats.record_page(
            url,
            status=None if result is None else result.status,
            elapsed=None if result is None else result.elapsed,
            skipped=True,
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

    def _warn_once_held_back(self, url: str, penalty: float) -> None:
        """Tell the user, once per host and crawl, about a Retry-After that holds the host back for long.

        The pauses before retries are logged as they are taken and never
        exceed `max_delay` of the retry strategy; a longer penalty comes
        from a Retry-After header and would otherwise show only as a crawl
        that makes no requests.
        """
        host = get_host(url)
        if penalty <= self.retry_strategy.max_delay or host in self._hosts_warned_held_back:
            return
        assert host is not None  # the queue holds valid URLs only
        self._hosts_warned_held_back.add(host)
        logger.warning("%s asked to wait %.0fs (Retry-After); its pages are put off until then", host, penalty)

    def _penalty_left(self, url: str) -> float:
        """Seconds the host of `url` is still held back for, after Retry-After or before a retry."""
        host = get_host(url)
        return 0.0 if host is None else self.rate_limiter.penalty_left(host)

    def _uncount_page(self, url: str, queue: CrawlerQueue) -> None:
        """A page taken by a worker goes back to the queue: it costs nothing of the limits until it is taken again."""
        self._pages_requested -= 1
        self._host_pages[get_host(url)] -= 1
        if queue.closed:
            # The page reached max_pages and closed the queue; now it is
            # back under the limit, and this worker goes on to crawl it,
            # or the page that takes its place, even if the others have stopped.
            queue.reopen()

    def _robots_back_in(self, url: str) -> float:
        """Seconds until the unreachable robots.txt of the site of `url` is downloaded again.

        A second when it is due already: another task may be downloading it.
        """
        assert self.robots is not None  # asked after it refused a URL
        return self.robots.unreachable_for(url) or 1.0

    def _wait_for_robots(
        self, url: str, queue: CrawlerQueue, refusal: RobotsUnreachableError, *, requested: bool
    ) -> bool:
        """Put off a page of the crawl until the robots.txt that refused it is downloaded again.

        A 5xx or a timeout on robots.txt is often a hiccup of a few seconds;
        failing every page of the site at once would end a crawl of that
        site with nothing. The refusal is for the page itself or for the
        target of its redirect; `requested` says whether the page was
        requested (it redirected): it is then uncounted, as the request was
        not answered with the page, and made again when it comes back. The
        page waits out `RobotsParser.UNREACHABLE_TTL` at most
        `MAX_WAITS_PER_PAGE` times: returns False once they are used up,
        and the caller marks the page unreachable.
        """
        delay = self._robots_back_in(refusal.url)
        return self._put_off_page(url, queue, delay, refusal.message, requested=requested)

    def _put_off_page(self, url: str, queue: CrawlerQueue, delay: float, reason: str, *, requested: bool) -> bool:
        """Put a page of the crawl back into the queue for `delay` seconds, unless it has waited `MAX_WAITS_PER_PAGE` times.

        With `requested`, the page is uncounted from the limits first: it
        is counted again when it is taken again. Returns whether the page
        was put off.
        """
        if self._page_waits[url] >= self.MAX_WAITS_PER_PAGE:
            del self._page_waits[url]
            return False
        self._page_waits[url] += 1
        if requested:
            self._uncount_page(url, queue)
        logger.info("Deferred %s for %.1fs: %s", url, delay, reason)
        queue.defer(url, delay, priority=queue.depth(url))
        return True

    def _outwaits_retries(self, error: FetchError | None) -> bool:
        """Whether `error` carries a Retry-After too long for the retry strategy, so the request was not retried."""
        return (
            isinstance(error, HTTPStatusError)
            and error.retry_after is not None
            and error.retry_after > self.retry_strategy.max_delay
        )

    def _wait_for_host(self, url: str, queue: CrawlerQueue, error: HTTPStatusError) -> bool:
        """Put off a page whose request got a Retry-After too long to retry, until its host may be asked again.

        The host is held back for that long anyway (see `_fetch_hop`): the
        retry strategy's reason not to retry, that coming back early earns
        another refusal, does not hold for a page that comes back with the
        host. The host is that of the failed request: the page may redirect
        to another one. Returns False once the page has waited
        `MAX_WAITS_PER_PAGE` times: the caller fails it then.
        """
        delay = self._penalty_left(error.url) or 1.0
        # The request was answered, but not with the page: it is not a page requested.
        if not self._put_off_page(url, queue, delay, error.message, requested=True):
            return False
        self._warn_once_held_back(error.url, delay)
        return True

    def _defer_or_fail(self, url: str, queue: CrawlerQueue, refusal: CircuitOpenError) -> bool:
        """Put off a page the circuit breaker refused until its host may be probed, or give up on it.

        The host is that of the refusal: the page may redirect to another one.
        Returns whether the page was put off rather than failed.
        """
        host = get_host(refusal.url)
        assert host is not None  # a URL without a host has no circuit
        opened = self.circuit_breaker.times_opened(host)
        if opened >= self.MAX_CIRCUIT_OPENINGS:
            logger.info("Gave up on %s: circuit breaker of %s opened %d times", url, host, opened)
            self._fail_page(url, queue, refusal)
            return False
        # Back when the probe may go; a page refused while the probe is in
        # flight comes back a second later.
        delay = self.circuit_breaker.probe_in(refusal.url) or 1.0
        logger.info("Deferred %s for %.1fs: %s", url, delay, refusal.message)
        queue.defer(url, delay, priority=queue.depth(url))
        return True

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
                robots_tag = tuple(response.headers.getall("X-Robots-Tag", ()))
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
                        robots_tag=robots_tag,
                    )
                body = await self._read_body(response, url, raw=raw, truncate_at=truncate_at)
                return _Response(
                    status=response.status,
                    content="" if raw else _decode(body, _encoding(response, body)),
                    size=len(body),
                    final_url=str(response.url),
                    content_type=content_type,
                    body=body if raw else None,
                    robots_tag=robots_tag,
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


def _signals_overload(error: FetchError) -> bool:
    """Whether the failure says the whole host is overloaded, not one page: HTTP 429, a Retry-After header or a timeout."""
    if isinstance(error, HTTPStatusError):
        return error.status == 429 or bool(error.retry_after)
    return isinstance(error, FetchTimeoutError)


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
