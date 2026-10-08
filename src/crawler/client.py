"""Asynchronous HTTP client that downloads many pages concurrently."""

import asyncio
import logging
import math
from collections.abc import Iterable, Mapping, Sequence
from http.cookiejar import Cookie
from types import TracebackType
from typing import Self

import aiohttp

from crawler.circuit_breaker import CircuitBreaker
from crawler.crawl_run import CrawlRun
from crawler.exceptions import ParseError, StorageError
from crawler.fetching import Fetcher
from crawler.filters import UrlFilter
from crawler.frontier import Frontier, MemoryFrontier
from crawler.models import CrawlStats, ErrorStats, FetchResult, ParsedPage, ProxyStats, RenderStats
from crawler.parser import HTMLParser
from crawler.proxy import ProxyPool
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.rendering import BrowserTransport, Rendering
from crawler.retry import RetryStrategy
from crawler.robots import RobotsParser, product_token
from crawler.semaphores import SemaphoreManager
from crawler.session import (
    cookie_domain_problem,
    cookie_name_problem,
    header_name_problem,
    header_value_problem,
)
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage.base import DataStorage
from crawler.transport import HttpTransport, Transport
from crawler.urls import get_host, is_valid_http_url

logger = logging.getLogger(__name__)


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
      redirect to them and the sitemaps of the site: it is downloaded
      again at most `MAX_ROBOTS_RETRIES` times per site and outage, each
      time a single attempt, and not at all after a failure that does not
      pass by itself, such as a host name that does not resolve. No page
      waits for a download of robots.txt longer than `ROBOTS_POLL`
      seconds: the download goes on, and the page comes back every that
      long until it is over, so a site that is slow to fail holds no
      worker back.
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
      in a minute have failed with a transient or network error (HTTP 429
      aside: the host is up), its requests fail with `CircuitOpenError`
      for 30 seconds without being sent (see `CircuitBreaker`). A request counts once in the window of
      the breaker, however many attempts it took: a page made good by a
      retry is a success of the host. A retry the breaker would refuse is
      not made: the request fails with the error of its last attempt.
      robots.txt that cannot be downloaded for this reason is not cached
      as unreachable. In `crawl()`, the pages of such a host wait for it:
      a page refused before it was sent, and one whose request failed
      while the circuit opened, is put off until the host may be probed.

    Every request has a `connect_timeout` (DNS, TCP and TLS, waiting for a
    pooled connection), a `read_timeout` (for each chunk of the response)
    and a `total_timeout` (the whole request). The n-th retry (from 1)
    multiplies all three by `timeout_growth**n`, at most by
    `MAX_TIMEOUT_GROWTH`: a server that is merely slow gets a chance to
    answer, and a dead one is not waited for forever.

    A page body over `max_page_size` bytes (None for no limit) fails with
    `PageTooLargeError`; the rest of it is not downloaded. The limit is on
    the unpacked body, so a small gzipped response that unpacks into
    gigabytes fails too. Parsing a page costs about forty times its size
    in memory and a couple of seconds per megabyte of CPU, so at most
    `max_parsing` pages are parsed at once: the memory of parsing is
    bounded by `max_parsing` times `max_page_size` times that forty.

    `user_agent` identifies the crawler, and robots.txt rules are looked up
    by its name ("MyBot/1.0 (+url)" is "mybot"). `user_agents` rotates
    several User-Agent strings between requests; they must all carry that
    same name, so rotation cannot sidestep robots.txt.

    Every request carries the `headers`, such as Authorization or
    Accept-Language, whatever its host: pages, robots.txt and sitemaps,
    the hosts of other sites and redirects to them included. The crawler
    keeps the cookies that sites set and sends them back as a browser
    does, robots.txt and sitemaps sharing them with the pages; `cookies`
    are there from the first request (see `load_cookies_file`), and
    `export_cookies()` gives them all (see `save_cookies_file`). With
    `keep_cookies=False` it sends none and keeps none: a site cannot keep
    a session of the crawler. aiohttp keeps no cookies of IP addresses, so
    a site reached by one gets none.

    With `proxies`, every request goes through a proxy of the pool: pages,
    robots.txt and sitemaps alike (see `ProxyPool` for the rotation and
    the proxies taken out of it). Politeness stays with the sites: the
    rate limit, robots.txt and `max_per_domain` are those of the host of
    the URL, whatever proxy the request goes through. A proxy that fails
    a request fails it with `ProxyNetworkError`, a network error that is
    retried, through another proxy at once; when every proxy is out of
    rotation, requests fail with `NoProxyError` without being sent, and
    are not retried; a page of `crawl()` is put off until the first proxy
    is back, at most `MAX_WAITS_PER_PAGE` times. The circuit breaker
    counts neither: a dead proxy must not block the sites behind it.
    robots.txt that cannot be downloaded for this reason is not cached as
    unreachable: the page fails with the error of the proxy, or waits for
    a proxy. `proxy_stats()` counts the
    requests and failures of every proxy; the proxies out of rotation
    stay out from one `crawl()` to the next.

    With `rendering`, HTML pages are rendered in a headless Chromium
    (Playwright, an optional dependency), so that the links and the text
    that JavaScript makes are found: every page, or those the patterns of
    `rendering` name (see `Rendering`). A page is downloaded as without a
    browser, through the limits, the proxies and the cookies of the
    crawler; the browser gets the document as downloaded and loads its
    scripts, styles and data itself, without asking robots.txt, as a
    browser does. The browser shares the cookies of the crawler both
    ways, those JavaScript sets included, and its requests carry the
    `user_agent` and the `headers`; with `keep_cookies=False` every page
    has a browser of its own, without cookies. A page that goes to another URL on its own (a
    JavaScript or `<meta>` redirect) is followed as a redirect: robots.txt,
    the filters of `crawl()` and `MAX_REDIRECTS` apply to it. A page the
    browser takes longer than `rendering.timeout` to render fails with
    `RenderTimeoutError`, a timeout that is retried and not held against
    the host; one that crashes the browser with `RenderError`, which is
    not retried. The circuit breaker counts neither. robots.txt and
    sitemaps are never rendered. The browser starts with the first page
    to render and is closed by `close()`; once it is given up (it crashed
    again or is not installed), pages are taken as downloaded.
    `render_stats()` counts the pages rendered, failed and taken as
    downloaded, and the time the browser took for them.

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

    The settings of a crawler, such as `max_concurrent`, `max_page_size`
    or `circuit_breaker`, are read-only: they are handed to the layers
    that make the requests when the crawler is made. Only `storage` may be
    replaced, between crawls. The constants of requests (`MAX_REDIRECTS`,
    `MAX_TIMEOUT_GROWTH`, `REDIRECT_STATUSES`) are read when the crawler
    is made too, so they apply when set on a subclass; those of a crawl
    (`ROBOTS_POLL`, `MAX_ROBOTS_RETRIES` ...) are read on every `crawl()`,
    so they apply when set on the crawler as well.

    Fetching from a closed crawler fails with `CrawlerClosedError`, reported
    the same way as any other per-URL failure. Closing does not interrupt
    requests that are already in flight: they finish on their own or hit
    `total_timeout`.
    """

    # Sites such as Wikipedia ask bots to identify themselves with a contact
    # URL and may block generic user agents.
    DEFAULT_USER_AGENT = "AsyncWebCrawler/0.1 (+https://github.com/AndreyKhodulapov/async-web-crawler)"
    # A page at this size takes a few seconds and over a hundred megabytes to parse.
    DEFAULT_MAX_PAGE_SIZE = 3 * 1024 * 1024
    MAX_TIMEOUT_GROWTH = Fetcher.MAX_TIMEOUT_GROWTH
    MAX_REDIRECTS = Fetcher.MAX_REDIRECTS
    REDIRECT_STATUSES = HttpTransport.REDIRECT_STATUSES
    # The longest Retry-After a host is held back for, in seconds.
    DEFAULT_MAX_RETRY_AFTER = 600.0
    MAX_CIRCUIT_OPENINGS = CrawlRun.MAX_CIRCUIT_OPENINGS
    MIN_PENALTY_TO_DEFER = CrawlRun.MIN_PENALTY_TO_DEFER
    MAX_WAITS_PER_PAGE = CrawlRun.MAX_WAITS_PER_PAGE
    MAX_ROBOTS_RETRIES = CrawlRun.MAX_ROBOTS_RETRIES
    ROBOTS_POLL = CrawlRun.ROBOTS_POLL
    MAX_STORAGE_PAUSE = CrawlRun.MAX_STORAGE_PAUSE
    FRONTIER_FACTOR = Frontier.FRONTIER_FACTOR
    # In crawl(), longer links are not followed: they are mostly generated ones.
    MAX_URL_LENGTH = 2048

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
        max_parsing: int = 2,
        max_retry_after: float = DEFAULT_MAX_RETRY_AFTER,
        user_agent: str = DEFAULT_USER_AGENT,
        user_agents: Sequence[str] = (),
        headers: Mapping[str, str] | None = None,
        cookies: Iterable[Cookie] = (),
        keep_cookies: bool = True,
        proxies: ProxyPool | None = None,
        rendering: Rendering | None = None,
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
        if max_parsing < 1:
            raise ValueError(f"max_parsing must be >= 1, got {max_parsing}")
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
        headers = dict(headers or {})
        for name, value in headers.items():
            # The value is not shown: it may be a secret, such as a token.
            problem = header_name_problem(name) or header_value_problem(value)
            if problem is not None:
                raise ValueError(f"header {name!r}: {problem}")
        cookies = list(cookies)
        if cookies and not keep_cookies:
            raise ValueError("cookies need keep_cookies=True")
        for cookie in cookies:
            problem = cookie_name_problem(cookie.name) or cookie_domain_problem(cookie.domain)
            if problem is not None:
                raise ValueError(f"cookie {cookie.name!r} of {cookie.domain!r}: {problem}")

        # These validate their own arguments.
        self._limits = SemaphoreManager(max_concurrent, max_per_domain)
        rate_limiter = RateLimiter(requests_per_second, per_domain_rate, min_delay=min_delay, jitter=jitter)
        self._max_concurrent = max_concurrent
        self._max_depth = max_depth
        # `connect` covers DNS resolution and waiting for a pooled connection,
        # unlike `sock_connect`, which is only the TCP handshake.
        timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=connect_timeout,
            sock_read=read_timeout,
        )
        self._max_page_size = max_page_size
        self._max_parsing = max_parsing
        # Parsing runs in a thread per page: this keeps the trees of large
        # pages from piling up in memory, as the GIL gives them no parallelism anyway.
        self._parsing = asyncio.Semaphore(max_parsing)
        transport = HttpTransport(
            max_concurrent=max_concurrent,
            timeout=timeout,
            user_agent=user_agent,
            user_agents=user_agents,
            max_page_size=max_page_size,
            headers=headers,
            cookies=cookies,
            keep_cookies=keep_cookies,
            proxies=proxies,
        )
        self._transport: Transport = transport
        if rendering is not None:
            self._transport = BrowserTransport(
                transport,
                rendering,
                user_agent=user_agent,
                max_page_size=max_page_size,
                headers=headers,
                keep_cookies=keep_cookies,
                no_proxy=None if proxies is None else proxies.no_proxy,
            )
        self._proxies = proxies
        self._rendering = rendering
        self._fetcher = Fetcher(
            self._transport,
            limits=self._limits,
            rate_limiter=rate_limiter,
            retry_strategy=retry_strategy or RetryStrategy(),
            circuit_breaker=circuit_breaker or CircuitBreaker(),
            respect_robots=respect_robots,
            timeout=timeout,
            timeout_growth=timeout_growth,
            max_retry_after=max_retry_after,
            user_agent=user_agent,
        )
        # Set on a subclass, they apply to its requests.
        for layer in (transport, self._fetcher):
            for name in layer.SETTINGS:
                setattr(layer, name, getattr(self, name))
        # Links marked rel="nofollow" are left out of the pages, as robots.txt is followed;
        # a robots meta tag may name the crawler ("asyncwebcrawler"), as X-Robots-Tag may.
        self._parser = parser or HTMLParser(skip_nofollow=respect_robots, robots_name=robots_name)
        self.storage = storage
        self._keep_pages = keep_pages
        self.stats = CrawlerStats()
        # The latest crawl and its frontier; empty ones before the first.
        self._frontier = MemoryFrontier()
        self._run = self._new_run(self._frontier)
        # A crawl() that is opening the storage, before its run has started.
        self._starting = False

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
        return self._fetcher.closed

    @property
    def max_concurrent(self) -> int:
        return self._max_concurrent

    @property
    def max_depth(self) -> int:
        return self._max_depth

    @property
    def keep_pages(self) -> bool:
        return self._keep_pages

    @property
    def max_page_size(self) -> int | None:
        return self._max_page_size

    @property
    def max_parsing(self) -> int:
        return self._max_parsing

    @property
    def timeout_growth(self) -> float:
        return self._fetcher.timeout_growth

    @property
    def max_retry_after(self) -> float:
        return self._fetcher.max_retry_after

    @property
    def rate_limiter(self) -> RateLimiter:
        return self._fetcher.rate_limiter

    @property
    def retry_strategy(self) -> RetryStrategy:
        return self._fetcher.retry_strategy

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        return self._fetcher.circuit_breaker

    @property
    def proxies(self) -> ProxyPool | None:
        return self._proxies

    @property
    def rendering(self) -> Rendering | None:
        return self._rendering

    @property
    def robots(self) -> RobotsParser | None:
        """robots.txt of the sites, downloaded and cached; None without `respect_robots`."""
        return self._fetcher.robots

    @property
    def sitemaps(self) -> SitemapParser:
        return self._fetcher.sitemaps

    @property
    def processed_urls(self) -> dict[str, ParsedPage]:
        """Normalized URL -> page the latest crawl parsed and kept; empty with `keep_pages=False`. Do not modify."""
        return self._run.processed_urls

    @property
    def visited_urls(self) -> set[str]:
        """URLs the latest crawl took for fetching, successful or not. Do not modify."""
        return self._queue().visited

    @property
    def failed_urls(self) -> dict[str, str]:
        """URL -> error description for pages the latest crawl could not fetch. Do not modify."""
        return self._queue().failed

    @property
    def skipped_urls(self) -> dict[str, str]:
        """URL -> reason for pages the latest crawl left out, e.g. not HTML. Do not modify.

        All of them were fetched, except those over `max_pages_per_host`.
        """
        return self._queue().skipped

    @property
    def blocked_urls(self) -> dict[str, str]:
        """URL -> reason for pages the latest crawl was not allowed to fetch. Do not modify."""
        return self._queue().blocked

    @property
    def unreachable_urls(self) -> dict[str, str]:
        """URL -> reason for pages the latest crawl skipped because robots.txt was unreachable. Do not modify."""
        return self._queue().unreachable

    @property
    def failed_sitemaps(self) -> dict[str, str]:
        """Sitemap URL -> error description for sitemaps the latest crawl could not read. Do not modify."""
        return self._run.failed_sitemaps

    @property
    def url_depths(self) -> Mapping[str, int]:
        """Depth of every URL the latest crawl accepted: 0 for start URLs and pages listed in sitemaps."""
        return self._queue().depths

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
        result = await self._fetcher.fetch(url, html_only=True)
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
        return await self._fetcher.fetch(url)

    async def _parse(self, result: FetchResult) -> ParsedPage:
        """Parse a successful fetch result, at most `max_parsing` at once; a failure counts in `error_stats()`."""
        assert result.content is not None
        try:
            async with self._parsing:
                return await self._parser.parse_html(
                    result.content,
                    result.url,
                    final_url=result.final_url,
                    content_type=result.content_type,
                )
        except ParseError as error:
            self._fetcher.errors.record_error(error)
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
        is downloaded again (see `RobotsParser.UNREACHABLE_TTL`), then
        listed in `unreachable_urls` once it has also failed the
        `MAX_ROBOTS_RETRIES` downloads after the first, or at once after a
        failure that does not pass by itself (a bad certificate, a host
        name that does not resolve); a
        site whose robots.txt failed for a moment is crawled once it is
        back. So does a page that redirects to such a site: it is requested
        again when it comes back, uncounted meanwhile. Each download after
        the first is a single attempt, and no page waits for a download
        longer than `ROBOTS_POLL` seconds: the page is put off for that
        long at a time while the download goes on, and the workers go on
        with other sites meanwhile. Pages that the circuit breaker refuses do not
        count either: they are put off until their host may be probed and
        tried again, and the crawl goes on with other pages meanwhile. So
        is a page whose request failed while the circuit of its host
        opened, on its own failure or on those of other requests in
        flight: the breaker refused the retries it would have had, so it
        is requested again when the host may be probed, uncounted
        meanwhile, rather than failed; so is a page whose redirect leads to
        a host the breaker refuses. The probe itself is such a retry: a
        page whose probe failed fails with its error, and so does one with
        an error never retried, such as HTTP 501. Once
        the circuit of a host has opened `MAX_CIRCUIT_OPENINGS` times in
        the crawl, its refused pages go to `failed_urls` with
        `CircuitOpenError`, so a host that stays down holds the crawl for
        about two cooldowns of the breaker; those that were requested count
        toward `max_pages`. A page whose host is held back
        for longer than `MIN_PENALTY_TO_DEFER` seconds, by a Retry-After or
        the pause before the retry of a request that found the host
        overloaded (HTTP 429, a timeout), is put off until the host may be
        asked again, without counting toward `max_pages` before then. A
        Retry-After longer than `max_delay` of the retry strategy is
        logged as a warning once per host, as the crawl may be quiet for
        that long; the page that got it, which the request did not retry,
        comes back with the host too, at most `MAX_WAITS_PER_PAGE` times,
        then goes to `failed_urls`; a page that got it with a permanent
        error (HTTP 403) goes there at once.

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
        first page is fetched, one after another and only until the queue
        is full: the rest of a sitemap and the sitemaps after it are not
        downloaded (see `SitemapParser`). A page a sitemap lists
        has depth 0, like a start URL, but must pass the filters, like a
        link; it comes after the start URLs and before the links.
        `same_domain_only` keeps the hosts of `sitemap_urls` as well as
        those of the start URLs and of the pages they redirect to. A sitemap
        that cannot be read does not stop the crawl: it is logged and listed
        in `failed_sitemaps`. A sitemap of a site whose robots.txt cannot
        be read waits for it as a page does, within the `MAX_ROBOTS_RETRIES`
        repeat downloads of the site, before the first page is fetched; so
        do the sitemaps named in such a robots.txt.

        Failed pages do not stop the crawl: they are listed in `failed_urls`.
        Every processed page is saved to the `storage` of the crawler, if it
        has one, and the storage is flushed before the crawl returns. The
        storage is opened before the first request (see `DataStorage.open`):
        one that cannot be written to fails the crawl before anything is
        requested, instead of a batch of pages later. A page
        that cannot be saved is still returned: the failure is logged and
        counted in `crawl_stats()`.
        The state of the crawl (`processed_urls`, `visited_urls`,
        `failed_urls`, `skipped_urls`, `blocked_urls`, `unreachable_urls`, `failed_sitemaps`, `url_depths`,
        `stats`, `crawl_stats()`, `error_stats()`, the counters of `circuit_breaker.get_stats()` and
        `proxy_stats()`) is reset on every call and stays available after it returns. The rate limits,
        the robots.txt cache, the states of the circuit breaker and the proxies out of rotation carry
        over; a site whose
        robots.txt the previous crawl gave up on is downloaded again.

        Raises:
            TypeError: a single string is passed instead of a list of URLs, patterns or extensions.
            ValueError: `max_pages` or `max_pages_per_host` is not positive, a start URL, a sitemap URL,
                a pattern or an extension is invalid, `robots_sitemaps` is asked of a crawler that does not
                read robots.txt.
            RuntimeError: another crawl is running on this crawler.
            StorageError: the storage cannot be opened; nothing is requested.
        """
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1, got {max_pages}")
        if max_pages_per_host is not None and max_pages_per_host < 1:
            raise ValueError(f"max_pages_per_host must be >= 1 or None, got {max_pages_per_host}")
        start_urls, sitemap_urls = self._crawl_urls(start_urls, sitemap_urls, robots_sitemaps)
        self._check_idle()
        url_filter = self._url_filter(
            start_urls + sitemap_urls,
            same_domain_only=same_domain_only,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            exclude_extensions=exclude_extensions,
        )
        frontier = MemoryFrontier(
            max_pages=max_pages, max_pages_per_host=max_pages_per_host, frontier_factor=self.FRONTIER_FACTOR
        )
        return await self._crawl(
            frontier, start_urls, url_filter, sitemap_urls=sitemap_urls, robots_sitemaps=robots_sitemaps
        )

    async def crawl_frontier(
        self,
        frontier: Frontier,
        start_urls: Iterable[str],
        *,
        same_domain_only: bool = False,
        include_patterns: Iterable[str] = (),
        exclude_patterns: Iterable[str] = (),
        exclude_extensions: Iterable[str] = (),
        sitemap_urls: Iterable[str] = (),
    ) -> dict[str, ParsedPage]:
        """Crawl the pages of `frontier`, which seed() filled; return pages by normalized URL, as crawl() does.

        This is how a worker crawls the frontier of a job that it shares
        with other workers. The arguments are those the frontier was
        seeded with, and the rules of crawl() apply, but the limits are
        those of the frontier and the sitemaps are not read again: under
        `same_domain_only`, `sitemap_urls` give their hosts to the scope.
        The start URLs are queued again, which queues only those the
        frontier has not seen. The crawl ends once `take` of the frontier
        hands out no page; the frontier is left open.

        The outcomes of the pages are kept by the frontier: `visited_urls`,
        `failed_urls`, `skipped_urls`, `blocked_urls`, `unreachable_urls`
        and `url_depths` are empty, unless it is a `MemoryFrontier`.

        With a `shared` frontier, no page is taken while the storage cannot
        write (`DataStorage.write_failed`): the storage is written again,
        after longer and longer pauses up to `MAX_STORAGE_PAUSE`, until it
        can, and the crawl does not end before it is.

        Cancelled, the crawl writes what the storage buffers (one try; a
        failure is logged) before the cancellation goes on, so that those
        pages are `saved` in the frontier; the pages in flight are left in
        progress for `close` of the frontier to put back.

        Raises:
            TypeError: as crawl().
            ValueError: as crawl(), but for the limits, which are those of the frontier.
            RuntimeError: another crawl is running on this crawler.
            StorageError: as crawl().
            FrontierError: an operation of the frontier failed with one of
                its `ERRORS`: the crawl stopped, the pages it had in progress
                left as they are; what the storage buffers is written first.
        """
        start_urls, sitemap_urls = self._crawl_urls(start_urls, sitemap_urls, robots_sitemaps=False)
        self._check_idle()
        url_filter = self._url_filter(
            start_urls + sitemap_urls,
            same_domain_only=same_domain_only,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            exclude_extensions=exclude_extensions,
        )
        return await self._crawl(frontier, start_urls, url_filter, sitemap_urls=[], robots_sitemaps=False)

    async def _crawl(
        self,
        frontier: Frontier,
        start_urls: list[str],
        url_filter: UrlFilter,
        *,
        sitemap_urls: list[str],
        robots_sitemaps: bool,
    ) -> dict[str, ParsedPage]:
        """Open the storage, then run the crawl of `frontier`; the arguments are checked."""
        if self.storage is not None:
            # Before anything is requested: a storage that cannot be
            # written to is found out now, not a batch of pages later.
            # The crawler is taken while it opens: another crawl may
            # start meanwhile, and the run starts only after it.
            self._starting = True
            try:
                await self.storage.open()
            finally:
                self._starting = False
        self._frontier = frontier
        self._run = self._new_run(frontier)
        return await self._run.run(
            start_urls,
            url_filter=url_filter,
            sitemap_urls=sitemap_urls,
            robots_sitemaps=robots_sitemaps,
        )

    async def seed(
        self,
        frontier: Frontier,
        start_urls: Iterable[str],
        *,
        same_domain_only: bool = False,
        include_patterns: Iterable[str] = (),
        exclude_patterns: Iterable[str] = (),
        exclude_extensions: Iterable[str] = (),
        sitemap_urls: Iterable[str] = (),
        robots_sitemaps: bool = False,
    ) -> dict[str, str]:
        """Queue the start URLs and the pages of the sitemaps in `frontier`, as crawl() does first; crawl nothing.

        This fills a frontier that others crawl, such as the frontier of a
        job that distributed workers share. The arguments are those of
        crawl(), and its rules for sitemaps apply: they are read only until
        the frontier is full, and their pages must pass the filters. Under
        `same_domain_only`, the pages of hosts out of scope are held in the
        frontier for a start URL that redirects to their host (see
        `Frontier.hold_out_of_scope`). Returns the sitemaps that could not
        be read, with the reasons.

        Raises:
            TypeError: as crawl().
            ValueError: as crawl(), but for the limits, which are those of the frontier.
            RuntimeError: a crawl is running on this crawler.
        """
        start_urls, sitemap_urls = self._crawl_urls(start_urls, sitemap_urls, robots_sitemaps)
        self._check_idle()
        url_filter = self._url_filter(
            start_urls + sitemap_urls,
            same_domain_only=same_domain_only,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            exclude_extensions=exclude_extensions,
        )
        run = self._new_run(frontier)
        # The crawler is taken while it reads the sitemaps: they share its rate limits and robots.txt.
        self._starting = True
        try:
            await run.seed(
                start_urls, url_filter=url_filter, sitemap_urls=sitemap_urls, robots_sitemaps=robots_sitemaps
            )
        finally:
            self._starting = False
        return run.failed_sitemaps

    def _crawl_urls(
        self, start_urls: Iterable[str], sitemap_urls: Iterable[str], robots_sitemaps: bool
    ) -> tuple[list[str], list[str]]:
        """The start URLs and the sitemap URLs of a crawl as lists, checked as crawl() says."""
        if isinstance(start_urls, str):
            raise TypeError(f"expected a list of start URLs, got a string: {start_urls!r}")
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
        return start_urls, sitemap_urls

    def _queue(self) -> CrawlerQueue:
        """The queue of the latest crawl; an empty one if its frontier keeps the pages elsewhere, as in a database."""
        frontier = self._frontier
        return frontier.queue if isinstance(frontier, MemoryFrontier) else CrawlerQueue()

    def _check_idle(self) -> None:
        if self._run.running or self._starting:
            raise RuntimeError("a crawl is already running on this crawler")

    def _url_filter(
        self,
        scope_urls: list[str],
        *,
        same_domain_only: bool,
        include_patterns: Iterable[str],
        exclude_patterns: Iterable[str],
        exclude_extensions: Iterable[str],
    ) -> UrlFilter:
        """The filter of the links of a crawl; under `same_domain_only`, the hosts of `scope_urls` are its scope."""
        return UrlFilter(
            allowed_hosts={get_host(url) for url in scope_urls} if same_domain_only else None,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            exclude_extensions=exclude_extensions,
            max_url_length=self.MAX_URL_LENGTH,
        )

    def _new_run(self, frontier: Frontier) -> CrawlRun:
        run = CrawlRun(
            self._fetcher,
            self._parse,
            frontier=frontier,
            limits=self._limits,
            stats=self.stats,
            storage=self.storage,
            keep_pages=self.keep_pages,
            max_concurrent=self.max_concurrent,
            max_depth=self.max_depth,
        )
        # Set on the crawler, or on a subclass, they apply to its crawls.
        for name in CrawlRun.SETTINGS:
            setattr(run, name, getattr(self, name))
        return run

    def crawl_stats(self) -> CrawlStats:
        """Progress of the running crawl, or the result of the latest one."""
        return self._run.crawl_stats()

    def export_cookies(self) -> list[Cookie]:
        """The cookies the crawler keeps, those sites have set included; empty with `keep_cookies=False`.

        They stay available after `close()`.
        """
        return self._transport.cookies()

    def proxy_stats(self) -> dict[str, ProxyStats]:
        """The proxies by label (their URLs with the password hidden); empty without `proxies`."""
        return {} if self.proxies is None else self.proxies.get_stats()

    def render_stats(self) -> RenderStats | None:
        """The pages rendered in the browser since the latest crawl() started; None without `rendering`."""
        return self._transport.render_stats() if isinstance(self._transport, BrowserTransport) else None

    def error_stats(self) -> ErrorStats:
        """Errors of page requests since the latest crawl() started, or since the crawler was created."""
        return self._fetcher.errors.get_stats()

    async def close(self) -> None:
        """Close the HTTP session, the browser and the storage. Safe to call more than once.

        A storage that cannot write its last pages is closed all the same;
        the failure is logged.
        """
        await self._fetcher.close()
        if self.storage is not None:
            try:
                await self.storage.close()
            except StorageError as error:
                logger.error("Failed to close %s: %s", type(self.storage).__name__, error)
            except Exception:
                logger.exception("Unexpected error while closing %s", type(self.storage).__name__)
