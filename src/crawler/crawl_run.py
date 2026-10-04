"""Crawl layer of the crawler: one crawl, with its queue, filters, limits and counters."""

import asyncio
import logging
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from urllib.parse import urlsplit

from crawler.circuit_breaker import CircuitState
from crawler.exceptions import (
    CircuitOpenError,
    CrawlerClosedError,
    FetchError,
    HTTPStatusError,
    ParseError,
    PermanentError,
    ProxyError,
    RobotsDisallowedError,
    RobotsUnreachableError,
    StorageError,
    UnexpectedError,
)
from crawler.fetching import Fetcher
from crawler.filters import UrlFilter
from crawler.models import CrawlStats, FetchResult, PageRecord, ParsedPage
from crawler.parser import is_html_content_type
from crawler.queue import CrawlerQueue, queue_form
from crawler.semaphores import SemaphoreManager
from crawler.stats import CrawlerStats
from crawler.storage.base import DataStorage
from crawler.urls import get_host, normalize_url, strip_tracking_params

logger = logging.getLogger(__name__)


class CrawlRun:
    """One crawl of `AsyncCrawler.crawl()`: its queue, filters, limits and counters.

    Pages are requested through the `Fetcher` of the crawler and parsed by
    `parse`; `stats` and `storage` are those of the crawler. A run is made
    once: every crawl() call makes a new one, so nothing of the previous
    crawl needs to be reset.
    """

    MAX_CIRCUIT_OPENINGS = 3
    # In crawl(), a page whose host is held back longer than this is put off.
    MIN_PENALTY_TO_DEFER = 1.0
    # In crawl(), a page waits at most this many times for a Retry-After
    # too long to retry it, before it is given up.
    MAX_WAITS_PER_PAGE = 3
    # In crawl(), the unreachable robots.txt of a site is downloaded again at
    # most this many times in a row before its pages, and the pages that
    # redirect to them, are given up; so are its sitemaps.
    MAX_ROBOTS_RETRIES = 3
    # In crawl(), a page waits at most this many seconds for a download of
    # robots.txt, then it is put off for this long at a time while the
    # download goes on: a site that is slow to fail holds no worker back.
    ROBOTS_POLL = 2.0
    # In crawl(), new links are not queued once the pages queued, in progress
    # and requested reach this many times max_pages: most would never be fetched.
    # Likewise a host has at most this many times max_pages_per_host queued.
    FRONTIER_FACTOR = 3
    # The constants above that AsyncCrawler hands over from itself to every run.
    SETTINGS = (
        "MAX_CIRCUIT_OPENINGS",
        "MIN_PENALTY_TO_DEFER",
        "MAX_WAITS_PER_PAGE",
        "MAX_ROBOTS_RETRIES",
        "ROBOTS_POLL",
        "FRONTIER_FACTOR",
    )

    def __init__(
        self,
        fetcher: Fetcher,
        parse: Callable[[FetchResult], Awaitable[ParsedPage]],
        *,
        limits: SemaphoreManager,
        stats: CrawlerStats,
        storage: DataStorage | None,
        keep_pages: bool,
        max_concurrent: int,
        max_depth: int,
    ) -> None:
        self._fetcher = fetcher
        self._parse = parse
        self._limits = limits
        self.robots = fetcher.robots
        self.sitemaps = fetcher.sitemaps
        self.rate_limiter = fetcher.rate_limiter
        self.retry_strategy = fetcher.retry_strategy
        self.circuit_breaker = fetcher.circuit_breaker
        self.stats = stats
        self.storage = storage
        self.keep_pages = keep_pages
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        # State of the crawl.
        self._queue = CrawlerQueue()
        self.processed_urls: dict[str, ParsedPage] = {}
        self._start_urls: set[str] = set()
        self._sitemap_pages_out_of_scope: list[str] = []
        self._redirect_sources: dict[str, str] = {}  # redirect target -> the page that led to it
        self._failed_sitemaps: dict[str, str] = {}
        self._pages_requested = 0
        self._host_pages: Counter[str] = Counter()  # pages requested by host
        self._over_host_limit = 0  # pages skipped without a request over max_pages_per_host
        self._hosts_warned_held_back: set[str] = set()  # hosts whose long Retry-After was logged by the crawl
        self._page_waits: Counter[str] = Counter()  # times a page waited for a Retry-After too long to retry
        self._max_frontier = 0  # pages queued, in progress and requested that crawl() allows
        self._links_dropped = 0
        self._host_queued: Counter[str] = Counter()  # pages ever queued by host
        self._max_host_queued: int | None = None  # pages queued by host that crawl() allows
        self._links_dropped_by_host = 0
        self._pages_to_save = 0
        self._written_before = 0
        self._pending_before = 0
        self._crawl_started: float | None = None
        self._crawl_finished: float | None = None

    @property
    def queue(self) -> CrawlerQueue:
        return self._queue

    @property
    def failed_sitemaps(self) -> dict[str, str]:
        return self._failed_sitemaps

    @property
    def running(self) -> bool:
        """Whether the crawl has started and is not over."""
        return self._crawl_started is not None and self._crawl_finished is None

    async def run(
        self,
        start_urls: list[str],
        max_pages: int,
        *,
        max_pages_per_host: int | None,
        url_filter: UrlFilter,
        sitemap_urls: list[str],
        robots_sitemaps: bool,
    ) -> dict[str, ParsedPage]:
        """Crawl from the start URLs, as `AsyncCrawler.crawl()` describes; the arguments are checked by it."""
        if self._crawl_started is not None:
            raise RuntimeError("a crawl run is made once")
        self._max_frontier = self.FRONTIER_FACTOR * max_pages
        self._max_host_queued = None if max_pages_per_host is None else self.FRONTIER_FACTOR * max_pages_per_host
        self._written_before = self.storage.written if self.storage is not None else 0
        self._pending_before = self.storage.pending if self.storage is not None else 0
        self._fetcher.reset_stats()
        self.rate_limiter.reset_stats()
        self.circuit_breaker.reset_stats()
        if self.robots is not None:
            self.robots.forget_outages()
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
            # A site given up on is given up for this crawl only: a
            # fetch_url() after it downloads robots.txt again.
            if self.robots is not None:
                self.robots.forget_outages()
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

        While it is unreachable, it is waited for and downloaded again, as
        the pages of the site wait for it (see `_robots_wait`).
        """
        assert self.robots is not None
        try:
            while True:
                rules = await self.robots.fetch_robots(url)
                if rules["unreachable"] is None:
                    return rules["sitemaps"]
                reason = f"robots.txt is unreachable ({rules['unreachable']})"
                if (delay := self._robots_wait(url)) is None:
                    break
                logger.info("Sitemaps of %s wait %.1fs: %s", url, delay, reason)
                await asyncio.sleep(delay)
        except (CrawlerClosedError, CircuitOpenError, ProxyError) as error:
            reason = f"{type(error).__name__}: {error.message}"
        logger.warning("No sitemaps from robots.txt of %s: %s", url, reason)
        return []

    async def _load_sitemap(self, url: str) -> list[str]:
        """The pages a sitemap lists; a sitemap that cannot be read is logged and lists none.

        While robots.txt of its site is unreachable, the sitemap waits for
        it to be downloaded again, as a page of the crawl does (see
        `_robots_wait`): a crawl fed by sitemaps alone would otherwise end
        empty after a 503 of a few seconds.
        """
        while True:
            try:
                return await self.sitemaps.fetch_sitemap(url)
            except FetchError as error:
                delay = self._robots_wait(error.url) if isinstance(error, RobotsUnreachableError) else None
                if delay is None:
                    reason = f"{type(error).__name__}: {error.message}"
                    logger.warning("Sitemap %s is left out: %s", url, reason)
                    self._failed_sitemaps[url] = reason
                    return []
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
            retries=self._fetcher.retries,
            current_rps=rate.current_rps,
            avg_delay=rate.avg_delay,
            avg_wait=rate.avg_wait,
            saved=saved,
            save_failed=save_failed,
        )

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
                # A host given up on comes first: its robots.txt is not
                # downloaded either, which would probe its circuit once more.
                refusal = (
                    self._check_probes_left(url)
                    or await self._fetcher.check_robots(url, wait=self.ROBOTS_POLL)
                    or self._fetcher.check_circuit(url)
                )
                if refusal is not None:
                    if isinstance(refusal, RobotsDisallowedError):
                        queue.mark_blocked(url, refusal.message)
                    elif isinstance(refusal, RobotsUnreachableError):
                        if not self._wait_for_robots(url, queue, refusal, requested=False):
                            reason = self._unreachable_reason(refusal)
                            logger.info("Gave up on %s: %s", url, reason)
                            queue.mark_unreachable(url, reason)
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
                # Fetcher.fetch() reports expected failures in the result, so this
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

        result = await self._fetcher.fetch(
            url, html_only=True, check_robots=False, follow=follow, robots_wait=self.ROBOTS_POLL
        )
        if (refusal := self._circuit_refusal(result)) is not None:
            # The circuit of the host, or of the host a redirect leads to,
            # opened while the request waited for its turn or was in flight.
            if refusal is not result.error:
                # The request was answered, but not with the page: it is not a page requested.
                self._uncount_page(url, queue)
            elif not sent:
                # Nothing was sent: the page costs nothing of the limits.
                self._uncount_page(url, queue)
            if not self._defer_or_fail(url, queue, refusal, result):
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
            reason = f"redirects to {result.error.url}, {self._unreachable_reason(result.error)}"
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
        """In a crawl, a host whose circuit has opened `MAX_CIRCUIT_OPENINGS` times gets no more probes.

        Checked before robots.txt, whose download would be a probe too.
        """
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

    def _robots_wait(self, url: str) -> float | None:
        """Seconds the crawl waits for the unreachable robots.txt of the site of `url` to be downloaded again; None to give up.

        A 5xx or a timeout on robots.txt is often a hiccup of a few seconds;
        failing every page of the site at once would end a crawl of that
        site with nothing. The site is given up once the
        `MAX_ROBOTS_RETRIES` downloads after the first have failed too,
        whoever waited for them (its pages, the pages that redirect to it,
        its sitemaps), and at once when the failure does not pass by itself
        (a bad certificate, a host name that does not resolve): three
        minutes change nothing about a typo. The wait is `ROBOTS_POLL`
        when the download is due already: another task is making it, or
        is about to, and nobody else waits for it.
        """
        assert self.robots is not None  # asked after it refused a URL
        if not self.robots.may_recover(url) or self.robots.failed_downloads(url) > self.MAX_ROBOTS_RETRIES:
            # Nothing of the crawl downloads it again: a page that looks in
            # on the site later would otherwise make one more download.
            self.robots.give_up(url)
            return None
        return self.robots.unreachable_for(url) or self.ROBOTS_POLL

    def _wait_for_robots(
        self, url: str, queue: CrawlerQueue, refusal: RobotsUnreachableError, *, requested: bool
    ) -> bool:
        """Put off a page of the crawl until the robots.txt that refused it is downloaded again, if it is worth waiting for.

        The refusal is for the page itself or for the target of its
        redirect; `requested` says whether the page was requested (it
        redirected): it is then uncounted, as the request was not answered
        with the page, and made again when it comes back. Returns False
        when the site is given up (see `_robots_wait`), and the caller
        marks the page unreachable.
        """
        delay = self._robots_wait(refusal.url)
        if delay is None:
            return False
        self._put_off_page(url, queue, delay, refusal.message, requested=requested)
        return True

    def _unreachable_reason(self, refusal: RobotsUnreachableError) -> str:
        """Why the site of `refusal` is given up: the failure of its robots.txt as cached, whatever the refusal said.

        A refusal may say only that robots.txt is being downloaded, when
        the page looked in on a download that was over by then; the site
        is given up for the failure of its downloads, which the cache knows.
        """
        assert self.robots is not None  # it refused the URL
        try:
            unreachable = self.robots.unreachable_reason(refusal.url)
        except LookupError:
            unreachable = None
        return refusal.message if unreachable is None else f"robots.txt is unreachable ({unreachable})"

    def _put_off_page(self, url: str, queue: CrawlerQueue, delay: float, reason: str, *, requested: bool) -> None:
        """Put a page of the crawl back into the queue for `delay` seconds.

        With `requested`, the page is uncounted from the limits first: it
        is counted again when it is taken again.
        """
        if requested:
            self._uncount_page(url, queue)
        logger.info("Deferred %s for %.1fs: %s", url, delay, reason)
        queue.defer(url, delay, priority=queue.depth(url))

    def _outwaits_retries(self, error: FetchError | None) -> bool:
        """Whether `error` carries a Retry-After too long for the retry strategy, so the request was not retried.

        Not for a permanent error, such as HTTP 403 with a Retry-After: the
        host is held back for as long as it asked, but the page would fail
        the same way when it comes back.
        """
        return (
            isinstance(error, HTTPStatusError)
            and not isinstance(error, PermanentError)
            and error.retry_after is not None
            and error.retry_after > self.retry_strategy.max_delay
        )

    def _wait_for_host(self, url: str, queue: CrawlerQueue, error: HTTPStatusError) -> bool:
        """Put off a page whose request got a Retry-After too long to retry, until its host may be asked again.

        The host is held back for that long anyway (see `Fetcher`): the
        retry strategy's reason not to retry, that coming back early earns
        another refusal, does not hold for a page that comes back with the
        host. The host is that of the failed request: the page may redirect
        to another one. Returns False once the page has waited
        `MAX_WAITS_PER_PAGE` times: the caller fails it then.
        """
        if self._page_waits[url] >= self.MAX_WAITS_PER_PAGE:
            del self._page_waits[url]
            return False
        self._page_waits[url] += 1
        delay = self._penalty_left(error.url) or 1.0
        # The request was answered, but not with the page: it is not a page requested.
        self._put_off_page(url, queue, delay, error.message, requested=True)
        self._warn_once_held_back(error.url, delay)
        return True

    def _circuit_refusal(self, result: FetchResult) -> CircuitOpenError | None:
        """The refusal of the circuit breaker that a page of the crawl waits out; None if the breaker has no say in its outcome.

        Either the request was refused, or it was sent and failed while
        the circuit of its host opened, on its failure or on those of
        other requests: the breaker then refused the retries the page
        would have had, and its failure says no more about the page than
        about the host. The probe of the host, though, is the retry the
        breaker gave the page: a page whose probe failed fails with its error.
        So does a page with a `PermanentError`, such as HTTP 501: it would
        have had no retries.
        """
        error = result.error
        if isinstance(error, CircuitOpenError):
            return error
        breaker = self.circuit_breaker
        if (
            error is None
            or isinstance(error, PermanentError)
            or not breaker.is_failure(error)
            or breaker.opened_by_probe(error.url)
        ):
            return None
        message = breaker.refusal(error.url)
        return None if message is None else CircuitOpenError(error.url, message)

    def _defer_or_fail(
        self, url: str, queue: CrawlerQueue, refusal: CircuitOpenError, result: FetchResult | None = None
    ) -> bool:
        """Put off a page the circuit breaker refused until its host may be probed, or give up on it.

        The host is that of the refusal: the page may redirect to another
        one. `result` is that of the page's request, if one was made: a
        page given up on fails with the error of its request, if it got
        one, else with the refusal. Returns whether the page was put off
        rather than failed.
        """
        host = get_host(refusal.url)
        assert host is not None  # a URL without a host has no circuit
        opened = self.circuit_breaker.times_opened(host)
        if opened >= self.MAX_CIRCUIT_OPENINGS:
            logger.info("Gave up on %s: circuit breaker of %s opened %d times", url, host, opened)
            if result is not None and result.error is not None and result.error is not refusal:
                self._fail_page(url, queue, result.error, result)
            else:
                self._fail_page(url, queue, refusal)
            return False
        # Back when the probe may go; a page refused while the probe is in
        # flight comes back a second later.
        delay = self.circuit_breaker.probe_in(refusal.url) or 1.0
        logger.info("Deferred %s for %.1fs: %s", url, delay, refusal.message)
        queue.defer(url, delay, priority=queue.depth(url))
        return True


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
