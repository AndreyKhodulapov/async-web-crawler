"""Crawl layer of the crawler: one crawl, with its queue, filters, limits and counters."""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from contextlib import aclosing
from datetime import UTC, datetime
from urllib.parse import urlsplit

from crawler.circuit_breaker import CircuitState
from crawler.exceptions import (
    CircuitOpenError,
    CrawlerClosedError,
    FetchError,
    FrontierError,
    HostHeldBackError,
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
from crawler.frontier import Admission, Frontier, FrontierPage, HostFailures, Outcome
from crawler.models import CrawlStats, FetchResult, PageRecord, ParsedPage
from crawler.parser import is_html_content_type
from crawler.queue import queue_form
from crawler.semaphores import SemaphoreManager
from crawler.stats import CrawlerStats
from crawler.storage.base import DataStorage
from crawler.urls import get_host, normalize_url, strip_tracking_params

logger = logging.getLogger(__name__)


class CrawlRun:
    """One crawl of `AsyncCrawler.crawl()`: what is done with every page of its `Frontier`, and the counters.

    Pages are taken from `frontier`, which also counts them toward the
    limits, requested through the `Fetcher` of the crawler and parsed by
    `parse`; `stats` and `storage` are those of the crawler. A run is made
    once: every crawl() call makes a new one, so nothing of the previous
    crawl needs to be reset.
    """

    MAX_CIRCUIT_OPENINGS = 3
    # In crawl(), a page whose host is held back longer than this is put off;
    # in a crawl of several processes, even once it waits for its turn.
    MIN_PENALTY_TO_DEFER = 1.0
    # In crawl(), a page waits at most this many times for a Retry-After
    # too long to retry it, before it is given up; in a crawl of several
    # processes the waits for the held host of its redirect count too.
    MAX_WAITS_PER_PAGE = 3
    # In crawl(), the unreachable robots.txt of a site is downloaded again at
    # most this many times in a row before its pages, and the pages that
    # redirect to them, are given up; so are its sitemaps.
    MAX_ROBOTS_RETRIES = 3
    # In crawl(), a page waits at most this many seconds for a download of
    # robots.txt, then it is put off for this long at a time while the
    # download goes on: a site that is slow to fail holds no worker back.
    ROBOTS_POLL = 2.0
    # In a crawl of several processes, a storage that cannot write is tried
    # again after its cooldown, then after twice as long each time, up to this.
    MAX_STORAGE_PAUSE = 60.0
    # The constants above that AsyncCrawler hands over from itself to every run.
    SETTINGS = (
        "MAX_CIRCUIT_OPENINGS",
        "MIN_PENALTY_TO_DEFER",
        "MAX_WAITS_PER_PAGE",
        "MAX_ROBOTS_RETRIES",
        "ROBOTS_POLL",
        "MAX_STORAGE_PAUSE",
    )

    def __init__(
        self,
        fetcher: Fetcher,
        parse: Callable[[FetchResult], Awaitable[ParsedPage]],
        *,
        frontier: Frontier,
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
        self._frontier = frontier
        self.processed_urls: dict[str, ParsedPage] = {}
        self._start_urls: set[str] = set()
        self._scope_synced = 0  # hosts of Frontier.scope_hosts() the filter has
        self._failed_sitemaps: dict[str, str] = {}
        self._hosts_warned_held_back: set[str] = set()  # hosts whose long Retry-After was logged by the crawl
        # In a shared frontier: the failures of a host this process told it of, and the counts of the crawl it answered.
        self._failures_told: dict[str, HostFailures] = {}
        self._crawl_failures: dict[str, HostFailures] = {}
        self._hosts_given_up: set[str] = set()  # hosts this process gave up in a shared frontier
        self._pages_to_save = 0
        self._storage_retry = asyncio.Lock()  # one worker writes the storage again, the others wait for it
        self._written_before = 0
        self._pending_before = 0
        self._crawl_started: float | None = None
        self._crawl_finished: float | None = None

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
        *,
        url_filter: UrlFilter,
        sitemap_urls: list[str],
        robots_sitemaps: bool,
    ) -> dict[str, ParsedPage]:
        """Crawl from the start URLs, as `AsyncCrawler.crawl()` describes; the arguments are checked by it.

        The limits on the pages requested are those of the frontier.
        """
        if self._crawl_started is not None:
            raise RuntimeError("a crawl run is made once")
        self._written_before = self.storage.written if self.storage is not None else 0
        self._pending_before = self.storage.pending if self.storage is not None else 0
        self._fetcher.reset_stats()
        self.rate_limiter.reset_stats()
        self.circuit_breaker.reset_stats()
        if self.robots is not None:
            self.robots.forget_outages()

        logger.info(
            "Crawl started: %d start URLs, max_depth=%d, max_pages=%s",
            len(start_urls),
            self.max_depth,
            self._frontier.max_pages,
        )
        self._crawl_started, self._crawl_finished = time.perf_counter(), None
        self.stats.start()
        if self.storage is not None:
            # A page saved is done in the frontier once its record is written;
            # other processes that wait for it are not kept waiting by the buffer.
            self.storage.on_settled = self._frontier.saved
            self._frontier.on_waiting = self._flush_storage
        if self._frontier.shared:
            # A host held back by this process is held back by the others
            # too, and its Crawl-delay spaces the requests of all of them.
            self._fetcher.on_host_held = self._frontier.hold_host
            self._fetcher.on_crawl_delay = self._frontier.set_host_interval
        try:
            try:
                await self.seed(
                    start_urls, url_filter=url_filter, sitemap_urls=sitemap_urls, robots_sitemaps=robots_sitemaps
                )
                await self._crawl_pages(url_filter)
            except self._frontier.ERRORS as error:
                logger.error("The crawl stops: its frontier failed: %s: %s", type(error).__name__, error)
                # The pages done so far are stored all the same; those in progress come back to others.
                await self._flush_storage()
                raise FrontierError(f"the frontier failed: {type(error).__name__}: {error}") from error
            except asyncio.CancelledError:
                if self.storage is not None:
                    # Written while `on_settled` still stands: the pages of
                    # the buffer are saved in the frontier before it is
                    # closed, so that no other process crawls them again.
                    logger.info(
                        "The crawl is cancelled: writing the %d pages the storage buffers", self.storage.pending
                    )
                    await self._flush_storage()
                raise
            await self._flush_storage()
            # A worker of a shared frontier stops only once the pages it did are stored.
            await self._wait_for_storage()
        finally:
            if self.storage is not None:
                self.storage.on_settled = None
                self._frontier.on_waiting = None
            if self._frontier.shared:
                self._fetcher.on_host_held = None
                self._fetcher.on_crawl_delay = None
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
        frontier = self._frontier.stats()
        if frontier.links_dropped:
            logger.info("%d links were not queued: the queue was full", frontier.links_dropped)
        if frontier.links_dropped_by_host:
            logger.info(
                "%d links were not queued: their host had %d x max_pages_per_host pages queued",
                frontier.links_dropped_by_host,
                self._frontier.frontier_factor,
            )
        if self.storage is not None:
            logger.info(
                "Saved %d pages to %s, %d not saved", stats.saved, type(self.storage).__name__, stats.save_failed
            )
        return self.processed_urls

    async def _crawl_pages(self, url_filter: UrlFilter) -> None:
        """Crawl the pages of the frontier with `max_concurrent` workers; an error of the frontier stops them all."""
        try:
            async with asyncio.TaskGroup() as group:
                for _ in range(self.max_concurrent):
                    group.create_task(self._crawl_worker(url_filter))
        except ExceptionGroup as errors:
            if (error := _first_of(errors, self._frontier.ERRORS)) is None:
                raise
            raise error from None

    async def seed(
        self, start_urls: list[str], *, url_filter: UrlFilter, sitemap_urls: list[str], robots_sitemaps: bool
    ) -> None:
        """Queue the start URLs, then the pages of the sitemaps, as `run` does before the first page is taken.

        Called alone, it fills a frontier that others crawl, such as that of
        a job in a database. A start URL seeded already is not queued again.
        """
        self._start_urls = set(await self._frontier.seed(start_urls))
        if sitemap_urls or robots_sitemaps:
            await self._queue_sitemap_pages(sitemap_urls, start_urls if robots_sitemaps else [], url_filter)

    async def _queue_sitemap_pages(self, sitemap_urls: list[str], robots_of: list[str], url_filter: UrlFilter) -> None:
        """Queue the pages listed in `sitemap_urls` and in the sitemaps robots.txt of the sites of `robots_of` names.

        The sitemaps are read one by one, in order, and only until the
        queue is full: the rest of a sitemap and the sitemaps after it are
        not downloaded, so a crawl of a few pages does not read an index
        of hundreds of files for them.
        """
        async with asyncio.TaskGroup() as group:
            named = [group.create_task(self._sitemaps_in_robots(url)) for url in robots_of]
        # Normalized, so a sitemap given twice, or given and named in robots.txt, is read once.
        sitemaps = dict.fromkeys(url for url in map(normalize_url, sitemap_urls) if url is not None)
        for task in named:
            sitemaps.update(dict.fromkeys(task.result()))
        opened = listed = queued = 0
        for sitemap in sitemaps:
            if await self._frontier.full():
                break
            opened += 1
            async with aclosing(self._read_sitemap(sitemap)) as batches:
                async for pages in batches:
                    listed += len(pages)
                    queued += await self._queue_sitemap_batch(pages, url_filter)
                    if await self._frontier.full():
                        logger.info("Stopped reading sitemaps at %s: the queue is full", sitemap)
                        break
        logger.info(
            "Sitemaps: %d read, %d failed, %d not read, %d pages listed, %d new queued",
            opened - len(self._failed_sitemaps),
            len(self._failed_sitemaps),
            len(sitemaps) - opened,
            listed,
            queued,
        )

    async def _queue_sitemap_batch(self, pages: list[str], url_filter: UrlFilter) -> int:
        """Queue the sitemap pages that pass the filter; the number queued.

        The sitemaps are read before the first page, when only the hosts of
        the start URLs are known: the pages of "example.com" are out of
        scope until "example.org" redirects there, so the frontier holds
        them until then (see `_widen_scope`).
        """
        allowed, turned_away = [], []
        for page in pages:
            (allowed if url_filter.allows(page) else turned_away).append(page)
        # Hosts join the scope only under `same_domain_only`.
        if turned_away and url_filter.allowed_hosts is not None:
            await self._frontier.hold_out_of_scope(turned_away)
        return await self._frontier.add(allowed, depth=0)

    async def _widen_scope(self, final_url: str, url_filter: UrlFilter) -> None:
        """Bring the host a start URL redirected to into the scope, with the sitemap pages held for it."""
        host = get_host(final_url)
        if url_filter.allowed_hosts is None or host is None or host in url_filter.allowed_hosts:
            return
        url_filter.allow_host(host)
        await self._frontier.widen_scope(host, url_filter.allows)

    def _sync_scope(self, url_filter: UrlFilter) -> None:
        """Let through the hosts other processes brought into the scope of a shared frontier."""
        hosts = self._frontier.scope_hosts()
        if len(hosts) != self._scope_synced:
            for host in hosts:
                url_filter.allow_host(host)
            self._scope_synced = len(hosts)

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
                    await self._seed_crawl_delay(url)
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

    async def _seed_crawl_delay(self, url: str) -> None:
        """In a frontier shared by several processes, space the pages of the host of `url` by its Crawl-delay before they are taken.

        Otherwise the workers would learn it from the first pages they take,
        those of the start URLs among them, and request them all at once.
        """
        host = get_host(url)
        if self._frontier.shared and host is not None and (delay := self._fetcher.crawl_delay(url)):
            await self._frontier.set_host_interval(host, delay)

    async def _read_sitemap(self, url: str) -> AsyncGenerator[list[str], None]:
        """The pages a sitemap lists, in the batches of `SitemapParser.iter_pages`; none if it cannot be read, which is logged.

        While robots.txt of its site is unreachable, the sitemap waits for
        it to be downloaded again, as a page of the crawl does (see
        `_robots_wait`): a crawl fed by sitemaps alone would otherwise end
        empty after a 503 of a few seconds. A sitemap fails, if at all,
        before its first pages are yielded, so reading it again from the
        start yields no page twice.
        """
        while True:
            try:
                async with aclosing(self.sitemaps.iter_pages(url)) as batches:
                    async for pages in batches:
                        yield pages
                return
            except FetchError as error:
                delay = self._robots_wait(error.url) if isinstance(error, RobotsUnreachableError) else None
                if delay is None:
                    reason = f"{type(error).__name__}: {error.message}"
                    logger.warning("Sitemap %s is left out: %s", url, reason)
                    self._failed_sitemaps[url] = reason
                    return
                logger.info("Sitemap %s waits %.1fs: %s", url, delay, error.message)
                await asyncio.sleep(delay)

    def crawl_stats(self) -> CrawlStats:
        """Progress of the running crawl, or the result of the latest one."""
        if self._crawl_started is None:
            return CrawlStats()
        frontier = self._frontier.stats()
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
            processed=frontier.processed,
            failed=frontier.failed,
            skipped=frontier.skipped,
            over_host_limit=frontier.over_host_limit,
            blocked=frontier.blocked,
            unreachable=frontier.unreachable,
            queued=frontier.queued,
            in_progress=frontier.in_progress,
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

    async def _crawl_worker(self, url_filter: UrlFilter) -> None:
        frontier = self._frontier
        while True:
            await self._wait_for_storage()
            if (page := await frontier.take()) is None:
                return
            url = page.url
            self._sync_scope(url_filter)
            try:
                if (given_up := frontier.given_up(page)) is not None:
                    # Its host was given up for the crawl, by this process or another one.
                    await frontier.finish(page, *given_up)
                    continue
                # A host that asked to wait (Retry-After) or waits out the
                # pause before a retry: the worker takes pages of other hosts
                # meanwhile instead of waiting in the rate limiter.
                if (penalty := self._penalty_left(url)) > self.MIN_PENALTY_TO_DEFER:
                    self._warn_once_held_back(url, penalty)
                    await self._put_off_for_host(page, url, penalty, "its host is held back", uncount=False)
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
                if frontier.shared:
                    # robots.txt may have been read after it failed: the crawl
                    # counts its failures from zero again.
                    await self._host_failures(url)
                if refusal is not None:
                    if isinstance(refusal, RobotsDisallowedError):
                        await frontier.finish(page, Outcome.BLOCKED, refusal.message)
                    elif isinstance(refusal, RobotsUnreachableError):
                        if not await self._wait_for_robots(page, refusal, requested=False):
                            reason = self._unreachable_reason(refusal)
                            logger.info("Gave up on %s: %s", url, reason)
                            await frontier.finish(page, Outcome.UNREACHABLE, reason)
                    elif isinstance(refusal, CircuitOpenError):
                        await self._defer_or_fail(page, refusal)
                    else:
                        await self._fail_page(page, refusal)
                    continue
                match await frontier.admit(page):
                    case Admission.OVER_MAX_PAGES:
                        await frontier.put_back(page, uncount=False)
                        continue
                    case Admission.OVER_HOST_LIMIT:
                        # Not requested, so not counted toward max_pages.
                        reason = f"max_pages_per_host reached: {frontier.max_pages_per_host} pages of {get_host(url)}"
                        await self._skip_page(page, reason)
                        continue
                await self._crawl_page(page, url_filter)
            except frontier.ERRORS:
                # The frontier cannot be reached: the page stays in progress
                # there, and comes back to another process once its lease expires.
                raise
            except Exception as exc:
                # Fetcher.fetch() reports expected failures in the result, so this
                # is a bug; it must not kill the worker, and the page
                # must leave the in-progress state, or take() would wait forever.
                logger.exception("Unexpected error while crawling %s", url)
                await self._fail_page(page, UnexpectedError(url, f"{type(exc).__name__}: {exc}"))

    async def _crawl_page(self, page: FrontierPage, url_filter: UrlFilter) -> None:
        url, depth = page
        skip_reason: str | None = None
        sent = False  # a request of the page got an answer: a redirect
        targets: list[str] = []  # the redirects followed, remembered as seen for this page

        async def follow(target: str) -> bool:
            nonlocal skip_reason, sent
            sent = True
            skip_reason = await self._redirect_refusal(url, target, url_filter)
            if skip_reason is None:
                targets.append(target)
            return skip_reason is None

        result = await self._fetcher.fetch(
            url,
            html_only=True,
            check_robots=False,
            follow=follow,
            robots_wait=self.ROBOTS_POLL,
            max_wait=self._max_wait(page),
        )
        if isinstance(result.error, HostHeldBackError):
            await self._put_back_held(page, result.error, requested=sent)
            return
        if (refusal := self._circuit_refusal(result)) is not None:
            # The circuit of the host, or of the host a redirect leads to,
            # opened while the request waited for its turn or was in flight.
            # A request was sent if it failed with its own error, or if it
            # was answered with a redirect whose target was refused.
            requested = sent or refusal is not result.error
            if not await self._defer_or_fail(page, refusal, result, counted=True, requested=requested):
                await self._forget_redirects(url, targets)
            return
        if self._outwaits_retries(result.error) and await self._wait_for_host(page, result.error):
            return
        # The worker has checked robots.txt for the page; these are about the target of its redirect.
        if isinstance(result.error, RobotsUnreachableError) and await self._wait_for_robots(
            page, result.error, requested=True
        ):
            return
        if result.error is not None:
            # The page is not crawled: its redirect targets are no longer
            # seen, so that a direct link to one of them is still followed.
            await self._forget_redirects(url, targets)
        if isinstance(result.error, RobotsDisallowedError):
            reason = f"redirects to {result.error.url}, {result.error.message}"
            await self._frontier.finish(page, Outcome.BLOCKED, reason)
            return
        if isinstance(result.error, RobotsUnreachableError):
            reason = f"redirects to {result.error.url}, {self._unreachable_reason(result.error)}"
            logger.info("Gave up on %s: %s", url, reason)
            await self._frontier.finish(page, Outcome.UNREACHABLE, reason)
            return
        if result.error is not None:
            # The circuit of the host may have opened on this failure: a
            # failed probe opens it again, and its page fails rather than waits.
            await self._hold_open_circuit(result.error.url)
            await self._fail_page(page, result.error, result)
            return
        if url in self._start_urls and result.redirected and skip_reason is None:
            # A start URL that redirects ("example.org" -> "example.com")
            # defines the site as much as the URL itself; the hosts the chain
            # only passed through (a consent page) do not. A page from a
            # sitemap has depth 0 too, but is filtered like a link.
            assert result.final_url is not None
            await self._widen_scope(result.final_url, url_filter)
        if skip_reason is None and not is_html_content_type(result.content_type):
            # A link without a file extension may still lead to a PDF or an
            # image. Not a failure: the page is fine, just not one to parse.
            skip_reason = f"not HTML: {result.content_type}"
        if skip_reason is not None:
            await self._skip_page(page, skip_reason, result)
            return

        try:
            parsed = await self._parse(result)
        except ParseError as error:
            logger.warning("Failed to parse %s: %s", url, error.message)
            await self._fail_page(page, error, result)
            return
        duplicate = await self._duplicate_of(url, result.final_url, parsed)
        if duplicate is not None:
            # A variant of a page already seen ("?sort=price" of "/list"):
            # its links are variants too, so they are not followed.
            await self._skip_page(page, f"duplicate of {duplicate}", result)
            return
        noindex, nofollow = self._robots_directives(result, parsed)
        queued = 0
        if depth < self.max_depth and not nofollow:
            links = [link for link in parsed["links"] if url_filter.allows(link)]
            queued = await self._frontier.add(links, depth=depth + 1)
        if noindex is not None:
            # The site asks not to keep the page; its links may still be followed.
            await self._skip_page(page, noindex, result, links_queued=queued)
            return
        if self.keep_pages:
            self.processed_urls[url] = parsed
        # With a storage, the page is done once its record is written (see run).
        await self._frontier.finish(page, Outcome.PROCESSED, pending_save=self.storage is not None)
        self.stats.record_page(url, status=result.status, elapsed=result.elapsed)
        logger.info("Crawled %s (depth %d): %d links, %d new queued", url, depth, len(parsed["links"]), queued)
        if self.storage is not None:
            await self._save_page(_page_record(result, parsed, depth))

    async def _duplicate_of(self, url: str, final_url: str | None, page: ParsedPage) -> str | None:
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
        return canonical if await self._frontier.is_pending_or_processed(target) else None

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

    async def _redirect_refusal(self, url: str, target: str, url_filter: UrlFilter) -> str | None:
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
        # loop; they are followed up to MAX_REDIRECTS. A page retried, or
        # put back and taken again, follows the targets it marked as seen.
        # Compared without tracking parameters, as the queue keeps URLs.
        if strip_tracking_params(target) != url and not await self._frontier.mark_seen(target, url):
            return f"redirected to a page already seen: {target}"
        return None

    async def _forget_redirects(self, url: str, targets: Iterable[str]) -> None:
        """Let the redirect targets of the page `url` be queued again: the page failed, so they were not crawled."""
        for target in targets:
            await self._frontier.forget(target, url)

    async def _fail_page(
        self, page: FrontierPage, error: FetchError, result: FetchResult | None = None, *, uncount: bool = False
    ) -> None:
        """Finish a page of the crawl as failed; `result` is that of its request, None if none was sent.

        With `uncount`, the page counted toward the limits is uncounted: nothing was sent for it.
        """
        reason = f"{type(error).__name__}: {error.message}"
        await self._frontier.finish(page, Outcome.FAILED, reason, uncount=uncount)
        self.stats.record_page(
            page.url,
            status=None if result is None else result.status,
            elapsed=None if result is None else result.elapsed,
            error=type(error).__name__,
        )

    async def _skip_page(
        self,
        page: FrontierPage,
        reason: str,
        result: FetchResult | None = None,
        *,
        links_queued: int | None = None,
    ) -> None:
        """Finish a page of the crawl as skipped; `result` is that of its request, None if none was sent.

        `links_queued` is how many new links of the page were queued, for the log; None if they were not followed.
        """
        if links_queued is None:
            logger.info("Skipped %s: %s", page.url, reason)
        else:
            logger.info("Skipped %s: %s; %d new links queued", page.url, reason, links_queued)
        await self._frontier.finish(page, Outcome.SKIPPED, reason)
        self.stats.record_page(
            page.url,
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
            logger.error("Failed to save the pages the storage buffers: %s", error)
        except Exception:
            logger.exception("Unexpected error while saving the pages the storage buffers")

    async def _wait_for_storage(self) -> None:
        """In a shared frontier, take no page while the storage cannot write; a worker writes it again meanwhile.

        The pages the storage buffers stay in progress in the frontier, so
        that no other process takes them, until they are written. The first
        try is after the storage's `cooldown`, then each pause is twice as
        long, up to `MAX_STORAGE_PAUSE`; it goes on for as long as it takes.
        """
        storage = self.storage
        if storage is None or not self._frontier.shared or not storage.write_failed:
            return
        async with self._storage_retry:
            if not storage.write_failed:
                return  # another worker of this process wrote it meanwhile
            pause = storage.cooldown
            logger.warning(
                "%s cannot write %d records: no page is taken until it can; trying again in %gs",
                type(storage).__name__,
                storage.pending,
                pause,
            )
            while storage.write_failed:
                await asyncio.sleep(pause)
                pause = min(2 * pause, self.MAX_STORAGE_PAUSE)
                try:
                    await storage.flush()
                except StorageError as error:
                    logger.warning(
                        "%s still cannot write %d records: %s; trying again in %gs",
                        type(storage).__name__,
                        storage.pending,
                        error,
                        pause,
                    )
                except Exception:
                    # Records no write can take are dropped: the others are written.
                    logger.exception("Unexpected error while saving the pages the storage buffers")
            logger.info("%s writes again: pages are taken again", type(storage).__name__)

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
        return CircuitOpenError(url, self._no_more_probes(host, opened))

    @staticmethod
    def _no_more_probes(host: str, opened: int) -> str:
        return f"circuit breaker of {host} opened {opened} times, no more probes in this crawl"

    async def _host_failures(self, url: str) -> HostFailures:
        """The failures of the host of `url` that count toward giving it up; in a frontier shared by several processes, those of all of them.

        This process tells the frontier of its failures it has not told
        yet, and of robots.txt read since it told the last ones, which
        counts them from zero. With nothing new to tell, the counts are those
        the frontier answered last: the process whose failure brings a
        count to a limit is the one that gives the host up (see `_give_up_host`).
        """
        host = get_host(url)
        assert host is not None  # the frontier holds valid URLs only
        here = HostFailures(
            self.circuit_breaker.times_opened(host),
            0 if self.robots is None else self.robots.failed_downloads(url),
        )
        if not self._frontier.shared:
            return here
        told = self._failures_told.get(host, HostFailures())
        if here == told:
            return self._crawl_failures.get(host, here)
        # Told before the frontier answers: another task of this process
        # that asks meanwhile must not tell the same failures again.
        self._failures_told[host] = here
        read = here.robots_failures < told.robots_failures
        failures = await self._frontier.count_host_failures(
            host,
            circuit_openings=here.circuit_openings - told.circuit_openings,
            robots_failures=here.robots_failures - (0 if read else told.robots_failures),
            robots_read=read,
        )
        self._crawl_failures[host] = failures
        return failures

    async def _give_up_host(self, url: str, outcome: Outcome, reason: str) -> None:
        """In a frontier shared by several processes, give the host of `url` up for all of them, once.

        Its pages are finished with `outcome` and `reason`, unrequested,
        whichever process takes them; in a crawl of one process each page
        is refused as it is taken.
        """
        host = get_host(url)
        assert host is not None  # a URL without a host is not requested
        if not self._frontier.shared or host in self._hosts_given_up:
            return
        self._hosts_given_up.add(host)
        await self._frontier.give_up_host(host, outcome, reason)

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

    def _max_wait(self, page: FrontierPage) -> float | None:
        """How long a request of the page waits for a host held back; None for as long as it is held.

        In a crawl of several processes, a page whose host is held back
        after it was taken goes back to the queue rather than keep the worker
        waiting (see `_put_back_held`); once it has waited `MAX_WAITS_PER_PAGE`
        times it waits in the rate limiter, as in a crawl of one process.
        """
        if not self._frontier.shared or self._frontier.waits(page) >= self.MAX_WAITS_PER_PAGE:
            return None
        return self.MIN_PENALTY_TO_DEFER

    async def _put_back_held(self, page: FrontierPage, error: HostHeldBackError, *, requested: bool) -> None:
        """Put back a page whose request was not sent: its host, or that of its redirect, is held back since it was taken.

        Nothing was sent to the host held back, so the page is uncounted. A
        page that was `requested` was answered with a redirect: it waits,
        and the wait counts toward `MAX_WAITS_PER_PAGE`, as for a page
        asked to wait (see `_wait_for_host`).
        """
        self._warn_once_held_back(error.url, error.seconds)
        reason = "its host is held back" if not requested else f"redirects to {error.url}, whose host is held back"
        await self._put_off_for_host(page, error.url, error.seconds, reason, uncount=True, waited=requested)

    def _penalty_left(self, url: str) -> float:
        """Seconds the host of `url` is still held back for, after Retry-After or before a retry."""
        host = get_host(url)
        return 0.0 if host is None else self.rate_limiter.penalty_left(host)

    def _robots_wait(self, url: str, failed_downloads: int | None = None) -> float | None:
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
        is about to, and nobody else waits for it. `failed_downloads` are
        those of the crawl, if not those of this process alone.
        """
        assert self.robots is not None  # asked after it refused a URL
        if failed_downloads is None:
            failed_downloads = self.robots.failed_downloads(url)
        if not self.robots.may_recover(url) or failed_downloads > self.MAX_ROBOTS_RETRIES:
            # Nothing of the crawl downloads it again: a page that looks in
            # on the site later would otherwise make one more download.
            self.robots.give_up(url)
            return None
        return self.robots.unreachable_for(url) or self.ROBOTS_POLL

    async def _wait_for_robots(self, page: FrontierPage, refusal: RobotsUnreachableError, *, requested: bool) -> bool:
        """Put off a page of the crawl until the robots.txt that refused it is downloaded again, if it is worth waiting for.

        The refusal is for the page itself or for the target of its
        redirect; `requested` says whether the page was requested (it
        redirected): it is then uncounted, as the request was not answered
        with the page, and made again when it comes back. Returns False
        when the site is given up (see `_robots_wait`), and the caller
        marks the page unreachable.

        In a frontier shared by several processes, the site is held back
        for all of them while its robots.txt is known to be unreachable. A
        download still under way holds back the page alone: robots.txt may
        well be read, and the other processes download their own. The
        failed downloads of all of them count, and the site is given up
        for all of them.
        """
        failures = await self._host_failures(refusal.url)
        delay = self._robots_wait(refusal.url, failures.robots_failures)
        if delay is None:
            await self._give_up_host(refusal.url, Outcome.UNREACHABLE, self._unreachable_reason(refusal))
            return False
        assert self.robots is not None  # it refused the URL
        if self.robots.unreachable_for(refusal.url) > 0:
            reason = self._unreachable_reason(refusal)
            await self._put_off_for_host(
                page, refusal.url, delay, refusal.message, uncount=requested, hold_reason=reason
            )
        else:
            await self._put_off_page(page, delay, refusal.message, uncount=requested)
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

    async def _put_off_page(
        self, page: FrontierPage, delay: float, reason: str, *, uncount: bool, waited: bool = False
    ) -> None:
        """Put a page of the crawl back into the queue for `delay` seconds.

        With `uncount`, the page is uncounted from the limits first: it
        is counted again when it is taken again. With `waited`, it counts
        a wait for its host (see `Frontier.waits`).
        """
        logger.info("Deferred %s for %.1fs: %s", page.url, delay, reason)
        await self._frontier.put_back(page, delay, uncount=uncount, waited=waited)

    async def _put_off_for_host(
        self,
        page: FrontierPage,
        url: str,
        delay: float,
        reason: str,
        *,
        uncount: bool,
        waited: bool = False,
        hold_reason: str | None = None,
    ) -> None:
        """Put a page of the crawl off while the host of `url` is held back, for `delay` seconds.

        `url` is the page's own or the target of its redirect. In a frontier
        shared by several processes the host is held back for all of them
        too, for `hold_reason`. None keeps the reason the fetcher gave: it
        has told the frontier of its holds already, and this one is for a
        page taken before the hold reached the frontier. A page of the host
        goes back without a delay of its own: it comes back with the host,
        however long the others hold it. A page that redirects to the host
        waits out the delay itself, as its own host is not held back: it
        would be handed out at once and redirect to the held one again.
        """
        if not self._frontier.shared:
            await self._put_off_page(page, delay, reason, uncount=uncount, waited=waited)
            return
        host = get_host(url)
        assert host is not None  # a URL without a host is not requested
        await self._fetcher.tell_host_held(host, delay, hold_reason)
        logger.info("Deferred %s for %.1fs: %s", page.url, delay, reason)
        own_delay = 0.0 if host == get_host(page.url) else delay
        await self._frontier.put_back(page, own_delay, uncount=uncount, waited=waited)

    async def _hold_open_circuit(self, url: str) -> None:
        """In a frontier shared by several processes, hold the host of `url` back for all of them while its circuit is open here.

        For a page that fails while the circuit is open, as one put off
        holds the host back (see `_defer_or_fail`). The breaker of each
        process is its own: the others would ask the host on otherwise.
        Once the circuits of all of them have opened `MAX_CIRCUIT_OPENINGS`
        times, the host is given up instead.
        """
        if not self._frontier.shared or (probe_in := self.circuit_breaker.probe_in(url)) == 0:
            return
        host = get_host(url)
        assert host is not None  # a URL without a host has no circuit
        opened = (await self._host_failures(url)).circuit_openings
        if opened >= self.MAX_CIRCUIT_OPENINGS:
            await self._give_up_host(url, Outcome.FAILED, self._no_more_probes(host, opened))
        else:
            await self._fetcher.tell_host_held(host, probe_in, self.circuit_breaker.refusal(url))

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

    async def _wait_for_host(self, page: FrontierPage, error: HTTPStatusError) -> bool:
        """Put off a page whose request got a Retry-After too long to retry, until its host may be asked again.

        The host is held back for that long anyway (see `Fetcher`): the
        retry strategy's reason not to retry, that coming back early earns
        another refusal, does not hold for a page that comes back with the
        host. The host is that of the failed request: the page may redirect
        to another one. Returns False once the page has waited
        `MAX_WAITS_PER_PAGE` times: the caller fails it then.
        """
        if self._frontier.waits(page) >= self.MAX_WAITS_PER_PAGE:
            return False
        delay = self._penalty_left(error.url) or 1.0
        # The request was answered, but not with the page: it is not a page requested.
        await self._put_off_for_host(page, error.url, delay, error.message, uncount=True, waited=True)
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

    async def _defer_or_fail(
        self,
        page: FrontierPage,
        refusal: CircuitOpenError,
        result: FetchResult | None = None,
        *,
        counted: bool = False,
        requested: bool = False,
    ) -> bool:
        """Put off a page the circuit breaker refused until its host may be probed, or give up on it.

        The host is that of the refusal: the page may redirect to another
        one. `result` is that of the page's request, if one was made: a
        page given up on fails with the error of its request, if it got
        one, else with the refusal. A page `counted` toward the limits is
        uncounted when put off, until it is taken again, as no request was
        answered with the page; given up on, it stays counted if it was
        `requested`. Returns whether the page was put off rather than
        failed.

        In a frontier shared by several processes, the host is held back
        for all of them until the probe is due. A probe in flight holds
        back the page alone: when it ends is not known, and the circuits
        of the other processes are their own. The openings of all of them
        count, and the host is given up for all of them.
        """
        host = get_host(refusal.url)
        assert host is not None  # a URL without a host has no circuit
        opened = (await self._host_failures(refusal.url)).circuit_openings
        if opened >= self.MAX_CIRCUIT_OPENINGS:
            logger.info("Gave up on %s: circuit breaker of %s opened %d times", page.url, host, opened)
            await self._give_up_host(refusal.url, Outcome.FAILED, self._no_more_probes(host, opened))
            uncount = counted and not requested
            if result is not None and result.error is not None and result.error is not refusal:
                await self._fail_page(page, result.error, result, uncount=uncount)
            else:
                await self._fail_page(page, refusal, uncount=uncount)
            return False
        # Back when the probe may go; a page refused while the probe is in
        # flight comes back a second later.
        if (probe_in := self.circuit_breaker.probe_in(refusal.url)) > 0:
            await self._put_off_for_host(
                page, refusal.url, probe_in, refusal.message, uncount=counted, hold_reason=refusal.message
            )
        else:
            await self._put_off_page(page, 1.0, refusal.message, uncount=counted)
        return True


def _first_of(errors: BaseExceptionGroup, kinds: tuple[type[Exception], ...]) -> BaseException | None:
    """The first exception of `errors`, nested groups included, that is one of `kinds`; None if there is none."""
    matched = errors.subgroup(kinds)
    while isinstance(matched, BaseExceptionGroup):
        matched = matched.exceptions[0]
    return matched


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
