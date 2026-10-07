"""The frontier of a crawl: the pages to crawl, their outcomes and the limits on how many are requested."""

import enum
import logging
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import ClassVar, NamedTuple

from crawler.queue import CrawlerQueue, queue_form
from crawler.urls import get_host

logger = logging.getLogger(__name__)


class FrontierPage(NamedTuple):
    """A page handed out by `Frontier.take`: its URL, in the form the frontier keeps it, and its depth."""

    url: str
    depth: int


class Outcome(enum.Enum):
    """How a page taken from the frontier was finished."""

    PROCESSED = "processed"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"
    UNREACHABLE = "unreachable"


class Admission(enum.Enum):
    """Whether a page taken from the frontier may be requested, see `Frontier.admit`."""

    ADMITTED = "admitted"
    OVER_MAX_PAGES = "over max_pages"
    OVER_HOST_LIMIT = "over max_pages_per_host"


class GivenUp(NamedTuple):
    """How a page of a host given up is finished, unrequested, see `Frontier.given_up`."""

    outcome: Outcome
    reason: str
    error: str | None = None  # the class of the error, for an outcome of failed


@dataclass(frozen=True)
class FrontierStats:
    queued: int = 0  # deferred pages included
    in_progress: int = 0
    processed: int = 0
    failed: int = 0
    skipped: int = 0
    blocked: int = 0
    unreachable: int = 0
    requested: int = 0  # pages counted toward max_pages
    over_host_limit: int = 0  # pages taken over max_pages_per_host
    links_dropped: int = 0  # links not queued: the frontier was full
    links_dropped_by_host: int = 0  # links not queued: their host had its share queued


@dataclass(frozen=True)
class HostFailures:
    """The failures of a host the crawl counts toward giving it up, see `Frontier.count_host_failures`."""

    circuit_openings: int = 0
    robots_failures: int = 0  # downloads of its robots.txt that failed since it was last read


class Frontier(ABC):
    """The pages of one crawl to request, whoever requests them, and the limits on how many.

    A page is accepted once, under the normalized form of its URL (see
    `queue_form`), and handed out by `take` lowest depth first:
    breadth-first. A page taken is finished with `finish` or goes back with
    `put_back`. A page processed whose record is still to be written by
    the storage is finished with `pending_save` and reported with `saved`
    once it is. Before it is requested it must be admitted: `admit` counts
    it toward `max_pages` and `max_pages_per_host`. A page that goes back
    unanswered, or is given up before its request, can be uncounted, so
    that it costs nothing of the limits until it is taken again. A page put
    back to wait for its host counts the wait (`waits`), so that the crawl
    gives it up after so many, whichever process takes it.

    The frontier is bounded: once the pages queued, in progress and
    requested reach `frontier_factor` times `max_pages`, found pages are not
    accepted (nor remembered), and a host has at most `frontier_factor`
    times `max_pages_per_host` pages accepted in the whole crawl. None
    means no limit.

    A frontier kept outside the process lists in `ERRORS` the exceptions
    its operations fail with when it cannot be reached. A crawl stops on
    them instead of failing the page: the pages it has in progress are not
    finished, and other processes crawl them once their leases expire.
    """

    # Room for the pages that do not count toward max_pages, such as those robots.txt disallows.
    FRONTIER_FACTOR = 3
    # Whether other processes take pages of this frontier too: what holds a
    # host back in one of them is then told to the frontier, see `hold_host`.
    shared = False
    # The errors of a frontier kept outside the process, such as in a
    # database, when it cannot be reached: a crawl stops on them.
    ERRORS: ClassVar[tuple[type[Exception], ...]] = ()

    def __init__(
        self,
        *,
        max_pages: int | None = None,
        max_pages_per_host: int | None = None,
        frontier_factor: int = FRONTIER_FACTOR,
    ) -> None:
        self.max_pages = max_pages
        self.max_pages_per_host = max_pages_per_host
        self.frontier_factor = frontier_factor
        # Called by `take` before it waits for the pages of other processes.
        self.on_waiting: Callable[[], Awaitable[None]] | None = None

    @abstractmethod
    async def seed(self, urls: Iterable[str]) -> list[str]:
        """Accept the start URLs at depth 0, whatever the bounds; the valid ones, in the form the frontier keeps them.

        A start URL accepted before, by this process or another one, is
        returned too, though it is not queued again.
        """

    @abstractmethod
    async def add(self, urls: Iterable[str], *, depth: int) -> int:
        """Accept the pages found at `depth`, as far as the bounds allow; the number of pages accepted."""

    @abstractmethod
    async def take(self) -> FrontierPage | None:
        """The next page to crawl; None once nothing is left to do or the frontier stops handing pages out.

        Waits while nothing is queued but pages in progress may find new
        ones, or pages put off are to come back. A frontier shared by
        several processes calls `on_waiting`, if it is set, before it waits
        for pages that other processes hold, with nothing queued: the caller
        writes out the records its storage buffers, whose pages the others
        wait for in turn. A frontier of one process never calls it.
        """

    @abstractmethod
    async def admit(self, page: FrontierPage) -> Admission:
        """Count a page taken toward the limits before it is requested, if they allow.

        A page over `max_pages` is to be put back, one over
        `max_pages_per_host` to be skipped.
        """

    @abstractmethod
    async def put_back(self, page: FrontierPage, delay: float = 0.0, *, uncount: bool, waited: bool = False) -> None:
        """Queue a page taken again, after `delay` seconds; with `uncount`, uncount it from the limits first.

        With `waited`, the page goes back to wait for its host, and the wait counts in `waits`.
        """

    @abstractmethod
    def waits(self, page: FrontierPage) -> int:
        """How many times a page taken was put back to wait for its host, by any process."""

    @abstractmethod
    async def finish(
        self,
        page: FrontierPage,
        outcome: Outcome,
        reason: str | None = None,
        *,
        uncount: bool = False,
        pending_save: bool = False,
        status: int | None = None,
        elapsed: float | None = None,
        error: str | None = None,
    ) -> None:
        """Record the outcome of a page taken; `reason` for all but `Outcome.PROCESSED`.

        With `uncount`, the page is uncounted from the limits: it was admitted, but nothing was sent.
        With `pending_save`, a page processed is done only once `saved`
        reports its record written: a frontier shared by several processes
        hands it out again if this one stops before that. The page counts as
        processed all the same, and `take` does not wait for it.

        `status` and `elapsed` are those of the response, if there was one,
        and `error` the class of the error a failed page failed with. A
        frontier shared by several processes keeps them for the statistics
        of the whole crawl; one of a single process does not need them, as
        the process counts its pages itself.
        """

    @abstractmethod
    async def saved(self, urls: Iterable[str]) -> None:
        """The records of these pages, finished with `pending_save`, are written, or dropped as ones no write can take."""

    @abstractmethod
    async def mark_seen(self, url: str, source: str) -> bool:
        """Remember a URL without accepting it, the target of a redirect of the page `source`; False if it was seen already.

        A URL is seen once accepted or remembered. An invalid one is never
        remembered and gives False. Checking and remembering are one step,
        so that two workers cannot both take a URL for new. A URL the page
        `source` remembered gives True to that page again: one put back
        after it followed the redirect follows it again, whoever takes it.
        A frontier that cannot keep a URL, one too long for a database,
        gives True every time and remembers nothing.
        """

    @abstractmethod
    async def forget(self, url: str, source: str) -> None:
        """Undo `mark_seen` of the page `source`: the URL may be accepted again. An accepted URL stays seen."""

    @abstractmethod
    async def is_pending_or_processed(self, url: str) -> bool:
        """Whether a URL was accepted and is still to be crawled, in progress or processed."""

    @abstractmethod
    async def full(self) -> bool:
        """Whether the pages queued, in progress and requested reach `frontier_factor` times `max_pages`."""

    @abstractmethod
    def stats(self) -> FrontierStats:
        """The counts of pages by state, as this process knows them; does not wait, so progress can be shown at any time."""

    @abstractmethod
    async def hold_out_of_scope(self, urls: Iterable[str]) -> None:
        """Keep pages the filters turned away, such as those of a sitemap on a host out of the scope of the crawl.

        A start URL may redirect to their host later and bring it into the
        scope, see `widen_scope`. They are not accepted, nor seen.
        """

    @abstractmethod
    async def widen_scope(self, host: str, allows: Callable[[str], bool]) -> int:
        """Bring `host` into the scope of the crawl; the number of pages held out of scope it queued.

        The pages held that `allows` lets through now are accepted at
        depth 0, as far as the bounds allow, and are not held any more; the
        others stay held. The host is listed by `scope_hosts`.
        """

    @abstractmethod
    def scope_hosts(self) -> list[str]:
        """The hosts `widen_scope` brought into the scope, in order, as far as this process knows them.

        Those of other processes become known once `take` hands out a page
        after them, so that the filters are brought up to date before it is crawled.
        """

    async def hold_host(self, host: str, seconds: float, reason: str | None) -> None:
        """Hand out no page of `host` for `seconds` from now, to any process, e.g. after a Retry-After.

        A hold never shortens one in place. `reason` says why, for the
        report of the crawl: it is that of the hold that ends last; None
        keeps the reason of the hold this one extends. A frontier of one
        process does nothing: its host is held back by the rate limiter of
        that process, and its pages are put off with a delay of their own.
        """

    async def set_host_interval(self, host: str, seconds: float) -> None:
        """Hand out the pages of `host` at least `seconds` apart, to all processes together, e.g. its Crawl-delay.

        An interval never goes down. The next page of the host waits for
        `seconds` from now: the process that tells the interval is about to
        send a request to the host. A frontier of one process does nothing:
        the rate limiter of that process keeps its requests apart.
        """

    async def count_host_failures(
        self, host: str, *, circuit_openings: int = 0, robots_failures: int = 0, robots_read: bool = False
    ) -> HostFailures:
        """Add the failures of `host` a process saw to those of the crawl; the counts of the crawl after it.

        `circuit_openings` counts the times its circuit opened anew,
        `robots_failures` the downloads of its robots.txt that failed anew;
        with `robots_read`, robots.txt was read since the failures told
        before, and they are counted from zero. The counts are added in one
        step: of the processes that count failures of a host at once, one
        sees the count reach a limit, and gives the host up (see
        `give_up_host`). A frontier of one process
        counts nothing and returns zeros: the counts of that process are
        those of the crawl.
        """
        return HostFailures()

    async def give_up_host(self, host: str, outcome: Outcome, reason: str, *, error: str | None = None) -> None:
        """Finish the pages of `host` queued, with `outcome`, `reason` and `error`, and so those accepted or put back later.

        A page of the host taken later says so with `given_up`, to be
        finished without a request. The first host given up keeps its
        outcome, reason and error. A frontier of one process does nothing:
        that process refuses the pages of the host itself.
        """

    def given_up(self, page: FrontierPage) -> GivenUp | None:
        """How a page taken is to be finished, unrequested, as its host is given up; None if it is not."""
        return None

    async def close(self) -> None:
        """Release what the frontier holds, such as its connections; a frontier in memory holds nothing."""


class MemoryFrontier(Frontier):
    """A `Frontier` held in memory, for one process: a `CrawlerQueue` and the counts of its limits.

    `queue` keeps the outcomes and depths of the pages. Once `max_pages`
    pages are admitted the queue is closed, so that the workers stop
    taking pages; a page uncounted reopens it.
    """

    def __init__(
        self,
        *,
        max_pages: int | None = None,
        max_pages_per_host: int | None = None,
        frontier_factor: int = Frontier.FRONTIER_FACTOR,
    ) -> None:
        super().__init__(max_pages=max_pages, max_pages_per_host=max_pages_per_host, frontier_factor=frontier_factor)
        self.queue = CrawlerQueue()
        self._max_queued = None if max_pages is None else frontier_factor * max_pages
        self._max_host_queued = None if max_pages_per_host is None else frontier_factor * max_pages_per_host
        self._requested = 0
        self._host_requested: Counter[str] = Counter()
        self._host_queued: Counter[str | None] = Counter()  # pages ever accepted by host
        self._over_host_limit = 0
        self._links_dropped = 0
        self._links_dropped_by_host = 0
        self._out_of_scope: list[str] = []
        self._scope_hosts: list[str] = []
        self._waits: Counter[str] = Counter()
        self._seen_from: dict[str, str] = {}  # URL remembered with mark_seen -> the page whose redirect led to it

    async def seed(self, urls: Iterable[str]) -> list[str]:
        seeded: dict[str, None] = {}
        for url in urls:
            form = queue_form(url)
            if form is None or form in seeded:
                continue
            seeded[form] = None
            if self.queue.add_url(form, priority=0, depth=0):
                self._host_queued[get_host(form)] += 1
        return list(seeded)

    async def add(self, urls: Iterable[str], *, depth: int) -> int:
        return sum(self._add(url, depth) for url in urls)

    async def take(self) -> FrontierPage | None:
        url = await self.queue.get_next()
        return None if url is None else FrontierPage(url, self.queue.depth(url))

    async def admit(self, page: FrontierPage) -> Admission:
        if self.max_pages is not None and self._requested >= self.max_pages:
            # Taken while another worker was still checking the page that reached the limit.
            return Admission.OVER_MAX_PAGES
        host = _host_of(page)
        if self.max_pages_per_host is not None and self._host_requested[host] >= self.max_pages_per_host:
            self._over_host_limit += 1
            return Admission.OVER_HOST_LIMIT
        self._requested += 1
        self._host_requested[host] += 1
        if self.max_pages is not None and self._requested >= self.max_pages:
            # This page is the last one allowed: the others stop taking new ones.
            self.queue.close()
        return Admission.ADMITTED

    async def put_back(self, page: FrontierPage, delay: float = 0.0, *, uncount: bool, waited: bool = False) -> None:
        # Uncounted first: a page put off reopens a closed queue and waits
        # its delay, rather than come back at once as it would after close.
        if uncount:
            self._uncount(page)
        if waited:
            self._waits[page.url] += 1
        if delay > 0:
            self.queue.defer(page.url, delay, priority=page.depth)
        else:
            self.queue.requeue(page.url, priority=page.depth)

    async def finish(
        self,
        page: FrontierPage,
        outcome: Outcome,
        reason: str | None = None,
        *,
        uncount: bool = False,
        pending_save: bool = False,
        status: int | None = None,
        elapsed: float | None = None,
        error: str | None = None,
    ) -> None:
        # One process: a page whose save is pending is done already, as it
        # cannot be handed out to another one.
        if uncount:
            self._uncount(page)
        self._waits.pop(page.url, None)
        if outcome is Outcome.PROCESSED:
            self.queue.mark_processed(page.url)
            return
        if reason is None:
            raise ValueError(f"a page {outcome.value} needs a reason")
        match outcome:
            case Outcome.FAILED:
                self.queue.mark_failed(page.url, reason)
            case Outcome.SKIPPED:
                self.queue.mark_skipped(page.url, reason)
            case Outcome.BLOCKED:
                self.queue.mark_blocked(page.url, reason)
            case Outcome.UNREACHABLE:
                self.queue.mark_unreachable(page.url, reason)

    def waits(self, page: FrontierPage) -> int:
        return self._waits[page.url]

    async def saved(self, urls: Iterable[str]) -> None:
        pass

    async def mark_seen(self, url: str, source: str) -> bool:
        form = queue_form(url)
        if form is None:
            return False
        if self.queue.is_seen(form):
            return self._seen_from.get(form) == source
        self.queue.mark_seen(form)
        self._seen_from[form] = source
        return True

    async def forget(self, url: str, source: str) -> None:
        form = queue_form(url)
        if form is not None and self._seen_from.get(form) == source:
            del self._seen_from[form]
            self.queue.forget(form)

    async def is_pending_or_processed(self, url: str) -> bool:
        return self.queue.is_pending_or_processed(url)

    async def full(self) -> bool:
        return self._full()

    async def hold_out_of_scope(self, urls: Iterable[str]) -> None:
        self._out_of_scope.extend(urls)

    async def widen_scope(self, host: str, allows: Callable[[str], bool]) -> int:
        if host not in self._scope_hosts:
            self._scope_hosts.append(host)
        in_scope, out_of_scope = [], []
        for url in self._out_of_scope:
            (in_scope if allows(url) else out_of_scope).append(url)
        self._out_of_scope = out_of_scope
        return await self.add(in_scope, depth=0)

    def scope_hosts(self) -> list[str]:
        return list(self._scope_hosts)

    def stats(self) -> FrontierStats:
        counts = self.queue.get_stats()
        return FrontierStats(
            queued=counts["queued"],
            in_progress=counts["in_progress"],
            processed=counts["processed"],
            failed=counts["failed"],
            skipped=counts["skipped"],
            blocked=counts["blocked"],
            unreachable=counts["unreachable"],
            requested=self._requested,
            over_host_limit=self._over_host_limit,
            links_dropped=self._links_dropped,
            links_dropped_by_host=self._links_dropped_by_host,
        )

    def _add(self, url: str, depth: int) -> bool:
        host = get_host(url)
        queue = self.queue
        if self._max_host_queued is not None and self._host_queued[host] >= self._max_host_queued:
            if not queue.closed and not queue.is_seen(url):
                self._links_dropped_by_host += 1
            return False
        if self._full():
            if not queue.is_seen(url):
                if not self._links_dropped:
                    logger.info(
                        "Queue is full: pages queued, in progress and requested reached %d (%d x max_pages); "
                        "new links are not queued until it has room",
                        self._max_queued,
                        self.frontier_factor,
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
                self.frontier_factor,
            )
        return True

    def _full(self) -> bool:
        if self._max_queued is None:
            return False
        return not self.queue.closed and self.queue.unfinished + self._requested >= self._max_queued

    def _uncount(self, page: FrontierPage) -> None:
        self._requested -= 1
        self._host_requested[_host_of(page)] -= 1
        if self.queue.closed:
            # The page reached max_pages and closed the queue; now it is
            # back under the limit, and the worker that holds it goes on to
            # crawl it, or the page that takes its place, even if the others have stopped.
            self.queue.reopen()


def _host_of(page: FrontierPage) -> str:
    host = get_host(page.url)
    assert host is not None  # the frontier holds valid URLs only
    return host
