"""Priority queue of URLs to crawl that also tracks the status of every URL."""

import asyncio
import heapq
import itertools
from collections.abc import Mapping
from types import MappingProxyType

from crawler.urls import normalize_url, strip_tracking_params


class CrawlerQueue:
    """URLs waiting to be crawled, ordered by priority, plus their outcomes.

    A lower `priority` value is served first; URLs with equal priority come
    out in the order they were added. Every URL is normalized, stripped of
    tracking parameters such as "utm_source" (see `strip_tracking_params`)
    and accepted at most once, so a page is never queued twice, whether it
    is still waiting, being fetched or already done.

    Lifecycle of a URL: `add_url` -> `get_next` (in progress) ->
    `mark_processed`, `mark_failed`, `mark_skipped`, `mark_blocked` or
    `mark_unreachable`; `requeue` and `defer` put it back unfetched.
    Workers loop until `get_next` returns None, which happens when there is
    nothing left to do (see `get_next`) or after `close`.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        # Tie-breaker for equal priorities: keeps FIFO order and means the
        # heap never has to compare URLs.
        self._sequence = itertools.count()
        self._depths: dict[str, int] = {}  # every accepted URL
        self._seen: set[str] = set()  # accepted URLs plus redirect targets
        self._in_progress: set[str] = set()
        # Deferred URL -> (its heap entry, the timer that pushes it).
        self._deferred: dict[str, tuple[tuple[int, int, str], asyncio.TimerHandle]] = {}
        self._processed_count = 0
        self._wakeup = asyncio.Event()
        self._closed = False
        self.visited: set[str] = set()  # URLs handed out by get_next
        self.failed: dict[str, str] = {}  # URL -> error description
        self.skipped: dict[str, str] = {}  # URL -> why it was left out
        self.blocked: dict[str, str] = {}  # URL -> why it may not be fetched
        self.unreachable: dict[str, str] = {}  # URL -> why its site's rules are unknown

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def unfinished(self) -> int:
        """URLs queued, deferred or in progress: accepted and not finished yet."""
        return len(self._heap) + len(self._deferred) + len(self._in_progress)

    @property
    def depths(self) -> Mapping[str, int]:
        """Read-only view: accepted URL -> depth it was found at."""
        return MappingProxyType(self._depths)

    def add_url(self, url: str, priority: int = 0, *, depth: int = 0) -> bool:
        """Queue a URL; return False if it is invalid, already seen, or the queue is closed."""
        normalized = _queue_form(url)
        if normalized is None or normalized in self._seen or self._closed:
            return False
        self._seen.add(normalized)
        self._depths[normalized] = depth
        heapq.heappush(self._heap, (priority, next(self._sequence), normalized))
        self._wakeup.set()
        return True

    def mark_seen(self, url: str) -> None:
        """Remember a URL without queuing it, e.g. the target of a redirect."""
        normalized = _queue_form(url)
        if normalized is not None:
            self._seen.add(normalized)

    def is_seen(self, url: str) -> bool:
        """Whether a URL was accepted or remembered with `mark_seen`."""
        normalized = _queue_form(url)
        return normalized is not None and normalized in self._seen

    async def get_next(self) -> str | None:
        """Take the next URL, waiting while the queue is empty but work is in progress.

        An empty queue does not mean the crawl is over: a page that is still
        being fetched may add new links, and a deferred URL comes back. None
        is returned only when the queue is empty and no URL is in progress
        or deferred, or after `close`.
        """
        while not self._closed:
            if self._heap:
                _, _, url = heapq.heappop(self._heap)
                self._in_progress.add(url)
                self.visited.add(url)
                return url
            if not self._in_progress and not self._deferred:
                return None
            # Woken by add_url, mark_*, a deferred URL or close; then everything is re-checked.
            # Event.set() wakes every current waiter, so clearing it here
            # cannot make another waiter miss a wakeup.
            self._wakeup.clear()
            await self._wakeup.wait()
        return None

    def depth(self, url: str) -> int:
        """Depth of an accepted URL; raises KeyError for an unknown one."""
        return self._depths[url]

    def mark_processed(self, url: str) -> None:
        self._finish(url)
        self._processed_count += 1

    def mark_failed(self, url: str, error: str) -> None:
        self._finish(url)
        self.failed[url] = error

    def mark_skipped(self, url: str, reason: str) -> None:
        """Finish a URL that was fetched fine but is not wanted, e.g. after a redirect."""
        self._finish(url)
        self.skipped[url] = reason

    def mark_blocked(self, url: str, reason: str) -> None:
        """Finish a URL that may not be fetched at all, e.g. disallowed by robots.txt."""
        self._finish(url)
        self.blocked[url] = reason

    def mark_unreachable(self, url: str, reason: str) -> None:
        """Finish a URL that was not fetched because the rules of its site could not be read."""
        self._finish(url)
        self.unreachable[url] = reason

    def requeue(self, url: str, priority: int = 0) -> None:
        """Put a URL taken by `get_next` back unfetched; works after `close` too.

        The URL is no longer visited or in progress and keeps its depth; it
        is counted as queued, as if it had never been taken.
        """
        self._finish(url)
        self.visited.discard(url)
        heapq.heappush(self._heap, (priority, next(self._sequence), url))

    def defer(self, url: str, delay: float, priority: int = 0) -> None:
        """Like `requeue`, but the URL is queued again only after `delay` seconds.

        Until then it counts as queued, and `get_next` waits for it rather
        than return None. Deferred URLs that come back at once keep the
        order they were deferred in. After `close` it is queued at once.
        """
        if self._closed:
            self.requeue(url, priority)
            return
        self._finish(url)
        self.visited.discard(url)
        entry = (priority, next(self._sequence), url)
        self._deferred[url] = (entry, asyncio.get_running_loop().call_later(delay, self._undefer, url))

    def close(self) -> None:
        """Stop handing out URLs: every current and future `get_next` returns None.

        URLs already in progress can still be marked processed or failed.
        Deferred URLs are queued at once, so they count as left in the queue.
        """
        self._closed = True
        for url, (_, timer) in list(self._deferred.items()):
            timer.cancel()
            self._undefer(url)
        self._wakeup.set()

    def get_stats(self) -> dict[str, int]:
        return {
            "queued": len(self._heap) + len(self._deferred),
            "in_progress": len(self._in_progress),
            "processed": self._processed_count,
            "failed": len(self.failed),
            "skipped": len(self.skipped),
            "blocked": len(self.blocked),
            "unreachable": len(self.unreachable),
            "seen": len(self._depths),
        }

    def _undefer(self, url: str) -> None:
        entry, _ = self._deferred.pop(url)
        heapq.heappush(self._heap, entry)
        self._wakeup.set()

    def _finish(self, url: str) -> None:
        if url not in self._in_progress:
            raise ValueError(f"{url} is not in progress")
        self._in_progress.remove(url)
        # The last finished URL may mean the crawl is over: waiters must re-check.
        self._wakeup.set()


def _queue_form(url: str) -> str | None:
    """The form in which the queue keeps a URL; None if the URL is invalid."""
    normalized = normalize_url(url)
    return None if normalized is None else strip_tracking_params(normalized)
