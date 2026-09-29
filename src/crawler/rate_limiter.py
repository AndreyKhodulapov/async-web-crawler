"""Request rate limits: a minimum interval between requests, per domain or overall."""

import asyncio
import random
import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DomainRate:
    """Requests to one domain: how many, the interval enforced now, the average gap seen."""

    requests: int
    interval: float
    avg_gap: float | None


@dataclass(frozen=True, slots=True)
class RateStats:
    """Request rate since the limiter was created or its stats were reset.

    `current_rps` counts requests over the last few seconds. `avg_delay` is
    the average gap between two consecutive requests to the same domain,
    `avg_wait` the average time a request waited for its turn.
    """

    requests: int = 0
    current_rps: float = 0.0
    avg_delay: float = 0.0
    avg_wait: float = 0.0
    domains: Mapping[str, DomainRate] = field(default_factory=dict)


@dataclass(slots=True)
class _DomainCounter:
    requests: int = 0
    last_start: float | None = None
    total_gap: float = 0.0
    gaps: int = 0


class RateLimiter:
    """Spaces requests out in time, per domain or for all domains together.

    Usage::

        limiter = RateLimiter(requests_per_second=2.0, min_delay=0.5, jitter=0.2)
        await limiter.acquire("example.com")  # returns when the request may start

    Requests to a domain start at least `interval` seconds apart, where the
    interval is the largest of `1 / requests_per_second`, `min_delay` and the
    domain's own delay from `set_delay` (robots.txt Crawl-delay). `jitter`
    adds a random 0..jitter seconds on top of every interval, so it can only
    slow requests down, never break the limit. There are no bursts: a domain
    that has been idle gets one request at once, not a batch.

    With `per_domain=False` all requests share one schedule. A domain's own
    delay then still holds between its requests: every request to it pushes
    the shared schedule by that delay.

    The algorithm is GCRA (generic cell rate algorithm): each schedule keeps
    only the time its next request may start. `acquire` books that time,
    moves it forward by one interval and sleeps until the booked time. The
    booking has no `await` inside, so it is atomic on the event loop and
    needs no lock, and waiting requests start in the order they arrived.
    A task cancelled while it sleeps does not give its time back: the
    schedule only ever errs on the slow side.
    """

    # Seconds over which `current_rps` is measured.
    WINDOW = 5.0

    def __init__(
        self,
        requests_per_second: float | None = 1.0,
        per_domain: bool = True,
        *,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if requests_per_second is not None and requests_per_second <= 0:
            raise ValueError(f"requests_per_second must be positive or None, got {requests_per_second}")
        if min_delay < 0:
            raise ValueError(f"min_delay must be >= 0, got {min_delay}")
        if jitter < 0:
            raise ValueError(f"jitter must be >= 0, got {jitter}")
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter
        self.interval = max(1 / requests_per_second if requests_per_second else 0.0, min_delay)
        self._clock = clock
        # Keyed by domain, or by None when all domains share one schedule.
        self._next_start: dict[str | None, float] = {}
        self._last_start: dict[str | None, float] = {}
        self._delays: dict[str, float] = {}
        self.reset_stats()

    def interval_for(self, domain: str | None) -> float:
        """The minimum gap enforced now between requests to `domain`, jitter aside."""
        if domain is None:
            return self.interval
        return max(self.interval, self._delays.get(domain, 0.0))

    def set_delay(self, domain: str, delay: float) -> None:
        """Keep requests to `domain` at least `delay` seconds apart, e.g. its Crawl-delay."""
        if delay < 0:
            raise ValueError(f"delay must be >= 0, got {delay}")
        self._delays[domain] = delay
        key = self._key(domain)
        # The request that fetched robots.txt has already booked the next
        # start with the old interval: move it.
        if key in self._last_start:
            self._next_start[key] = max(self._next_start[key], self._last_start[key] + delay)

    def penalize(self, domain: str, seconds: float) -> None:
        """Let no request to `domain` start in the next `seconds`, e.g. after HTTP 429."""
        key = self._key(domain)
        self._next_start[key] = max(self._next_start.get(key, 0.0), self._clock() + seconds)

    def reserve(self, domain: str | None = None) -> float:
        """Book the next start time for `domain`; return the seconds to wait for it.

        `acquire` is `reserve` plus the sleep. `domain` may be None only
        when `per_domain` is False.
        """
        key = self._key(domain)
        now = self._clock()
        start = max(now, self._next_start.get(key, now))
        interval = self.interval_for(domain)
        if self.jitter:
            interval += random.uniform(0, self.jitter)
        self._next_start[key] = start + interval
        self._last_start[key] = start
        self._record(domain, start, start - now)
        self._forget_before(now - self.WINDOW)
        return start - now

    async def acquire(self, domain: str | None = None) -> None:
        """Wait until a request to `domain` may start."""
        delay = self.reserve(domain)
        if delay > 0:
            await asyncio.sleep(delay)

    def get_stats(self) -> RateStats:
        now = self._clock()
        cutoff = now - self.WINDOW
        self._forget_before(cutoff)
        # Start times are booked ahead, and not in order across domains:
        # count only those that have come.
        recent = sum(cutoff < start <= now for start in self._recent)
        # Early in a crawl the window is not full yet; at least one second
        # keeps the first request from reading as thousands per second.
        span = min(self.WINDOW, max(now - self._stats_started, 1.0))
        gaps = sum(counter.gaps for counter in self._domains.values())
        total_gap = sum(counter.total_gap for counter in self._domains.values())
        return RateStats(
            requests=self._requests,
            current_rps=recent / span,
            avg_delay=total_gap / gaps if gaps else 0.0,
            avg_wait=self._total_wait / self._requests if self._requests else 0.0,
            domains={
                domain: DomainRate(
                    requests=counter.requests,
                    interval=self.interval_for(domain),
                    avg_gap=counter.total_gap / counter.gaps if counter.gaps else None,
                )
                for domain, counter in self._domains.items()
            },
        )

    def reset_stats(self) -> None:
        """Start counting from zero; the schedule itself is kept."""
        self._stats_started = self._clock()
        self._requests = 0
        self._total_wait = 0.0
        self._recent: deque[float] = deque()
        self._domains: dict[str, _DomainCounter] = {}

    def _key(self, domain: str | None) -> str | None:
        if not self.per_domain:
            return None
        if domain is None:
            raise ValueError("a domain is required when limits are per domain")
        return domain

    def _forget_before(self, cutoff: float) -> None:
        # Starts are not sorted across domains, so an old one can stay behind
        # a later one for a while; get_stats() filters them out anyway.
        while self._recent and self._recent[0] <= cutoff:
            self._recent.popleft()

    def _record(self, domain: str | None, start: float, wait: float) -> None:
        self._requests += 1
        self._total_wait += wait
        self._recent.append(start)
        if domain is None:
            return
        counter = self._domains.setdefault(domain, _DomainCounter())
        counter.requests += 1
        if counter.last_start is not None:
            counter.total_gap += start - counter.last_start
            counter.gaps += 1
        counter.last_start = start
