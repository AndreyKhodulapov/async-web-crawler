"""Request rate limits: a minimum interval between requests, per domain or overall."""

import asyncio
import contextlib
import math
import random
import time
from collections import deque
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass

from crawler.models import DomainRate, RateStats


class HostPenalizedError(Exception):
    """Raised by `RateLimiter.slot` for a domain penalized longer than the request may wait."""

    def __init__(self, domain: str, seconds: float) -> None:
        super().__init__(f"{domain} is held back for {seconds:.1f}s")
        self.domain = domain
        self.seconds = seconds


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

    With `per_domain=False` all requests share one schedule with the
    common interval. A domain's own delay and penalty (`penalize`) then
    still apply to that domain alone: it has a schedule of its own, and a
    request waits for its turn there first, then books the shared schedule.
    Booking both at once would let a domain waiting out a long delay hold
    the shared schedule, and every other domain with it.

    A domain that answers HTTP 429 asks for fewer requests: `slow_down`
    doubles its interval, and the slowdown wears off by itself over time
    (see `slow_down`). It is kept apart from the domain's Crawl-delay.

    The algorithm is GCRA (generic cell rate algorithm): each schedule keeps
    only the time its next request may start. `acquire` books that time,
    moves it forward by one interval and sleeps until the booked time. The
    booking has no `await` inside, so it is atomic on the event loop and
    needs no lock, and start times are booked in the order requests arrived.
    Requests start in that order while every start comes on time. When one
    comes late by more than an interval, because the event loop was busy or
    the gate (`slot`) was taken, the requests due in the meantime wait for
    the interval again, and one that finds another started too recently
    lets the gate go and books a new time: later requests may overtake it.
    The interval between starts holds either way.
    A task cancelled while it sleeps does not give its time back: the
    schedule only ever errs on the slow side. A request whose domain was
    penalized while it slept books a new time after the penalty.
    """

    # Seconds over which `current_rps` is measured.
    WINDOW = 5.0
    # Bounds of the interval of a domain slowed down by `slow_down`, and the
    # seconds over which the slowdown halves.
    MIN_SLOWDOWN = 1.0
    MAX_SLOWDOWN = 60.0
    SLOWDOWN_HALF_LIFE = 60.0

    def __init__(
        self,
        requests_per_second: float | None = 1.0,
        per_domain: bool = True,
        *,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if requests_per_second is not None and not (math.isfinite(requests_per_second) and requests_per_second > 0):
            raise ValueError(f"requests_per_second must be a positive number or None, got {requests_per_second}")
        _check_seconds("min_delay", min_delay)
        _check_seconds("jitter", jitter)
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter
        self.interval = max(1 / requests_per_second if requests_per_second else 0.0, min_delay)
        self._clock = clock
        # Schedules are keyed by domain, and by None for the shared one.
        self._next_start: dict[str | None, float] = {}
        self._last_start: dict[str | None, float] = {}
        self._delays: dict[str, float] = {}
        self._penalized_until: dict[str, float] = {}
        # The interval of a slowed down domain and when it was set.
        self._slowdowns: dict[str, tuple[float, float]] = {}
        self.reset_stats()

    def interval_for(self, domain: str | None) -> float:
        """The minimum gap enforced now between requests to `domain`, jitter aside."""
        if domain is None:
            return self.interval
        return max(self.interval, self._own_delay(domain))

    def set_delay(self, domain: str, delay: float) -> bool:
        """Keep requests to `domain` at least `delay` seconds apart, e.g. its Crawl-delay; whether its delay grew.

        A delay never goes down: a host may serve several sites (ports,
        schemes), each with a robots.txt of its own, and the longest delay wins.
        """
        _check_seconds("delay", delay)
        if delay <= self._delays.get(domain, 0.0):
            return False
        self._delays[domain] = delay
        # The request that fetched robots.txt has already booked the next
        # start with the old interval: move it.
        if domain in self._last_start:
            self._next_start[domain] = max(self._next_start[domain], self._last_start[domain] + delay)
        return True

    def slow_down(self, domain: str, sent_at: float) -> bool:
        """Double the interval of `domain` after it answered HTTP 429 to a request started at `sent_at`; whether it grew.

        `sent_at` is the start time `slot` gave the request. The interval
        becomes at least MIN_SLOWDOWN and at most MAX_SLOWDOWN seconds, then
        halves every SLOWDOWN_HALF_LIFE seconds, down to the interval the
        domain has without the slowdown. A request started before the last
        slowdown does not slow the domain down again: the requests already
        on their way at the old pace double the interval once, not once
        each. The Crawl-delay of `set_delay` is not changed.
        """
        slowed = self._slowdowns.get(domain)
        if slowed is not None and sent_at < slowed[1]:
            return False
        current = self.interval_for(domain)
        interval = min(self.MAX_SLOWDOWN, max(2 * current, self.MIN_SLOWDOWN))
        if interval <= current:
            return False
        self._slowdowns[domain] = (interval, self._clock())
        # The next start may have been booked with the old interval.
        if domain in self._last_start:
            self._next_start[domain] = max(self._next_start[domain], self._last_start[domain] + interval)
        return True

    def penalize(self, domain: str, seconds: float) -> None:
        """Let no request to `domain` start in the next `seconds`, e.g. after HTTP 429.

        Requests already waiting for their turn wait for the penalty too.
        """
        _check_seconds("seconds", seconds)
        until = self._clock() + seconds
        self._penalized_until[domain] = max(self._penalized_until.get(domain, 0.0), until)
        self._next_start[domain] = max(self._next_start.get(domain, 0.0), until)

    def penalty_left(self, domain: str) -> float:
        """Seconds until the penalty of `domain` ends; 0 if it has none."""
        return max(0.0, self._penalty_end(domain) - self._clock())

    def reserve(self, domain: str | None = None) -> float:
        """Book the next start time for `domain`; return the seconds to wait for it.

        For callers that sleep on their own. Unlike `acquire`, it books all
        schedules at once: under a global limit a domain's own delay then
        holds back the shared schedule too, and the booking does not move
        if the domain is penalized after it. `domain` may be None only when
        `per_domain` is False.
        """
        now = self._clock()
        start = self._book(self._schedules(domain), now)
        self._mark_started(domain, start)
        self._record(domain, start, start - now)
        return start - now

    async def acquire(self, domain: str | None = None) -> None:
        """Wait until a request to `domain` may start."""
        async with self.slot(domain):
            pass

    @contextlib.asynccontextmanager
    async def slot(
        self,
        domain: str | None = None,
        gate: Callable[[], AbstractAsyncContextManager[object]] = contextlib.nullcontext,
        *,
        max_wait: float | None = None,
    ) -> AsyncGenerator[float, None]:
        """Wait for the turn of `domain`, then hold `gate()`, e.g. a concurrency slot, for the request.

        Gives the time the request started, by the clock of the limiter.

        Nothing is waited for inside the gate, so a request waiting for its
        domain does not hold a gate that other domains could use. When the
        gate comes late, the interval is checked again inside it, counted
        from the request to the domain that started last: if another one
        started too recently in the meantime, or the domain has been
        penalized, the request lets the gate go and waits for a new turn
        outside it.

        With `max_wait`, a penalty of the domain longer than that is not
        waited for: `HostPenalizedError` is raised instead, before the
        request books a turn, or once it finds the penalty that came while
        it waited for one. The interval and the domain's own delay are
        waited for however long they are.
        """
        waited = 0.0
        while True:
            start, slept = await self._wait_turn(domain, max_wait)
            waited += slept
            async with gate():
                if not self._start(domain, start):
                    continue
                started = self._clock()
                self._record(domain, started, waited)
                yield started
                return

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

    def _schedules(self, domain: str | None) -> list[tuple[str | None, float]]:
        """The schedules a request to `domain` waits for, with the interval of each, in order."""
        if self.per_domain:
            if domain is None:
                raise ValueError("a domain is required when limits are per domain")
            return [(domain, self.interval_for(domain))]
        shared: list[tuple[str | None, float]] = [(None, self.interval)]
        return shared if domain is None else [(domain, self._own_delay(domain)), *shared]

    def _own_delay(self, domain: str) -> float:
        """The interval of `domain` alone: its Crawl-delay or what is left of its slowdown, whichever is longer."""
        delay = self._delays.get(domain, 0.0)
        if (slowed := self._slowdowns.get(domain)) is None:
            return delay
        interval, since = slowed
        return max(delay, interval * 0.5 ** ((self._clock() - since) / self.SLOWDOWN_HALF_LIFE))

    def _book(self, schedules: list[tuple[str | None, float]], now: float) -> float:
        """Take the next start time common to `schedules`; return it."""
        start = max(now, *(self._next_start.get(key, now) for key, _ in schedules))
        extra = random.uniform(0, self.jitter) if self.jitter else 0.0
        for key, interval in schedules:
            self._next_start[key] = start + interval + extra
        return start

    async def _wait_turn(self, domain: str | None, max_wait: float | None) -> tuple[float, float]:
        """Sleep until a booked start for `domain`; return the start and the seconds slept.

        The schedules are booked one by one, each after the wait for the one
        before. A penalty longer than `max_wait` raises `HostPenalizedError`.
        """
        slept = 0.0
        while True:
            if domain is not None and max_wait is not None and (penalty := self.penalty_left(domain)) > max_wait:
                raise HostPenalizedError(domain, penalty)
            for schedule in self._schedules(domain):
                now = self._clock()
                start = self._book([schedule], now)
                if start > now:
                    await asyncio.sleep(start - now)
                    slept += start - now
                # A penalty that came after the booking and outlasts it: the
                # booked time is void, and a new one comes after the penalty.
                # The next schedule is not booked, so the shared one does
                # not lose a turn to a request that cannot start.
                if self._penalty_end(domain) > start:
                    break
            else:
                # The request before may have started later than booked, when
                # its gate came late: the interval since then is waited for
                # here too, so that the gate does not send this one back.
                now = self._clock()
                while (ready := self._ready_at(domain, now)) > now:
                    await asyncio.sleep(ready - now)
                    slept += ready - now
                    now = self._clock()
                if self._penalty_end(domain) <= start:
                    return start, slept

    def _start(self, domain: str | None, start: float) -> bool:
        """Mark a request to `domain` started if it may start now; return whether it has.

        It may not if the domain has been penalized since `start` was
        booked, or the intervals since the requests that started last have
        not passed yet.
        """
        now = self._clock()
        if self._penalty_end(domain) > max(now, start) or self._ready_at(domain, now) > now:
            return False
        self._mark_started(domain, now)
        return True

    def _ready_at(self, domain: str | None, now: float) -> float:
        """When the intervals since the requests that started last have passed; `now` if none started."""
        last_starts = ((self._last_start.get(key), interval) for key, interval in self._schedules(domain))
        return max((last + interval for last, interval in last_starts if last is not None), default=now)

    def _penalty_end(self, domain: str | None) -> float:
        return 0.0 if domain is None else self._penalized_until.get(domain, 0.0)

    def _mark_started(self, domain: str | None, start: float) -> None:
        for key, _ in self._schedules(domain):
            self._last_start[key] = start

    def _forget_before(self, cutoff: float) -> None:
        # Starts are not sorted across domains, so an old one can stay behind
        # a later one for a while; get_stats() filters them out anyway.
        while self._recent and self._recent[0] <= cutoff:
            self._recent.popleft()

    def _record(self, domain: str | None, start: float, wait: float) -> None:
        self._requests += 1
        self._total_wait += wait
        self._recent.append(start)
        self._forget_before(self._clock() - self.WINDOW)
        if domain is None:
            return
        counter = self._domains.setdefault(domain, _DomainCounter())
        counter.requests += 1
        if counter.last_start is not None:
            counter.total_gap += start - counter.last_start
            counter.gaps += 1
        counter.last_start = start


def _check_seconds(name: str, value: float) -> None:
    if not (math.isfinite(value) and value >= 0):
        raise ValueError(f"{name} must be a number of seconds >= 0, got {value}")
