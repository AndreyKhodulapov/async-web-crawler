"""Unit tests for RateLimiter: intervals per domain and overall, delays, jitter, stats."""

import asyncio
import contextlib
import itertools
import random
import time

import pytest
from helpers import FakeClock

from crawler import DomainRate, RateLimiter
from crawler.rate_limiter import HostPenalizedError


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


# Timer precision for the tests that run in real time.
EPSILON = 0.005


def waits(limiter: RateLimiter, domains: list[str | None]) -> list[float]:
    return [limiter.reserve(domain) for domain in domains]


async def start_times(limiter: RateLimiter, domains: list[str]) -> list[float]:
    """Seconds after the call at which concurrent requests to `domains` may start."""
    began = time.monotonic()

    async def start(domain: str) -> float:
        await limiter.acquire(domain)
        return time.monotonic() - began

    return await asyncio.gather(*(start(domain) for domain in domains))


@pytest.mark.parametrize(
    "options",
    [
        {"requests_per_second": 0},
        {"requests_per_second": -1},
        {"requests_per_second": float("inf")},
        {"requests_per_second": float("nan")},
        {"min_delay": -1},
        {"min_delay": float("inf")},
        {"jitter": -0.1},
        {"jitter": float("nan")},
    ],
)
def test_rejects_invalid_options(options):
    with pytest.raises(ValueError):
        RateLimiter(**options)


@pytest.mark.parametrize("seconds", [-1, float("inf"), float("nan")])
def test_rejects_invalid_delays_and_penalties(seconds):
    limiter = RateLimiter()
    with pytest.raises(ValueError, match="delay"):
        limiter.set_delay("a", seconds)
    with pytest.raises(ValueError, match="seconds"):
        limiter.penalize("a", seconds)


@pytest.mark.parametrize(
    ("requests_per_second", "min_delay", "interval"),
    [(2.0, 0.0, 0.5), (2.0, 1.5, 1.5), (None, 0.3, 0.3), (None, 0.0, 0.0)],
)
def test_interval_is_the_stricter_of_rate_and_min_delay(requests_per_second, min_delay, interval):
    assert RateLimiter(requests_per_second, min_delay=min_delay).interval == interval


class TestSchedule:
    def test_requests_to_one_domain_are_spaced_out(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        assert waits(limiter, ["a", "a", "a"]) == [0.0, 0.5, 1.0]

    def test_idle_domain_does_not_save_up_requests(self, clock):
        # No bursts: after a long pause the next request goes at once, the
        # one after it still waits a full interval.
        limiter = RateLimiter(2.0, clock=clock)
        limiter.reserve("a")
        clock.now += 10
        assert waits(limiter, ["a", "a"]) == [0.0, 0.5]

    def test_domains_are_limited_independently(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        assert waits(limiter, ["a", "b", "a", "b"]) == [0.0, 0.0, 0.5, 0.5]

    def test_global_limit_is_shared_by_all_domains(self, clock):
        limiter = RateLimiter(2.0, per_domain=False, clock=clock)
        assert waits(limiter, ["a", "b", None]) == [0.0, 0.5, 1.0]

    def test_domain_is_required_when_limits_are_per_domain(self):
        with pytest.raises(ValueError, match="domain is required"):
            RateLimiter().reserve()

    def test_no_rate_means_no_waiting(self, clock):
        limiter = RateLimiter(None, clock=clock)
        assert waits(limiter, ["a", "a", "a"]) == [0.0, 0.0, 0.0]


class TestDomainDelays:
    def test_crawl_delay_raises_the_interval_of_its_domain_only(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        limiter.set_delay("a", 3.0)
        assert limiter.interval_for("a") == 3.0
        assert limiter.interval_for("b") == 0.5
        assert waits(limiter, ["a", "a", "b", "b"]) == [0.0, 3.0, 0.0, 0.5]

    def test_crawl_delay_moves_a_start_booked_before_it(self, clock):
        # robots.txt is the first request to a site: the page after it
        # was booked with the default interval before Crawl-delay was known.
        limiter = RateLimiter(2.0, clock=clock)
        limiter.reserve("a")
        limiter.set_delay("a", 3.0)
        assert limiter.reserve("a") == 3.0

    def test_longest_crawl_delay_of_a_domain_wins(self, clock):
        # One host, two sites (ports) with their own robots.txt.
        limiter = RateLimiter(2.0, clock=clock)
        limiter.set_delay("a", 5.0)
        limiter.set_delay("a", 1.0)
        assert limiter.interval_for("a") == 5.0

    def test_penalty_holds_back_one_domain(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        limiter.reserve("a")
        limiter.penalize("a", 5.0)
        assert waits(limiter, ["a", "b"]) == [5.0, 0.0]

    def test_penalty_left(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        limiter.penalize("a", 5.0)
        clock.now += 2.0
        assert (limiter.penalty_left("a"), limiter.penalty_left("b")) == (3.0, 0.0)
        clock.now += 4.0
        assert limiter.penalty_left("a") == 0.0

    def test_penalty_never_brings_a_start_forward(self, clock):
        limiter = RateLimiter(0.1, clock=clock)  # one request per 10 s
        limiter.reserve("a")
        limiter.penalize("a", 1.0)
        assert limiter.reserve("a") == 10.0


class TestJitter:
    def test_jitter_is_added_on_top_of_the_interval(self, clock, monkeypatch):
        monkeypatch.setattr(random, "uniform", lambda low, high: high)
        limiter = RateLimiter(2.0, jitter=0.25, clock=clock)
        assert waits(limiter, ["a", "a", "a"]) == [0.0, 0.75, 1.5]

    def test_jitter_stays_within_bounds(self, clock):
        limiter = RateLimiter(2.0, jitter=0.25, clock=clock)
        limiter.reserve("a")
        gaps = []
        for _ in range(50):
            gaps.append(limiter.reserve("a"))
            clock.now += gaps[-1]
        assert all(0.5 <= gap <= 0.75 for gap in gaps)
        assert len(set(gaps)) > 1


class TestStats:
    def test_counts_requests_gaps_and_waits(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        waits(limiter, ["a", "a", "a", "b"])
        stats = limiter.get_stats()

        assert stats.requests == 4
        assert stats.avg_delay == 0.5  # gaps between requests to one domain only
        assert stats.avg_wait == (0.0 + 0.5 + 1.0 + 0.0) / 4
        assert stats.domains == {
            "a": DomainRate(requests=3, interval=0.5, avg_gap=0.5),
            "b": DomainRate(requests=1, interval=0.5, avg_gap=None),
        }

    def test_current_rate_counts_only_requests_that_have_started(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        waits(limiter, ["a", "a", "a", "b"])  # a starts at +0, +0.5, +1.0
        assert limiter.get_stats().current_rps == 2.0

        clock.now += 1.0
        assert limiter.get_stats().current_rps == 4.0

        clock.now += RateLimiter.WINDOW + 1
        assert limiter.get_stats().current_rps == 0.0

    def test_reset_keeps_the_schedule(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        limiter.reserve("a")
        limiter.reset_stats()
        assert limiter.reserve("a") == 0.5
        assert limiter.get_stats().requests == 1


class TestAcquire:
    async def test_acquire_sleeps_until_the_booked_start(self):
        limiter = RateLimiter(20.0)  # 0.05 s apart
        started = time.perf_counter()
        for _ in range(3):
            await limiter.acquire("a")
        assert time.perf_counter() - started >= 0.1 - EPSILON

    async def test_concurrent_requests_are_spaced_out(self):
        limiter = RateLimiter(20.0)  # 0.05 s apart
        times = sorted(await start_times(limiter, ["a"] * 5))
        assert all(later - earlier >= 0.05 - EPSILON for earlier, later in itertools.pairwise(times))

    async def test_penalty_holds_back_requests_already_waiting(self):
        limiter = RateLimiter(20.0)  # 0.05 s apart
        await limiter.acquire("a")
        waiting = asyncio.create_task(start_times(limiter, ["a", "a", "a"]))
        await asyncio.sleep(0)  # all three have booked their starts
        limiter.penalize("a", 0.3)
        times = sorted(await waiting)

        assert times[0] >= 0.3 - EPSILON
        assert all(later - earlier >= 0.05 - EPSILON for earlier, later in itertools.pairwise(times))

    async def test_crawl_delay_under_a_global_limit_holds_back_its_domain_only(self):
        limiter = RateLimiter(20.0, per_domain=False)  # 0.05 s apart
        limiter.set_delay("slow", 0.3)
        slow, _, slow_again, fast_again = await start_times(limiter, ["slow", "fast", "slow", "fast"])

        assert slow_again - slow >= 0.3 - EPSILON
        assert fast_again < 0.2

    async def test_penalty_under_a_global_limit_holds_back_its_domain_only(self):
        limiter = RateLimiter(20.0, per_domain=False)
        await limiter.acquire("a")
        limiter.penalize("a", 0.3)
        penalized, _, other_again = await start_times(limiter, ["a", "b", "b"])

        assert penalized >= 0.3 - EPSILON
        assert other_again < 0.2

    async def test_penalty_under_a_global_limit_costs_the_others_no_turns(self):
        limiter = RateLimiter(10.0, per_domain=False)  # 0.1 s apart
        limiter.set_delay("a", 0.05)
        waiting = asyncio.gather(*(limiter.acquire("a") for _ in range(5)))
        await asyncio.sleep(0)  # the first one has started, the others wait for "a"
        limiter.penalize("a", 1.0)
        await asyncio.sleep(0.3)
        began = time.monotonic()
        await limiter.acquire("b")
        waited = time.monotonic() - began
        waiting.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await waiting

        # Woken during the penalty, the requests to "a" book it again without
        # taking turns of the global limit: "b" finds it free.
        assert waited < 0.05


class TestSlot:
    async def test_turn_is_waited_for_outside_the_gate(self):
        limiter = RateLimiter(5.0)  # 0.2 s apart
        gate = asyncio.Semaphore(1)
        began = time.monotonic()
        started: dict[str, float] = {}

        async def request(name: str, domain: str) -> None:
            async with limiter.slot(domain, lambda: gate):
                started[name] = time.monotonic() - began

        await asyncio.gather(request("a", "a"), request("a again", "a"), request("b", "b"))
        # "a again" waits for its turn without the gate, so "b" gets it at once.
        assert started["b"] < 0.1
        assert started["a again"] >= 0.2 - EPSILON

    async def test_interval_holds_when_the_gate_comes_late(self):
        limiter = RateLimiter(20.0)  # 0.05 s apart
        gate = asyncio.Semaphore(1)
        started: list[float] = []

        async def request(hold: float) -> None:
            async with limiter.slot("a", lambda: gate):
                started.append(time.monotonic())
                await asyncio.sleep(hold)

        # The first request keeps the gate past the turns of the others.
        await asyncio.gather(request(0.2), request(0), request(0), request(0))
        assert all(later - earlier >= 0.05 - EPSILON for earlier, later in itertools.pairwise(started))

    async def test_late_start_does_not_make_the_next_requests_wait_inside_the_gate(self):
        limiter = RateLimiter(10.0)  # 0.1 s apart
        gate = asyncio.Semaphore(1)
        held: list[float] = []

        @contextlib.asynccontextmanager
        async def timed_gate():
            async with gate:
                taken = time.monotonic()
                yield
                held.append(time.monotonic() - taken)

        async def request() -> None:
            async with limiter.slot("a", timed_gate):
                pass

        async def other_host() -> None:
            async with gate:
                await asyncio.sleep(0.05)

        # The first request gets the gate 0.05 s late, and every later start
        # is 0.05 s behind its booking: that lag is waited for outside the gate.
        blocker = asyncio.create_task(other_host())
        await asyncio.sleep(0)
        await asyncio.gather(*(request() for _ in range(4)), blocker)
        assert max(held) < 0.02

    @pytest.mark.parametrize(
        ("per_domain", "domains", "slots", "busy"),
        [
            (False, [f"host{number}" for number in range(10)], 3, 0.25),
            (True, ["a"] * 6, 2, 0.5),
        ],
    )
    async def test_requests_let_in_late_never_wait_inside_the_gate(self, per_domain, domains, slots, busy):
        limiter = RateLimiter(10.0, per_domain=per_domain)  # 0.1 s apart
        gate = asyncio.Semaphore(slots)
        held: list[float] = []
        started: list[float] = []

        @contextlib.asynccontextmanager
        async def timed_gate():
            async with gate:
                taken = time.monotonic()
                yield
                held.append(time.monotonic() - taken)

        async def request(domain: str) -> None:
            async with limiter.slot(domain, timed_gate):
                started.append(time.monotonic())

        async def other_work() -> None:
            async with gate:
                await asyncio.sleep(busy)

        # The gate is busy at first. A request that gets it too soon after
        # another one started lets it go and waits for a new turn outside.
        blockers = [asyncio.create_task(other_work()) for _ in range(slots)]
        await asyncio.sleep(0)
        await asyncio.gather(*(request(domain) for domain in domains), *blockers)
        assert max(held) < 0.02
        assert all(later - earlier >= 0.1 - EPSILON for earlier, later in itertools.pairwise(sorted(started)))


class TestMaxWait:
    """A request that waits no longer than `max_wait` for a penalty of its domain."""

    async def test_penalty_longer_than_max_wait_is_not_waited_for(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        limiter.penalize("a", 5.0)

        with pytest.raises(HostPenalizedError) as raised:
            async with limiter.slot("a", max_wait=1.0):
                pytest.fail("the request started")

        assert (raised.value.domain, raised.value.seconds) == ("a", 5.0)
        assert limiter.get_stats().requests == 0
        # Nothing was booked for it: the next request after the penalty starts on time.
        clock.now += 5.0
        assert limiter.reserve("a") == 0.0

    async def test_penalty_that_comes_while_the_request_waits_for_its_turn_is_not_waited_for(self):
        limiter = RateLimiter(5.0)  # 0.2 s apart
        await limiter.acquire("a")
        began = time.monotonic()

        async def request() -> None:
            async with limiter.slot("a", max_wait=1.0):
                pytest.fail("the request started")

        waiting = asyncio.create_task(request())
        await asyncio.sleep(0.05)
        limiter.penalize("a", 5.0)

        with pytest.raises(HostPenalizedError):
            await waiting
        # Given up at the turn it had booked, not after the penalty.
        assert time.monotonic() - began < 0.5

    async def test_long_crawl_delay_is_waited_for(self):
        limiter = RateLimiter(None)
        limiter.set_delay("a", 0.3)
        await limiter.acquire("a")
        began = time.monotonic()

        async with limiter.slot("a", max_wait=0.1):
            pass

        assert time.monotonic() - began >= 0.3 - EPSILON

    async def test_penalty_within_max_wait_is_waited_for(self):
        limiter = RateLimiter(None)
        limiter.penalize("a", 0.2)
        began = time.monotonic()

        async with limiter.slot("a", max_wait=1.0):
            pass

        assert time.monotonic() - began >= 0.2 - EPSILON

    async def test_penalty_of_another_domain_is_not_waited_for_under_a_global_limit(self):
        limiter = RateLimiter(20.0, per_domain=False)
        limiter.penalize("a", 5.0)

        async with limiter.slot("b", max_wait=1.0):
            pass
        with pytest.raises(HostPenalizedError):
            async with limiter.slot("a", max_wait=1.0):
                pytest.fail("the request started")
