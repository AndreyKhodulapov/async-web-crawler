"""Unit tests for RateLimiter: intervals per domain and overall, delays, jitter, stats."""

import asyncio
import random
import time

import pytest

from crawler import DomainRate, RateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def waits(limiter: RateLimiter, domains: list[str | None]) -> list[float]:
    """Book requests at one moment; return how long each one has to wait."""
    return [limiter.reserve(domain) for domain in domains]


@pytest.mark.parametrize(
    "options",
    [{"requests_per_second": 0}, {"requests_per_second": -1}, {"min_delay": -1}, {"jitter": -0.1}],
)
def test_rejects_invalid_options(options):
    with pytest.raises(ValueError):
        RateLimiter(**options)


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

    def test_crawl_delay_holds_under_a_global_limit(self, clock):
        limiter = RateLimiter(10.0, per_domain=False, clock=clock)
        limiter.set_delay("slow", 2.0)
        assert waits(limiter, ["slow", "fast", "slow"]) == pytest.approx([0.0, 2.0, 2.1])

    def test_penalty_holds_back_one_domain(self, clock):
        limiter = RateLimiter(2.0, clock=clock)
        limiter.reserve("a")
        limiter.penalize("a", 5.0)
        assert waits(limiter, ["a", "b"]) == [5.0, 0.0]

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

    def test_old_starts_are_forgotten_without_reading_stats(self, clock):
        limiter = RateLimiter(None, clock=clock)
        for _ in range(100):
            limiter.reserve("a")
            clock.now += 1
        assert len(limiter._recent) <= RateLimiter.WINDOW + 1

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
        assert time.perf_counter() - started >= 0.1 - 0.005

    async def test_waiting_requests_start_in_arrival_order(self):
        limiter = RateLimiter(50.0)
        order: list[int] = []

        async def request(number: int) -> None:
            await limiter.acquire("a")
            order.append(number)

        await asyncio.gather(*(request(number) for number in range(5)))
        assert order == [0, 1, 2, 3, 4]
