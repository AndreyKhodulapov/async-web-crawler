"""Unit tests for RetryPolicy, RetryStrategy and Retry-After parsing."""

import asyncio
import random
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest

from crawler import (
    CertificateError,
    CrawlerClosedError,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    ParseError,
    PermanentError,
    RetryPolicy,
    RetryRule,
    RetryStrategy,
    RobotsDisallowedError,
    TooManyRedirectsError,
    TransientError,
    UnexpectedError,
)
from crawler.retry import parse_retry_after

URL = "https://site/page"


def http_error(status: int, retry_after: float | None = None) -> HTTPStatusError:
    return HTTPStatusError(URL, status, "Error", retry_after=retry_after)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (http_error(429), True),
        (http_error(503), True),
        (http_error(500), True),
        (http_error(408), True),
        (http_error(404), False),
        (http_error(403), False),
        (FetchTimeoutError(URL, "request timed out"), True),
        (NetworkError(URL, "connection reset"), True),
        (TooManyRedirectsError(URL, "too many redirects (10)"), False),
        (CertificateError(URL, "certificate has expired"), False),
        (InvalidURLError(URL, "bad port"), False),
        (RobotsDisallowedError(URL, "disallowed by robots.txt"), False),
        (CrawlerClosedError(URL, "crawler is closed"), False),
        (UnexpectedError(URL, "KeyError: 'x'"), False),
    ],
)
def test_only_transient_errors_are_retried(error, expected):
    assert RetryPolicy().should_retry(error, 0) is expected


def test_retry_after_longer_than_max_delay_is_not_retried():
    policy = RetryPolicy(max_delay=30.0)
    assert policy.should_retry(http_error(429, retry_after=30), 0)
    assert not policy.should_retry(http_error(429, retry_after=31), 0)


def test_retries_stop_at_the_limit():
    policy = RetryPolicy(max_retries=2)
    assert [policy.should_retry(http_error(503), done) for done in range(4)] == [True, True, False, False]


@pytest.mark.parametrize(
    "options",
    [
        {"max_retries": -1},
        {"base_delay": 0},
        {"max_delay": -1},
        {"base_delay": float("nan")},
        {"max_delay": float("inf")},
    ],
)
def test_rejects_invalid_options(options):
    with pytest.raises(ValueError):
        RetryPolicy(**options)


class TestDelay:
    def test_backoff_doubles_with_jitter_in_its_upper_half(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=100.0)
        for retries_done in range(5):
            full = 2.0**retries_done
            delays = [policy.delay(http_error(503), retries_done) for _ in range(20)]
            assert all(full / 2 <= delay <= full for delay in delays)

    def test_backoff_is_capped(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0)
        assert all(policy.delay(http_error(503), 10) <= 5.0 for _ in range(20))

    def test_backoff_is_capped_after_any_number_of_retries(self):
        policy = RetryPolicy(max_retries=5000, base_delay=1.0, max_delay=5.0)
        assert 2.5 <= policy.delay(http_error(503), 2000) <= 5.0

    def test_longer_retry_after_is_honored(self):
        policy = RetryPolicy(base_delay=0.1, max_delay=30.0)
        assert policy.delay(http_error(429, retry_after=7), 0) == 7

    def test_shorter_retry_after_does_not_shorten_the_backoff(self):
        policy = RetryPolicy(base_delay=4.0, max_delay=30.0)
        assert policy.delay(http_error(429, retry_after=0), 0) >= 2.0

    def test_retry_after_is_capped(self):
        policy = RetryPolicy(max_delay=30.0)
        assert policy.delay(http_error(429, retry_after=3600), 0) == 30.0


class TestParseRetryAfter:
    @pytest.mark.parametrize(("value", "seconds"), [("120", 120.0), (" 5 ", 5.0), ("0", 0.0)])
    def test_seconds(self, value, seconds):
        assert parse_retry_after(value) == seconds

    def test_http_date(self):
        moment = datetime.now(UTC) + timedelta(seconds=60)
        assert parse_retry_after(format_datetime(moment, usegmt=True)) == pytest.approx(60, abs=2)

    def test_date_in_the_past_means_no_wait(self):
        assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0

    @pytest.mark.parametrize("value", [None, "", "soon", "-5", "1.5", "²", "١٢"])
    def test_invalid_values(self, value):
        assert parse_retry_after(value) is None


class Attempts:
    """A coroutine function that fails or succeeds as told, one outcome per call."""

    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[tuple, dict]] = []

    async def __call__(self, *args, **kwargs) -> object:
        self.calls.append((args, kwargs))
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class Waits:
    """Records the waits between attempts instead of sleeping."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, error: Exception, delay: float) -> None:
        self.delays.append(delay)


@pytest.fixture
def waits() -> Waits:
    return Waits()


@pytest.fixture
def no_jitter(monkeypatch):
    # Equal jitter adds uniform(0, delay / 2); taking the top keeps the full delay.
    monkeypatch.setattr(random, "uniform", lambda low, high: high)


def strategy(waits: Waits, **options) -> RetryStrategy:
    return RetryStrategy(wait=waits, **options)


class TestRetryStrategy:
    async def test_transient_errors_are_retried_until_success(self, waits):
        attempts = Attempts(http_error(503), FetchTimeoutError(URL, "request timed out"), "page")
        assert await strategy(waits).execute_with_retry(attempts, URL) == "page"
        assert len(attempts.calls) == 3
        assert len(waits.delays) == 2

    async def test_network_errors_are_retried(self, waits):
        attempts = Attempts(NetworkError(URL, "connection refused"), "page")
        assert await strategy(waits).execute_with_retry(attempts) == "page"
        assert len(attempts.calls) == 2

    @pytest.mark.parametrize("status", [404, 403, 401])
    async def test_permanent_errors_are_not_retried(self, waits, status):
        attempts = Attempts(http_error(status), "page")
        with pytest.raises(PermanentError):
            await strategy(waits).execute_with_retry(attempts)
        assert len(attempts.calls) == 1
        assert waits.delays == []

    async def test_permanent_errors_are_not_retried_whatever_retry_on_says(self, waits):
        attempts = Attempts(TooManyRedirectsError(URL, "too many redirects (10)"), "page")
        with pytest.raises(TooManyRedirectsError):
            await strategy(waits, retry_on=[FetchError]).execute_with_retry(attempts)
        assert len(attempts.calls) == 1

    async def test_only_errors_in_retry_on_are_retried(self, waits):
        attempts = Attempts(http_error(503), "page")
        with pytest.raises(HTTPStatusError):
            await strategy(waits, retry_on=[NetworkError]).execute_with_retry(attempts)
        assert len(attempts.calls) == 1

    async def test_any_exception_class_can_be_retried(self, waits):
        attempts = Attempts(ConnectionResetError(), KeyError("x"), "page")
        with pytest.raises(KeyError):
            await strategy(waits, retry_on=[OSError]).execute_with_retry(attempts)
        assert len(attempts.calls) == 2

    async def test_parse_errors_are_not_retried_by_default(self, waits):
        attempts = Attempts(ParseError(URL, "empty document"), "page")
        with pytest.raises(ParseError):
            await strategy(waits).execute_with_retry(attempts)
        assert len(attempts.calls) == 1

    async def test_last_error_is_raised_after_max_retries(self, waits):
        last = http_error(504)
        attempts = Attempts(http_error(503), http_error(503), http_error(502), last)
        with pytest.raises(HTTPStatusError) as exc_info:
            await strategy(waits, max_retries=3).execute_with_retry(attempts)
        assert exc_info.value is last
        assert len(attempts.calls) == 4
        assert len(waits.delays) == 3

    async def test_zero_retries_makes_one_attempt(self, waits):
        attempts = Attempts(http_error(503), "page")
        with pytest.raises(HTTPStatusError):
            await strategy(waits, max_retries=0).execute_with_retry(attempts)
        assert len(attempts.calls) == 1

    async def test_arguments_are_passed_to_every_attempt(self, waits):
        attempts = Attempts(http_error(503), "page")
        await strategy(waits).execute_with_retry(attempts, URL, html_only=True)
        assert attempts.calls == [((URL,), {"html_only": True})] * 2

    async def test_cancellation_is_not_retried(self, waits):
        attempts = Attempts(asyncio.CancelledError(), "page")
        with pytest.raises(asyncio.CancelledError):
            await strategy(waits).execute_with_retry(attempts)
        assert len(attempts.calls) == 1

    async def test_default_wait_sleeps(self):
        attempts = Attempts(http_error(503), "page")
        retry = RetryStrategy(base_delay=0.02)
        started = time.perf_counter()
        assert await retry.execute_with_retry(attempts) == "page"
        assert time.perf_counter() - started >= 0.01  # the fixed half of the jittered delay


class TestRetryStrategyBackoff:
    async def test_delays_grow_exponentially(self, waits, no_jitter):
        attempts = Attempts(http_error(503))
        with pytest.raises(HTTPStatusError):
            await strategy(waits, max_retries=4, max_delay=100.0).execute_with_retry(attempts)
        assert waits.delays == [1.0, 2.0, 4.0, 8.0]

    async def test_backoff_factor_and_base_delay(self, waits, no_jitter):
        attempts = Attempts(http_error(503))
        retry = strategy(waits, max_retries=3, backoff_factor=3.0, base_delay=0.5, max_delay=100.0)
        with pytest.raises(HTTPStatusError):
            await retry.execute_with_retry(attempts)
        assert waits.delays == [0.5, 1.5, 4.5]

    async def test_delays_are_capped(self, waits, no_jitter):
        attempts = Attempts(http_error(503))
        with pytest.raises(HTTPStatusError):
            await strategy(waits, max_retries=4, max_delay=5.0).execute_with_retry(attempts)
        assert waits.delays == [1.0, 2.0, 4.0, 5.0]

    async def test_huge_backoff_factor_is_capped(self, waits, no_jitter):
        attempts = Attempts(http_error(503))
        retry = strategy(waits, max_retries=200, backoff_factor=1e10, max_delay=5.0)
        with pytest.raises(HTTPStatusError):
            await retry.execute_with_retry(attempts)
        assert set(waits.delays[1:]) == {5.0}

    async def test_jitter_stays_in_upper_half(self, waits):
        attempts = Attempts(http_error(503))
        with pytest.raises(HTTPStatusError):
            await strategy(waits, max_retries=5, max_delay=100.0).execute_with_retry(attempts)
        for retries_done, delay in enumerate(waits.delays):
            full = 2.0**retries_done
            assert full / 2 <= delay <= full

    async def test_retry_after_is_honored_when_longer(self, waits, no_jitter):
        attempts = Attempts(http_error(503, retry_after=7), http_error(503, retry_after=0.1), "page")
        await strategy(waits).execute_with_retry(attempts)
        assert waits.delays == [7.0, 2.0]

    async def test_retry_after_longer_than_max_delay_is_not_retried(self, waits):
        attempts = Attempts(http_error(429, retry_after=31), "page")
        with pytest.raises(HTTPStatusError):
            await strategy(waits, max_delay=30.0).execute_with_retry(attempts)
        assert len(attempts.calls) == 1


class TestRetryRules:
    async def test_server_error_is_retried_once(self, waits):
        attempts = Attempts(http_error(500), http_error(500), "page")
        with pytest.raises(HTTPStatusError):
            await strategy(waits, max_retries=3).execute_with_retry(attempts)
        assert len(attempts.calls) == 2

    async def test_limit_counts_retries_of_its_own_kind(self, waits):
        # The second 500 is not retried, though only one of the retries was for a 500.
        attempts = Attempts(http_error(503), http_error(500), http_error(503), http_error(500), "page")
        with pytest.raises(HTTPStatusError, match="HTTP 500"):
            await strategy(waits, max_retries=5).execute_with_retry(attempts)
        assert len(attempts.calls) == 4

    async def test_too_many_requests_waits_longer(self, waits, no_jitter):
        attempts = Attempts(http_error(429), http_error(503), "page")
        await strategy(waits).execute_with_retry(attempts)
        # 429: base_delay * 4; 503 (second retry): base_delay * 2.
        assert waits.delays == [4.0, 2.0]

    async def test_rules_replace_the_defaults(self, waits):
        attempts = Attempts(http_error(500), http_error(500), "page")
        assert await strategy(waits, rules={}).execute_with_retry(attempts) == "page"

    async def test_rule_of_a_base_class_applies(self, waits, no_jitter):
        attempts = Attempts(FetchTimeoutError(URL, "request timed out"), http_error(503), "page")
        rules = {TransientError: RetryRule(max_retries=1, delay_multiplier=10.0)}
        with pytest.raises(HTTPStatusError):
            await strategy(waits, rules=rules, max_delay=100.0).execute_with_retry(attempts)
        assert waits.delays == [10.0]

    async def test_status_rule_wins_over_class_rule(self, waits):
        rules = {TransientError: RetryRule(max_retries=0), 503: RetryRule()}
        attempts = Attempts(http_error(503), "page")
        assert await strategy(waits, rules=rules).execute_with_retry(attempts) == "page"
        attempts = Attempts(http_error(504), "page")
        with pytest.raises(HTTPStatusError):
            await strategy(waits, rules=rules).execute_with_retry(attempts)

    async def test_rule_limit_under_the_overall_limit(self, waits):
        rules = {NetworkError: RetryRule(max_retries=5)}
        attempts = Attempts(NetworkError(URL, "connection reset"))
        with pytest.raises(NetworkError):
            await strategy(waits, rules=rules, max_retries=2).execute_with_retry(attempts)
        assert len(attempts.calls) == 3


@pytest.mark.parametrize(
    "options",
    [
        {"max_retries": -1},
        {"backoff_factor": 0.5},
        {"backoff_factor": float("nan")},
        {"base_delay": 0},
        {"max_delay": float("inf")},
    ],
)
def test_strategy_rejects_invalid_options(options):
    with pytest.raises(ValueError):
        RetryStrategy(**options)


@pytest.mark.parametrize("retry_on", [["TransientError"], [int], "NetworkError"])
def test_retry_on_must_list_exception_classes(retry_on):
    with pytest.raises(TypeError):
        RetryStrategy(retry_on=retry_on)


@pytest.mark.parametrize("options", [{"max_retries": -1}, {"delay_multiplier": 0}, {"delay_multiplier": float("inf")}])
def test_rule_rejects_invalid_options(options):
    with pytest.raises(ValueError):
        RetryRule(**options)
