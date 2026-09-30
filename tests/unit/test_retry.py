"""Unit tests for RetryPolicy and Retry-After parsing."""

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest

from crawler import (
    CrawlerClosedError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    RetryPolicy,
    RobotsDisallowedError,
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
        (InvalidURLError(URL, "bad port"), False),
        (RobotsDisallowedError(URL, "disallowed by robots.txt"), False),
        (CrawlerClosedError(URL, "crawler is closed"), False),
        (UnexpectedError(URL, "KeyError: 'x'"), False),
    ],
)
def test_only_transient_errors_are_retried(error, expected):
    assert RetryPolicy().should_retry(error, 0) is expected


def test_retries_stop_at_the_limit():
    policy = RetryPolicy(max_retries=2)
    assert [policy.should_retry(http_error(503), done) for done in range(4)] == [True, True, False, False]


@pytest.mark.parametrize("options", [{"max_retries": -1}, {"base_delay": 0}, {"max_delay": -1}])
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

    @pytest.mark.parametrize("value", [None, "", "soon", "-5", "1.5"])
    def test_invalid_values(self, value):
        assert parse_retry_after(value) is None
