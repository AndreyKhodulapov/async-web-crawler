"""Helpers shared by unit and integration tests."""

from datetime import UTC, datetime

from crawler import CircuitBreaker, PageRecord, RetryStrategy

BOT = "TestBot/1.0 (+https://example.com/bot)"

# Crawler options for tests that check something other than politeness:
# without the rate limit, robots.txt, retries and the circuit breaker they
# run fast and see only the requests they make themselves.
UNTHROTTLED = {
    "requests_per_second": None,
    "respect_robots": False,
    "retry_strategy": RetryStrategy(max_retries=0),
    "circuit_breaker": CircuitBreaker(failure_threshold=None),
}


class FakeClock:
    """A clock for the `clock` option that moves only when a test sets `now`."""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def make_record(url: str = "https://site/page", **fields: object) -> PageRecord:
    """A page record for storage tests; `fields` replace the defaults."""
    record: PageRecord = {
        "url": url,
        "title": "Page",
        "text": "Some text",
        "links": ["https://site/a", "https://site/b"],
        "metadata": {"description": "A page", "keywords": ["one", "two"], "language": "en", "depth": 1},
        "crawled_at": datetime(2025, 3, 14, 15, 9, 26, 535897, tzinfo=UTC),
        "status_code": 200,
        "content_type": "text/html",
    }
    return record | fields
