"""Helpers shared by unit and integration tests."""

from crawler import CircuitBreaker, RetryStrategy

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
