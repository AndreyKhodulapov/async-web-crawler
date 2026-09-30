"""When to retry a failed request and how long to wait before it."""

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from crawler.exceptions import FetchError, HTTPStatusError, NetworkError, TransientError


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retries transient failures with exponential backoff.

    Transient and network errors (a timeout, HTTP 503, a refused
    connection) are retried up to `max_retries` times. Other errors, such as
    HTTP 404 or a redirect loop, would fail the same way again.

    The n-th retry (from 0) waits `base_delay * 2**n` seconds, at most
    `max_delay`, with "equal jitter": a random half of it is added to a fixed
    half. Jitter keeps many clients that failed together from retrying in
    lockstep; the fixed half keeps a retry from coming right back, as "full
    jitter" (0..delay) can. A Retry-After header from the server is honored
    when it asks for longer. A server that asks to wait longer than
    `max_delay` is not retried at all: coming back early would only earn
    another refusal.
    """

    max_retries: int = 2
    base_delay: float = 1.0
    max_delay: float = 30.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if not all(math.isfinite(delay) and delay > 0 for delay in (self.base_delay, self.max_delay)):
            raise ValueError(f"backoff delays must be positive numbers, got {self.base_delay} and {self.max_delay}")

    def should_retry(self, error: FetchError, retries_done: int) -> bool:
        if retries_done >= self.max_retries:
            return False
        if isinstance(error, HTTPStatusError) and error.retry_after is not None and error.retry_after > self.max_delay:
            return False
        return isinstance(error, TransientError | NetworkError)

    def delay(self, error: FetchError, retries_done: int) -> float:
        # 2**1024 does not fit a float; the cap is reached long before 2**64 anyway.
        backoff = min(self.max_delay, self.base_delay * 2 ** min(retries_done, 64))
        backoff = backoff / 2 + random.uniform(0, backoff / 2)
        if isinstance(error, HTTPStatusError) and error.retry_after is not None:
            backoff = max(backoff, error.retry_after)
        return min(backoff, self.max_delay)


def parse_retry_after(value: str | None) -> float | None:
    """Seconds to wait from a Retry-After header: "120" or an HTTP date; None if absent or invalid."""
    if value is None:
        return None
    value = value.strip()
    # isdigit() alone accepts non-ASCII digits such as "²", which float() rejects.
    if value.isascii() and value.isdigit():
        return float(value)
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:  # "-0000" in an HTTP date: UTC with no stated zone
        moment = moment.replace(tzinfo=UTC)
    return max(0.0, (moment - datetime.now(UTC)).total_seconds())
