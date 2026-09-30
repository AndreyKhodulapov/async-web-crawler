"""When to retry a failed request and how long to wait before it."""

import random
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import ClassVar

from crawler.exceptions import FetchError, FetchTimeoutError, HTTPStatusError, NetworkError


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retries transient failures with exponential backoff.

    Timeouts, network errors and the HTTP statuses in `RETRY_STATUSES` are
    retried up to `max_retries` times. Other errors, such as 404, would fail
    the same way again.

    The n-th retry (from 0) waits `base_delay * 2**n` seconds, at most
    `max_delay`, with "equal jitter": a random half of it is added to a fixed
    half. Jitter keeps many clients that failed together from retrying in
    lockstep; the fixed half keeps a retry from coming right back, as "full
    jitter" (0..delay) can. A Retry-After header from the server is honored
    when it asks for longer, up to `max_delay`.
    """

    RETRY_STATUSES: ClassVar[frozenset[int]] = frozenset({408, 429, 500, 502, 503, 504})

    max_retries: int = 2
    base_delay: float = 1.0
    max_delay: float = 30.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.base_delay <= 0 or self.max_delay <= 0:
            raise ValueError(f"backoff delays must be positive, got {self.base_delay} and {self.max_delay}")

    def should_retry(self, error: FetchError, retries_done: int) -> bool:
        if retries_done >= self.max_retries:
            return False
        if isinstance(error, HTTPStatusError):
            return error.status in self.RETRY_STATUSES
        return isinstance(error, FetchTimeoutError | NetworkError)

    def delay(self, error: FetchError, retries_done: int) -> float:
        backoff = min(self.max_delay, self.base_delay * 2**retries_done)
        backoff = backoff / 2 + random.uniform(0, backoff / 2)
        if isinstance(error, HTTPStatusError) and error.retry_after is not None:
            backoff = max(backoff, error.retry_after)
        return min(backoff, self.max_delay)


def parse_retry_after(value: str | None) -> float | None:
    """Seconds to wait from a Retry-After header: "120" or an HTTP date; None if absent or invalid."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:  # "-0000" in an HTTP date: UTC with no stated zone
        moment = moment.replace(tzinfo=UTC)
    return max(0.0, (moment - datetime.now(UTC)).total_seconds())
