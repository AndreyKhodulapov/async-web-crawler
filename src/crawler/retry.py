"""When to retry a failed request and how long to wait before it."""

import asyncio
import functools
import math
import random
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import ClassVar, ParamSpec, TypeVar

from crawler.exceptions import HTTPStatusError, NetworkError, PermanentError, TransientError

P = ParamSpec("P")
T = TypeVar("T")
# Waits before a retry, given the error that caused it and the delay.
Waiter = Callable[[Exception, float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class RetryRule:
    """How one kind of error is retried.

    `max_retries` caps the retries this kind of error may cause, on top of
    the overall limit of the strategy; None leaves only the overall limit.
    `delay_multiplier` scales the backoff delay for it.
    """

    max_retries: int | None = None
    delay_multiplier: float = 1.0

    def __post_init__(self) -> None:
        if self.max_retries is not None and self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if not (math.isfinite(self.delay_multiplier) and self.delay_multiplier > 0):
            raise ValueError(f"delay_multiplier must be a positive number, got {self.delay_multiplier}")


async def _sleep(error: Exception, delay: float) -> None:
    await asyncio.sleep(delay)


class RetryStrategy:
    """Runs a coroutine function again when it fails with a retryable error.

    An error is retried when it is an instance of a class in `retry_on`
    (transient and network errors by default), and never when it is a
    `PermanentError`, whatever `retry_on` says. At most `max_retries`
    retries are made in total.

    The n-th retry (from 0) waits `base_delay * backoff_factor**n` seconds,
    at most `max_delay`, with "equal jitter": a random half of it is added
    to a fixed half. Jitter keeps many clients that failed together from
    retrying in lockstep; the fixed half keeps a retry from coming right
    back, as "full jitter" (0..delay) can. A Retry-After header is honored
    when it asks for longer. A server that asks to wait longer than
    `max_delay` is not retried at all: coming back early would only earn
    another refusal.

    `rules` tune kinds of errors, keyed by an HTTP status or an exception
    class; for a class, the rule of its closest base class applies. The
    default rules retry HTTP 500 only once, since a server error often
    comes from a bug rather than load, and wait four times longer after
    HTTP 429, when the server says it gets too many requests. Passing
    `rules` replaces the defaults.

    `wait(error, delay)` waits before a retry; by default it sleeps. `run`
    takes another one for a single call, e.g. to wait in a rate limiter.
    """

    DEFAULT_RULES: ClassVar[Mapping[int | type[Exception], RetryRule]] = {
        500: RetryRule(max_retries=1),
        429: RetryRule(delay_multiplier=4.0),
    }

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: Iterable[type[Exception]] | None = None,
        *,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        rules: Mapping[int | type[Exception], RetryRule] | None = None,
        wait: Waiter = _sleep,
    ) -> None:
        if max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {max_retries}")
        if not (math.isfinite(backoff_factor) and backoff_factor >= 1):
            raise ValueError(f"backoff_factor must be a number >= 1, got {backoff_factor}")
        if not all(math.isfinite(delay) and delay > 0 for delay in (base_delay, max_delay)):
            raise ValueError(f"backoff delays must be positive numbers, got {base_delay} and {max_delay}")
        retry_on = (TransientError, NetworkError) if retry_on is None else tuple(retry_on)
        if not all(isinstance(kind, type) and issubclass(kind, Exception) for kind in retry_on):
            raise TypeError(f"retry_on must list exception classes, got {retry_on!r}")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.rules = dict(self.DEFAULT_RULES if rules is None else rules)
        self._wait = wait

    async def execute_with_retry(self, coro: Callable[P, Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> T:
        """Call `coro(*args, **kwargs)` until it succeeds or the error is not retried.

        Raises:
            Exception: the error of the last attempt.
        """
        return await self.run(functools.partial(coro, *args, **kwargs))

    async def run(self, call: Callable[[], Awaitable[T]], *, wait: Waiter | None = None) -> T:
        """Like `execute_with_retry`, waiting before a retry with `wait` instead of the strategy's own."""
        wait = wait or self._wait
        retries_by_rule: Counter[int | type[Exception] | None] = Counter()
        while True:
            try:
                return await call()
            except Exception as error:
                key, rule = self._rule_for(error)
                if not self._should_retry(error, rule, retries_by_rule[key], retries_by_rule.total()):
                    raise
                delay = self._delay(error, rule, retries_by_rule.total())
                retries_by_rule[key] += 1
                await wait(error, delay)

    def _rule_for(self, error: Exception) -> tuple[int | type[Exception] | None, RetryRule]:
        if isinstance(error, HTTPStatusError) and error.status in self.rules:
            return error.status, self.rules[error.status]
        for cls in type(error).__mro__:
            if cls in self.rules:
                return cls, self.rules[cls]
        return None, RetryRule()

    def _should_retry(self, error: Exception, rule: RetryRule, rule_retries: int, retries: int) -> bool:
        if isinstance(error, PermanentError) or not isinstance(error, self.retry_on):
            return False
        if retries >= self.max_retries or (rule.max_retries is not None and rule_retries >= rule.max_retries):
            return False
        retry_after = error.retry_after if isinstance(error, HTTPStatusError) else None
        return retry_after is None or retry_after <= self.max_delay

    def _delay(self, error: Exception, rule: RetryRule, retries: int) -> float:
        try:
            backoff = min(self.max_delay, self.base_delay * rule.delay_multiplier * self.backoff_factor**retries)
        except OverflowError:
            backoff = self.max_delay
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
