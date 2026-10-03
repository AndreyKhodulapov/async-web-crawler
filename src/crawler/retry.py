"""When to retry a failed request and how long to wait before it."""

import asyncio
import functools
import logging
import math
import random
import reprlib
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import ClassVar, ParamSpec, TypeVar

from crawler.exceptions import FetchError, HTTPStatusError, NetworkError, PermanentError, TransientError

logger = logging.getLogger(__name__)
# Shortens arguments in the log, but keeps a URL whole.
_short_repr = reprlib.Repr()
_short_repr.maxstring = _short_repr.maxother = 200

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
    another refusal. (`AsyncCrawler` still holds the host back for as long
    as it asked.)

    `rules` tune kinds of errors, keyed by an HTTP status or an exception
    class; for a class, the rule of its closest base class applies. The
    default rules retry HTTP 500 only once, since a server error often
    comes from a bug rather than load, and wait four times longer after
    HTTP 429, when the server says it gets too many requests. Passing
    `rules` replaces the defaults.

    `wait(error, delay)` waits before a retry; by default it sleeps. `run`
    takes another one for a single call, e.g. to wait in a rate limiter.

    Every failed attempt is logged with the error, the attempt number and
    the delay before the next one; so is the outcome: a failure with the
    reason it was not retried, or a success that took retries.
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
        return await self.run(functools.partial(coro, *args, **kwargs), target=_describe_call(coro, args, kwargs))

    async def run(
        self,
        call: Callable[[], Awaitable[T]],
        *,
        wait: Waiter | None = None,
        target: str | None = None,
        failure_level: int = logging.WARNING,
        veto: Callable[[Exception], str | None] | None = None,
    ) -> T:
        """Like `execute_with_retry`, with options for a single call.

        `wait` replaces the strategy's own wait. `target` names the call in
        the log, e.g. by its URL. `failure_level` is the log level of the
        final failure; retries are always logged as warnings. `veto(error)`
        can forbid a retry the strategy would make, before its wait: it
        returns the reason, or None to let the retry go.
        """
        wait = wait or self._wait
        target = target or getattr(call, "__qualname__", repr(call))
        max_attempts = self.max_retries + 1
        retries_by_rule: Counter[int | type[Exception] | None] = Counter()
        started = time.perf_counter()
        while True:
            attempt = retries_by_rule.total() + 1
            try:
                result = await call()
            except Exception as error:
                key, rule = self._rule_for(error)
                refusal = self._refusal(error, key, rule, retries_by_rule[key], retries_by_rule.total())
                if refusal is None and veto is not None:
                    refusal = veto(error)
                if refusal is not None:
                    logger.log(
                        failure_level,
                        "Failed %s on attempt %d/%d after %.2fs, %s: %s",
                        target,
                        attempt,
                        max_attempts,
                        time.perf_counter() - started,
                        refusal,
                        _describe_error(error),
                    )
                    raise
                delay = self._delay(error, rule, retries_by_rule.total())
                logger.warning(
                    "Attempt %d/%d for %s failed: %s; retrying in %.1fs",
                    attempt,
                    max_attempts,
                    target,
                    _describe_error(error),
                    delay,
                )
                retries_by_rule[key] += 1
                await wait(error, delay)
            else:
                logger.log(
                    logging.INFO if attempt > 1 else logging.DEBUG,
                    "Succeeded %s on attempt %d/%d after %.2fs",
                    target,
                    attempt,
                    max_attempts,
                    time.perf_counter() - started,
                )
                return result

    def _rule_for(self, error: Exception) -> tuple[int | type[Exception] | None, RetryRule]:
        if isinstance(error, HTTPStatusError) and error.status in self.rules:
            return error.status, self.rules[error.status]
        for cls in type(error).__mro__:
            if cls in self.rules:
                return cls, self.rules[cls]
        return None, RetryRule()

    def _refusal(
        self,
        error: Exception,
        key: int | type[Exception] | None,
        rule: RetryRule,
        rule_retries: int,
        retries: int,
    ) -> str | None:
        """Why `error` is not retried, or None if it is."""
        if isinstance(error, PermanentError):
            return "permanent error"
        if not isinstance(error, self.retry_on):
            return "not a retried error type"
        if retries >= self.max_retries:
            return "no retries left"
        if rule.max_retries is not None and rule_retries >= rule.max_retries:
            kind = f"HTTP {key}" if isinstance(key, int) else getattr(key, "__name__", "this error")
            return f"no retries left for {kind}"
        retry_after = error.retry_after if isinstance(error, HTTPStatusError) else None
        if retry_after is not None and retry_after > self.max_delay:
            return f"Retry-After of {retry_after:g}s is longer than max_delay of {self.max_delay:g}s"
        return None

    def _delay(self, error: Exception, rule: RetryRule, retries: int) -> float:
        try:
            backoff = min(self.max_delay, self.base_delay * rule.delay_multiplier * self.backoff_factor**retries)
        except OverflowError:
            backoff = self.max_delay
        backoff = backoff / 2 + random.uniform(0, backoff / 2)
        if isinstance(error, HTTPStatusError) and error.retry_after is not None:
            backoff = max(backoff, error.retry_after)
        return min(backoff, self.max_delay)


def _describe_call(coro: Callable[..., object], args: tuple[object, ...], kwargs: dict[str, object]) -> str:
    """A short form of a call for the log, e.g. fetch_url('https://example.com')."""
    name = getattr(coro, "__name__", None) or repr(coro)
    arguments = [_short_repr.repr(arg) for arg in args]
    arguments += [f"{key}={_short_repr.repr(value)}" for key, value in kwargs.items()]
    return f"{name}({', '.join(arguments)})"


def _describe_error(error: Exception) -> str:
    # A FetchError repeats its URL in str(), and the log line already names the target.
    message = error.message if isinstance(error, FetchError) else str(error)
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


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
