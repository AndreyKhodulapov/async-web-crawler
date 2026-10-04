"""Circuit breaker: stops sending requests to a host that keeps failing."""

import logging
import math
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from types import TracebackType
from typing import Self

from crawler.exceptions import (
    CircuitOpenError,
    FetchError,
    HTTPStatusError,
    NetworkError,
    ProxyError,
    TransientError,
)
from crawler.models import CircuitStats
from crawler.urls import get_host

logger = logging.getLogger(__name__)


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"


@dataclass(slots=True)
class _Outcome:
    """The outcome of one call, as counted in the window of its circuit."""

    time: float
    failed: bool
    counted: bool = True  # False once it has left the window


@dataclass(slots=True)
class _Circuit:
    state: CircuitState = CircuitState.CLOSED
    # The outcomes counted while closed, oldest first.
    outcomes: deque[_Outcome] = field(default_factory=deque)
    failures: int = 0  # the failed ones among `outcomes`
    opened_at: float = 0.0
    reason: str = ""
    probe: "BreakerCall | None" = None
    failed_probe: str | None = None  # the URL of the probe whose failure opened the circuit, if one did
    times_opened: int = 0
    rejected: int = 0


class CircuitBreaker:
    """Stops requests to a host that keeps failing, then lets them through again.

    Usage::

        breaker = CircuitBreaker(failure_threshold=0.5, cooldown=30.0)
        with breaker.call(url) as call:  # CircuitOpenError while the host is blocked
            ...  # wait for the turn of the host
            call.admit()  # CircuitOpenError if the circuit has opened meanwhile
            error = ...  # send the request
            call.record(error)  # None on success

    Every host has a circuit of its own, in one of three states:

    - closed: requests go through, and their outcomes are counted over the
      last `window` seconds. Once at least `min_requests` are counted and
      the share of failures among them reaches `failure_threshold`, the
      circuit opens.
    - open: requests fail at once with `CircuitOpenError`, without reaching
      the server, for `cooldown` seconds. A host in trouble gets time to
      recover instead of a retry of every page it failed (a retry storm).
    - half-open: after the cooldown, one request goes through as a probe
      and the rest are still refused. The probe's success closes the
      circuit with an empty window, its failure opens it for another cooldown.

    Failures are the errors that say the host is in trouble: a
    `TransientError` (a timeout, HTTP 408, 429), a `NetworkError` or any
    HTTP 5xx, even one not worth a retry, such as 501. Any other response
    of the server, HTTP 404 included, is a success: the host is up, and a
    site with broken links must not be blocked for them. Other errors, such
    as a bad certificate, count neither way, and so does a `ProxyError`
    (a `ProxyNetworkError` included): a dead proxy says nothing of the host.

    A call counts once, however many attempts it takes: the retries of a
    request reuse its `BreakerCall`, and each `record` replaces the outcome
    of the one before. The first failure counts at once, so a host that
    goes down is spotted after its first failed requests, not after their
    retries; a failed retry adds nothing; a retry that succeeds turns the
    failure into a success, so one broken page retried three times does
    not open the circuit, and neither does a slow host whose pages come
    through on the second attempt, as long as the retries land before
    `min_requests` first attempts have failed: with that many requests to
    the host in flight at once, their timeouts open the circuit before any
    retry. Separate calls to the same URL count separately.

    `failure_threshold=None` turns the breaker off: every request goes through.
    """

    def __init__(
        self,
        failure_threshold: float | None = 0.5,
        *,
        min_requests: int = 5,
        window: float = 60.0,
        cooldown: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold is not None and not 0 < failure_threshold <= 1:
            raise ValueError(f"failure_threshold must be in (0, 1] or None, got {failure_threshold}")
        if min_requests < 1:
            raise ValueError(f"min_requests must be >= 1, got {min_requests}")
        if not (math.isfinite(window) and window > 0):
            raise ValueError(f"window must be a positive number of seconds, got {window}")
        if not (math.isfinite(cooldown) and cooldown >= 0):
            raise ValueError(f"cooldown must be a number of seconds >= 0, got {cooldown}")
        self.failure_threshold = failure_threshold
        self.min_requests = min_requests
        self.window = window
        self.cooldown = cooldown
        self._clock = clock
        self._circuits: dict[str, _Circuit] = {}

    @property
    def enabled(self) -> bool:
        return self.failure_threshold is not None

    def state(self, host: str) -> CircuitState:
        circuit = self._circuits.get(host)
        return CircuitState.CLOSED if circuit is None else self._state(circuit)

    def check(self, url: str) -> None:
        """Raise `CircuitOpenError` if a request to `url` would be refused now.

        Changes nothing otherwise: it refuses a request early, before it
        waits for a rate limit or a free slot; `call` decides for good.
        """
        if (found := self._lookup(url)) is None:
            return
        host, circuit = found
        if (message := self._verdict(host, circuit, None)) is not None:
            self._reject(url, circuit, message)

    def refusal(self, url: str) -> str | None:
        """Why a request to `url` would be refused now, None if it would go through; not counted as refused."""
        found = self._lookup(url)
        return None if found is None else self._verdict(*found, None)

    def probe_in(self, url: str) -> float:
        """Seconds until the circuit of the host of `url` lets a probe through; 0 unless it is open."""
        if (found := self._lookup(url)) is None:
            return 0.0
        _, circuit = found
        if self._state(circuit) is not CircuitState.OPEN:
            return 0.0
        return max(circuit.opened_at + self.cooldown - self._clock(), 0.0)

    def times_opened(self, host: str) -> int:
        """How many times the circuit of `host` has opened since `reset_stats`."""
        circuit = self._circuits.get(host)
        return 0 if circuit is None else circuit.times_opened

    def opened_by_probe(self, url: str) -> bool:
        """Whether the circuit of the host of `url` is open because the probe of `url` itself failed.

        False while the circuit is closed, and when it opened on the
        failures counted in its window, or on the probe of another URL.
        """
        if (found := self._lookup(url)) is None:
            return False
        _, circuit = found
        return self._state(circuit) is not CircuitState.CLOSED and circuit.failed_probe == url

    @staticmethod
    def is_failure(error: FetchError | None) -> bool:
        """Whether an outcome counts as a failure of the host: a transient or network error, or an HTTP 5xx.

        An error of a proxy is not one.
        """
        return _is_failure(error) is True

    def call(self, url: str) -> "BreakerCall":
        """A request to `url`: entering it raises `CircuitOpenError` if the request is refused."""
        return BreakerCall(self, url, get_host(url))

    def get_stats(self) -> dict[str, CircuitStats]:
        """The circuits of the hosts requested so far, by host."""
        now = self._clock()
        stats = {}
        for host, circuit in self._circuits.items():
            self._forget(circuit, now)
            stats[host] = CircuitStats(
                state=self._state(circuit).value,
                requests=len(circuit.outcomes),
                failures=circuit.failures,
                times_opened=circuit.times_opened,
                rejected=circuit.rejected,
            )
        return stats

    def reset_stats(self) -> None:
        """Count `times_opened` and `rejected` from zero; the states are kept."""
        for circuit in self._circuits.values():
            circuit.times_opened = circuit.rejected = 0

    def _lookup(self, url: str) -> tuple[str, _Circuit] | None:
        """The host of `url` and its circuit; None when the breaker is off or the host has no circuit yet."""
        host = get_host(url)
        circuit = self._circuits.get(host) if self.enabled and host is not None else None
        return None if circuit is None else (host, circuit)

    def _circuit(self, call: "BreakerCall") -> _Circuit | None:
        """The circuit of the host of `call`; none when the breaker is off or the URL has no host."""
        if not self.enabled or call.host is None:
            return None
        return self._circuits.setdefault(call.host, _Circuit())

    def _state(self, circuit: _Circuit) -> CircuitState:
        if circuit.state is CircuitState.OPEN and self._clock() >= circuit.opened_at + self.cooldown:
            circuit.state = CircuitState.HALF_OPEN
        return circuit.state

    def _verdict(self, host: str, circuit: _Circuit, call: "BreakerCall | None") -> str | None:
        """Why a request is refused, None if it may go; a `call` takes the probe of a half-open circuit."""
        state = self._state(circuit)
        if state is CircuitState.CLOSED or (call is not None and circuit.probe is call):
            return None
        if state is CircuitState.HALF_OPEN and circuit.probe is None:
            if call is not None:
                circuit.probe = call
                logger.info("Circuit breaker of %s is half-open: probing it with %s", host, call.url)
            return None
        if state is CircuitState.OPEN:
            left = circuit.opened_at + self.cooldown - self._clock()
            return f"circuit breaker of {host} is open ({circuit.reason}), next probe in {left:.1f}s"
        return f"circuit breaker of {host} is half-open, waiting for the probe request"

    def _admit(self, call: "BreakerCall") -> None:
        circuit = self._circuit(call)
        if circuit is None:
            return
        assert call.host is not None
        if (message := self._verdict(call.host, circuit, call)) is not None:
            self._reject(call.url, circuit, message)

    def _reject(self, url: str, circuit: _Circuit, message: str) -> None:
        circuit.rejected += 1
        raise CircuitOpenError(url, message)

    def _record(self, call: "BreakerCall", error: FetchError | None) -> None:
        circuit = self._circuit(call)
        if circuit is None:
            return
        assert call.host is not None  # it has a circuit
        host, url = call.host, call.url
        probe = circuit.probe is call
        if probe:
            circuit.probe = None
        failed = _is_failure(error)
        if failed is None:
            return
        if probe:
            if failed:
                assert error is not None
                self._open(host, circuit, f"probe {url} failed: {type(error).__name__}: {error.message}", probe=url)
            else:
                circuit.state = CircuitState.CLOSED
                self._clear(circuit)
                logger.info("Circuit breaker of %s closed: probe %s succeeded", host, url)
            return
        if self._state(circuit) is not CircuitState.CLOSED:
            return  # a request sent before the circuit opened: the probe decides
        now = self._clock()
        self._forget(circuit, now)
        outcome = call._outcome
        if outcome is not None and outcome.counted:
            # A retry of the call: its outcome stands for the call now.
            circuit.failures += failed - outcome.failed
            outcome.failed = failed
        else:
            outcome = call._outcome = _Outcome(now, failed)
            circuit.outcomes.append(outcome)
            circuit.failures += failed
        if not failed:
            return
        assert self.failure_threshold is not None  # the breaker is on
        requests = len(circuit.outcomes)
        if requests >= self.min_requests and circuit.failures >= self.failure_threshold * requests:
            self._open(host, circuit, f"{circuit.failures} of {requests} requests failed in {self.window:g}s")

    def _release(self, call: "BreakerCall") -> None:
        """A call is over: a probe that recorded nothing lets another request probe."""
        circuit = self._circuit(call)
        if circuit is not None and circuit.probe is call:
            circuit.probe = None

    def _open(self, host: str, circuit: _Circuit, reason: str, *, probe: str | None = None) -> None:
        circuit.state = CircuitState.OPEN
        circuit.opened_at = self._clock()
        circuit.reason = reason
        circuit.failed_probe = probe
        self._clear(circuit)
        circuit.times_opened += 1
        logger.warning("Circuit breaker of %s opened: %s; requests to it fail for %gs", host, reason, self.cooldown)

    def _forget(self, circuit: _Circuit, now: float) -> None:
        while circuit.outcomes and circuit.outcomes[0].time <= now - self.window:
            outcome = circuit.outcomes.popleft()
            outcome.counted = False
            circuit.failures -= outcome.failed

    @staticmethod
    def _clear(circuit: _Circuit) -> None:
        for outcome in circuit.outcomes:
            outcome.counted = False
        circuit.outcomes.clear()
        circuit.failures = 0


class BreakerCall:
    """One request under a `CircuitBreaker`, a context manager.

    Entering it admits the request or raises `CircuitOpenError`; a request
    to a half-open circuit may take the probe then. `admit` asks again, e.g.
    after a wait for the rate limit, in case the circuit has opened
    meanwhile; it changes nothing for a request already let through as the
    probe. Inside, `record` tells how the request went. Exiting frees a
    probe that recorded nothing. A retry of the request enters the same
    call again: its outcome replaces the one recorded before (see
    `CircuitBreaker`).
    """

    def __init__(self, breaker: CircuitBreaker, url: str, host: str | None) -> None:
        self._breaker = breaker
        self.url = url
        self.host = host
        self._outcome: _Outcome | None = None  # the outcome of the call in the window of its circuit

    def __enter__(self) -> Self:
        self.admit()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._breaker._release(self)

    def admit(self) -> None:
        """Raise `CircuitOpenError` if the request may not go now."""
        self._breaker._admit(self)

    def record(self, error: FetchError | None) -> None:
        """The outcome of the request: the error it failed with, None on success; a retry's replaces it."""
        self._breaker._record(self, error)


def _is_failure(error: FetchError | None) -> bool | None:
    """Whether an outcome counts as a failure of the host; None if it counts neither way."""
    if isinstance(error, ProxyError):
        return None  # a dead proxy must not block the sites behind it
    if isinstance(error, TransientError | NetworkError):
        return True
    if isinstance(error, HTTPStatusError):
        return error.status >= 500
    if error is None:
        return False
    return None
