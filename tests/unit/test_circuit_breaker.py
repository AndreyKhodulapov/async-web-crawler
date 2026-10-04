"""Unit tests for CircuitBreaker: when a host is blocked, the probe, the stats."""

import logging

import pytest
from helpers import FakeClock

from crawler import (
    CertificateError,
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    CircuitStats,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    NetworkError,
)

URL = "http://a.test/page"
OTHER_HOST_URL = "http://b.test/page"
TIMEOUT = FetchTimeoutError(URL, "timed out")
REFUSED = NetworkError(URL, "connection refused")
NOT_FOUND = HTTPStatusError(URL, 404, "Not Found")
BAD_CERTIFICATE = CertificateError(URL, "certificate verify failed")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def breaker(clock) -> CircuitBreaker:
    return CircuitBreaker(0.5, min_requests=4, window=60.0, cooldown=30.0, clock=clock)


def request(breaker: CircuitBreaker, *outcomes: FetchError | None, url: str = URL) -> None:
    """Make a request to `url` for every outcome; None is a success."""
    for outcome in outcomes:
        with breaker.call(url) as call:
            call.record(outcome)


def open_circuit(breaker: CircuitBreaker) -> None:
    request(breaker, *[TIMEOUT] * breaker.min_requests)
    assert breaker.state("a.test") is CircuitState.OPEN


class TestClosed:
    def test_unknown_host_is_closed(self, breaker):
        assert breaker.state("a.test") is CircuitState.CLOSED
        breaker.check(URL)
        request(breaker, None)

    def test_opens_once_the_failures_reach_the_threshold(self, breaker):
        request(breaker, None, TIMEOUT, None)
        assert breaker.state("a.test") is CircuitState.CLOSED  # 3 requests, fewer than min_requests
        request(breaker, REFUSED)  # 2 of 4 failed
        assert breaker.state("a.test") is CircuitState.OPEN

    def test_stays_closed_below_the_threshold(self, breaker):
        request(breaker, None, None, None, TIMEOUT, TIMEOUT)  # 2 of 5
        assert breaker.state("a.test") is CircuitState.CLOSED

    def test_waits_for_min_requests(self, breaker):
        request(breaker, TIMEOUT, TIMEOUT, TIMEOUT)
        assert breaker.state("a.test") is CircuitState.CLOSED
        request(breaker, TIMEOUT)
        assert breaker.state("a.test") is CircuitState.OPEN

    @pytest.mark.parametrize("status", [408, 429, 500, 503, 522])
    def test_transient_http_errors_are_failures(self, breaker, status):
        request(breaker, *[HTTPStatusError(URL, status, "Error")] * 4)
        assert breaker.state("a.test") is CircuitState.OPEN

    @pytest.mark.parametrize("status", [501, 505, 525])
    def test_server_errors_not_worth_a_retry_are_failures_too(self, breaker, status):
        request(breaker, *[HTTPStatusError(URL, status, "Error")] * 4)
        assert breaker.state("a.test") is CircuitState.OPEN

    def test_any_response_is_a_success(self, breaker):
        # A site with broken links is up; it must not be blocked for them.
        request(breaker, *[NOT_FOUND, HTTPStatusError(URL, 403, "Forbidden")] * 3)
        assert breaker.state("a.test") is CircuitState.CLOSED
        assert breaker.get_stats()["a.test"] == CircuitStats(state="closed", requests=6, failures=0)

    def test_other_errors_count_neither_way(self, breaker):
        request(breaker, BAD_CERTIFICATE, BAD_CERTIFICATE, TIMEOUT, TIMEOUT, TIMEOUT)
        assert breaker.state("a.test") is CircuitState.CLOSED
        assert breaker.get_stats()["a.test"] == CircuitStats(state="closed", requests=3, failures=3)

    def test_old_outcomes_leave_the_window(self, breaker, clock):
        request(breaker, TIMEOUT, TIMEOUT, TIMEOUT)
        clock.now += 60
        request(breaker, TIMEOUT)
        assert breaker.state("a.test") is CircuitState.CLOSED
        assert breaker.get_stats()["a.test"].requests == 1

    def test_hosts_are_independent(self, breaker):
        open_circuit(breaker)
        request(breaker, None, url=OTHER_HOST_URL)
        assert breaker.state("b.test") is CircuitState.CLOSED


class TestRetries:
    """The retries of a request record their outcomes on the call of its first attempt."""

    @staticmethod
    def attempts(breaker: CircuitBreaker, *outcomes: FetchError | None) -> None:
        call = breaker.call(URL)
        for outcome in outcomes:
            with call:
                call.record(outcome)

    def test_a_retry_that_succeeds_makes_the_request_a_success(self, breaker):
        self.attempts(breaker, TIMEOUT, None)
        assert breaker.get_stats()["a.test"] == CircuitStats(state="closed", requests=1, failures=0)

    def test_failed_retries_count_once(self, breaker):
        # Three requests failing four times each: fewer than min_requests, 4 of them.
        for _ in range(3):
            self.attempts(breaker, TIMEOUT, TIMEOUT, TIMEOUT, TIMEOUT)
        assert breaker.state("a.test") is CircuitState.CLOSED
        assert breaker.get_stats()["a.test"] == CircuitStats(state="closed", requests=3, failures=3)

    def test_the_first_failure_counts_at_once(self, breaker):
        # Four requests failing on their first attempt open the circuit before any retry.
        calls = [breaker.call(URL) for _ in range(4)]
        for call in calls:
            with call:
                call.record(REFUSED)
        assert breaker.state("a.test") is CircuitState.OPEN
        with pytest.raises(CircuitOpenError), calls[0]:
            pass

    def test_a_retry_after_the_window_counts_anew(self, breaker, clock):
        call = breaker.call(URL)
        with call:
            call.record(TIMEOUT)
        clock.now += 60
        with call:
            call.record(None)
        assert breaker.get_stats()["a.test"] == CircuitStats(state="closed", requests=1, failures=0)

    def test_a_retry_after_the_circuit_closed_counts_anew(self, breaker, clock):
        # The failure left the window when the circuit opened; the probe closed it.
        call = breaker.call(URL)
        with call:
            call.record(TIMEOUT)
        request(breaker, TIMEOUT, TIMEOUT, TIMEOUT)  # 4 of 4 with the call's failure
        assert breaker.state("a.test") is CircuitState.OPEN
        clock.now += breaker.cooldown
        request(breaker, None)
        with call:
            call.record(TIMEOUT)
        assert breaker.get_stats()["a.test"] == CircuitStats(state="closed", requests=1, failures=1, times_opened=1)


class TestOpen:
    def test_requests_are_refused(self, breaker, clock):
        open_circuit(breaker)
        clock.now += 10
        message = r"circuit breaker of a\.test is open \(4 of 4 requests failed in 60s\), next probe in 20\.0s"
        with pytest.raises(CircuitOpenError, match=message) as refusal:
            breaker.check(URL)
        assert refusal.value.url == URL
        with pytest.raises(CircuitOpenError, match=message):
            request(breaker, None)

    def test_half_open_after_the_cooldown(self, breaker, clock):
        open_circuit(breaker)
        clock.now += 29.9
        assert breaker.state("a.test") is CircuitState.OPEN
        clock.now += 0.1
        assert breaker.state("a.test") is CircuitState.HALF_OPEN

    def test_admit_again_refuses_once_it_has_opened(self, breaker):
        # A request let through, then kept waiting for its turn.
        with breaker.call(URL) as waiting:
            open_circuit(breaker)
            with pytest.raises(CircuitOpenError, match="is open"):
                waiting.admit()
        assert breaker.get_stats()["a.test"].rejected == 1

    def test_late_outcomes_do_not_close_it(self, breaker):
        # A request sent before the circuit opened ends after it.
        with breaker.call(URL) as late:
            open_circuit(breaker)
            late.record(None)
        assert breaker.state("a.test") is CircuitState.OPEN


class TestHalfOpen:
    @pytest.fixture
    def half_open(self, breaker, clock) -> CircuitBreaker:
        open_circuit(breaker)
        clock.now += breaker.cooldown
        return breaker

    def test_one_probe_at_a_time(self, half_open):
        with half_open.call(URL):
            with pytest.raises(CircuitOpenError, match="half-open, waiting for the probe request"):
                half_open.check(URL)
            with pytest.raises(CircuitOpenError, match="half-open"):
                request(half_open, None)

    def test_check_does_not_take_the_probe(self, half_open):
        half_open.check(URL)
        half_open.check(URL)
        request(half_open, None)
        assert half_open.state("a.test") is CircuitState.CLOSED

    def test_probe_success_closes_with_an_empty_window(self, half_open):
        request(half_open, None)
        assert half_open.get_stats()["a.test"] == CircuitStats(state="closed", times_opened=1)
        request(half_open, TIMEOUT, TIMEOUT, TIMEOUT)
        assert half_open.state("a.test") is CircuitState.CLOSED

    def test_response_with_an_error_status_closes_it(self, half_open):
        request(half_open, NOT_FOUND)
        assert half_open.state("a.test") is CircuitState.CLOSED

    def test_probe_failure_opens_it_again(self, half_open, clock):
        request(half_open, REFUSED)
        assert half_open.state("a.test") is CircuitState.OPEN
        with pytest.raises(CircuitOpenError, match=r"probe http://a\.test/page failed: NetworkError"):
            half_open.check(URL)
        clock.now += half_open.cooldown
        assert half_open.state("a.test") is CircuitState.HALF_OPEN
        assert half_open.get_stats()["a.test"].times_opened == 2

    def test_probe_without_an_outcome_lets_another_request_probe(self, half_open):
        with half_open.call(URL):
            pass
        request(half_open, BAD_CERTIFICATE)
        with pytest.raises(RuntimeError), half_open.call(URL):
            raise RuntimeError("cancelled")
        assert half_open.state("a.test") is CircuitState.HALF_OPEN
        request(half_open, None)
        assert half_open.state("a.test") is CircuitState.CLOSED

    def test_probe_is_taken_on_entry(self, half_open):
        with half_open.call(URL) as probe:
            with pytest.raises(CircuitOpenError, match="half-open"):
                half_open.check(URL)
            probe.admit()  # asked again after a wait: still the probe
            probe.record(None)
        assert half_open.state("a.test") is CircuitState.CLOSED


class TestStats:
    def test_counts_openings_and_refusals(self, breaker, clock):
        open_circuit(breaker)
        for _ in range(2):
            with pytest.raises(CircuitOpenError):
                breaker.check(URL)
        with pytest.raises(CircuitOpenError):
            request(breaker, None)
        request(breaker, None, url=OTHER_HOST_URL)

        assert breaker.get_stats() == {
            "a.test": CircuitStats(state="open", times_opened=1, rejected=3),
            "b.test": CircuitStats(state="closed", requests=1),
        }

    def test_reset_keeps_the_state(self, breaker):
        open_circuit(breaker)
        with pytest.raises(CircuitOpenError):
            breaker.check(URL)
        breaker.reset_stats()
        assert breaker.get_stats()["a.test"] == CircuitStats(state="open")


class TestProbeIn:
    def test_time_left_of_the_cooldown_while_open(self, breaker, clock):
        assert breaker.probe_in(URL) == 0
        open_circuit(breaker)
        clock.now += 10
        assert breaker.probe_in(URL) == 20
        assert breaker.probe_in(OTHER_HOST_URL) == 0

    def test_nothing_to_wait_for_once_half_open(self, breaker, clock):
        open_circuit(breaker)
        clock.now += breaker.cooldown
        with breaker.call(URL):  # the probe is in flight
            assert breaker.probe_in(URL) == 0


def test_times_opened_counts_since_the_reset(breaker, clock):
    assert breaker.times_opened("a.test") == 0
    open_circuit(breaker)
    clock.now += breaker.cooldown
    request(breaker, TIMEOUT)  # the probe fails
    assert breaker.times_opened("a.test") == 2
    breaker.reset_stats()
    assert breaker.times_opened("a.test") == 0


def test_checks_do_not_create_circuits(breaker):
    breaker.check(URL)
    assert breaker.refusal(URL) is None
    assert breaker.get_stats() == {}


def test_disabled_breaker_lets_everything_through():
    breaker = CircuitBreaker(failure_threshold=None)
    request(breaker, *[TIMEOUT] * 20)
    breaker.check(URL)
    assert not breaker.enabled
    assert breaker.get_stats() == {}


def test_url_without_host_is_let_through(breaker):
    request(breaker, *[TIMEOUT] * 10, url="not a url")
    assert breaker.get_stats() == {}


def test_transitions_are_logged(breaker, clock, caplog):
    caplog.set_level(logging.INFO, logger="crawler.circuit_breaker")
    open_circuit(breaker)
    clock.now += breaker.cooldown
    request(breaker, None)
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (
            logging.WARNING,
            "Circuit breaker of a.test opened: 4 of 4 requests failed in 60s; requests to it fail for 30s",
        ),
        (logging.INFO, "Circuit breaker of a.test is half-open: probing it with http://a.test/page"),
        (logging.INFO, "Circuit breaker of a.test closed: probe http://a.test/page succeeded"),
    ]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"failure_threshold": 0}, "failure_threshold"),
        ({"failure_threshold": 1.5}, "failure_threshold"),
        ({"min_requests": 0}, "min_requests"),
        ({"window": 0}, "window"),
        ({"window": float("inf")}, "window"),
        ({"cooldown": -1}, "cooldown"),
        ({"cooldown": float("nan")}, "cooldown"),
    ],
)
def test_rejects_invalid_arguments(options, message):
    with pytest.raises(ValueError, match=message):
        CircuitBreaker(**options)
