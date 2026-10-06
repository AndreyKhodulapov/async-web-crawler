"""Tests for the request layer: what it tells of a host it holds back, and the requests it does not wait for."""

import logging
import time

from test_transport_contract import TIMEOUT, ScriptedTransport

from crawler.circuit_breaker import CircuitBreaker
from crawler.exceptions import HostHeldBackError, HTTPStatusError
from crawler.fetching import Fetcher
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryStrategy
from crawler.semaphores import SemaphoreManager
from crawler.transport import Response

URL = "https://a.test/"


def make_fetcher(
    transport: ScriptedTransport, *, max_retry_after: float = 600, breaker: CircuitBreaker | None = None, **retry
) -> Fetcher:
    return Fetcher(
        transport,
        limits=SemaphoreManager(10, None),
        rate_limiter=RateLimiter(None, True),
        retry_strategy=RetryStrategy(**{"max_retries": 1, "base_delay": 0.001, **retry}),
        circuit_breaker=breaker or CircuitBreaker(),
        respect_robots=False,
        timeout=TIMEOUT,
        timeout_growth=1.5,
        max_retry_after=max_retry_after,
        user_agent="TestBot/1.0",
    )


class HoldRecorder:
    """An `on_host_held` callback that records what it is told."""

    def __init__(self) -> None:
        self.holds: list[tuple[str, float, str | None]] = []

    async def __call__(self, host: str, seconds: float, reason: str | None) -> None:
        self.holds.append((host, seconds, reason))


class TestHostHeld:
    async def test_retry_after_too_long_to_retry_is_told_as_asked(self) -> None:
        transport = ScriptedTransport({URL: HTTPStatusError(URL, 429, "Too Many Requests", retry_after=5)})
        fetcher = make_fetcher(transport, max_delay=1)
        fetcher.on_host_held = recorder = HoldRecorder()

        result = await fetcher.fetch(URL)

        assert transport.requests == [URL]
        assert isinstance(result.error, HTTPStatusError)
        assert recorder.holds == [("a.test", 5.0, "HTTP 429 Too Many Requests, Retry-After 5s")]

    async def test_retry_after_is_told_as_long_as_the_host_is_held_back(self) -> None:
        transport = ScriptedTransport({URL: HTTPStatusError(URL, 503, "Service Unavailable", retry_after=900)})
        fetcher = make_fetcher(transport, max_delay=1, max_retry_after=3)
        fetcher.on_host_held = recorder = HoldRecorder()

        await fetcher.fetch(URL)

        assert recorder.holds == [("a.test", 3.0, "HTTP 503 Service Unavailable, Retry-After 900s")]

    async def test_pause_before_the_retry_of_an_overloaded_host_is_told(self) -> None:
        transport = ScriptedTransport({URL: HTTPStatusError(URL, 429, "Too Many Requests")})
        fetcher = make_fetcher(transport)
        fetcher.on_host_held = recorder = HoldRecorder()

        await fetcher.fetch(URL)

        assert transport.requests == [URL, URL]
        ((host, seconds, reason),) = recorder.holds
        assert (host, reason) == ("a.test", "HTTP 429 Too Many Requests, pause before a retry")
        assert 0 < seconds < 0.1  # base_delay, grown for HTTP 429

    async def test_pause_before_the_retry_of_one_page_holds_no_host(self) -> None:
        transport = ScriptedTransport({URL: HTTPStatusError(URL, 500, "Internal Server Error")})
        fetcher = make_fetcher(transport)
        fetcher.on_host_held = recorder = HoldRecorder()

        await fetcher.fetch(URL)

        assert transport.requests == [URL, URL]
        assert recorder.holds == []

    async def test_failure_to_tell_is_logged_and_the_host_is_held_back_all_the_same(self, caplog) -> None:
        transport = ScriptedTransport({URL: HTTPStatusError(URL, 429, "Too Many Requests", retry_after=5)})
        fetcher = make_fetcher(transport, max_delay=1)

        async def broken(host: str, seconds: float, reason: str | None) -> None:
            raise ConnectionResetError("the database went away")

        fetcher.on_host_held = broken
        with caplog.at_level(logging.WARNING, logger="crawler.fetching"):
            result = await fetcher.fetch(URL)

        assert isinstance(result.error, HTTPStatusError)
        assert fetcher.rate_limiter.penalty_left("a.test") > 4
        assert "Could not tell the other workers that a.test is held back" in caplog.text
        assert "the database went away" in caplog.text

    async def test_tell_host_held_without_a_callback_does_nothing(self) -> None:
        fetcher = make_fetcher(ScriptedTransport({}))
        await fetcher.tell_host_held("a.test", 5.0, None)


def page(url: str) -> Response:
    return Response(status=200, content="page", size=4, final_url=url, content_type="text/html")


class TestMaxWait:
    """A request whose host is held back longer than `max_wait` is not sent, nor waited for."""

    async def test_request_to_a_host_held_back_too_long_is_not_sent(self, caplog) -> None:
        transport = ScriptedTransport({URL: page(URL)})
        fetcher = make_fetcher(transport)
        fetcher.rate_limiter.penalize("a.test", 5.0)

        with caplog.at_level(logging.INFO, logger="crawler"):
            result = await fetcher.fetch(URL, max_wait=1.0)

        assert transport.requests == []
        assert isinstance(result.error, HostHeldBackError)
        assert result.error.url == URL
        assert 4 < result.error.seconds <= 5
        # Neither a failure of the request nor one of the site.
        stats = fetcher.errors.get_stats()
        assert (sum(stats.by_kind.values()), stats.by_class, stats.retries) == (0, {}, 0)
        assert "Failed" not in caplog.text
        assert not fetcher.circuit_breaker.is_failure(result.error)

    async def test_probe_of_a_half_open_circuit_is_let_go(self) -> None:
        bad = "https://a.test/bad"
        transport = ScriptedTransport({bad: HTTPStatusError(bad, 503, "Service Unavailable"), URL: page(URL)})
        breaker = CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0)
        fetcher = make_fetcher(transport, breaker=breaker, max_retries=0)
        await fetcher.fetch(bad)  # opens the circuit; it is half-open at once
        fetcher.rate_limiter.penalize("a.test", 5.0)

        result = await fetcher.fetch(URL, max_wait=1.0)

        assert isinstance(result.error, HostHeldBackError)
        assert breaker.refusal(URL) is None  # the probe is free for the next request

    async def test_redirect_to_a_host_held_back_too_long_is_not_followed(self) -> None:
        target = "https://b.test/new"
        moved = Response(status=301, content="", size=0, final_url=target, content_type=None, redirected=True)
        transport = ScriptedTransport({URL: moved, target: page(target)})
        fetcher = make_fetcher(transport)
        fetcher.rate_limiter.penalize("b.test", 5.0)

        result = await fetcher.fetch(URL, max_wait=1.0)

        assert transport.requests == [URL]
        assert isinstance(result.error, HostHeldBackError)
        assert result.error.url == target
        assert result.url == URL

    async def test_pause_before_a_retry_is_waited_for_however_long(self) -> None:
        # The pause is the request's own: a page put back would start its
        # retries anew, and the host would get more requests than allowed.
        transport = ScriptedTransport({URL: HTTPStatusError(URL, 429, "Too Many Requests")})
        fetcher = make_fetcher(transport, base_delay=0.2)
        began = time.monotonic()

        result = await fetcher.fetch(URL, max_wait=0.01)

        assert transport.requests == [URL, URL]
        assert isinstance(result.error, HTTPStatusError)
        assert time.monotonic() - began >= 0.2

    async def test_without_max_wait_the_penalty_is_waited_for(self) -> None:
        transport = ScriptedTransport({URL: page(URL)})
        fetcher = make_fetcher(transport)
        fetcher.rate_limiter.penalize("a.test", 0.2)
        began = time.monotonic()

        result = await fetcher.fetch(URL)

        assert result.error is None
        assert time.monotonic() - began >= 0.19
