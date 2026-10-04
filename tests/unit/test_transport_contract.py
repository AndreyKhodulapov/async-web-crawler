"""Tests for the contract between the request layer and its transport."""

from collections.abc import Iterable
from http.cookiejar import Cookie

import aiohttp

from crawler.circuit_breaker import CircuitBreaker
from crawler.exceptions import NetworkError
from crawler.fetching import Fetcher
from crawler.rate_limiter import RateLimiter
from crawler.retry import RetryStrategy
from crawler.semaphores import SemaphoreManager
from crawler.transport import HttpTransport, Response, Transport

TIMEOUT = aiohttp.ClientTimeout(total=5, connect=2, sock_read=3)


class ScriptedTransport:
    """A transport that is not HttpTransport: answers each URL from a script, failing as it says."""

    def __init__(self, script: dict[str, Response | Exception]) -> None:
        self.script = script
        self.requests: list[str] = []
        self.closed = False
        self.resets = 0
        self.updates: list[tuple[list[Cookie], list[Cookie]]] = []

    async def get(
        self,
        url: str,
        *,
        html_only: bool,
        raw_limit: int | None,
        truncate_at: int | None,
        timeout: aiohttp.ClientTimeout,
    ) -> Response:
        self.requests.append(url)
        answer = self.script[url]
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def close(self) -> None:
        self.closed = True

    def reset_stats(self) -> None:
        self.resets += 1

    def cookies(self) -> list[Cookie]:
        return []

    def update_cookies(self, changed: Iterable[Cookie], removed: Iterable[Cookie]) -> None:
        self.updates.append((list(changed), list(removed)))


def make_fetcher(transport: Transport) -> Fetcher:
    return Fetcher(
        transport,
        limits=SemaphoreManager(10, None),
        rate_limiter=RateLimiter(None, True),
        retry_strategy=RetryStrategy(max_retries=1, base_delay=0.001),
        circuit_breaker=CircuitBreaker(),
        respect_robots=False,
        timeout=TIMEOUT,
        timeout_growth=1.5,
        max_retry_after=600,
        user_agent="TestBot/1.0",
    )


def page(url: str, text: str) -> Response:
    return Response(status=200, content=text, size=len(text), final_url=url, content_type="text/html")


class TestTransportContract:
    def test_the_http_transport_is_a_transport(self) -> None:
        transport = HttpTransport(max_concurrent=1, timeout=TIMEOUT, user_agent="TestBot/1.0", max_page_size=None)
        assert isinstance(transport, Transport)
        assert isinstance(ScriptedTransport({}), Transport)  # the stand-in keeps up with the contract

    async def test_the_fetcher_follows_a_redirect_any_transport_reports(self) -> None:
        transport = ScriptedTransport(
            {
                "https://a.test/old": Response(
                    status=301, content="", size=0, final_url="/new", content_type=None, redirected=True
                ),
                "https://a.test/new": page("https://a.test/new", "<p>moved</p>"),
            }
        )
        result = await make_fetcher(transport).fetch("https://a.test/old")
        assert transport.requests == ["https://a.test/old", "https://a.test/new"]
        assert result.error is None
        assert result.redirected
        assert result.final_url == "https://a.test/new"
        assert result.content == "<p>moved</p>"

    async def test_the_fetcher_retries_a_failure_any_transport_raises(self) -> None:
        transport = ScriptedTransport({"https://a.test/": NetworkError("https://a.test/", "reset")})
        result = await make_fetcher(transport).fetch("https://a.test/")
        assert transport.requests == ["https://a.test/", "https://a.test/"]
        assert isinstance(result.error, NetworkError)

    async def test_the_fetcher_resets_and_closes_its_transport(self) -> None:
        transport = ScriptedTransport({})
        fetcher = make_fetcher(transport)
        fetcher.reset_stats()
        await fetcher.close()
        assert transport.resets == 1
        assert transport.closed
