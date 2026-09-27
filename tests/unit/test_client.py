"""Unit tests for AsyncCrawler with the HTTP session replaced by fakes.

Only cases that a real server cannot reproduce live here; the happy path and
the error mapping are covered by the integration tests.
"""

import asyncio
from unittest.mock import MagicMock

import aiohttp
import pytest

from crawler import (
    AsyncCrawler,
    CrawlerClosedError,
    FetchResult,
    FetchTimeoutError,
    NetworkError,
)


class FakeResponse:
    def __init__(self, body: bytes = b"page", status: int = 200) -> None:
        self.status = status
        self._body = body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=self.status,
                message="Error",
            )

    async def read(self) -> bytes:
        return self._body

    def get_encoding(self) -> str:
        return "utf-8"


class FakeRequest:
    """Mimics the async context manager returned by ClientSession.get()."""

    def __init__(self, session: "FakeSession", url: str) -> None:
        self._session = session
        self._url = url

    async def __aenter__(self) -> FakeResponse:
        return await self._session.handle(self._url)

    async def __aexit__(self, *exc_info) -> None:
        return None


class FakeSession:
    """Serves canned responses or raises canned exceptions per URL."""

    def __init__(self) -> None:
        self.routes: dict[str, FakeResponse | BaseException] = {}
        self.latency = 0.0
        self.closed = False
        self.in_flight = 0
        self.peak_in_flight = 0
        self.requested: list[str] = []

    def get(self, url: str) -> FakeRequest:
        return FakeRequest(self, url)

    async def handle(self, url: str) -> FakeResponse:
        self.requested.append(url)
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.latency)
            outcome = self.routes.get(url, FakeResponse())
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        finally:
            self.in_flight -= 1

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def crawler(monkeypatch, fake_session) -> AsyncCrawler:
    crawler = AsyncCrawler(max_concurrent=3)
    monkeypatch.setattr(crawler, "_create_session", lambda: fake_session)
    return crawler


class TestInit:
    @pytest.mark.parametrize("value", [0, -1])
    def test_rejects_non_positive_concurrency(self, value):
        with pytest.raises(ValueError, match="max_concurrent"):
            AsyncCrawler(max_concurrent=value)

    @pytest.mark.parametrize(
        "name", ["total_timeout", "connect_timeout", "read_timeout"]
    )
    def test_rejects_non_positive_timeouts(self, name):
        with pytest.raises(ValueError, match=name):
            AsyncCrawler(**{name: 0})

    def test_does_not_create_session_eagerly(self):
        crawler = AsyncCrawler()
        assert crawler._session is None


class TestLifecycle:
    async def test_session_is_created_once_and_reused(self, crawler, fake_session):
        await crawler.fetch_url("http://a")
        await crawler.fetch_url("http://b")
        assert crawler._session is fake_session
        assert fake_session.requested == ["http://a", "http://b"]

    async def test_close_is_idempotent(self, crawler, fake_session):
        await crawler.fetch_url("http://a")
        await crawler.close()
        await crawler.close()
        assert fake_session.closed
        assert crawler.closed

    async def test_close_without_requests(self):
        crawler = AsyncCrawler()
        await crawler.close()
        assert crawler.closed

    async def test_fetch_after_close_raises(self, crawler):
        await crawler.close()
        with pytest.raises(RuntimeError, match="closed"):
            await crawler.fetch_url("http://a")
        with pytest.raises(RuntimeError, match="closed"):
            await crawler.fetch_many(["http://a", "http://b"])

    async def test_close_during_batch_fails_only_queued_urls(
        self, crawler, fake_session
    ):
        fake_session.latency = 0.1
        urls = [f"http://site/{i}" for i in range(5)]
        batch = asyncio.create_task(crawler.fetch_many(urls))
        while fake_session.in_flight < crawler.max_concurrent:
            await asyncio.sleep(0)
        await crawler.close()

        results = await batch  # must not raise

        assert [r.ok for r in results] == [True, True, True, False, False]
        assert all(isinstance(r.error, CrawlerClosedError) for r in results[3:])
        assert fake_session.requested == urls[:3]

    async def test_context_manager_closes_session(self, crawler, fake_session):
        async with crawler as active:
            await active.fetch_url("http://a")
        assert fake_session.closed


class TestErrorMapping:
    async def test_server_timeout_is_a_timeout(self, crawler, fake_session):
        # aiohttp.ServerTimeoutError is also a ClientError: it must still be
        # reported as a timeout, not as a generic network error.
        fake_session.routes["http://a"] = aiohttp.ServerTimeoutError("read timeout")
        with pytest.raises(FetchTimeoutError):
            await crawler.fetch_url("http://a")

    async def test_network_error_keeps_cause(self, crawler, fake_session):
        fake_session.routes["http://a"] = aiohttp.ClientConnectionError("refused")
        with pytest.raises(NetworkError, match="refused") as exc_info:
            await crawler.fetch_url("http://a")
        assert isinstance(exc_info.value.__cause__, aiohttp.ClientConnectionError)

    async def test_unexpected_exception_propagates(self, crawler, fake_session):
        fake_session.routes["http://a"] = KeyError("bug")
        with pytest.raises(KeyError):
            await crawler.fetch_url("http://a")


class TestFetchMany:
    async def test_results_keep_input_order(self, crawler, fake_session):
        fake_session.routes["http://b"] = FakeResponse(status=500)
        results = await crawler.fetch_many(["http://a", "http://b", "http://c"])
        assert [r.url for r in results] == ["http://a", "http://b", "http://c"]
        assert [r.ok for r in results] == [True, False, True]
        assert results[1].status == 500

    async def test_failure_result_fields(self, crawler, fake_session):
        fake_session.routes["http://a"] = aiohttp.ClientConnectionError()
        [result] = await crawler.fetch_many(["http://a"])
        assert isinstance(result, FetchResult)
        assert result.content is None
        assert result.status is None
        assert isinstance(result.error, NetworkError)

    async def test_success_result_fields(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse("héllo".encode())
        [result] = await crawler.fetch_many(["http://a"])
        assert result.content == "héllo"
        assert result.size == 6  # bytes, not characters
        assert result.status == 200
        assert result.elapsed >= 0

    async def test_concurrency_is_limited(self, crawler, fake_session):
        fake_session.latency = 0.02
        await crawler.fetch_many([f"http://site/{i}" for i in range(10)])
        assert fake_session.peak_in_flight == crawler.max_concurrent


class TestFetchUrls:
    async def test_duplicates_are_fetched_once(self, crawler, fake_session):
        await crawler.fetch_urls(["http://a", "http://a", "http://b"])
        assert fake_session.requested == ["http://a", "http://b"]

    async def test_empty_input(self, crawler):
        assert await crawler.fetch_urls([]) == {}
