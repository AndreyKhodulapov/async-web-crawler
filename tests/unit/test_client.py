"""Unit tests for AsyncCrawler with the HTTP session replaced by fakes."""

import asyncio
import logging
import socket
import ssl
import time
from collections.abc import AsyncIterator
from unittest.mock import MagicMock

import aiohttp
import pytest
from helpers import UNTHROTTLED, FakeClock
from multidict import CIMultiDict

from crawler import (
    AsyncCrawler,
    CertificateError,
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    CrawlerClosedError,
    FetchResult,
    FetchTimeoutError,
    HTMLParser,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    ParseError,
    PermanentError,
    RetryStrategy,
    RobotsDisallowedError,
    RobotsUnreachableError,
    TransientError,
    UnexpectedError,
)


class FakeResponse:
    def __init__(
        self,
        body: bytes = b"page",
        status: int = 200,
        encoding: str = "utf-8",
        content_type: str | None = "text/html",
        url: str | None = None,
        retry_after: str | None = None,
        location: str | None = None,
        robots_tag: tuple[str, ...] = (),
    ) -> None:
        self.status = status
        self._body = body
        self.headers: CIMultiDict[str] = CIMultiDict()
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        for value in robots_tag:
            self.headers.add("X-Robots-Tag", value)
        if retry_after is not None:
            self.headers["Retry-After"] = retry_after
        if location is not None:
            self.headers["Location"] = location
        self.content_type = content_type or "application/octet-stream"
        # None means the requested URL, which FakeSession fills in.
        self.url = url
        self.charset = encoding
        self.content_length: int | None = None
        self.content = self  # a body read in chunks
        self.read_count = 0

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=self.status,
                message="Error",
                headers=self.headers,
            )

    async def read(self) -> bytes:
        self.read_count += 1
        return self._body

    async def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        self.read_count += 1
        for start in range(0, len(self._body), size):
            yield self._body[start : start + size]


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
    """Serves canned responses or raises canned exceptions per URL.

    A list of outcomes is served one per request; the last one repeats.
    """

    def __init__(self) -> None:
        self.routes: dict[str, FakeResponse | BaseException | list[FakeResponse | BaseException]] = {}
        self.latency = 0.0
        self.closed = False
        self.in_flight = 0
        self.peak_in_flight = 0
        self.requested: list[str] = []
        self.user_agents: list[str | None] = []  # per-request User-Agent headers
        self.timeouts: list[aiohttp.ClientTimeout | None] = []  # per-request timeouts

    def get(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        allow_redirects: bool = True,
    ) -> FakeRequest:
        assert not allow_redirects  # the crawler follows redirects itself
        self.user_agents.append(None if headers is None else headers.get("User-Agent"))
        self.timeouts.append(timeout)
        return FakeRequest(self, url)

    async def handle(self, url: str) -> FakeResponse:
        self.requested.append(url)
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.latency)
            outcome = self.routes.get(url, FakeResponse())
            if isinstance(outcome, list):
                outcome = outcome.pop(0) if len(outcome) > 1 else outcome[0]
            if isinstance(outcome, BaseException):
                raise outcome
            if outcome.url is None:
                outcome.url = url
            return outcome
        finally:
            self.in_flight -= 1

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def make_crawler(monkeypatch, fake_session):
    def make(**options) -> AsyncCrawler:
        crawler = AsyncCrawler(**{"max_concurrent": 3, **UNTHROTTLED, **options})
        monkeypatch.setattr(crawler, "_create_session", lambda: fake_session)
        return crawler

    return make


@pytest.fixture
def crawler(make_crawler) -> AsyncCrawler:
    return make_crawler()


class TestInit:
    @pytest.mark.parametrize("value", [0, -1])
    def test_rejects_non_positive_concurrency(self, value):
        with pytest.raises(ValueError, match="max_concurrent"):
            AsyncCrawler(max_concurrent=value)

    @pytest.mark.parametrize(("name", "value"), [("max_depth", -1), ("max_per_domain", 0)])
    def test_rejects_invalid_crawl_limits(self, name, value):
        with pytest.raises(ValueError, match=name):
            AsyncCrawler(**{name: value})

    @pytest.mark.parametrize("name", ["total_timeout", "connect_timeout", "read_timeout"])
    def test_rejects_non_positive_timeouts(self, name):
        with pytest.raises(ValueError, match=name):
            AsyncCrawler(**{name: 0})

    @pytest.mark.parametrize("value", [0.5, 0, float("inf"), float("nan")])
    def test_rejects_invalid_timeout_growth(self, value):
        with pytest.raises(ValueError, match="timeout_growth"):
            AsyncCrawler(timeout_growth=value)

    def test_does_not_create_session_eagerly(self):
        crawler = AsyncCrawler()
        assert crawler._session is None

    def test_rate_options_configure_the_limiter(self):
        limiter = AsyncCrawler(requests_per_second=4, per_domain_rate=False, min_delay=0.5, jitter=0.1).rate_limiter
        assert (limiter.interval, limiter.per_domain, limiter.jitter) == (0.5, False, 0.1)


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

    async def test_fetch_after_close(self, crawler, fake_session):
        await crawler.close()
        with pytest.raises(CrawlerClosedError):
            await crawler.fetch_url("http://a")
        results = await crawler.fetch_many(["http://a", "http://b"])
        assert all(isinstance(r.error, CrawlerClosedError) for r in results)
        assert fake_session.requested == []

    async def test_close_scheduled_right_after_batch(self, crawler, fake_session):
        # close() runs after fetch_many() created its tasks but before any of
        # them started: the batch must still return results, not raise.
        batch = asyncio.create_task(crawler.fetch_many(["http://a", "http://b"]))
        closer = asyncio.create_task(crawler.close())
        results = await batch
        await closer
        assert all(isinstance(r.error, CrawlerClosedError) for r in results)
        assert fake_session.requested == []

    async def test_close_during_batch_fails_only_queued_urls(self, crawler, fake_session):
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
    @pytest.mark.parametrize(
        ("outcome", "kind"),
        [
            (FakeResponse(status=404), PermanentError),
            (FakeResponse(status=403), PermanentError),
            (FakeResponse(status=401), PermanentError),
            (FakeResponse(status=429), TransientError),
            (FakeResponse(status=503), TransientError),
            (FakeResponse(status=500), TransientError),
            (TimeoutError(), TransientError),
            (aiohttp.ClientConnectorError(MagicMock(), ConnectionRefusedError("connection refused")), NetworkError),
            (aiohttp.ClientConnectorError(MagicMock(), socket.gaierror("Name or service not known")), NetworkError),
            (aiohttp.TooManyRedirects(MagicMock(), ()), PermanentError),
        ],
    )
    async def test_failures_are_classified(self, crawler, fake_session, outcome, kind):
        fake_session.routes["http://a"] = outcome
        with pytest.raises(kind):
            await crawler.fetch_url("http://a")

    async def test_server_timeout_is_a_timeout(self, crawler, fake_session):
        # aiohttp.ServerTimeoutError is also a ClientError: it must still be
        # reported as a timeout, not as a generic network error.
        fake_session.routes["http://a"] = aiohttp.ServerTimeoutError("read timeout")
        with pytest.raises(FetchTimeoutError):
            await crawler.fetch_url("http://a")

    @pytest.mark.parametrize(
        ("outcome", "message"),
        [
            (aiohttp.ConnectionTimeoutError(), "connect timeout (2.0s)"),
            (aiohttp.SocketTimeoutError(), "read timeout (3.0s)"),
            (TimeoutError(), "total timeout (4.0s)"),
        ],
    )
    async def test_timeout_message_names_the_timeout(self, make_crawler, fake_session, outcome, message):
        crawler = make_crawler(connect_timeout=2, read_timeout=3, total_timeout=4)
        fake_session.routes["http://a"] = outcome
        with pytest.raises(FetchTimeoutError) as exc_info:
            await crawler.fetch_url("http://a")
        assert exc_info.value.message == message

    async def test_network_error_keeps_cause(self, crawler, fake_session):
        fake_session.routes["http://a"] = aiohttp.ClientConnectionError("refused")
        with pytest.raises(NetworkError, match="refused") as exc_info:
            await crawler.fetch_url("http://a")
        assert isinstance(exc_info.value.__cause__, aiohttp.ClientConnectionError)

    async def test_certificate_error_is_not_retried(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=2, base_delay=0.001))
        fake_session.routes["http://a"] = aiohttp.ClientConnectorCertificateError(
            MagicMock(), ssl.SSLCertVerificationError("certificate has expired")
        )
        with pytest.raises(CertificateError, match="certificate has expired"):
            await crawler.fetch_url("http://a")
        assert fake_session.requested == ["http://a"]

    async def test_non_ascii_retry_after_is_ignored(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=1, base_delay=0.001))
        fake_session.routes["http://a"] = [FakeResponse(status=429, retry_after="²"), FakeResponse(b"ok")]
        assert await crawler.fetch_url("http://a") == "ok"

    async def test_unexpected_exception_is_wrapped_and_logged(self, crawler, fake_session, caplog):
        fake_session.routes["http://a"] = KeyError("bug")
        with pytest.raises(UnexpectedError, match="KeyError") as exc_info:
            await crawler.fetch_url("http://a")
        assert isinstance(exc_info.value.__cause__, KeyError)
        # The traceback must reach the log, otherwise the bug goes unnoticed.
        [record] = [r for r in caplog.records if r.levelname == "ERROR"]
        assert record.exc_info is not None

    async def test_cancellation_is_not_swallowed(self, crawler, fake_session):
        fake_session.routes["http://a"] = asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError):
            await crawler.fetch_url("http://a")

    @pytest.mark.parametrize("url", ["//no-scheme", "not a url", "ftp://example.com", "http://[::1"])
    async def test_malformed_url_is_rejected_before_request(self, crawler, fake_session, url):
        with pytest.raises(InvalidURLError):
            await crawler.fetch_url(url)
        assert fake_session.requested == []


class TestFetchMany:
    async def test_results_keep_input_order(self, crawler, fake_session):
        fake_session.routes["http://b"] = FakeResponse(status=500)
        results = await crawler.fetch_many(["http://a", "http://b", "http://c"])
        assert [r.url for r in results] == ["http://a", "http://b", "http://c"]
        assert [r.ok for r in results] == [True, False, True]

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
        assert result.final_url == "http://a"
        assert result.content_type == "text/html"
        assert result.redirected is False
        assert result.robots_tag == ()

    async def test_robots_tag_for_this_crawler(self, crawler, fake_session):
        headers = ("noindex", "otherbot: nofollow", "AsyncWebCrawler: noarchive")
        fake_session.routes["http://a"] = FakeResponse(robots_tag=headers)
        [result] = await crawler.fetch_many(["http://a"])
        assert result.robots_tag == ("noindex", "noarchive")

    async def test_redirect_and_missing_content_type(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(status=302, location="https://a/home")
        fake_session.routes["https://a/home"] = FakeResponse(content_type=None)
        [result] = await crawler.fetch_many(["http://a"])
        assert result.url == "http://a"
        assert result.final_url == "https://a/home"
        assert result.content_type is None
        assert result.redirected is True
        assert fake_session.requested == ["http://a", "https://a/home"]

    async def test_unexpected_error_does_not_cancel_batch(self, crawler, fake_session):
        fake_session.latency = 0.01
        fake_session.routes["http://b"] = KeyError("bug")
        results = await crawler.fetch_many(["http://a", "http://b", "http://c"])
        assert [r.ok for r in results] == [True, False, True]
        assert isinstance(results[1].error, UnexpectedError)

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


class TestFetchAndParse:
    async def test_returns_parsed_page(self, crawler, fake_session):
        html = b"<title>Home</title><body><p>Hello</p><a href='/about'>About</a></body>"
        fake_session.routes["http://site/"] = FakeResponse(html)
        page = await crawler.fetch_and_parse("http://site/")
        assert page["url"] == "http://site/"
        assert page["title"] == "Home"
        assert page["text"] == "Hello About"
        assert page["links"] == ["http://site/about"]
        assert page["errors"] == []

    async def test_missing_content_type_is_parsed_as_html(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(b"<h1>Hi</h1>", content_type=None)
        page = await crawler.fetch_and_parse("http://a")
        assert page["headings"] == [{"level": 1, "text": "Hi"}]

    async def test_non_html_body_is_not_downloaded(self, crawler, fake_session):
        archive = FakeResponse(b"PK\x03\x04", content_type="application/zip")
        fake_session.routes["http://a/file.zip"] = archive
        with pytest.raises(ParseError, match="unsupported content type: application/zip"):
            await crawler.fetch_and_parse("http://a/file.zip")
        assert archive.read_count == 0

    async def test_plain_fetch_still_reads_non_html_body(self, crawler, fake_session):
        fake_session.routes["http://a/data"] = FakeResponse(b"{}", content_type="application/json")
        assert await crawler.fetch_url("http://a/data") == "{}"

    async def test_uses_injected_parser(self, make_crawler, fake_session):
        crawler = make_crawler(parser=HTMLParser(same_host_only=True))
        fake_session.routes["http://a/"] = FakeResponse(b"<a href='/x'>in</a><a href='http://b/'>out</a>")
        page = await crawler.fetch_and_parse("http://a/")
        assert page["links"] == ["http://a/x"]


class TestRetries:
    async def test_transient_failure_is_retried(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=2, base_delay=0.001))
        fake_session.routes["http://a"] = [FakeResponse(status=503), aiohttp.ServerTimeoutError(), FakeResponse(b"ok")]
        assert await crawler.fetch_url("http://a") == "ok"
        assert fake_session.requested == ["http://a"] * 3

    async def test_gives_up_after_max_retries(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=2, base_delay=0.001))
        fake_session.routes["http://a"] = FakeResponse(status=503)
        with pytest.raises(HTTPStatusError):
            await crawler.fetch_url("http://a")
        assert len(fake_session.requested) == 3

    async def test_retry_after_header_is_kept_in_the_error(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(status=429, retry_after="120")
        with pytest.raises(HTTPStatusError) as exc_info:
            await crawler.fetch_url("http://a")
        assert exc_info.value.retry_after == 120

    async def test_retry_after_holds_back_the_host_without_a_retry(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=0, max_delay=5.0))
        fake_session.routes["http://a"] = FakeResponse(status=429, retry_after="2")
        with pytest.raises(HTTPStatusError):
            await crawler.fetch_url("http://a")

        assert crawler.rate_limiter.reserve("a") == pytest.approx(2.0, abs=0.1)
        assert crawler.rate_limiter.reserve("b") == 0

    async def test_retry_after_beyond_max_delay_is_not_retried(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=2, max_delay=5.0))
        fake_session.routes["http://a"] = [FakeResponse(status=429, retry_after="120"), FakeResponse(b"ok")]
        with pytest.raises(HTTPStatusError):
            await crawler.fetch_url("http://a")

        assert fake_session.requested == ["http://a"]
        # The host waits the two minutes asked for, though a retry could not.
        assert crawler.rate_limiter.reserve("a") == pytest.approx(120.0, abs=0.1)

    async def test_retry_after_is_capped(self, make_crawler, fake_session, caplog):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=0))
        fake_session.routes["http://a"] = FakeResponse(status=429, retry_after="86400")
        with pytest.raises(HTTPStatusError):
            await crawler.fetch_url("http://a")

        assert crawler.rate_limiter.reserve("a") == pytest.approx(AsyncCrawler.MAX_RETRY_AFTER, abs=0.1)
        assert "a asked to wait 86400s (Retry-After), waiting 600s" in caplog.text

    async def test_timeouts_grow_with_every_retry(self, make_crawler, fake_session):
        crawler = make_crawler(
            retry_strategy=RetryStrategy(max_retries=3, base_delay=0.001),
            connect_timeout=1,
            read_timeout=2,
            total_timeout=4,
            timeout_growth=2,
        )
        fake_session.routes["http://a"] = [aiohttp.SocketTimeoutError()] * 3 + [FakeResponse(b"ok")]
        assert await crawler.fetch_url("http://a") == "ok"

        timeouts = [(t.connect, t.sock_read, t.total) for t in fake_session.timeouts]
        # The growth stops at MAX_TIMEOUT_GROWTH (4x) instead of reaching 8x.
        assert timeouts == [(1, 2, 4), (2, 4, 8), (4, 8, 16), (4, 8, 16)]

    async def test_timeouts_start_over_for_every_url(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=1, base_delay=0.001), read_timeout=2)
        fake_session.routes["http://a"] = [aiohttp.SocketTimeoutError(), FakeResponse()]
        await crawler.fetch_url("http://a")
        await crawler.fetch_url("http://b")
        assert [t.sock_read for t in fake_session.timeouts] == [2, 3, 2]

    async def test_huge_timeout_growth_is_capped(self, make_crawler):
        crawler = make_crawler(timeout_growth=1e300, read_timeout=1)
        assert crawler._timeout_for(retries=5).sock_read == AsyncCrawler.MAX_TIMEOUT_GROWTH

    async def test_backoff_holds_back_the_whole_host(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=1, base_delay=0.2))
        fake_session.routes["http://a/slow"] = [FakeResponse(status=503), FakeResponse()]
        retrying = asyncio.create_task(crawler.fetch_url("http://a/slow"))
        await asyncio.sleep(0.01)  # the first attempt has failed, the retry waits

        assert crawler.rate_limiter.reserve("a") > 0  # other pages of the host wait too
        assert crawler.rate_limiter.reserve("b") == 0
        await retrying


class TestRetryLogging:
    @staticmethod
    def warnings(caplog) -> list[str]:
        return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]

    async def test_every_attempt_is_logged_once(self, make_crawler, fake_session, caplog):
        caplog.set_level(logging.INFO)
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=2, base_delay=0.001))
        fake_session.routes["http://a/"] = [FakeResponse(status=503), FakeResponse(b"ok")]
        await crawler.fetch_url("http://a/")

        [retry] = self.warnings(caplog)
        assert retry.startswith("Attempt 1/3 for http://a/ failed: TransientHTTPError: HTTP 503 Error; retrying in ")
        assert any(r.getMessage().startswith("Succeeded http://a/ on attempt 2/3") for r in caplog.records)

    async def test_final_failure_is_a_warning(self, crawler, fake_session, caplog):
        fake_session.routes["http://a/"] = FakeResponse(status=404)
        with pytest.raises(HTTPStatusError):
            await crawler.fetch_url("http://a/")
        [failure] = self.warnings(caplog)
        assert failure.startswith("Failed http://a/ on attempt 1/1 after ")
        assert failure.endswith("permanent error: PermanentHTTPError: HTTP 404 Error")

    async def test_missing_robots_txt_is_not_a_warning(self, make_crawler, fake_session, caplog):
        crawler = make_crawler(respect_robots=True)
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=404)
        assert await crawler.fetch_url("http://a/page") == "page"
        assert self.warnings(caplog) == []


class TestErrorStats:
    async def test_attempts_and_outcomes_are_counted(self, make_crawler, fake_session):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=2, base_delay=0.001))
        fake_session.latency = 0.01
        fake_session.routes["http://a/"] = [FakeResponse(status=503), aiohttp.SocketTimeoutError(), FakeResponse()]
        fake_session.routes["http://b/"] = FakeResponse(status=404)
        fake_session.routes["http://c/"] = aiohttp.ClientConnectionError("refused")
        fake_session.routes["http://d/"] = FakeResponse(b"%PDF", content_type="application/pdf")
        await crawler.fetch_many(["http://a/", "http://b/", "http://c/"])
        with pytest.raises(ParseError):
            await crawler.fetch_and_parse("http://d/")

        stats = crawler.error_stats()
        assert stats.by_kind == {
            "TransientError": 2,
            "PermanentError": 1,
            "NetworkError": 3,
            "ParseError": 1,
            "other": 0,
        }
        assert stats.by_class["FetchTimeoutError"] == 1
        assert (stats.retries, stats.successful_retries) == (4, 1)
        # A retry takes at least the request itself.
        assert stats.avg_retry_time >= 0.01
        assert stats.permanent_errors == {"http://b/": "PermanentHTTPError: HTTP 404 Error"}

    async def test_robots_txt_is_not_counted(self, make_crawler, fake_session):
        crawler = make_crawler(respect_robots=True)
        fake_session.routes["http://a/robots.txt"] = FakeResponse(
            b"User-agent: *\nDisallow: /private/", content_type="text/plain"
        )
        fake_session.routes["http://b/robots.txt"] = FakeResponse(status=404)
        with pytest.raises(RobotsDisallowedError):
            await crawler.fetch_url("http://a/private/page")
        await crawler.fetch_url("http://b/page")

        stats = crawler.error_stats()
        assert stats.total == 0
        assert stats.permanent_errors == {}


class TestCircuitBreaker:
    async def test_open_circuit_stops_requests_and_retries(self, make_crawler, fake_session):
        crawler = make_crawler(
            retry_strategy=RetryStrategy(max_retries=3, base_delay=0.001),
            circuit_breaker=CircuitBreaker(min_requests=3),
        )
        fake_session.routes["http://a/1"] = FakeResponse(status=503)

        # The third failed attempt opens the circuit: no retry after it.
        first = await crawler.fetch_result("http://a/1")
        second = await crawler.fetch_result("http://a/2")
        other_host = await crawler.fetch_result("http://b/")

        # The failure reported is that of the last request sent.
        assert isinstance(first.error, HTTPStatusError)
        assert first.error.status == 503
        assert isinstance(second.error, CircuitOpenError)
        assert second.error.message.startswith("circuit breaker of a is open (3 of 3 requests failed")
        assert other_host.ok
        assert fake_session.requested == ["http://a/1"] * 3 + ["http://b/"]
        # Refused requests were not made: they are not errors of an attempt.
        stats = crawler.error_stats()
        assert (stats.total, stats.retries) == (3, 2)
        assert crawler.circuit_breaker.get_stats()["a"].rejected == 1

    async def test_retry_refused_after_its_wait_reports_the_last_error(self, make_crawler, fake_session):
        crawler = make_crawler(
            retry_strategy=RetryStrategy(max_retries=3, base_delay=0.05),
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=2),
        )
        fake_session.latency = 0.02
        fake_session.routes["http://a/1"] = fake_session.routes["http://a/2"] = FakeResponse(status=503)

        # The first failure leaves the circuit closed, so its retry goes on to
        # wait; the second one opens it, and the retry is refused after the wait.
        results = await crawler.fetch_many(["http://a/1", "http://a/2"])

        assert [result.status for result in results] == [503, 503]
        assert all(isinstance(result.error, HTTPStatusError) for result in results)
        assert sorted(fake_session.requested) == ["http://a/1", "http://a/2"]
        assert crawler.circuit_breaker.get_stats()["a"].rejected == 1

    async def test_refused_after_waiting_for_the_rate_limit(self, make_crawler, fake_session):
        crawler = make_crawler(
            requests_per_second=20,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1),
        )
        fake_session.routes["http://a/1"] = FakeResponse(status=503)

        # a/2 passes the first check, then waits for its turn while a/1 opens the circuit.
        results = await crawler.fetch_many(["http://a/1", "http://a/2"])

        assert isinstance(results[0].error, HTTPStatusError)
        assert isinstance(results[1].error, CircuitOpenError)
        assert fake_session.requested == ["http://a/1"]
        assert crawler.rate_limiter.get_stats().requests == 1

    async def test_refused_after_waiting_for_a_slot(self, make_crawler, fake_session):
        crawler = make_crawler(
            max_per_domain=1,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1),
        )
        fake_session.latency = 0.01
        fake_session.routes["http://a/1"] = FakeResponse(status=503)

        # a/2 and a/3 pass both checks, then wait for the slot of the host while a/1 opens the circuit.
        results = await crawler.fetch_many(["http://a/1", "http://a/2", "http://a/3"])

        assert isinstance(results[0].error, HTTPStatusError)
        assert [type(result.error) for result in results[1:]] == [CircuitOpenError, CircuitOpenError]
        assert fake_session.requested == ["http://a/1"]
        assert crawler.rate_limiter.get_stats().requests == 1

    async def test_only_the_probe_waits_for_the_rate_limit(self, make_crawler, fake_session):
        clock = FakeClock()
        crawler = make_crawler(
            requests_per_second=2,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, clock=clock),
        )
        fake_session.routes["http://a/down"] = FakeResponse(status=503)
        await crawler.fetch_result("http://a/down")
        clock.now += crawler.circuit_breaker.cooldown

        # Refused before booking a turn: the others do not wait 0.5s each for nothing.
        started = time.perf_counter()
        results = await crawler.fetch_many(["http://a/1", "http://a/2", "http://a/3"])
        assert time.perf_counter() - started < 0.9

        assert results[0].ok
        assert [type(result.error) for result in results[1:]] == [CircuitOpenError, CircuitOpenError]
        assert crawler.rate_limiter.get_stats().requests == 2

    async def test_robots_txt_counts(self, make_crawler, fake_session):
        crawler = make_crawler(
            respect_robots=True,
            retry_strategy=RetryStrategy(max_retries=1, base_delay=0.001),
            circuit_breaker=CircuitBreaker(min_requests=2),
        )
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=503)
        with pytest.raises(RobotsUnreachableError):
            await crawler.fetch_url("http://a/page")
        assert crawler.circuit_breaker.state("a") is CircuitState.OPEN

    async def test_refused_robots_txt_is_not_cached(self, make_crawler, fake_session):
        crawler = make_crawler(respect_robots=True, circuit_breaker=CircuitBreaker(min_requests=2))
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=404)
        fake_session.routes["http://a/down"] = aiohttp.ClientConnectionError("refused")
        await crawler.fetch_many(["http://a/down", "http://a/down"])

        # Another origin of the same host needs its own robots.txt.
        with pytest.raises(CircuitOpenError):
            await crawler.fetch_url("https://a/page")
        with pytest.raises(LookupError):
            crawler.robots.unreachable_reason("https://a/page")

    async def test_refused_robots_txt_fails_the_page_under_its_own_url(self, make_crawler, fake_session):
        crawler = make_crawler(respect_robots=True, circuit_breaker=CircuitBreaker(min_requests=2))
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=404)
        fake_session.routes["http://a/down"] = aiohttp.ClientConnectionError("refused")
        await crawler.fetch_many(["http://a/down", "http://a/down"])

        result = await crawler.fetch_result("https://a/page")
        assert isinstance(result.error, CircuitOpenError)
        assert result.error.url == "https://a/page"
        assert str(result.error).startswith("https://a/page: circuit breaker of a is open")

    async def test_host_recovers_after_the_cooldown(self, make_crawler, fake_session):
        clock = FakeClock()
        crawler = make_crawler(circuit_breaker=CircuitBreaker(min_requests=2, cooldown=10, clock=clock))
        fake_session.routes["http://a/"] = [aiohttp.ClientConnectionError("refused")] * 2 + [FakeResponse()]
        await crawler.fetch_many(["http://a/", "http://a/"])
        with pytest.raises(CircuitOpenError):
            await crawler.fetch_url("http://a/")

        clock.now += 10
        assert await crawler.fetch_url("http://a/") == "page"
        assert crawler.circuit_breaker.state("a") is CircuitState.CLOSED
        assert len(fake_session.requested) == 3


class TestCrawlBlockedHost:
    async def test_gives_up_on_a_host_that_stays_down(self, make_crawler, fake_session, caplog):
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(
            max_concurrent=1,
            max_depth=0,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.05),
        )
        pages = [f"http://a/{page}" for page in range(4)]
        for page in pages:
            fake_session.routes[page] = aiohttp.ClientConnectionError("refused")

        await crawler.crawl([*pages, "http://b/"])

        # Opened by a/0, then by the failed probes a/1 and a/2.
        assert fake_session.requested == ["http://a/0", "http://b/", "http://a/1", "http://a/2"]
        assert list(crawler.processed_urls) == ["http://b/"]
        assert [crawler.failed_urls[page].split(":")[0] for page in pages] == ["NetworkError"] * 3 + [
            "CircuitOpenError"
        ]
        assert crawler.circuit_breaker.times_opened("a") == AsyncCrawler.MAX_CIRCUIT_OPENINGS
        messages = [record.getMessage() for record in caplog.records]
        assert any(message.startswith("Deferred http://a/1 for 0.") for message in messages)
        assert "Gave up on http://a/3: circuit breaker of a opened 3 times" in messages
        # Deferred pages are counted once, when they are done; a/3 was never requested.
        stats = crawler.stats.get_stats()
        assert (stats["total_pages"], stats["successful"], stats["failed"]) == (5, 1, 4)
        assert stats["errors"] == {"NetworkError": 3, "CircuitOpenError": 1}
        assert stats["top_domains"] == {"a": 4, "b": 1}

    async def test_no_probe_after_the_last_opening(self, make_crawler, fake_session):
        crawler = make_crawler(
            max_concurrent=2,
            max_depth=0,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.05),
        )
        pages = [f"http://a/{page}" for page in range(5)]
        for page in pages:
            fake_session.routes[page] = aiohttp.ClientConnectionError("refused")
        # A probe is still in flight when the pages deferred along with it come back.
        fake_session.latency = 0.01

        # a/0 and a/1 are sent together, a/2 and a/3 are probes. A page
        # refused while a probe is in flight comes back a second later, when
        # the circuit is half-open again: a/4 after the third opening.
        await crawler.crawl(pages)

        assert fake_session.requested == pages[:4]
        assert crawler.failed_urls[pages[4]] == (
            "CircuitOpenError: circuit breaker of a opened 3 times, no more probes in this crawl"
        )
        assert crawler.circuit_breaker.times_opened("a") == 3

    async def test_page_refused_after_its_wait_costs_nothing_of_max_pages(self, make_crawler, fake_session):
        crawler = make_crawler(
            max_concurrent=2,
            max_depth=0,
            requests_per_second=20,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.2),
        )
        fake_session.routes["http://a/1"] = aiohttp.ClientConnectionError("refused")

        # a/2 waits for its turn while a/1 opens the circuit; a/3 is refused at once.
        await crawler.crawl(["http://a/1", "http://a/2", "http://a/3"], max_pages=3)

        assert crawler.failed_urls.keys() == {"http://a/1"}
        assert crawler.processed_urls.keys() == {"http://a/2", "http://a/3"}
        assert crawler.crawl_stats().queued == 0

    async def test_last_page_of_max_pages_refused_after_its_wait_is_crawled_later(self, make_crawler, fake_session):
        crawler = make_crawler(
            max_concurrent=2,
            max_depth=0,
            requests_per_second=20,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.1),
        )
        fake_session.routes["http://a/1"] = aiohttp.ClientConnectionError("refused")

        # a/2 reaches max_pages and closes the queue, then waits for its turn
        # while a/1 opens the circuit: it is refused, deferred and probes the host later.
        await crawler.crawl(["http://a/1", "http://a/2"], max_pages=2)

        assert fake_session.requested == ["http://a/1", "http://a/2"]
        assert crawler.failed_urls.keys() == {"http://a/1"}
        assert crawler.processed_urls.keys() == {"http://a/2"}
        assert crawler.crawl_stats().queued == 0


class TestRedirectLimit:
    @staticmethod
    def chain(fake_session: FakeSession, redirects: int) -> None:
        for hop in range(redirects):
            fake_session.routes[f"http://a/{hop}"] = FakeResponse(status=302, location=f"http://a/{hop + 1}")

    async def test_max_redirects_are_followed(self, crawler, fake_session):
        self.chain(fake_session, AsyncCrawler.MAX_REDIRECTS)

        result = await crawler.fetch_result("http://a/0")

        assert result.ok
        assert result.final_url == "http://a/10"
        assert len(fake_session.requested) == 11

    async def test_one_more_fails_without_asking_for_its_target(self, make_crawler, fake_session):
        crawler = make_crawler(max_concurrent=1, max_depth=1)
        self.chain(fake_session, AsyncCrawler.MAX_REDIRECTS + 1)
        fake_session.routes["http://b/"] = FakeResponse(b'<a href="http://a/11">11</a>')

        await crawler.crawl(["http://a/0", "http://b/"])

        assert crawler.failed_urls == {"http://a/0": "TooManyRedirectsError: too many redirects (more than 10)"}
        # The target the chain did not reach is not taken for a page already seen.
        assert crawler.processed_urls.keys() == {"http://b/", "http://a/11"}
        assert fake_session.requested.count("http://a/11") == 1


class TestCrawlDuplicates:
    async def test_variant_of_a_page_that_failed_is_kept(self, make_crawler, fake_session):
        crawler = make_crawler(max_concurrent=1, max_depth=0, retry_strategy=RetryStrategy(max_retries=0))
        fake_session.routes["http://a/list"] = FakeResponse(status=404)
        fake_session.routes["http://a/list?page=2"] = FakeResponse(b'<link rel="canonical" href="http://a/list">')

        await crawler.crawl(["http://a/list", "http://a/list?page=2"])

        assert crawler.failed_urls.keys() == {"http://a/list"}
        assert crawler.processed_urls.keys() == {"http://a/list?page=2"}

    async def test_variant_of_a_page_still_queued_is_skipped(self, make_crawler, fake_session):
        crawler = make_crawler(max_concurrent=1, max_depth=0)
        fake_session.routes["http://a/list?page=2"] = FakeResponse(b'<link rel="canonical" href="http://a/list">')

        await crawler.crawl(["http://a/list?page=2", "http://a/list"])

        assert crawler.skipped_urls == {"http://a/list?page=2": "duplicate of http://a/list"}
        assert crawler.processed_urls.keys() == {"http://a/list"}


class TestCrawlPageStats:
    async def test_bug_while_crawling_a_page_fails_that_page_only(self, make_crawler, fake_session, monkeypatch):
        crawler = make_crawler(max_concurrent=1, max_depth=0)
        crawl_page = crawler._crawl_page

        async def broken(url, queue, url_filter):
            if url == "http://a/1":
                raise KeyError("x")
            await crawl_page(url, queue, url_filter)

        monkeypatch.setattr(crawler, "_crawl_page", broken)
        await crawler.crawl(["http://a/1", "http://a/2"])

        assert crawler.failed_urls == {"http://a/1": "UnexpectedError: KeyError: 'x'"}
        assert list(crawler.processed_urls) == ["http://a/2"]
        stats = crawler.stats.get_stats()
        assert (stats["total_pages"], stats["successful"], stats["failed"]) == (2, 1, 1)
        assert stats["errors"] == {"UnexpectedError": 1}
        assert stats["status_codes"] == {200: 1}

    async def test_retried_page_is_counted_once(self, make_crawler, fake_session):
        crawler = make_crawler(max_depth=0, retry_strategy=RetryStrategy(max_retries=2, base_delay=0.001))
        fake_session.routes["http://a/"] = [FakeResponse(status=503), FakeResponse(status=503), FakeResponse()]

        await crawler.crawl(["http://a/"])

        stats = crawler.stats.get_stats()
        assert (stats["total_pages"], stats["successful"], stats["failed"]) == (1, 1, 0)
        assert stats["status_codes"] == {200: 1}


class TestUserAgents:
    async def test_session_user_agent_by_default(self, crawler, fake_session):
        await crawler.fetch_url("http://a")
        assert fake_session.user_agents == [None]

    async def test_rotation_between_requests(self, make_crawler, fake_session):
        agents = ["TestBot/1.0 (desktop)", "TestBot/1.0 (mobile)"]
        crawler = make_crawler(user_agent="TestBot/1.0", user_agents=agents)
        for url in ("http://a", "http://b", "http://c"):
            await crawler.fetch_url(url)
        assert fake_session.user_agents == [agents[0], agents[1], agents[0]]

    def test_rotated_agents_must_share_the_robots_name(self):
        with pytest.raises(ValueError, match="'testbot'"):
            AsyncCrawler(user_agent="TestBot/1.0", user_agents=["TestBot/1.0", "Mozilla/5.0 (Windows NT 10.0)"])

    def test_single_string_is_rejected(self):
        with pytest.raises(TypeError, match="got a string"):
            AsyncCrawler(user_agents="TestBot/1.0")


class TestRobots:
    async def test_disallowed_url_is_not_requested(self, make_crawler, fake_session):
        crawler = make_crawler(respect_robots=True)
        fake_session.routes["http://a/robots.txt"] = FakeResponse(
            b"User-agent: *\nDisallow: /private/", content_type="text/plain"
        )
        with pytest.raises(RobotsDisallowedError):
            await crawler.fetch_url("http://a/private/page")
        assert await crawler.fetch_url("http://a/public") == "page"
        assert fake_session.requested == ["http://a/robots.txt", "http://a/public"]

    async def test_closed_crawler_does_not_fetch_robots_txt(self, make_crawler, fake_session):
        crawler = make_crawler(respect_robots=True)
        await crawler.close()
        with pytest.raises(CrawlerClosedError):
            await crawler.fetch_url("http://a/page")
        assert fake_session.requested == []
