"""Unit tests for AsyncCrawler with the HTTP session replaced by fakes."""

import asyncio
import logging
import socket
import ssl
from unittest.mock import MagicMock

import aiohttp
import pytest
from helpers import UNTHROTTLED

from crawler import (
    AsyncCrawler,
    CertificateError,
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
    ) -> None:
        self.status = status
        self._body = body
        self._encoding = encoding
        self.headers = {} if content_type is None else {"Content-Type": content_type}
        if retry_after is not None:
            self.headers["Retry-After"] = retry_after
        self.content_type = content_type or "application/octet-stream"
        # None means "not redirected": FakeSession fills in the requested URL.
        self.url = url
        self.history = () if url is None else (MagicMock(),)
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

    def get_encoding(self) -> str:
        return self._encoding


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
        self, url: str, headers: dict[str, str] | None = None, timeout: aiohttp.ClientTimeout | None = None
    ) -> FakeRequest:
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

    async def test_redirect_and_missing_content_type(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(content_type=None, url="https://a/home")
        [result] = await crawler.fetch_many(["http://a"])
        assert result.final_url == "https://a/home"
        assert result.content_type is None
        assert result.redirected is True

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
        # The host waits as long as a retry could, not the two minutes asked for.
        assert crawler.rate_limiter.reserve("a") == pytest.approx(5.0, abs=0.1)

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
