"""Unit tests for AsyncCrawler with the HTTP session replaced by fakes."""

import asyncio
import logging
import socket
import ssl
import time
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from unittest.mock import MagicMock

import aiohttp
import pytest
from helpers import UNTHROTTLED, FakeClock, MemoryStorage
from multidict import CIMultiDict

from crawler import (
    AsyncCrawler,
    CertificateError,
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    CrawlerClosedError,
    DNSError,
    FetchResult,
    FetchTimeoutError,
    HTMLParser,
    HTTPStatusError,
    InvalidURLError,
    MemoryFrontier,
    NetworkError,
    NoProxyError,
    ParseError,
    PermanentError,
    PermanentHTTPError,
    ProgressTracker,
    ProxyNetworkError,
    ProxyPool,
    ProxyStats,
    RateLimiter,
    RetryStrategy,
    RobotsDisallowedError,
    RobotsUnreachableError,
    StorageError,
    TooManyRedirectsError,
    TransientError,
    UnexpectedError,
)
from crawler.crawl_run import CrawlRun
from crawler.frontier import Outcome as PageOutcome


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
        self.reason = "Error"
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


Outcome = FakeResponse | BaseException | Callable[[], Awaitable[FakeResponse | BaseException]]


class FakeSession:
    """Serves canned responses or raises canned exceptions per URL.

    A list of outcomes is served one per request; the last one repeats. An
    outcome may be a coroutine function that gives the response, e.g. one
    that holds the request until the test lets it go.
    """

    def __init__(self) -> None:
        self.routes: dict[str, Outcome | list[Outcome]] = {}
        self.latency = 0.0
        self.closed = False
        self.in_flight = 0
        self.peak_in_flight = 0
        self.requested: list[str] = []
        self.user_agents: list[str | None] = []  # per-request User-Agent headers
        self.timeouts: list[aiohttp.ClientTimeout | None] = []  # per-request timeouts
        self.headers: list[dict[str, str]] = []  # per-request headers, beyond those of the session
        self.proxies: list[tuple[str | None, dict[str, str] | None]] = []  # per-request proxy and its headers

    def get(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        allow_redirects: bool = True,
        proxy: str | None = None,
        proxy_headers: dict[str, str] | None = None,
    ) -> FakeRequest:
        assert not allow_redirects  # the crawler follows redirects itself
        self.user_agents.append(None if headers is None else headers.get("User-Agent"))
        self.headers.append(dict(headers or {}))
        self.timeouts.append(timeout)
        self.proxies.append((proxy, proxy_headers))
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
            if callable(outcome):
                outcome = await outcome()
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
        monkeypatch.setattr(crawler._fetcher._transport, "_create_session", lambda: fake_session)
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

    @pytest.mark.parametrize("value", [0, -1])
    def test_rejects_non_positive_max_retry_after(self, value):
        with pytest.raises(ValueError, match="max_retry_after"):
            AsyncCrawler(max_retry_after=value)

    @pytest.mark.parametrize("value", [0, -1])
    def test_rejects_non_positive_max_parsing(self, value):
        with pytest.raises(ValueError, match="max_parsing"):
            AsyncCrawler(max_parsing=value)

    def test_does_not_create_session_eagerly(self):
        crawler = AsyncCrawler()
        assert crawler._fetcher._transport._session is None

    def test_rate_options_configure_the_limiter(self):
        limiter = AsyncCrawler(requests_per_second=4, per_domain_rate=False, min_delay=0.5, jitter=0.1).rate_limiter
        assert (limiter.interval, limiter.per_domain, limiter.jitter) == (0.5, False, 0.1)

    @pytest.mark.parametrize(
        ("name", "value"),
        [
            ("max_concurrent", 1),
            ("max_depth", 0),
            ("keep_pages", False),
            ("max_page_size", 100),
            ("max_parsing", 1),
            ("timeout_growth", 1.0),
            ("max_retry_after", 1.0),
            ("rate_limiter", RateLimiter(None)),
            ("retry_strategy", RetryStrategy()),
            ("circuit_breaker", CircuitBreaker()),
            ("robots", None),
            ("sitemaps", None),
        ],
    )
    def test_settings_are_read_only(self, name, value):
        # Handed to the layers when the crawler is made: a new value would not reach them.
        crawler = AsyncCrawler()
        with pytest.raises(AttributeError):
            setattr(crawler, name, value)


class TestConstantsOfASubclass:
    async def test_max_redirects(self, monkeypatch, fake_session):
        class ShortChains(AsyncCrawler):
            MAX_REDIRECTS = 1

        crawler = ShortChains(**UNTHROTTLED)
        monkeypatch.setattr(crawler._fetcher._transport, "_create_session", lambda: fake_session)
        TestRedirectLimit.chain(fake_session, 2)

        result = await crawler.fetch_result("http://a/0")

        assert isinstance(result.error, TooManyRedirectsError)
        assert result.error.message == "too many redirects (more than 1)"
        assert fake_session.requested == ["http://a/0", "http://a/1"]

    async def test_redirect_statuses(self, monkeypatch, fake_session):
        class PermanentOnly(AsyncCrawler):
            REDIRECT_STATUSES = frozenset({301, 308})

        crawler = PermanentOnly(**UNTHROTTLED)
        monkeypatch.setattr(crawler._fetcher._transport, "_create_session", lambda: fake_session)
        fake_session.routes["http://a/"] = FakeResponse(b"moved", status=302, location="http://a/new")

        result = await crawler.fetch_result("http://a/")

        assert (result.status, result.content, result.redirected) == (302, "moved", False)
        assert fake_session.requested == ["http://a/"]

    def test_max_timeout_growth(self):
        class ShortTimeouts(AsyncCrawler):
            MAX_TIMEOUT_GROWTH = 2.0

        crawler = ShortTimeouts(timeout_growth=3, read_timeout=1)
        assert crawler._fetcher._timeout_for(retries=1).sock_read == 2.0


class TestLifecycle:
    async def test_session_is_created_once_and_reused(self, crawler, fake_session):
        await crawler.fetch_url("http://a")
        await crawler.fetch_url("http://b")
        assert crawler._fetcher._transport._session is fake_session
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
            (aiohttp.ClientConnectorDNSError(MagicMock(), socket.gaierror("Name or service not known")), DNSError),
            (aiohttp.TooManyRedirects(MagicMock(), ()), PermanentError),
        ],
    )
    async def test_failures_are_classified(self, crawler, fake_session, outcome, kind):
        fake_session.routes["http://a"] = outcome
        with pytest.raises(kind):
            await crawler.fetch_url("http://a")

    @pytest.mark.parametrize(
        ("os_error", "kind"),
        [
            (socket.gaierror(socket.EAI_NONAME, "nodename nor servname provided, or not known"), DNSError),
            (socket.gaierror(socket.EAI_NODATA, "No address associated with hostname"), DNSError),
            # aiodns gives no code: its "Domain name not found" is taken at its word.
            (OSError(None, "Domain name not found"), DNSError),
            # The resolver could not be asked for now: an outage, not a typo.
            (socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution"), NetworkError),
            (socket.gaierror(socket.EAI_FAIL, "Non-recoverable failure in name resolution"), NetworkError),
        ],
    )
    async def test_only_a_name_that_does_not_exist_is_a_dns_error(self, crawler, fake_session, os_error, kind):
        fake_session.routes["http://a"] = aiohttp.ClientConnectorDNSError(MagicMock(), os_error)
        with pytest.raises(NetworkError) as raised:
            await crawler.fetch_url("http://a")
        assert type(raised.value) is kind
        assert os_error.strerror in raised.value.message

    async def test_407_of_a_site_is_an_http_error(self, crawler, fake_session):
        # Without a proxy, it is the site that asks for one.
        fake_session.routes["http://a"] = FakeResponse(status=407)
        with pytest.raises(PermanentHTTPError):
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

    async def test_parses_at_most_max_parsing_pages_at_once(self, make_crawler, fake_session):
        # Parsing takes about forty times the size of a page in memory and
        # gets no parallelism from the GIL: the trees must not pile up.
        parser = SlowParser()
        crawler = make_crawler(max_concurrent=6, max_parsing=2, parser=parser)
        urls = [f"http://a/{n}" for n in range(6)]

        pages = await asyncio.gather(*(crawler.fetch_and_parse(url) for url in urls))

        assert [page["url"] for page in pages] == urls
        assert parser.peak == 2


class SlowParser(HTMLParser):
    """Counts the pages being parsed at once; each parse takes a moment."""

    def __init__(self) -> None:
        super().__init__()
        self.parsing = self.peak = 0

    async def parse_html(self, html, url, *, final_url=None, content_type=None):
        self.parsing += 1
        self.peak = max(self.peak, self.parsing)
        try:
            await asyncio.sleep(0.01)
            return await super().parse_html(html, url, final_url=final_url, content_type=content_type)
        finally:
            self.parsing -= 1


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

        assert crawler.rate_limiter.reserve("a") == pytest.approx(AsyncCrawler.DEFAULT_MAX_RETRY_AFTER, abs=0.1)
        assert "a asked to wait 86400s (Retry-After), waiting 600s" in caplog.text

    async def test_retry_after_cap_is_an_option(self, make_crawler, fake_session, caplog):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=0), max_retry_after=5)
        fake_session.routes["http://a"] = FakeResponse(status=429, retry_after="120")
        with pytest.raises(HTTPStatusError):
            await crawler.fetch_url("http://a")

        assert crawler.rate_limiter.reserve("a") == pytest.approx(5.0, abs=0.1)
        assert "a asked to wait 120s (Retry-After), waiting 5s" in caplog.text

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
        assert crawler._fetcher._timeout_for(retries=5).sock_read == AsyncCrawler.MAX_TIMEOUT_GROWTH

    @pytest.mark.parametrize(
        ("failure", "held_back"),
        [
            (FakeResponse(status=429), True),
            (FakeResponse(status=503, retry_after="1"), True),
            (aiohttp.ServerTimeoutError(), True),
            (FakeResponse(status=503), False),
            (FakeResponse(status=500), False),
            (aiohttp.ClientConnectionError(), False),
        ],
        ids=["429", "retry-after", "timeout", "503", "500", "connection"],
    )
    async def test_backoff_holds_back_the_whole_host_only_when_it_is_overloaded(
        self, make_crawler, fake_session, failure, held_back
    ):
        crawler = make_crawler(retry_strategy=RetryStrategy(max_retries=1, base_delay=0.1))
        fake_session.routes["http://a/slow"] = [failure, FakeResponse()]
        retrying = asyncio.create_task(crawler.fetch_url("http://a/slow"))
        await asyncio.sleep(0.01)  # the first attempt has failed, the retry waits

        # Other pages of the host wait too after HTTP 429, a Retry-After or a
        # timeout, signs that the whole site is overloaded; after a failure of
        # one page they do not.
        assert (crawler.rate_limiter.reserve("a") > 0) is held_back
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
        for page in ("http://a/1", "http://a/2", "http://a/3"):
            fake_session.routes[page] = FakeResponse(status=503)

        # Every request counts once, retries or not: the first failed attempt
        # of the third one opens the circuit, and it is not retried after that.
        await crawler.fetch_result("http://a/1")
        await crawler.fetch_result("http://a/2")
        third = await crawler.fetch_result("http://a/3")
        fourth = await crawler.fetch_result("http://a/4")
        other_host = await crawler.fetch_result("http://b/")

        # The failure reported is that of the last request sent.
        assert isinstance(third.error, HTTPStatusError)
        assert third.error.status == 503
        assert isinstance(fourth.error, CircuitOpenError)
        assert fourth.error.message.startswith("circuit breaker of a is open (3 of 3 requests failed")
        assert other_host.ok
        assert fake_session.requested == ["http://a/1"] * 4 + ["http://a/2"] * 4 + ["http://a/3", "http://b/"]
        # Refused requests were not made: they are not errors of an attempt.
        stats = crawler.error_stats()
        assert (stats.total, stats.retries) == (9, 6)
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
            circuit_breaker=CircuitBreaker(min_requests=1),
        )
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=503)
        with pytest.raises(RobotsUnreachableError):
            await crawler.fetch_url("http://a/page")
        assert crawler.circuit_breaker.state("a") is CircuitState.OPEN
        # The retry the open circuit would refuse is not made.
        assert fake_session.requested == ["http://a/robots.txt"]

    async def test_a_request_counts_once_whatever_its_retries(self, make_crawler, fake_session):
        crawler = make_crawler(
            retry_strategy=RetryStrategy(max_retries=3, base_delay=0.001),
            circuit_breaker=CircuitBreaker(min_requests=3),
        )
        fake_session.routes["http://a/down"] = FakeResponse(status=503)
        fake_session.routes["http://a/slow"] = [FetchTimeoutError("http://a/slow", "timed out"), FakeResponse()]

        await crawler.fetch_result("http://a/down")  # four failed attempts: one failure
        assert await crawler.fetch_url("http://a/slow") == "page"  # a failure made good by the retry: a success
        assert await crawler.fetch_url("http://a/ok") == "page"

        # Three requests, one of them failed: were every attempt counted, 5 of 7 would have opened the circuit.
        assert len(fake_session.requested) == 7
        assert crawler.circuit_breaker.state("a") is CircuitState.CLOSED
        circuit = crawler.circuit_breaker.get_stats()["a"]
        assert (circuit.requests, circuit.failures) == (3, 1)

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

        # Opened by a/0, which lost its retries to that and is put off like
        # the others, then by the failed probes a/0 and a/1: a page whose
        # probe failed is not put off again, so a/2 and a/3 are never requested.
        assert fake_session.requested == ["http://a/0", "http://b/", "http://a/0", "http://a/1"]
        assert list(crawler.processed_urls) == ["http://b/"]
        assert [crawler.failed_urls[page].split(":")[0] for page in pages] == ["NetworkError"] * 2 + [
            "CircuitOpenError"
        ] * 2
        assert crawler.circuit_breaker.times_opened("a") == AsyncCrawler.MAX_CIRCUIT_OPENINGS
        messages = [record.getMessage() for record in caplog.records]
        assert any(message.startswith("Deferred http://a/0 for 0.") for message in messages)
        assert any(message.startswith("Deferred http://a/1 for 0.") for message in messages)
        assert "Gave up on http://a/3: circuit breaker of a opened 3 times" in messages
        # Deferred pages are counted once, when they are done; a/2 and a/3 were never requested.
        stats = crawler.stats.get_stats()
        assert (stats["total_pages"], stats["successful"], stats["failed"]) == (5, 1, 4)
        assert stats["errors"] == {"NetworkError": 2, "CircuitOpenError": 2}
        assert stats["top_domains"] == {"a": 4, "b": 1}

    async def test_a_page_with_an_error_never_retried_is_not_put_off(self, make_crawler, fake_session):
        # Its 501 opens the circuit, but the breaker took no retry from it:
        # a probe would only get the same 501.
        crawler = make_crawler(
            max_depth=0,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.05),
        )
        fake_session.routes["http://a/1"] = FakeResponse(status=501)

        await crawler.crawl(["http://a/1"])

        assert fake_session.requested == ["http://a/1"]
        assert crawler.failed_urls["http://a/1"].startswith("PermanentHTTPError: HTTP 501")
        assert crawler.circuit_breaker.times_opened("a") == 1

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

        # a/0 and a/1 are sent together and put off when a/0 opens the
        # circuit, a/1 last, as its failure lands after a/2 to a/4 were
        # refused; a/0 and a/2 are the probes. A page refused while a probe
        # is in flight comes back a second later, when the circuit is
        # half-open again: a/1, a/3 and a/4 after the third opening.
        await crawler.crawl(pages)

        assert fake_session.requested == ["http://a/0", "http://a/1", "http://a/0", "http://a/2"]
        assert [crawler.failed_urls[page].split(":")[0] for page in pages] == [
            "NetworkError",
            "CircuitOpenError",
            "NetworkError",
            "CircuitOpenError",
            "CircuitOpenError",
        ]
        assert crawler.failed_urls[pages[4]] == (
            "CircuitOpenError: circuit breaker of a opened 3 times, no more probes in this crawl"
        )
        assert crawler.circuit_breaker.times_opened("a") == 3

    async def test_no_robots_txt_download_for_a_host_given_up_on(self, make_crawler, fake_session, caplog):
        # robots.txt of the host fails, each download a probe of its
        # circuit: after the third opening the page is given up without a
        # fourth download, as any page of the host is.
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(
            max_concurrent=1,
            max_depth=0,
            respect_robots=True,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.02),
        )
        crawler.robots.UNREACHABLE_TTL = 0.05
        fake_session.routes["http://a/robots.txt"] = aiohttp.ClientConnectionError("refused")

        await crawler.crawl(["http://a/1"])

        assert fake_session.requested == ["http://a/robots.txt"] * AsyncCrawler.MAX_CIRCUIT_OPENINGS
        assert crawler.circuit_breaker.times_opened("a") == AsyncCrawler.MAX_CIRCUIT_OPENINGS
        assert crawler.unreachable_urls == {}
        assert crawler.failed_urls == {
            "http://a/1": "CircuitOpenError: circuit breaker of a opened 3 times, no more probes in this crawl"
        }

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
        # while a/1 opens the circuit: both are deferred, a/1 probes the
        # host and fails, a/2 probes it next and is crawled.
        await crawler.crawl(["http://a/1", "http://a/2"], max_pages=2)

        assert fake_session.requested == ["http://a/1", "http://a/1", "http://a/2"]
        assert crawler.failed_urls.keys() == {"http://a/1"}
        assert crawler.processed_urls.keys() == {"http://a/2"}
        assert crawler.crawl_stats().queued == 0

    async def test_pages_in_flight_when_the_circuit_opens_are_put_off_and_crawled_later(
        self, make_crawler, fake_session, caplog
    ):
        # The review's probe: a host answers 503 for a moment. Every page
        # in flight fails, the first ones open the circuit, and none is
        # retried, as the breaker refuses the retries; without the crawl
        # putting them off, they would be the pages lost to the outage.
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(
            max_concurrent=4,
            max_depth=0,
            retry_strategy=RetryStrategy(max_retries=3, base_delay=0.001),
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=2, cooldown=0.05),
        )
        pages = [f"http://a/{page}" for page in range(8)]
        for page in pages[:4]:
            fake_session.routes[page] = [FakeResponse(status=503), FakeResponse()]
        fake_session.latency = 0.01

        await crawler.crawl(pages)

        assert crawler.failed_urls == {}
        assert set(crawler.processed_urls) == set(pages)
        # The four in flight were requested twice, the others once, after the cooldown.
        assert sorted(fake_session.requested) == sorted(pages[:4] * 2 + pages[4:])
        assert crawler.circuit_breaker.times_opened("a") == 1
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred ")]
        reason = "circuit breaker of a is open (2 of 2 requests failed in 60s)"
        assert {message.split()[1] for message in deferred if reason in message} == set(pages)
        stats = crawler.stats.get_stats()
        assert (stats["total_pages"], stats["successful"], stats["failed"]) == (8, 8, 0)

    async def test_pages_in_flight_when_the_circuit_opens_fail_once_the_host_is_given_up(
        self, make_crawler, fake_session
    ):
        crawler = make_crawler(
            max_concurrent=4,
            max_depth=0,
            retry_strategy=RetryStrategy(max_retries=1, base_delay=0.001),
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=2, cooldown=0.05),
        )
        pages = [f"http://a/{page}" for page in range(8)]
        for page in pages:
            fake_session.routes[page] = FakeResponse(status=503)
        fake_session.latency = 0.01

        await crawler.crawl(pages)

        # The two probes fail with their own error; the rest were refused.
        assert set(crawler.failed_urls) == set(pages)
        failures = Counter(error.split(":")[0] for error in crawler.failed_urls.values())
        assert failures == {"TransientHTTPError": 2, "CircuitOpenError": 6}
        assert crawler.processed_urls == {}
        assert len(fake_session.requested) == 6
        assert crawler.circuit_breaker.times_opened("a") == AsyncCrawler.MAX_CIRCUIT_OPENINGS

    async def test_page_whose_probe_fails_is_not_put_off_again(self, make_crawler, fake_session):
        # One page of a healthy host answers 500 every time, and opens the
        # circuit. Put off again after its probe failed, it would probe the
        # host again and again, and the host would be given up for one
        # broken page.
        crawler = make_crawler(
            max_concurrent=1,
            max_depth=0,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.05),
        )
        fake_session.routes["http://a/broken"] = FakeResponse(status=500)

        await crawler.crawl(["http://a/broken", "http://a/1", "http://a/2"])

        assert fake_session.requested == ["http://a/broken", "http://a/broken", "http://a/1", "http://a/2"]
        assert crawler.failed_urls == {"http://a/broken": "TransientHTTPError: HTTP 500 Error"}
        assert set(crawler.processed_urls) == {"http://a/1", "http://a/2"}
        assert crawler.circuit_breaker.times_opened("a") == 2
        assert crawler.circuit_breaker.state("a") is CircuitState.CLOSED

    @staticmethod
    def give_up_at_once(monkeypatch, fake_session, **options) -> AsyncCrawler:
        """A crawler that gives a host up the first time its circuit opens."""

        class OneOpening(AsyncCrawler):
            MAX_CIRCUIT_OPENINGS = 1

        crawler = OneOpening(**{**UNTHROTTLED, **options})
        monkeypatch.setattr(crawler._fetcher._transport, "_create_session", lambda: fake_session)
        return crawler

    async def test_page_refused_at_its_redirect_target_is_uncounted_until_taken_again(self, make_crawler, fake_session):
        crawler = make_crawler(
            max_concurrent=1,
            max_depth=0,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=0.05),
        )
        fake_session.routes["http://b/down"] = aiohttp.ClientConnectionError("refused")
        fake_session.routes["http://a/1"] = FakeResponse(status=302, location="http://b/1")
        await crawler.fetch_result("http://b/down")

        # a/1 is requested, its redirect to b is refused: it is put off
        # like a page refused before its request, and a/2 takes its place.
        await crawler.crawl(["http://a/1", "http://a/2"], max_pages=2)

        assert fake_session.requested == ["http://b/down", "http://a/1", "http://a/2", "http://a/1", "http://b/1"]
        assert crawler.processed_urls.keys() == {"http://a/2", "http://a/1"}
        assert crawler.crawl_stats().queued == 0

    async def test_pages_that_redirect_to_a_host_that_stays_down_all_get_an_outcome(self, make_crawler, fake_session):
        # The review's probe: every link of a site redirects to a host that
        # answers 503. Counted again each time they came back, the pages put
        # off would use up max_pages and be left in the queue, neither
        # crawled nor failed.
        crawler = make_crawler(max_concurrent=1, circuit_breaker=CircuitBreaker(min_requests=3, cooldown=0.3))
        pages = [f"http://a/{page}" for page in range(12)]
        fake_session.routes["http://a/"] = FakeResponse("".join(f'<a href="{page}">' for page in pages).encode())
        for page in range(12):
            fake_session.routes[f"http://a/{page}"] = FakeResponse(status=302, location=f"http://b/{page}")
            fake_session.routes[f"http://b/{page}"] = FakeResponse(status=503)

        await crawler.crawl(["http://a/"], max_pages=13)

        assert crawler.processed_urls.keys() == {"http://a/"}
        assert crawler.failed_urls.keys() == set(pages)
        assert crawler.crawl_stats().queued == 0
        assert crawler.circuit_breaker.times_opened("b") == AsyncCrawler.MAX_CIRCUIT_OPENINGS

    async def test_page_given_up_at_its_redirect_target_counts_toward_max_pages(self, monkeypatch, fake_session):
        crawler = self.give_up_at_once(
            monkeypatch,
            fake_session,
            max_concurrent=1,
            max_depth=0,
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=1, cooldown=60),
        )
        # Its 501 opens the circuit of b in the crawl, and fails at once: no retry was refused.
        fake_session.routes["http://b/down"] = FakeResponse(status=501)
        fake_session.routes["http://a/1"] = FakeResponse(status=302, location="http://b/1")

        # a/1 has sent its request and is not put off: it counts, so a/3 is not requested.
        await crawler.crawl(["http://b/down", "http://a/1", "http://a/2", "http://a/3"], max_pages=3)

        assert fake_session.requested == ["http://b/down", "http://a/1", "http://a/2"]
        assert crawler.failed_urls.keys() == {"http://b/down", "http://a/1"}
        assert crawler.failed_urls["http://a/1"].startswith("CircuitOpenError")
        assert crawler.processed_urls.keys() == {"http://a/2"}
        assert crawler.crawl_stats().queued == 1

    async def test_page_in_flight_given_up_counts_toward_max_pages(self, monkeypatch, fake_session):
        crawler = self.give_up_at_once(
            monkeypatch,
            fake_session,
            max_concurrent=2,
            max_depth=0,
            retry_strategy=RetryStrategy(max_retries=1, base_delay=0.001),
            circuit_breaker=CircuitBreaker(failure_threshold=1.0, min_requests=2, cooldown=60),
        )
        for page in ("http://a/0", "http://a/1"):
            fake_session.routes[page] = FakeResponse(status=503)
        fake_session.latency = 0.01

        # Both requests fail together and open the circuit, which takes
        # the retry of the first; the host is given up, and the pages fail
        # with their own error. They were requested, so c/1 is not.
        await crawler.crawl(["http://a/0", "http://a/1", "http://c/1"], max_pages=2)

        assert fake_session.requested == ["http://a/0", "http://a/1"]
        assert crawler.failed_urls == {
            "http://a/0": "TransientHTTPError: HTTP 503 Error",
            "http://a/1": "TransientHTTPError: HTTP 503 Error",
        }
        assert crawler.crawl_stats().queued == 1


class TestCrawlHeldBackHost:
    async def test_long_retry_after_is_a_warning_once_per_host(self, make_crawler, fake_session, caplog):
        # The host asks for 1 s, longer than any retry may wait (0.5 s): the
        # request is not retried, its other pages are put off, which is said
        # once at WARNING.
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(
            max_concurrent=1, max_depth=0, retry_strategy=RetryStrategy(max_retries=1, max_delay=0.5)
        )
        crawler.MIN_PENALTY_TO_DEFER = 0.05
        fake_session.routes["http://a/1"] = [FakeResponse(status=429, retry_after="1"), FakeResponse(b"ok")]
        for page in ("http://a/2", "http://a/3", "http://b/"):
            fake_session.routes[page] = FakeResponse(b"ok")

        await crawler.crawl(["http://a/1", "http://a/2", "http://a/3", "http://b/"])

        # a/1 comes back with the host, after the second it asked for.
        assert fake_session.requested[:2] == ["http://a/1", "http://b/"]
        assert sorted(fake_session.requested[2:]) == ["http://a/1", "http://a/2", "http://a/3"]
        assert set(crawler.processed_urls) == {"http://a/1", "http://a/2", "http://a/3", "http://b/"}
        assert crawler.failed_urls == {}
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings.count("a asked to wait 1s (Retry-After); its pages are put off until then") == 1
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred http://a/")]
        assert len(deferred) == 3

    async def test_page_forbidden_with_a_long_retry_after_fails_at_once(self, make_crawler, fake_session, caplog):
        # HTTP 403 with a Retry-After: the host is held back as it asked,
        # but the page is not requested again, it would be forbidden again.
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(
            max_concurrent=1, max_depth=0, retry_strategy=RetryStrategy(max_retries=1, max_delay=0.05)
        )
        crawler.MIN_PENALTY_TO_DEFER = 0.05
        fake_session.routes["http://a/1"] = FakeResponse(status=403, retry_after="1")
        fake_session.routes["http://a/2"] = FakeResponse(b"ok")

        await crawler.crawl(["http://a/1", "http://a/2"])

        assert fake_session.requested == ["http://a/1", "http://a/2"]
        assert crawler.failed_urls == {"http://a/1": "PermanentHTTPError: HTTP 403 Error"}
        assert set(crawler.processed_urls) == {"http://a/2"}
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred ")]
        assert deferred == ["Deferred http://a/2 for 1.0s: its host is held back"]

    async def test_page_asked_to_wait_is_the_last_one_of_max_pages(self, make_crawler, fake_session):
        # The page reached max_pages and closed the queue; put off, it is
        # back under the limit and crawled once the host may be asked again.
        crawler = make_crawler(
            max_concurrent=1,
            max_depth=0,
            retry_strategy=RetryStrategy(max_retries=1, max_delay=0.05),
            max_retry_after=0.1,
        )
        fake_session.routes["http://a/1"] = [FakeResponse(status=429, retry_after="1"), FakeResponse(b"ok")]

        await crawler.crawl(["http://a/1"], max_pages=1)

        assert fake_session.requested == ["http://a/1", "http://a/1"]
        assert set(crawler.processed_urls) == {"http://a/1"}
        assert crawler.crawl_stats().queued == 0

    async def test_page_that_keeps_asking_to_wait_fails_in_the_end(self, make_crawler, fake_session, caplog):
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(
            max_concurrent=1,
            max_depth=0,
            retry_strategy=RetryStrategy(max_retries=1, max_delay=0.05),
            max_retry_after=0.1,
        )
        fake_session.routes["http://a/1"] = FakeResponse(status=429, retry_after="1")

        await crawler.crawl(["http://a/1"])

        assert fake_session.requested == ["http://a/1"] * (1 + AsyncCrawler.MAX_WAITS_PER_PAGE)
        assert crawler.failed_urls == {"http://a/1": "TransientHTTPError: HTTP 429 Error"}
        assert crawler.stats.get_stats()["total_pages"] == 1
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred http://a/1 for 0.1s")]
        assert len(deferred) == AsyncCrawler.MAX_WAITS_PER_PAGE

    async def test_pause_before_a_retry_is_not_a_warning(self, make_crawler, fake_session, caplog):
        # A 429 without Retry-After holds the host back for the retry pause,
        # which is logged as the retry itself is; the other page is just put off.
        crawler = make_crawler(
            max_concurrent=1, max_depth=0, retry_strategy=RetryStrategy(max_retries=1, base_delay=0.3)
        )
        crawler.MIN_PENALTY_TO_DEFER = 0.05
        fake_session.routes["http://a/1"] = [FakeResponse(status=429), FakeResponse(b"ok")]
        fake_session.routes["http://a/2"] = FakeResponse(b"ok")

        await crawler.crawl(["http://a/1", "http://a/2"])

        assert set(crawler.processed_urls) == {"http://a/1", "http://a/2"}
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert not [message for message in warnings if "put off" in message]


class TestCrawlStorage:
    async def test_the_storage_is_opened_before_the_first_request(self, make_crawler, fake_session):
        opened = []

        class Opening(MemoryStorage):
            async def _open_storage(self) -> None:
                opened.append(list(fake_session.requested))

        crawler = make_crawler(storage=Opening(), max_depth=0)
        await crawler.crawl(["http://a/1"])

        assert opened == [[]]
        assert fake_session.requested == ["http://a/1"]

    async def test_a_storage_that_cannot_be_opened_fails_the_crawl_before_it_requests_anything(
        self, make_crawler, fake_session
    ):
        class Unopenable(MemoryStorage):
            async def _open_storage(self) -> None:
                raise OSError("read-only file system")

        crawler = make_crawler(storage=Unopenable(), max_depth=0)

        with pytest.raises(StorageError, match="Unopenable cannot be opened: read-only file system"):
            await crawler.crawl(["http://a/1"])

        assert fake_session.requested == []
        assert crawler.crawl_stats().processed == 0
        # The crawler is free for another crawl.
        crawler.storage = MemoryStorage()
        await crawler.crawl(["http://a/1"])
        assert fake_session.requested == ["http://a/1"]

    async def test_a_second_crawl_is_refused_while_the_storage_of_the_first_opens(self, make_crawler, fake_session):
        opening, opened = asyncio.Event(), asyncio.Event()

        class Slow(MemoryStorage):
            async def _open_storage(self) -> None:
                opening.set()
                await opened.wait()

        crawler = make_crawler(storage=Slow(), max_depth=0)
        first = asyncio.create_task(crawler.crawl(["http://a/1"]))
        await opening.wait()

        # Let in, the second crawl would wait for the same storage.
        async with asyncio.timeout(1):
            with pytest.raises(RuntimeError, match="a crawl is already running on this crawler"):
                await crawler.crawl(["http://a/2"])

        opened.set()
        await first
        assert fake_session.requested == ["http://a/1"]
        assert crawler.processed_urls.keys() == {"http://a/1"}

    @staticmethod
    def recording_frontier(monkeypatch) -> list[tuple[str, ...]]:
        """The pages the crawl finishes as processed, and those it reports saved, in order."""
        events: list[tuple[str, ...]] = []

        class Recording(MemoryFrontier):
            async def finish(self, page, outcome, reason=None, *, uncount=False, pending_save=False):
                if outcome is PageOutcome.PROCESSED:
                    events.append(("pending save" if pending_save else "processed", page.url))
                await super().finish(page, outcome, reason, uncount=uncount, pending_save=pending_save)

            async def saved(self, urls):
                events.append(("saved", *urls))

        monkeypatch.setattr("crawler.client.MemoryFrontier", Recording)
        return events

    async def test_pages_are_reported_saved_to_the_frontier_once_written(self, make_crawler, fake_session, monkeypatch):
        events = self.recording_frontier(monkeypatch)
        storage = MemoryStorage(batch_size=2)
        crawler = make_crawler(storage=storage, max_concurrent=1, max_depth=1)
        fake_session.routes["http://a/1"] = FakeResponse(b'<a href="http://a/2">2</a><a href="http://a/3">3</a>')

        await crawler.crawl(["http://a/1"])

        assert events == [
            ("pending save", "http://a/1"),
            ("pending save", "http://a/2"),
            ("saved", "http://a/1", "http://a/2"),
            ("pending save", "http://a/3"),
            ("saved", "http://a/3"),  # the last batch, flushed at the end of the crawl
        ]
        # The storage reports to the frontier of its crawl only.
        assert storage.on_settled is None

    async def test_without_a_storage_pages_are_processed_at_once(self, make_crawler, fake_session, monkeypatch):
        events = self.recording_frontier(monkeypatch)
        crawler = make_crawler(max_depth=0)

        await crawler.crawl(["http://a/1"])

        assert events == [("processed", "http://a/1")]


class TestCrawlScope:
    async def test_same_domain_only_keeps_www_and_subdomains(self, make_crawler, fake_session):
        crawler = make_crawler(max_depth=1)
        links = ["http://www.example.com/a", "http://docs.example.com/b", "http://example.com.other.org/c"]
        fake_session.routes["http://example.com/"] = FakeResponse(
            "".join(f'<a href="{link}">x</a>' for link in links).encode()
        )
        for link in links:
            fake_session.routes[link] = FakeResponse(b"ok")

        await crawler.crawl(["http://example.com/"], same_domain_only=True)

        assert set(crawler.processed_urls) == {"http://example.com/", *links[:2]}


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


class TestHostQueueLimit:
    async def test_host_over_its_limit_leaves_room_for_the_others(self, make_crawler, fake_session, caplog):
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(max_concurrent=1, max_depth=1)
        links = [f"http://a/{n}" for n in range(1, 101)] + ["http://b/", "http://c/", "http://d/"]
        fake_session.routes["http://a/0"] = FakeResponse("".join(f'<a href="{link}">x</a>' for link in links).encode())

        # The queue holds 3 x 12 pages; a host with a limit of 3 pages gets 3 x 3 of them.
        await crawler.crawl(["http://a/0"], max_pages=12, max_pages_per_host=3, same_domain_only=False)

        assert crawler.processed_urls.keys() == {
            "http://a/0",
            "http://a/1",
            "http://a/2",
            "http://b/",
            "http://c/",
            "http://d/",
        }
        assert len(crawler.skipped_urls) == 6  # a/3 to a/8, over max_pages_per_host
        stats = crawler.crawl_stats()
        assert (stats.skipped, stats.over_host_limit) == (6, 6)
        # Not requested: the crawl ended with half of max_pages done, not all of it.
        assert ProgressTracker(max_pages=12).update(stats, finished=True).done == 6
        messages = [record.getMessage() for record in caplog.records]
        assert "Host a has 9 pages queued (3 x max_pages_per_host): its new links are not queued" in messages
        assert "92 links were not queued: their host had 3 x max_pages_per_host pages queued" in messages

    async def test_no_limit_by_host_without_max_pages_per_host(self, make_crawler, fake_session):
        crawler = make_crawler(max_concurrent=1, max_depth=1)
        fake_session.routes["http://a/0"] = FakeResponse(
            "".join(f'<a href="http://a/{n}">x</a>' for n in range(1, 11)).encode()
        )

        await crawler.crawl(["http://a/0"], max_pages=20)

        assert len(crawler.processed_urls) == 11


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
        crawl_page = CrawlRun._crawl_page

        async def broken(self, page, url_filter):
            if page.url == "http://a/1":
                raise KeyError("x")
            await crawl_page(self, page, url_filter)

        # The run of a crawl is made inside crawl().
        monkeypatch.setattr(CrawlRun, "_crawl_page", broken)
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

    async def test_crawl_waits_for_an_unreachable_robots_txt_to_be_downloaded_again(
        self, make_crawler, fake_session, caplog
    ):
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.1
        fake_session.routes["http://a/robots.txt"] = [FakeResponse(status=503), FakeResponse(status=404)]
        fake_session.routes["http://a/1"] = FakeResponse(b"ok")

        await crawler.crawl(["http://a/1"])

        assert fake_session.requested == ["http://a/robots.txt", "http://a/robots.txt", "http://a/1"]
        assert set(crawler.processed_urls) == {"http://a/1"}
        assert crawler.unreachable_urls == {}
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred ")]
        assert deferred == ["Deferred http://a/1 for 0.1s: robots.txt is unreachable (HTTP 503)"]

    async def test_crawl_gives_up_on_a_site_whose_robots_txt_stays_unreachable(
        self, make_crawler, fake_session, caplog
    ):
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.05
        crawler.ROBOTS_POLL = 0.01
        fake_session.routes["http://a/robots.txt"] = aiohttp.ClientConnectionError("refused")

        await crawler.crawl(["http://a/1", "http://a/2"])

        # Downloaded once more after each wait; the pages wait together.
        assert fake_session.requested == ["http://a/robots.txt"] * (1 + AsyncCrawler.MAX_ROBOTS_RETRIES)
        reason = "robots.txt is unreachable (NetworkError: ClientConnectionError: refused)"
        assert crawler.unreachable_urls == {"http://a/1": reason, "http://a/2": reason}
        assert crawler.crawl_stats().unreachable == 2
        assert f"Gave up on http://a/1: {reason}" in [r.getMessage() for r in caplog.records]

    async def test_a_site_is_given_up_for_the_failure_of_its_robots_txt_not_for_a_wait_that_ran_out(
        self, make_crawler, fake_session, monkeypatch
    ):
        # The wait for the last download may run out in the very moment the
        # download fails: the page is told only that robots.txt is being
        # downloaded, but the site is given up for what the download found.
        crawler = make_crawler(respect_robots=True, max_depth=0, max_concurrent=1)
        crawler.robots.UNREACHABLE_TTL = 0.05
        crawler.ROBOTS_POLL = 0.01
        fake_session.routes["http://a/robots.txt"] = aiohttp.ClientConnectionError("refused")
        robots, is_allowed, raced = crawler.robots, crawler.robots.is_allowed, []

        async def is_allowed_as_the_wait_runs_out(url, user_agent="*", *, wait=None):
            allowed = await is_allowed(url, user_agent, wait=wait)
            if robots.failed_downloads(url) > AsyncCrawler.MAX_ROBOTS_RETRIES and not raced:
                raced.append(url)
                raise TimeoutError
            return allowed

        monkeypatch.setattr(robots, "is_allowed", is_allowed_as_the_wait_runs_out)

        await crawler.crawl(["http://a/1"])

        assert raced == ["http://a/1"]
        assert crawler.unreachable_urls == {
            "http://a/1": "robots.txt is unreachable (NetworkError: ClientConnectionError: refused)"
        }

    async def test_crawl_does_not_wait_for_a_robots_txt_whose_host_does_not_resolve(
        self, make_crawler, fake_session, caplog
    ):
        # A name that does not resolve is a typo, not an outage: waiting
        # three minutes for it would change nothing.
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.05
        fake_session.routes["http://a/robots.txt"] = aiohttp.ClientConnectorDNSError(
            MagicMock(), socket.gaierror("Name or service not known")
        )

        await crawler.crawl(["http://a/1", "http://a/2"])

        assert fake_session.requested == ["http://a/robots.txt"]
        reason = "robots.txt is unreachable (DNSError: ClientConnectorDNSError: Cannot connect to host"
        assert all(why.startswith(reason) for why in crawler.unreachable_urls.values())
        assert set(crawler.unreachable_urls) == {"http://a/1", "http://a/2"}
        assert not [r for r in caplog.records if r.getMessage().startswith("Deferred ")]

    async def test_crawl_waits_for_a_robots_txt_whose_lookup_failed_for_now(self, make_crawler, fake_session):
        # A resolver that cannot be reached is an outage like any other: the
        # site is downloaded again, and its pages are crawled once it is back.
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.05
        fake_session.routes["http://a/robots.txt"] = [
            aiohttp.ClientConnectorDNSError(
                MagicMock(), socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")
            ),
            FakeResponse(status=404),
        ]

        await crawler.crawl(["http://a/1"])

        assert fake_session.requested == ["http://a/robots.txt", "http://a/robots.txt", "http://a/1"]
        assert crawler.processed_urls.keys() == {"http://a/1"}
        assert not crawler.unreachable_urls

    async def test_sitemaps_and_pages_share_the_downloads_of_an_unreachable_robots_txt(
        self, make_crawler, fake_session, caplog
    ):
        # The sitemaps named in robots.txt, the sitemap given and the start
        # URL all wait for the same site: it is downloaded again three
        # times in all, not three times for each of them.
        caplog.set_level(logging.WARNING, logger="crawler")
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.1
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=503)

        await crawler.crawl(["http://a/1"], sitemap_urls=["http://a/sitemap.xml"], robots_sitemaps=True)

        assert fake_session.requested == ["http://a/robots.txt"] * (1 + AsyncCrawler.MAX_ROBOTS_RETRIES)
        reason = "robots.txt is unreachable (HTTP 503)"
        assert crawler.unreachable_urls == {"http://a/1": reason}
        assert crawler.failed_sitemaps == {"http://a/sitemap.xml": f"RobotsUnreachableError: {reason}"}
        assert f"No sitemaps from robots.txt of http://a/1: {reason}" in [r.getMessage() for r in caplog.records]

    async def test_a_site_given_up_on_is_tried_again_after_the_crawl(self, make_crawler, fake_session):
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.05
        downloads = 1 + AsyncCrawler.MAX_ROBOTS_RETRIES
        fake_session.routes["http://a/robots.txt"] = [FakeResponse(status=503)] * downloads + [FakeResponse(status=404)]

        await crawler.crawl(["http://a/1"])
        assert crawler.unreachable_urls == {"http://a/1": "robots.txt is unreachable (HTTP 503)"}

        # The crawl gave up on the site, the crawler did not.
        await crawler.fetch_url("http://a/1")

        assert fake_session.requested == ["http://a/robots.txt"] * (downloads + 1) + ["http://a/1"]
        assert crawler.unreachable_urls == {"http://a/1": "robots.txt is unreachable (HTTP 503)"}

    async def test_crawl_waits_for_the_robots_txt_of_the_host_a_page_redirects_to(
        self, make_crawler, fake_session, caplog
    ):
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.1
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=404)
        fake_session.routes["http://a/1"] = FakeResponse(status=302, location="http://b/1")
        fake_session.routes["http://b/robots.txt"] = [FakeResponse(status=503), FakeResponse(status=404)]
        fake_session.routes["http://b/1"] = FakeResponse(b"ok")

        await crawler.crawl(["http://a/1"], max_pages=1)

        # The page is requested again once robots.txt of the target is back.
        assert fake_session.requested == [
            "http://a/robots.txt",
            "http://a/1",
            "http://b/robots.txt",
            "http://a/1",
            "http://b/robots.txt",
            "http://b/1",
        ]
        assert set(crawler.processed_urls) == {"http://a/1"}
        assert crawler.unreachable_urls == {}
        assert crawler.crawl_stats().queued == 0
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred ")]
        assert deferred == ["Deferred http://a/1 for 0.1s: robots.txt is unreachable (HTTP 503)"]

    async def test_crawl_gives_up_on_a_page_redirecting_to_a_site_whose_robots_txt_stays_unreachable(
        self, make_crawler, fake_session, caplog
    ):
        caplog.set_level(logging.INFO, logger="crawler")
        crawler = make_crawler(respect_robots=True, max_depth=0)
        crawler.robots.UNREACHABLE_TTL = 0.05
        crawler.ROBOTS_POLL = 0.01
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=404)
        fake_session.routes["http://a/1"] = FakeResponse(status=302, location="http://b/1")
        fake_session.routes["http://b/robots.txt"] = FakeResponse(status=503)

        await crawler.crawl(["http://a/1"])

        reason = "redirects to http://b/1, robots.txt is unreachable (HTTP 503)"
        assert fake_session.requested.count("http://a/1") == 1 + AsyncCrawler.MAX_ROBOTS_RETRIES
        assert fake_session.requested.count("http://b/robots.txt") == 1 + AsyncCrawler.MAX_ROBOTS_RETRIES
        assert crawler.unreachable_urls == {"http://a/1": reason}
        assert crawler.crawl_stats().unreachable == 1
        assert f"Gave up on http://a/1: {reason}" in [r.getMessage() for r in caplog.records]

    async def test_a_robots_txt_downloaded_again_is_a_single_attempt(self, make_crawler, fake_session, caplog):
        # The first download goes through the retries like a page. The site
        # is then known to be unreachable, and each download after that is
        # one request: the retries with their growing timeouts would hold
        # the page that started it for minutes on a host that never answers.
        caplog.set_level(logging.INFO, logger="crawler.retry")
        crawler = make_crawler(
            respect_robots=True, max_depth=0, retry_strategy=RetryStrategy(max_retries=2, base_delay=0.001)
        )
        crawler.robots.UNREACHABLE_TTL = 0.05
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=503)

        await crawler.crawl(["http://a/1"])

        assert fake_session.requested == ["http://a/robots.txt"] * (3 + AsyncCrawler.MAX_ROBOTS_RETRIES)
        assert crawler.unreachable_urls == {"http://a/1": "robots.txt is unreachable (HTTP 503)"}
        single = [r.getMessage() for r in caplog.records if "a single attempt was asked for" in r.getMessage()]
        assert len(single) == AsyncCrawler.MAX_ROBOTS_RETRIES
        assert single[0].startswith("Failed http://a/robots.txt on attempt 1/3 after")

    async def test_pages_of_other_hosts_do_not_wait_for_a_robots_txt_downloaded_again(self, make_crawler, fake_session):
        # Two workers. The first download of robots.txt of "a" fails at
        # once; the next one hangs until the test lets it go, as a host
        # that accepts the connection and never answers would. The worker
        # that started it waits for it; the other one, taking the other
        # page of "a", gets the stale rules and goes on with the pages of
        # "b" found meanwhile, instead of waiting for the download too.
        crawler = make_crawler(respect_robots=True, max_concurrent=2, max_depth=1)
        crawler.robots.UNREACHABLE_TTL = 0  # due to be downloaded again at once
        crawler.ROBOTS_POLL = 0.01
        answered = asyncio.Event()

        async def hang() -> FakeResponse:
            await answered.wait()
            return FakeResponse(status=404)

        fake_session.routes["http://a/robots.txt"] = [FakeResponse(status=503), hang]
        fake_session.routes["http://b/robots.txt"] = FakeResponse(status=404)
        fake_session.routes["http://b/1"] = FakeResponse(b"<a href='/2'>2</a><a href='/3'>3</a>")

        crawl = asyncio.create_task(crawler.crawl(["http://a/1", "http://a/2", "http://b/1"]))
        async with asyncio.timeout(1):
            while "http://b/3" not in fake_session.requested:
                await asyncio.sleep(0.001)
        assert not crawl.done()
        answered.set()
        await crawl

        assert set(crawler.processed_urls) == {"http://a/1", "http://a/2", "http://b/1", "http://b/2", "http://b/3"}
        assert crawler.unreachable_urls == {}
        assert fake_session.requested.count("http://a/robots.txt") == 2

    async def test_a_site_given_up_on_is_not_downloaded_again_before_the_next_crawl(self, make_crawler, fake_session):
        # Once the site is given up, a page that looks in on it later does
        # not download robots.txt once more; the next crawl does.
        crawler = make_crawler(respect_robots=True, max_concurrent=1, max_depth=1)
        crawler.robots.UNREACHABLE_TTL = 0.01
        crawler.ROBOTS_POLL = 0.01
        fake_session.routes["http://a/robots.txt"] = FakeResponse(status=503)
        fake_session.routes["http://b/robots.txt"] = FakeResponse(status=404)
        fake_session.routes["http://b/1"] = FakeResponse(b"<a href='http://a/2'>2</a>")

        await crawler.crawl(["http://a/1", "http://b/1"])
        downloads = fake_session.requested.count("http://a/robots.txt")
        await crawler.crawl(["http://a/3"])

        assert downloads == 1 + AsyncCrawler.MAX_ROBOTS_RETRIES
        assert fake_session.requested.count("http://a/robots.txt") == 2 * downloads
        assert set(crawler.unreachable_urls) == {"http://a/3"}

    async def test_pages_wait_only_so_long_for_the_first_download_of_a_robots_txt(self, make_crawler, fake_session):
        # The first download of robots.txt of "a" hangs, as a host that
        # accepts the connection and never answers would. Neither worker
        # stands still with it: the pages of "a" are put off and the pages
        # of "b" found meanwhile are crawled, then the download ends.
        crawler = make_crawler(respect_robots=True, max_concurrent=2, max_depth=1)
        crawler.ROBOTS_POLL = 0.01
        answered = asyncio.Event()

        async def hang() -> FakeResponse:
            await answered.wait()
            return FakeResponse(status=404)

        fake_session.routes["http://a/robots.txt"] = hang
        fake_session.routes["http://b/robots.txt"] = FakeResponse(status=404)
        fake_session.routes["http://b/1"] = FakeResponse(b"<a href='/2'>2</a><a href='/3'>3</a>")

        crawl = asyncio.create_task(crawler.crawl(["http://a/1", "http://a/2", "http://b/1"]))
        async with asyncio.timeout(1):
            while "http://b/3" not in fake_session.requested:
                await asyncio.sleep(0.001)
        assert not crawl.done()
        answered.set()
        await crawl

        assert set(crawler.processed_urls) == {"http://a/1", "http://a/2", "http://b/1", "http://b/2", "http://b/3"}
        assert crawler.unreachable_urls == {}
        assert fake_session.requested.count("http://a/robots.txt") == 1


class TestProxies:
    """Requests through a proxy: its URL and password, the errors of proxies, the outcomes the pool is told."""

    PROXY = "http://proxy:3128"
    AUTHORIZATION = "Basic dXNlcjpzM2NyM3Q="  # user:s3cr3t

    @pytest.fixture
    def make_proxied(self, make_crawler):
        def make(*urls: str, **options) -> AsyncCrawler:
            return make_crawler(proxies=ProxyPool(urls or ["http://user:s3cr3t@proxy:3128"], max_failures=1), **options)

        return make

    async def test_the_password_goes_to_the_proxy_of_an_http_url_in_the_request(self, make_proxied, fake_session):
        await make_proxied().fetch_url("http://a/")
        assert fake_session.proxies == [(self.PROXY, None)]
        assert fake_session.headers[0]["Proxy-Authorization"] == self.AUTHORIZATION

    async def test_the_password_goes_to_the_proxy_of_an_https_url_with_connect(self, make_proxied, fake_session):
        # The request itself goes to the site, through the tunnel: it must not carry the password.
        await make_proxied().fetch_url("https://a/")
        assert fake_session.proxies == [(self.PROXY, {"Proxy-Authorization": self.AUTHORIZATION})]
        assert "Proxy-Authorization" not in fake_session.headers[0]

    async def test_a_proxy_without_a_password_sends_no_header(self, make_proxied, fake_session):
        await make_proxied("http://proxy:3128").fetch_url("http://a/")
        assert fake_session.proxies == [(self.PROXY, None)]
        assert fake_session.headers == [{}]

    @pytest.mark.parametrize(
        ("outcome", "url"),
        [
            (
                aiohttp.ClientProxyConnectionError(MagicMock(), ConnectionRefusedError("connection refused")),
                "https://a/",
            ),
            # Only the proxy is looked up by the client.
            (
                aiohttp.ClientConnectorDNSError(MagicMock(), socket.gaierror(socket.EAI_NONAME, "not known")),
                "https://a/",
            ),
            (
                aiohttp.ClientHttpProxyError(MagicMock(), (), status=407, message="Proxy Authentication Required"),
                "https://a/",
            ),
            # An http URL is requested from the proxy itself.
            (FakeResponse(status=407), "http://a/"),
        ],
        ids=["refused", "dns", "connect-407", "407"],
    )
    async def test_failures_of_the_proxy(self, make_proxied, fake_session, outcome, url):
        crawler = make_proxied()
        fake_session.routes[url] = outcome
        result = await crawler.fetch_result(url)

        assert isinstance(result.error, ProxyNetworkError)
        assert result.error.message.startswith("proxy http://user:***@proxy:3128")
        assert crawler.proxy_stats()["http://user:***@proxy:3128"] == ProxyStats(
            state="out", requests=1, failures=1, times_removed=1
        )

    @pytest.mark.parametrize(
        ("outcome", "kind"),
        [
            # The proxy cannot reach the site, or may not.
            (aiohttp.ClientHttpProxyError(MagicMock(), (), status=502, message="Bad Gateway"), NetworkError),
            (aiohttp.ClientHttpProxyError(MagicMock(), (), status=403, message="Forbidden"), NetworkError),
            (TimeoutError(), FetchTimeoutError),
            (aiohttp.ServerDisconnectedError(), NetworkError),
            # Inside the tunnel of an https URL the site answers, not the proxy.
            (FakeResponse(status=407), PermanentHTTPError),
            (
                aiohttp.ClientConnectorCertificateError(
                    MagicMock(host="a", port=443), ssl.SSLCertVerificationError("certificate has expired")
                ),
                CertificateError,
            ),
        ],
        ids=["connect-502", "connect-403", "timeout", "disconnected", "site-407", "site-certificate"],
    )
    async def test_failures_of_the_site_count_neither_way(self, make_proxied, fake_session, outcome, kind):
        crawler = make_proxied()
        fake_session.routes["https://a/"] = outcome
        result = await crawler.fetch_result("https://a/")

        assert type(result.error) is kind
        assert crawler.proxy_stats()["http://user:***@proxy:3128"] == ProxyStats(state="active", requests=1)

    @pytest.mark.parametrize(
        "outcome",
        [
            aiohttp.ClientConnectorCertificateError(
                MagicMock(host="proxy", port=3129), ssl.SSLCertVerificationError("self-signed certificate")
            ),
            aiohttp.ClientConnectorSSLError(MagicMock(host="proxy", port=3129), ssl.SSLError("wrong version number")),
        ],
        ids=["certificate", "tls"],
    )
    async def test_tls_failures_with_an_https_proxy_are_its_own(self, make_proxied, fake_session, outcome):
        # aiohttp names the connection that failed: here that to the proxy, not to the site in the tunnel.
        crawler = make_proxied("https://proxy:3129")
        fake_session.routes["https://a/"] = outcome
        result = await crawler.fetch_result("https://a/")

        assert isinstance(result.error, ProxyNetworkError)
        assert crawler.proxy_stats()["https://proxy:3129"].failures == 1

    @pytest.mark.parametrize(
        "outcome",
        [FakeResponse(), FakeResponse(status=404), FakeResponse(status=503), FakeResponse(body=b"x" * 100)],
        ids=["200", "404", "503", "too-large"],
    )
    async def test_any_response_is_a_success_of_the_proxy(self, make_proxied, fake_session, outcome):
        crawler = make_proxied(max_page_size=10)
        fake_session.routes["http://a/"] = [
            aiohttp.ClientProxyConnectionError(MagicMock(), ConnectionRefusedError("connection refused")),
            outcome,
        ]
        pool = crawler.proxies
        pool.max_failures = 2
        await crawler.fetch_result("http://a/")
        await crawler.fetch_result("http://a/")
        await crawler.fetch_result("http://a/")  # would take it out, after two failures in a row

        assert crawler.proxy_stats()["http://user:***@proxy:3128"].state == "active"

    async def test_a_robots_txt_downloaded_again_passes_over_a_proxy_that_failed(self, make_proxied, fake_session):
        # A download after a failed one is a single attempt at the site, and
        # the failure of a proxy is not one: the page would fail for it.
        crawler = make_proxied(
            "http://proxy-1:3128",
            "http://proxy-2:3128",
            respect_robots=True,
            max_depth=0,
            retry_strategy=RetryStrategy(max_retries=1, base_delay=0.001),
        )
        crawler.robots.UNREACHABLE_TTL = 0.05
        crawler.ROBOTS_POLL = 0.01
        fake_session.routes["http://a/robots.txt"] = [
            *[FakeResponse(status=503)] * 2,  # the first download, with its retry
            aiohttp.ClientProxyConnectionError(MagicMock(), ConnectionRefusedError("connection refused")),
            FakeResponse(status=404),
        ]

        pages = await crawler.crawl(["http://a/1"])

        assert list(pages) == ["http://a/1"]
        assert fake_session.requested == [*["http://a/robots.txt"] * 4, "http://a/1"]

    async def test_no_request_is_sent_without_a_proxy(self, make_proxied, fake_session):
        crawler = make_proxied(retry_strategy=RetryStrategy(max_retries=3, base_delay=0.01))
        fake_session.routes["http://a/"] = aiohttp.ClientProxyConnectionError(
            MagicMock(), ConnectionRefusedError("connection refused")
        )
        result = await crawler.fetch_result("http://a/")

        assert isinstance(result.error, NoProxyError)  # the retry found no proxy, and it is not retried
        assert fake_session.requested == ["http://a/"]
        assert crawler.error_stats().retries == 1

    async def test_a_crawl_counts_the_proxies_anew(self, make_proxied, fake_session):
        crawler = make_proxied("http://proxy-1:3128", "http://proxy-2:3128")
        fake_session.routes["http://a/"] = aiohttp.ClientProxyConnectionError(
            MagicMock(), ConnectionRefusedError("connection refused")
        )
        await crawler.fetch_result("http://a/")
        assert sum(stats.requests for stats in crawler.proxy_stats().values()) == 1

        await crawler.crawl(["http://b/"], max_pages=1)

        stats = crawler.proxy_stats()
        assert sum(each.requests for each in stats.values()) == 1  # the page of the crawl alone
        assert [each.state for each in stats.values()].count("out") == 1  # the proxy taken out stays out

    async def test_without_proxies_the_stats_are_empty(self, crawler):
        assert crawler.proxies is None
        assert crawler.proxy_stats() == {}
