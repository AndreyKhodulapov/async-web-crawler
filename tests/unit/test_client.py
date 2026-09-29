"""Unit tests for AsyncCrawler with the HTTP session replaced by fakes."""

import asyncio
import codecs
from unittest.mock import MagicMock

import aiohttp
import pytest

from crawler import (
    AsyncCrawler,
    CrawlerClosedError,
    FetchResult,
    FetchTimeoutError,
    HTMLParser,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    UnexpectedError,
)
from crawler.client import _decode, _sniff_charset


class FakeResponse:
    def __init__(
        self,
        body: bytes = b"page",
        status: int = 200,
        encoding: str = "utf-8",
        content_type: str | None = "text/html",
        url: str | None = None,
    ) -> None:
        self.status = status
        self._body = body
        self._encoding = encoding
        self.headers = {} if content_type is None else {"Content-Type": content_type}
        self.content_type = content_type or "application/octet-stream"
        # None means "not redirected": FakeSession fills in the requested URL.
        self.url = url

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
def crawler(monkeypatch, fake_session) -> AsyncCrawler:
    crawler = AsyncCrawler(max_concurrent=3)
    monkeypatch.setattr(crawler, "_create_session", lambda: fake_session)
    return crawler


class TestInit:
    @pytest.mark.parametrize("value", [0, -1])
    def test_rejects_non_positive_concurrency(self, value):
        with pytest.raises(ValueError, match="max_concurrent"):
            AsyncCrawler(max_concurrent=value)

    @pytest.mark.parametrize("name", ["total_timeout", "connect_timeout", "read_timeout"])
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

    async def test_redirect_and_missing_content_type(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(content_type=None, url="https://a/home")
        [result] = await crawler.fetch_many(["http://a"])
        assert result.final_url == "https://a/home"
        assert result.content_type is None

    async def test_non_text_charset_falls_back_to_utf8(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(b"page", encoding="base64")
        assert await crawler.fetch_url("http://a") == "page"

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

    async def test_links_are_resolved_against_final_url(self, crawler, fake_session):
        html = b"<a href='next'>next</a>"
        fake_session.routes["http://site/docs"] = FakeResponse(html, url="https://site/docs/")
        page = await crawler.fetch_and_parse("http://site/docs")
        assert page["url"] == "http://site/docs"
        assert page["final_url"] == "https://site/docs/"
        assert page["links"] == ["https://site/docs/next"]

    async def test_non_html_is_not_parsed(self, crawler, fake_session, caplog):
        fake_session.routes["http://api"] = FakeResponse(b'{"a": "<b>"}', content_type="application/json")
        page = await crawler.fetch_and_parse("http://api")
        assert page["text"] == ""
        assert page["errors"] == ["unsupported content type: application/json"]
        assert "unsupported content type" in caplog.text

    async def test_missing_content_type_is_parsed_as_html(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(b"<h1>Hi</h1>", content_type=None)
        page = await crawler.fetch_and_parse("http://a")
        assert page["headings"] == [{"level": 1, "text": "Hi"}]

    async def test_fetch_error_is_raised(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(status=404)
        with pytest.raises(HTTPStatusError):
            await crawler.fetch_and_parse("http://a")

    async def test_uses_injected_parser(self, monkeypatch, fake_session):
        crawler = AsyncCrawler(parser=HTMLParser(same_host_only=True))
        monkeypatch.setattr(crawler, "_create_session", lambda: fake_session)
        fake_session.routes["http://a/"] = FakeResponse(b"<a href='/x'>in</a><a href='http://b/'>out</a>")
        page = await crawler.fetch_and_parse("http://a/")
        assert page["links"] == ["http://a/x"]


class TestSniffCharset:
    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            (b'<meta charset="windows-1252"><p>caf\xe9</p>', "windows-1252"),
            (b'<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-2">', "iso-8859-2"),
            (b'<meta charset="no-such-charset">', "utf-8"),
            # Found in ASCII bytes, so they cannot really be UTF-16/32 (HTML spec).
            (b'<meta charset="utf-16">', "utf-8"),
            (b'<meta charset="utf-32">', "utf-8"),
            # Codecs that exist but cannot decode a page.
            (b'<meta charset="undefined">', "utf-8"),
            (b'<meta charset="idna">', "utf-8"),
            (b'<meta charset="punycode">', "utf-8"),
            (b'<meta charset="base64">', "utf-8"),
            (b'<meta charset="shift_jis">', "shift_jis"),
            (b"<p>no declaration</p>", "utf-8"),
            (b"\x89PNG\r\n", "utf-8"),
        ],
    )
    def test_declared_charset(self, body, expected):
        assert _sniff_charset(MagicMock(), body) == expected


class TestDecode:
    @pytest.mark.parametrize(
        ("body", "declared"),
        [
            (codecs.BOM_UTF8 + "café".encode(), "windows-1252"),
            (codecs.BOM_UTF16_LE + "café".encode("utf-16-le"), "utf-8"),
            (codecs.BOM_UTF16_BE + "café".encode("utf-16-be"), "utf-8"),
        ],
    )
    def test_byte_order_mark_wins_and_is_stripped(self, body, declared):
        assert _decode(body, declared) == "café"

    @pytest.mark.parametrize("encoding", ["undefined", "idna", "base64", "no-such-charset"])
    def test_unusable_charset_falls_back_to_utf8(self, encoding):
        assert _decode("café".encode(), encoding) == "café"

    async def test_undefined_header_charset_is_not_an_invalid_url(self, crawler, fake_session):
        fake_session.routes["http://a"] = FakeResponse(b"page", encoding="undefined")
        assert await crawler.fetch_url("http://a") == "page"
