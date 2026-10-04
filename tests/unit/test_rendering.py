"""Unit tests for rendering without a browser: its settings, which pages go to the browser, its errors."""

import sys

import aiohttp
import pytest
from test_transport_contract import ScriptedTransport, make_fetcher, page

from crawler import AsyncCrawler, CrawlerClosedError, PageTooLargeError, RenderError, Rendering
from crawler.rendering import BrowserTransport, Rendered, Renderer
from crawler.transport import HttpTransport, Response, Transport

URL = "https://a.test/page"
TIMEOUT = aiohttp.ClientTimeout(total=5, connect=2, sock_read=3)
DEFAULT = Rendering()


class FakeRenderer:
    """Renders a page by what it is told, and remembers the pages it got."""

    def __init__(self, rendered: Rendered | Exception | None = None) -> None:
        self.rendered = rendered
        self.pages: list[tuple[str, Response]] = []
        self.closed = False

    async def render(self, url: str, document: Response) -> Rendered:
        self.pages.append((url, document))
        if isinstance(self.rendered, Exception):
            raise self.rendered
        return self.rendered or Rendered(f"<p>rendered {document.content}</p>")

    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("the browser would not close")


def make_transport(
    script: dict[str, Response | Exception],
    rendering: Rendering = DEFAULT,
    renderer: FakeRenderer | None = None,
    max_page_size: int | None = None,
) -> tuple[BrowserTransport, ScriptedTransport, FakeRenderer]:
    http = ScriptedTransport(script)
    transport = BrowserTransport(http, rendering, user_agent="TestBot/1.0", max_page_size=max_page_size)
    transport.renderer = renderer or FakeRenderer()  # type: ignore[assignment]
    return transport, http, transport.renderer  # type: ignore[return-value]


async def get(transport: Transport, url: str = URL, **options) -> Response:
    request = {"html_only": True, "raw_limit": None, "truncate_at": None, "timeout": TIMEOUT} | options
    return await transport.get(url, **request)


class TestRenderingSettings:
    def test_defaults(self) -> None:
        rendering = Rendering()
        assert (rendering.wait_until, rendering.wait_for, rendering.timeout, rendering.max_open_pages) == (
            "load",
            None,
            30.0,
            2,
        )
        assert rendering.block_resources == {"image", "font", "media"}
        assert rendering.renders("https://a.test/anything")

    def test_patterns_name_the_pages_to_render(self) -> None:
        rendering = Rendering(include=[r"/app/", r"\?view=js"])
        assert rendering.renders("https://a.test/app/list")
        assert rendering.renders("https://a.test/page?view=js")
        assert not rendering.renders("https://a.test/blog/")

    @pytest.mark.parametrize(
        ("options", "message"),
        [
            ({"wait_until": "idle"}, "wait_until must be one of load, domcontentloaded, networkidle"),
            ({"wait_for": " "}, "wait_for must be a CSS selector"),
            ({"timeout": 0}, "timeout must be positive"),
            ({"timeout": float("nan")}, "timeout must be positive"),
            ({"max_open_pages": 0}, "max_open_pages must be >= 1"),
            ({"block_resources": ["image", "document"]}, "unknown resource types 'document'"),
            ({"include": ["("]}, r"invalid pattern '\('"),
        ],
    )
    def test_invalid_settings(self, options, message) -> None:
        with pytest.raises(ValueError, match=message):
            Rendering(**options)

    @pytest.mark.parametrize("name", ["include", "block_resources"])
    def test_a_string_is_not_a_list(self, name) -> None:
        with pytest.raises(TypeError, match=f"expected a list for {name}"):
            Rendering(**{name: "image"})

    def test_the_crawler_renders_with_the_settings_given(self) -> None:
        rendering = Rendering(include=["/app/"])
        crawler = AsyncCrawler(rendering=rendering)
        assert crawler.rendering is rendering
        assert isinstance(crawler._transport, BrowserTransport)
        assert isinstance(AsyncCrawler()._transport, HttpTransport)


class TestBrowserTransport:
    async def test_an_html_page_is_rendered(self) -> None:
        transport, http, renderer = make_transport({URL: page(URL, "page")})
        response = await get(transport)

        assert response.content == "<p>rendered page</p>"
        assert response.size == len("<p>rendered page</p>")
        assert (response.status, response.final_url, response.content_type) == (200, URL, "text/html")
        assert renderer.pages == [(URL, page(URL, "page"))]
        assert http.requests == [URL]

    async def test_a_page_is_measured_in_bytes_once_rendered(self) -> None:
        transport, _, _ = make_transport({URL: page(URL, "page")}, renderer=FakeRenderer(Rendered("é" * 10)))
        assert (await get(transport)).size == 20

    @pytest.mark.parametrize(
        ("answer", "options"),
        [
            (Response(status=301, content="", size=0, final_url="/new", content_type=None, redirected=True), {}),
            (Response(status=200, content="{}", size=2, final_url=URL, content_type="application/json"), {}),
            (page(URL, "robots"), {"truncate_at": 1000, "html_only": False}),
            (
                Response(status=200, content="", size=5, final_url=URL, content_type="text/xml", body=b"<x/>"),
                {"raw_limit": 1000, "html_only": False},
            ),
        ],
        ids=["redirect", "not-html", "robots.txt", "sitemap"],
    )
    async def test_what_is_not_a_page_is_not_rendered(self, answer, options) -> None:
        transport, _, renderer = make_transport({URL: answer})
        assert await get(transport, **options) is answer
        assert renderer.pages == []

    async def test_a_page_the_patterns_do_not_name_is_not_rendered(self) -> None:
        other = "https://a.test/blog/"
        transport, _, renderer = make_transport(
            {URL: page(URL, "page"), other: page(other, "blog")}, Rendering(include=["/page$"])
        )
        assert (await get(transport, other)).content == "blog"
        assert (await get(transport, URL)).content == "<p>rendered page</p>"
        assert [url for url, _ in renderer.pages] == [URL]

    async def test_a_page_that_goes_elsewhere_comes_back_as_a_redirect(self) -> None:
        target = "https://a.test/target"
        transport, _, _ = make_transport({URL: page(URL, "page")}, renderer=FakeRenderer(Rendered("", target)))
        response = await get(transport)

        assert response.redirected
        assert (response.status, response.final_url, response.content, response.size) == (200, target, "", 0)

    async def test_the_fetcher_follows_where_the_page_went(self) -> None:
        target = "https://a.test/target"
        transport, http, _ = make_transport(
            {URL: page(URL, "page"), target: page(target, "target")},
            Rendering(include=["/page$"]),
            renderer=FakeRenderer(Rendered("", target)),
        )
        result = await make_fetcher(transport).fetch(URL)

        assert http.requests == [URL, target]
        assert (result.error, result.redirected, result.final_url, result.content) == (None, True, target, "target")

    async def test_a_page_over_the_size_limit_once_rendered_fails(self) -> None:
        transport, _, _ = make_transport(
            {URL: page(URL, "small")}, renderer=FakeRenderer(Rendered("x" * 101)), max_page_size=100
        )
        with pytest.raises(PageTooLargeError, match="larger than 100 bytes once rendered"):
            await get(transport)

    async def test_a_failure_of_the_download_is_not_rendered(self) -> None:
        transport, _, renderer = make_transport({URL: PageTooLargeError(URL, "larger than 100 bytes")})
        with pytest.raises(PageTooLargeError):
            await get(transport)
        assert renderer.pages == []

    async def test_close_closes_the_session_even_when_the_browser_fails_to_close(self) -> None:
        transport, http, renderer = make_transport({})
        with pytest.raises(RuntimeError):
            await transport.close()
        assert renderer.closed
        assert http.closed

    def test_stats_and_cookies_are_those_of_the_session(self) -> None:
        transport, http, _ = make_transport({})
        transport.reset_stats()
        assert http.resets == 1
        assert transport.cookies() == []
        assert isinstance(transport, Transport)


class TestRenderErrors:
    async def test_without_playwright_pages_fail_with_the_command_to_install_it(self, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "playwright.async_api", None)
        renderer = Renderer(Rendering(), user_agent="TestBot/1.0")

        with pytest.raises(RenderError, match=r'Playwright is not installed; run: pip install -e "\.\[js\]"'):
            await renderer.render(URL, page(URL, "page"))
        with pytest.raises(RenderError, match="Playwright is not installed"):
            await renderer.render(URL, page(URL, "page"))
        assert renderer._launches == 1  # not tried again
        await renderer.close()

    async def test_a_closed_renderer_fails_with_crawler_closed(self) -> None:
        renderer = Renderer(Rendering(), user_agent="TestBot/1.0")
        await renderer.close()
        await renderer.close()
        with pytest.raises(CrawlerClosedError, match="crawler is closed"):
            await renderer.render(URL, page(URL, "page"))

    async def test_a_render_error_is_not_retried_and_not_blamed_on_the_site(self) -> None:
        transport, http, _ = make_transport(
            {URL: page(URL, "page")}, renderer=FakeRenderer(RenderError(URL, "crashed"))
        )
        fetcher = make_fetcher(transport)
        result = await fetcher.fetch(URL)

        assert isinstance(result.error, RenderError)
        assert http.requests == [URL]
        circuit = fetcher.circuit_breaker.get_stats()["a.test"]
        assert (circuit.requests, circuit.failures) == (0, 0)
