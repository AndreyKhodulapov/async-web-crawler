"""Unit tests for rendering without a browser: its settings, which pages go to the browser, its errors."""

import sys
import time

import aiohttp
import pytest
from test_transport_contract import ScriptedTransport, make_fetcher, page

from crawler import (
    AsyncCrawler,
    CrawlerClosedError,
    PageTooLargeError,
    ProxyPool,
    RenderError,
    Rendering,
    make_cookie,
)
from crawler.proxy import Proxy
from crawler.rendering import (
    BrowserTransport,
    CookieSync,
    Rendered,
    Renderer,
    browser_problem,
    bypass_rules,
    from_playwright,
    playwright_problem,
    to_playwright,
)
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

    async def test_a_page_keeps_the_proxy_of_its_download(self) -> None:
        proxy = Proxy.from_url("http://proxy.test:3128")
        transport, _, renderer = make_transport({URL: page(URL, "page")._replace(proxy=proxy)})
        rendered = await get(transport)
        renderer.rendered = Rendered("", "https://a.test/elsewhere")
        redirect = await get(transport)

        assert renderer.pages[0][1].proxy == proxy
        assert rendered.proxy == redirect.proxy == proxy

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

    def test_cookies_are_updated_in_the_session(self) -> None:
        transport, http, _ = make_transport({})
        cookie, gone = make_cookie("sid", "1", "a.test"), make_cookie("old", "", "a.test")

        transport.update_cookies([cookie], [gone])

        assert http.updates == [([cookie], [gone])]

    def test_the_browser_shares_the_cookies_and_headers_of_the_crawler(self) -> None:
        http = ScriptedTransport({})
        shared = BrowserTransport(http, DEFAULT, user_agent="TestBot/1.0", max_page_size=None, headers={"X-Key": "1"})
        alone = BrowserTransport(http, DEFAULT, user_agent="TestBot/1.0", max_page_size=None, keep_cookies=False)

        assert (shared.renderer._cookies, shared.renderer._headers) == (http, {"X-Key": "1"})
        assert (alone.renderer._cookies, alone.renderer._headers) == (None, {})

    def test_the_browser_takes_no_proxy_of_the_environment(self, monkeypatch) -> None:
        for name in ["http_proxy", "https_proxy", "no_proxy", "REQUEST_METHOD"]:
            monkeypatch.delenv(name, raising=False)
            monkeypatch.delenv(name.upper(), raising=False)
        monkeypatch.setenv("HTTP_PROXY", "http://proxy.test:3128")
        monkeypatch.setenv("NO_PROXY", "a.test")

        from_env = AsyncCrawler(rendering=DEFAULT, proxies=ProxyPool.from_env())
        listed = AsyncCrawler(rendering=DEFAULT, proxies=ProxyPool(["http://proxy.test:3128"]))

        assert from_env._transport.renderer._bypass == "<-loopback>, a.test, *.a.test"
        assert listed._transport.renderer._bypass is None


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


class TestInstallation:
    def test_playwright_is_found_without_being_imported(self, monkeypatch) -> None:
        pytest.importorskip("playwright")
        for name in [name for name in sys.modules if name == "playwright" or name.startswith("playwright.")]:
            monkeypatch.delitem(sys.modules, name)

        assert playwright_problem() is None
        assert "playwright" not in sys.modules

    async def test_without_playwright_the_command_to_install_it(self, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "playwright", None)  # find_spec() takes it for a missing package

        assert playwright_problem() == 'Playwright is not installed; run: pip install -e ".[js]"'
        assert await browser_problem() == playwright_problem()

    async def test_without_chromium_the_command_to_install_it(self, monkeypatch, tmp_path) -> None:
        pytest.importorskip("playwright")
        monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", str(tmp_path))  # where Playwright looks for its browsers

        assert await browser_problem() == "Chromium is not installed; run: playwright install chromium"


def names(cookies) -> list[tuple[str, str, str]]:
    return sorted((cookie.domain, cookie.name, cookie.value) for cookie in cookies)


SID = make_cookie("sid", "1", "a.test")
LANG = make_cookie("lang", "en", ".a.test")


class TestCookieSync:
    def test_the_jar_goes_to_the_browser_once(self) -> None:
        sync = CookieSync()

        assert sync.to_browser([SID, LANG]) == ([SID, LANG], [])
        assert sync.to_browser([SID, LANG]) == ([], [])

    def test_only_what_the_jar_changed_goes_to_the_browser(self) -> None:
        sync = CookieSync()
        sync.to_browser([SID, LANG])
        changed = make_cookie("sid", "2", "a.test")

        assert sync.to_browser([changed]) == ([changed], [LANG])

    def test_what_the_browser_changed_goes_to_the_jar_and_not_back(self) -> None:
        sync = CookieSync()
        sync.sent(sync.to_browser([SID])[0], [SID])
        made = make_cookie("consent", "yes", "a.test")
        changed = make_cookie("sid", "2", "a.test")

        assert names(sync.to_jar([changed, made])[0]) == names([changed, made])
        sync.written([changed, made], [changed, made])
        assert sync.to_browser([changed, made]) == ([], [])

    def test_a_cookie_the_site_deleted_is_removed_from_the_jar(self) -> None:
        sync = CookieSync()
        sync.sent(sync.to_browser([SID, LANG])[0], [SID, LANG])

        assert sync.to_jar([LANG]) == ([], [SID])

    def test_a_cookie_the_browser_refused_is_not_taken_for_deleted(self) -> None:
        sync = CookieSync()
        sync.sent(sync.to_browser([SID, LANG])[0], [LANG])  # Chromium did not keep sid

        assert sync.to_jar([LANG]) == ([], [])
        assert sync.to_browser([SID, LANG]) == ([], [])  # nor sent again and again

    def test_a_page_opened_meanwhile_does_not_overwrite_what_another_page_set(self) -> None:
        sync = CookieSync()
        sync.sent(sync.to_browser([SID])[0], [SID])
        # The script of page A sets a cookie; the crawler gets one for page B meanwhile.
        by_script = make_cookie("sid", "from-script", "a.test")
        by_crawler = make_cookie("token", "t", "a.test")

        changed, removed = sync.to_browser([SID, by_crawler])  # page B opens
        assert (changed, removed) == ([by_crawler], [])
        sync.sent(changed, [by_script, by_crawler])

        # Page A ends: its cookie goes to the jar, that of the crawler does not come back.
        assert sync.to_jar([by_script, by_crawler]) == ([by_script], [])

    def test_a_cookie_both_changed_is_that_of_the_browser(self) -> None:
        sync = CookieSync()
        sync.sent(sync.to_browser([SID])[0], [SID])
        in_browser = make_cookie("sid", "browser", "a.test")

        # The crawler got sid=jar meanwhile: the jar takes that of the browser in its place.
        assert sync.to_jar([in_browser]) == ([in_browser], [])
        sync.written([in_browser], [in_browser])
        assert sync.to_browser([in_browser]) == ([], [])

    def test_how_the_jar_keeps_a_cookie_is_not_a_change(self) -> None:
        sync = CookieSync()
        sync.sent(sync.to_browser([])[0], [])
        given = make_cookie("sid", "1", "a.test", expires=int(time.time()) + 10**9)
        kept = make_cookie("sid", "1", "a.test", expires=int(time.time()) + 3600)

        sync.written(sync.to_jar([given])[0], [kept])

        assert sync.to_browser([kept]) == ([], [])

    @pytest.mark.parametrize(
        "cookie",
        [
            make_cookie("sid", "1", "127.0.0.1"),  # the crawler keeps no cookies of IP addresses
            make_cookie("note", "two words", "a.test"),  # nor sends a value with a space
            make_cookie("bad name", "1", "a.test"),
        ],
    )
    def test_cookies_the_crawler_cannot_send_stay_in_the_browser(self, cookie) -> None:
        sync = CookieSync()
        sync.sent(sync.to_browser([])[0], [])

        assert sync.to_jar([cookie]) == ([], [])
        assert sync.to_browser([]) == ([], [])  # and are not taken away from it


class TestPlaywrightCookies:
    def test_a_cookie_goes_to_playwright_with_its_scope(self) -> None:
        later = int(time.time()) + 3600
        assert to_playwright(make_cookie("sid", "1", "a.test", secure=True, http_only=True, expires=later)) == {
            "name": "sid",
            "value": "1",
            "domain": "a.test",
            "path": "/",
            "secure": True,
            "httpOnly": True,
            "expires": later,
        }
        assert "expires" not in to_playwright(LANG)  # a session cookie

    def test_a_cookie_of_playwright_comes_back_as_it_was(self) -> None:
        later = int(time.time()) + 3600
        for cookie in (make_cookie("sid", "1", "a.test", secure=True, http_only=True, expires=later), LANG):
            back = from_playwright(to_playwright(cookie) | {"sameSite": "Lax"})
            assert (back.domain, back.path, back.name, back.value, back.secure, back.expires) == (
                cookie.domain,
                cookie.path,
                cookie.name,
                cookie.value,
                cookie.secure,
                cookie.expires,
            )
            assert back.has_nonstandard_attr("HttpOnly") == cookie.has_nonstandard_attr("HttpOnly")

    def test_the_expiry_of_playwright_is_in_whole_seconds_and_minus_one_for_the_session(self) -> None:
        cookie = {"name": "sid", "value": "1", "domain": "a.test", "path": "/", "secure": False, "httpOnly": False}

        assert from_playwright(cookie | {"expires": 1791140938.75}).expires == 1791140938
        assert from_playwright(cookie | {"expires": -1}).expires is None


class TestBypassRules:
    @pytest.mark.parametrize(
        ("no_proxy", "rules"),
        [
            (None, None),
            ("", None),
            (" , ", None),
            ("a.test", "<-loopback>, a.test, *.a.test"),
            (".A.test, b.test:8080", "<-loopback>, a.test, *.a.test, b.test:8080, *.b.test:8080"),
            ("localhost", "<-loopback>, localhost, *.localhost"),
            # The crawler does not read a range of addresses: it sends those hosts through the proxy.
            ("10.0.0.0/8, a.test", "<-loopback>, a.test, *.a.test"),
            ("10.0.0.0/8", None),
        ],
    )
    def test_no_proxy_as_rules_of_the_browser(self, no_proxy, rules) -> None:
        assert bypass_rules(no_proxy) == rules


class FakeContext:
    closed = False

    async def route(self, pattern, handler) -> None:
        pass

    async def close(self) -> None:
        self.closed = True


class FakeBrowser:
    """A running browser that makes contexts and remembers how."""

    def __init__(self) -> None:
        self.settings: list[dict] = []

    def is_connected(self) -> bool:
        return True

    async def new_context(self, **settings) -> FakeContext:
        self.settings.append(settings)
        return FakeContext()


class TestContextsOfProxies:
    FIRST = Proxy.from_url("http://user:p%40ss@first.test:3128")
    SECOND = Proxy.from_url("http://second.test:3128")

    def make_renderer(self, **options) -> tuple[Renderer, FakeBrowser]:
        renderer = Renderer(DEFAULT, user_agent="TestBot/1.0", **options)
        browser = renderer._browser = FakeBrowser()  # type: ignore[assignment]
        return renderer, browser

    async def test_a_context_goes_through_its_proxy_with_its_password(self) -> None:
        renderer, browser = self.make_renderer(no_proxy="a.test")
        await renderer._new_context(browser, self.FIRST)  # type: ignore[arg-type]
        await renderer._new_context(browser, self.SECOND)  # type: ignore[arg-type]
        await renderer._new_context(browser, None)  # type: ignore[arg-type]

        bypass = "<-loopback>, a.test, *.a.test"
        assert [settings.get("proxy") for settings in browser.settings] == [
            {"server": "http://first.test:3128", "username": "user", "password": "p@ss", "bypass": bypass},
            {"server": "http://second.test:3128", "bypass": bypass},
            None,
        ]

    async def test_the_pages_of_a_proxy_share_its_context(self) -> None:
        renderer, browser = self.make_renderer(cookies=ScriptedTransport({}))
        first = await renderer._get_context(URL, self.FIRST)
        second = await renderer._get_context(URL, self.SECOND)
        direct = await renderer._get_context(URL, None)

        assert await renderer._get_context(URL, Proxy.from_url("http://user:p%40ss@first.test:3128")) == first
        assert await renderer._get_context(URL, None) == direct
        assert len({id(first[0]), id(second[0]), id(direct[0])}) == 3
        assert first[1] is not second[1]
        assert len(browser.settings) == 3

    async def test_without_cookies_each_page_has_a_context_through_its_proxy(self) -> None:
        renderer, browser = self.make_renderer()
        context, sync = await renderer._get_context(URL, self.SECOND)
        again, _ = await renderer._get_context(URL, self.SECOND)

        assert sync is None and context is not again
        assert [settings["proxy"]["server"] for settings in browser.settings] == ["http://second.test:3128"] * 2

    async def test_close_closes_every_context(self) -> None:
        renderer, _ = self.make_renderer(cookies=ScriptedTransport({}))
        contexts = [(await renderer._get_context(URL, proxy))[0] for proxy in (self.FIRST, None)]
        renderer._browser = None  # nothing to close in the fake one

        await renderer.close()

        assert all(context.closed for context in contexts)
        assert renderer._contexts == {}
