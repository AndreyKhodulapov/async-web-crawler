"""Integration tests: pages are rendered in a headless Chromium, and what they do on their own is checked."""

import asyncio
import base64
import json
import logging
import os
import re
import subprocess

import pytest
from helpers import BOT, FAST_CONFIG, UNTHROTTLED

import main
from crawler import (
    AdvancedCrawler,
    AsyncCrawler,
    CircuitBreaker,
    CircuitState,
    CrawlerConfig,
    FetchTimeoutError,
    PageTooLargeError,
    ProxyPool,
    ProxyStats,
    RenderError,
    Rendering,
    RenderStats,
    RobotsDisallowedError,
    load_cookies_file,
    make_cookie,
)
from crawler.rendering import browser_problem

pytestmark = [pytest.mark.browser, pytest.mark.usefixtures("restore_logging", "chromium")]

PRIVATE = "User-agent: *\nDisallow: /js/private/\n"
DEFAULT = Rendering()


def make_crawler(rendering: Rendering | None = DEFAULT, **options) -> AsyncCrawler:
    return AsyncCrawler(**UNTHROTTLED | {"user_agent": BOT} | options, rendering=rendering)


def child_processes() -> set[int]:
    output = subprocess.run(["pgrep", "-P", str(os.getpid())], capture_output=True, text=True, check=False).stdout
    return {int(pid) for pid in output.split()}


async def test_links_and_text_made_by_javascript_are_found(url, site):
    async with make_crawler() as crawler:
        page = await crawler.fetch_and_parse(url("/js/links"))

    assert url("/js/target") in page["links"]
    assert "made by javascript" in page["text"]
    # The script was loaded, as the crawler; the image was not.
    assert site.hits["/js/app.js"] == 1
    assert site.headers["/js/app.js"]["User-Agent"] == BOT
    assert site.hits["/js/image.png"] == 0
    # The page itself was downloaded once, not again by the browser.
    assert site.hits["/js/links"] == 1


async def test_without_rendering_those_links_are_not_found(url):
    async with make_crawler(rendering=None) as crawler:
        page = await crawler.fetch_and_parse(url("/js/links"))

    assert url("/js/target") not in page["links"]


async def test_a_crawl_follows_links_only_javascript_shows(url):
    async with make_crawler(max_depth=2) as crawler:
        await crawler.crawl([url("/js/start")])

    assert {url("/js/target"), url("/js/other-target")} <= crawler.visited_urls
    stats = crawler.render_stats()
    assert (stats.rendered, stats.failed) == (len(crawler.visited_urls), 0)
    assert 0 < stats.avg_render_time < DEFAULT.timeout


async def test_a_crawl_counts_the_rendering_anew(url):
    async with make_crawler() as crawler:
        await crawler.crawl([url("/js/target")])
        await crawler.crawl([url("/data.json")])

        assert crawler.render_stats() == RenderStats()


async def test_patterns_render_only_the_pages_they_match(url):
    async with make_crawler(Rendering(include=[r"/js/links$"]), max_depth=2) as crawler:
        await crawler.crawl([url("/js/start")])

    assert url("/js/target") in crawler.visited_urls
    assert url("/js/other-target") not in crawler.visited_urls
    assert url("/js/other") in crawler.processed_urls


@pytest.mark.parametrize("path", ["/js/redirect", "/js/meta-refresh"])
async def test_a_page_that_goes_elsewhere_on_its_own_is_a_redirect(url, site, path):
    async with make_crawler() as crawler:
        result = await crawler.fetch_result(url(path))

    assert result.error is None
    assert result.redirected
    assert result.final_url == url("/js/target")
    assert "the target" in result.content
    # The crawler fetched the target as a redirect, not the browser.
    assert site.hits["/js/target"] == 1


async def test_a_javascript_redirect_to_a_url_robots_txt_disallows_is_refused(url, site):
    site.robots = PRIVATE
    async with make_crawler(respect_robots=True) as crawler:
        result = await crawler.fetch_result(url("/js/redirect-private"))
        await crawler.crawl([url("/js/redirect-private")])

    assert isinstance(result.error, RobotsDisallowedError)
    assert result.error.url == url("/js/private/page")
    assert url("/js/redirect-private") in crawler.blocked_urls
    assert site.hits["/js/private/page"] == 0


async def test_an_http_redirect_to_a_url_robots_txt_disallows_is_refused(url, site):
    site.robots = PRIVATE
    async with make_crawler(respect_robots=True) as crawler:
        result = await crawler.fetch_result(url("/js/to-private"))

    assert isinstance(result.error, RobotsDisallowedError)
    assert site.hits["/js/private/page"] == 0


async def test_a_javascript_redirect_the_filters_reject_is_skipped(url, site):
    async with make_crawler() as crawler:
        # A link: the redirect of a start URL may lead anywhere.
        await crawler.crawl([url("/js/to-redirect")], exclude_patterns=[r"/js/target$"])

    assert "/js/target" in crawler.skipped_urls[url("/js/redirect")]
    assert site.hits["/js/target"] == 0


async def test_frames_and_popups_load_nothing(url, site):
    async with make_crawler() as crawler:
        page = await crawler.fetch_url(url("/js/popup"))

    assert "popup" in page
    assert site.hits["/js/framed"] == 0
    assert site.hits["/js/opened"] == 0


async def test_the_page_is_waited_for_until_the_selector_appears(url):
    async with make_crawler(Rendering(wait_for="#late")) as crawler:
        page = await crawler.fetch_url(url("/js/late"))

    assert "late text" in page


async def test_a_page_rendered_too_slowly_times_out(url):
    async with make_crawler(Rendering(wait_for="#never", timeout=0.5)) as crawler:
        result = await crawler.fetch_result(url("/js/late"))

    assert isinstance(result.error, FetchTimeoutError)
    assert "rendering timeout (0.5s)" in result.error.message
    assert crawler.render_stats() == RenderStats(failed=1)


async def test_a_page_over_the_size_limit_once_rendered_fails(url):
    async with make_crawler(max_page_size=10_000) as crawler:
        result = await crawler.fetch_result(url("/js/big"))

    assert isinstance(result.error, PageTooLargeError)
    assert "once rendered" in result.error.message


async def test_robots_txt_and_other_types_are_not_rendered(url, site):
    site.robots = PRIVATE
    async with make_crawler(respect_robots=True) as crawler:
        data = await crawler.fetch_url(url("/data.json"))
        renderer = crawler._transport.renderer

        assert not renderer.running
    assert data.startswith("{")


async def test_the_browser_crashed_is_started_again_once(url, site):
    async with make_crawler(circuit_breaker=CircuitBreaker(0.5, min_requests=1)) as crawler:
        renderer = crawler._transport.renderer
        assert "Target" in await crawler.fetch_url(url("/js/target"))

        await renderer._browser.close()  # as if it crashed
        assert "Target" in await crawler.fetch_url(url("/js/target"))

        await renderer._browser.close()
        result = await crawler.fetch_result(url("/js/target"))
        again = await crawler.fetch_result(url("/js/links"))

    assert isinstance(result.error, RenderError)
    assert "crashed 2 times" in result.error.message
    assert isinstance(again.error, RenderError)
    assert (crawler.render_stats().rendered, crawler.render_stats().failed) == (2, 2)
    # Not retried, and the site is not to blame.
    assert site.hits["/js/target"] == 3
    breaker = crawler.circuit_breaker.get_stats()["127.0.0.1"]
    assert (breaker.state, breaker.failures) == ("closed", 0)


# aiohttp keeps no cookies of IP addresses, so the tests of cookies reach the site by its name.
HOST = "localhost"
# /js/cookie-set is done once its request has come back.
COOKIES_SET = Rendering(wait_until="networkidle")


async def test_cookies_of_the_crawler_and_of_the_page_itself_are_seen_by_javascript(url):
    async with make_crawler(cookies=[make_cookie("given", "1", HOST)]) as crawler:
        # The document sets doc=2 as it is downloaded by the crawler, not by the browser.
        page = await crawler.fetch_and_parse(url("/js/cookie-read?doc=2", HOST))

    assert "given=1" in page["text"]
    assert "doc=2" in page["text"]


async def test_cookies_javascript_and_its_requests_set_go_with_the_next_page(url):
    async with make_crawler(COOKIES_SET) as crawler:
        await crawler.fetch_url(url("/js/cookie-set", HOST))
        # The document of the next page is downloaded by the crawler.
        page = await crawler.fetch_url(url("/cookies/echo", HOST))
        cookies = {cookie.name: cookie.value for cookie in crawler.export_cookies()}

    assert "cookie:from_js=1" in page
    assert "cookie:from_fetch=2" in page
    assert cookies == {"from_js": "1", "from_fetch": "2"}


async def test_a_cookie_javascript_deletes_is_deleted_for_the_crawler(url):
    async with make_crawler(cookies=[make_cookie("sid", "1", HOST), make_cookie("lang", "en", HOST)]) as crawler:
        await crawler.fetch_url(url("/js/cookie-delete", HOST))

        assert [cookie.name for cookie in crawler.export_cookies()] == ["lang"]


async def test_without_keep_cookies_a_page_sees_no_cookie_of_another(url):
    async with make_crawler(COOKIES_SET, keep_cookies=False) as crawler:
        await crawler.fetch_url(url("/js/cookie-set", HOST))
        page = await crawler.fetch_and_parse(url("/js/cookie-read?doc=2", HOST))

        assert crawler.export_cookies() == []
    assert "seen:" in page["text"]
    assert "from_js" not in page["text"]
    assert "doc=2" not in page["text"]


async def test_headers_of_the_crawler_reach_the_requests_of_the_browser(url, site):
    async with make_crawler(headers={"X-Key": "1", "Accept-Language": "de"}) as crawler:
        await crawler.fetch_url(url("/js/links"))

    assert site.headers["/js/app.js"]["X-Key"] == "1"
    assert site.headers["/js/app.js"]["Accept-Language"] == "de"
    assert site.headers["/js/app.js"]["User-Agent"] == BOT


async def test_saved_cookies_hold_those_javascript_set(url, tmp_path):
    saved = tmp_path / "cookies.txt"
    config = CrawlerConfig.from_dict(
        {
            **FAST_CONFIG,
            "urls": [url("/js/cookie-set", HOST)],
            "rendering": {"mode": "always", "wait_for": "#done"},
            "session": {"save_cookies": str(saved)},
        }
    )

    async with AdvancedCrawler(config) as crawler:
        await crawler.crawl()

    assert sorted((cookie.domain, cookie.name, cookie.value) for cookie in load_cookies_file(saved)) == [
        (HOST, "from_fetch", "2"),
        (HOST, "from_js", "1"),
    ]


PASSWORD = "s3cr3t-pw"
AUTHORIZATION = "Basic " + base64.b64encode(f"crawler:{PASSWORD}".encode()).decode()


def requests_of_links(url, query: str = "") -> list[str]:
    """What a proxy sees of /js/links: the page, then its script; the image is blocked."""
    return [f"GET {url('/js/links' + query)}", f"GET {url('/js/app.js')}"]


async def test_the_requests_of_a_page_go_through_the_proxy_of_its_document(url, make_proxy):
    proxies = [await make_proxy(), await make_proxy()]
    async with make_crawler(proxies=ProxyPool([proxy.url for proxy in proxies])) as crawler:
        page = await crawler.fetch_and_parse(url("/js/links"))

    assert "made by javascript" in page["text"]
    # per_host: the host goes through one of them, its page and the script of the page alike.
    assert sorted([proxy.requests for proxy in proxies], key=len) == [[], requests_of_links(url)]


async def test_with_a_proxy_per_request_each_page_goes_through_its_own(url, make_proxy):
    proxies = [await make_proxy(), await make_proxy()]
    pool = ProxyPool([proxy.url for proxy in proxies], rotation="per_request")
    async with make_crawler(proxies=pool) as crawler:
        await crawler.fetch_url(url("/js/links"))
        await crawler.fetch_url(url("/js/links?second"))

    assert proxies[0].requests == requests_of_links(url)
    assert proxies[1].requests == requests_of_links(url, "?second")


async def test_the_browser_gives_the_password_of_the_proxy_and_the_log_does_not(url, site, make_proxy, caplog):
    proxy = await make_proxy(authorization=AUTHORIZATION)
    pool = ProxyPool([proxy.url.replace("http://", f"http://crawler:{PASSWORD}@")])
    with caplog.at_level(logging.DEBUG):
        async with make_crawler(proxies=pool) as crawler:
            page = await crawler.fetch_and_parse(url("/js/links"))

    assert "made by javascript" in page["text"]
    assert site.hits["/js/app.js"] == 1
    # The browser sends it once the proxy asks for it.
    assert proxy.requests[-1] == f"GET {url('/js/app.js')}"
    assert proxy.authorizations[-1] == AUTHORIZATION
    assert "crawler:***@127.0.0.1" in caplog.text
    assert PASSWORD not in caplog.text


async def test_a_request_of_the_browser_the_proxy_fails_fails_neither_the_proxy_nor_the_site(url, site, make_proxy):
    proxy = await make_proxy(drop="/js/app.js")
    breaker = CircuitBreaker(0.5, min_requests=1)
    pool = ProxyPool([proxy.url], max_failures=1)
    async with make_crawler(proxies=pool, circuit_breaker=breaker) as crawler:
        page = await crawler.fetch_and_parse(url("/js/links"))
        stats = crawler.proxy_stats()

    # The page is rendered without its script.
    assert "made by undefined" in page["text"]
    assert f"GET {url('/js/app.js')}" in proxy.requests
    assert site.hits["/js/app.js"] == 0
    # Only the requests of the crawler count.
    assert stats == {proxy.url: ProxyStats(state="active", requests=1)}
    assert breaker.state("127.0.0.1") is CircuitState.CLOSED


async def test_the_browser_renders_the_pages_of_no_proxy_without_the_proxy(url, site, make_proxy, monkeypatch):
    proxy = await make_proxy()
    for name in ["http_proxy", "https_proxy", "no_proxy", "REQUEST_METHOD"]:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.upper(), raising=False)
    monkeypatch.setenv("HTTP_PROXY", proxy.url)
    monkeypatch.setenv("NO_PROXY", "localhost")
    async with make_crawler(proxies=ProxyPool.from_env()) as crawler:
        # The context of the proxy takes the rules of NO_PROXY.
        proxied = await crawler.fetch_and_parse(url("/js/links"))
        direct = await crawler.fetch_and_parse(url("/js/links", "localhost"))

    assert "made by javascript" in proxied["text"]
    assert "made by javascript" in direct["text"]
    assert proxy.requests == requests_of_links(url)
    assert site.hits["/js/app.js"] == 2


async def test_close_ends_the_browser_and_its_processes(url):
    before = child_processes()
    crawler = make_crawler()
    await crawler.fetch_url(url("/js/target"))
    assert child_processes() - before

    await crawler.close()

    for _ in range(50):
        if not child_processes() - before:
            break
        await asyncio.sleep(0.1)
    assert not child_processes() - before
    assert not crawler._transport.renderer.running


async def test_chromium_is_found():
    assert await browser_problem() is None


async def test_the_command_line_renders_with_render(url, tmp_path, capsys):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({**FAST_CONFIG, "logging": {"level": "WARNING"}}), encoding="utf-8")
    output = tmp_path / "pages.jsonl"
    argv = ["--config", str(config), "--urls", url("/js/links"), "--render", "--output", str(output)]

    code = await main.run(main.build_config(main.parse_args(argv)), progress=False)

    assert code == 0
    # The one link of the page is made by JavaScript.
    saved = {json.loads(line)["url"] for line in output.read_text(encoding="utf-8").splitlines()}
    assert saved == {url("/js/links"), url("/js/target")}
    summary = capsys.readouterr().out
    assert "Pages: 2 (2 successful" in summary
    assert re.search(r"^Rendering: 2 pages rendered, 0 failed, average \d+\.\d\ds$", summary, re.MULTILINE)
