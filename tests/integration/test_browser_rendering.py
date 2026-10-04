"""Integration tests: pages are rendered in a headless Chromium, and what they do on their own is checked."""

import asyncio
import os
import subprocess

import pytest
from helpers import BOT, UNTHROTTLED

from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    FetchTimeoutError,
    PageTooLargeError,
    RenderError,
    Rendering,
    RobotsDisallowedError,
)

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
    # Not retried, and the site is not to blame.
    assert site.hits["/js/target"] == 3
    breaker = crawler.circuit_breaker.get_stats()["127.0.0.1"]
    assert (breaker.state, breaker.failures) == ("closed", 0)


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
