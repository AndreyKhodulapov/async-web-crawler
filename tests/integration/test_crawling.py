"""Integration tests: crawl a small site served by a local aiohttp server."""

import asyncio

import pytest

from crawler import AsyncCrawler


@pytest.fixture
def url(server):
    def make(path: str) -> str:
        return str(server.make_url(path))

    return make


async def crawl(start_url: str, *, max_concurrent: int = 5, max_depth: int = 2, **options) -> AsyncCrawler:
    """Run a crawl and return the closed crawler with its state."""
    options.setdefault("same_domain_only", True)
    async with AsyncCrawler(max_concurrent=max_concurrent, max_depth=max_depth) as crawler:
        await crawler.crawl([start_url], **options)
    return crawler


async def test_crawls_site_up_to_max_depth(url):
    crawler = await crawl(url("/site/"), max_depth=2)

    assert set(crawler.processed_urls) == {
        url("/site/"),
        url("/site/a.html"),
        url("/site/b.html"),
        url("/site/moved"),
        url("/site/a/deeper.html"),
    }
    assert crawler.processed_urls[url("/site/moved")]["final_url"] == url("/site/c.html")
    assert set(crawler.failed_urls) == {url("/site/missing.html"), url("/site/files/manual.pdf")}
    assert crawler.failed_urls[url("/site/missing.html")].startswith("HTTPStatusError: HTTP 404")
    assert crawler.visited_urls == set(crawler.processed_urls) | set(crawler.failed_urls)
    # Links from depth 2 are not followed.
    assert url("/site/a/deepest.html") not in crawler.url_depths
    assert crawler.url_depths[url("/site/a.html")] == 1
    assert crawler.url_depths[url("/site/a/deeper.html")] == 2


async def test_max_depth_zero_fetches_start_urls_only(url, site):
    crawler = await crawl(url("/site/"), max_depth=0)
    assert list(crawler.processed_urls) == [url("/site/")]
    assert site.hits.total() == 1


async def test_every_page_is_fetched_once(url, site):
    # The site has cycles, duplicate links and self-links.
    site.latency = 0.01
    crawler = await crawl(url("/site/"), max_concurrent=10, max_depth=5)

    # The redirect target is the one exception: when "moved" and a direct
    # link to c.html are in flight at the same time, nothing tells the crawler
    # they are the same page until the response arrives.
    fetched_twice = {path for path, hits in site.hits.items() if hits > 1}
    assert fetched_twice <= {"/site/c.html"}
    assert site.hits["/site/"] == 1
    assert crawler.url_depths[url("/site/a/deepest.html")] == 3


async def test_redirect_target_is_not_fetched_again(url, site):
    # One worker makes the order fixed: "moved" redirects to c.html before
    # a/deeper.html, which links to c.html directly, is crawled.
    crawler = await crawl(url("/site/"), max_concurrent=1, max_depth=3)

    assert site.hits["/site/moved"] == 1
    assert site.hits["/site/c.html"] == 1
    assert url("/site/c.html") not in crawler.visited_urls


async def test_max_pages_counts_failed_pages_too(url, site):
    # Breadth-first order: the start page, then a.html, b.html and missing.html.
    crawler = await crawl(url("/site/"), max_pages=4)

    assert set(crawler.failed_urls) == {url("/site/missing.html")}
    assert len(crawler.visited_urls) == 4
    assert site.hits.total() == 4
    assert crawler.crawl_stats().queued > 0


async def test_same_domain_only(url, server):
    other_host = f"http://localhost:{server.port}/site/"

    restricted = await crawl(url("/site/"), max_depth=1, same_domain_only=True)
    unrestricted = await crawl(url("/site/"), max_depth=1, same_domain_only=False)

    assert other_host not in restricted.visited_urls
    assert other_host in unrestricted.visited_urls


async def test_start_url_redirect_to_other_host_keeps_that_host(server, url):
    crawler = await crawl(url("/site/to-other-host"), max_depth=1, same_domain_only=True)

    assert f"http://localhost:{server.port}/site/a.html" in crawler.processed_urls


async def test_redirect_out_of_the_start_hosts_is_skipped(url, server):
    crawler = await crawl(url("/site/exits.html"), max_depth=1, same_domain_only=True)

    assert set(crawler.processed_urls) == {url("/site/exits.html"), url("/site/moved")}
    assert crawler.failed_urls == {}
    assert crawler.skipped_urls == {
        url("/site/to-other-host"): f"redirected out of scope: http://localhost:{server.port}/site/"
    }
    assert crawler.crawl_stats().skipped == 1


async def test_redirect_to_an_excluded_url_is_skipped(url):
    crawler = await crawl(url("/site/exits.html"), max_depth=1, exclude_patterns=[r"/c\.html$"])

    assert url("/site/moved") not in crawler.processed_urls
    assert crawler.skipped_urls[url("/site/moved")] == f"redirected out of scope: {url('/site/c.html')}"


async def test_urls_needing_percent_encoding(url, site):
    # No redirect happens here, although the client reports the final URL
    # percent-encoded; raw and encoded links to one page are one URL; the
    # include patterns are written in the decoded form.
    crawler = await crawl(url("/site/names.html"), max_depth=1, include_patterns=[r"/café\.html", r"/a b\.html"])

    assert set(crawler.processed_urls) == {
        url("/site/names.html"),
        url("/site/caf%C3%A9.html"),
        url("/site/a%20b.html"),
    }
    assert crawler.skipped_urls == crawler.failed_urls == {}
    assert site.hits["/site/café.html"] == 1


async def test_exclude_patterns(url, site):
    crawler = await crawl(url("/site/"), exclude_patterns=[r"\.pdf$", r"missing"])

    assert crawler.failed_urls == {}
    assert site.hits["/site/files/manual.pdf"] == 0


async def test_include_patterns(url):
    # The start URL is not filtered; "/a" matches a.html and a/deeper.html.
    crawler = await crawl(url("/site/"), include_patterns=[r"/a"])

    assert set(crawler.processed_urls) == {url("/site/"), url("/site/a.html"), url("/site/a/deeper.html")}


async def test_per_domain_limit(url, site):
    site.latency = 0.02
    await crawl(url("/site/"), max_concurrent=5)
    assert site.peak_in_flight > 1

    site.peak_in_flight = 0
    async with AsyncCrawler(max_concurrent=5, max_per_domain=1) as crawler:
        await crawler.crawl([url("/site/")], same_domain_only=True)
    assert site.peak_in_flight == 1


async def test_stats_after_crawl(url):
    crawler = await crawl(url("/site/"))
    stats = crawler.crawl_stats()

    assert stats.processed == len(crawler.processed_urls) == 5
    assert stats.failed == len(crawler.failed_urls) == 2
    assert stats.skipped == 0
    assert stats.in_progress == stats.active_requests == 0
    assert stats.queued == 0
    assert stats.pages_per_second > 0
    assert crawler.crawl_stats().elapsed == stats.elapsed  # the clock stops with the crawl


async def test_state_is_reset_between_crawls(url):
    async with AsyncCrawler(max_depth=1) as crawler:
        first = await crawler.crawl([url("/site/")], same_domain_only=True)
        second = await crawler.crawl([url("/site/b.html")], max_pages=1)

    assert len(first) > 1
    assert list(second) == [url("/site/b.html")]
    assert crawler.visited_urls == {url("/site/b.html")}
    assert crawler.failed_urls == {}


async def test_invalid_start_urls_are_rejected():
    async with AsyncCrawler() as crawler:
        with pytest.raises(ValueError, match="invalid start URLs: 'ftp://site/'"):
            await crawler.crawl(["https://example.com", "ftp://site/"])
        with pytest.raises(ValueError, match="max_pages"):
            await crawler.crawl(["https://example.com"], max_pages=0)
        with pytest.raises(TypeError, match="got a string"):
            await crawler.crawl("https://example.com")


async def test_second_concurrent_crawl_is_rejected(url, site):
    site.latency = 0.05
    async with AsyncCrawler() as crawler:
        running = asyncio.create_task(crawler.crawl([url("/site/")], max_pages=1))
        await asyncio.sleep(0.01)
        with pytest.raises(RuntimeError, match="already running"):
            await crawler.crawl([url("/site/")])
        await running
