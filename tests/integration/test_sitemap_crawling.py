"""Integration tests: crawls that take their pages from sitemaps served by a local aiohttp server."""

import gzip
import logging

import pytest
from helpers import BOT, UNTHROTTLED, index, urlset

from crawler import AsyncCrawler, RetryStrategy, SitemapParser

SITEMAP = "/sitemaps/sitemap.xml"


def make_crawler(**options) -> AsyncCrawler:
    return AsyncCrawler(**{**UNTHROTTLED, "max_depth": 0, "user_agent": BOT, **options})


async def test_pages_of_a_sitemap_index_are_crawled(url, site):
    # The index lists a plain sitemap and a gzipped one.
    site.sitemaps = {
        "sitemap.xml": index(url("/sitemaps/pages.xml"), url("/sitemaps/more.xml.gz")),
        "pages.xml": urlset(url("/site/a.html"), url("/site/b.html")),
        "more.xml.gz": gzip.compress(urlset(url("/site/c.html"), url("/site/missing.html"))),
    }
    async with make_crawler() as crawler:
        pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP)])

    assert set(pages) == {url("/site/a.html"), url("/site/b.html"), url("/site/c.html")}
    assert pages[url("/site/c.html")]["title"] == "C"
    assert list(crawler.failed_urls) == [url("/site/missing.html")]
    assert crawler.failed_sitemaps == {}
    assert set(crawler.url_depths.values()) == {0}
    # The sitemaps are requests of the crawl, but not pages of it.
    assert crawler.crawl_stats().requests == 7
    assert url(SITEMAP) not in crawler.visited_urls


async def test_sitemap_pages_come_after_the_start_urls_and_their_links_are_followed(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a/deeper.html"), url("/site/c.html"))}
    async with make_crawler(max_depth=1, max_concurrent=1) as crawler:
        pages = await crawler.crawl([url("/site/b.html")], sitemap_urls=[url(SITEMAP)], same_domain_only=True)

    # b.html links to a.html; a/deeper.html to a/deepest.html and to c.html, which is queued already.
    assert list(pages) == [
        url("/site/b.html"),
        url("/site/a/deeper.html"),
        url("/site/c.html"),
        url("/site/a.html"),
        url("/site/a/deepest.html"),
    ]
    assert crawler.url_depths[url("/site/c.html")] == 0
    assert crawler.url_depths[url("/site/a/deepest.html")] == 1
    assert site.hits["/site/c.html"] == 1


async def test_large_sitemap_does_not_fill_the_queue(url, site):
    site.sitemaps = {"sitemap.xml": urlset(*(url(f"/wide/{n}") for n in range(1, 101)))}
    async with make_crawler(max_concurrent=1) as crawler:
        pages = await crawler.crawl([url("/wide/0")], 5, sitemap_urls=[url(SITEMAP)])

    assert list(pages) == [url(f"/wide/{n}") for n in range(5)]
    # The start URL is queued before the sitemaps are read; 14 of their pages join it.
    assert len(crawler.url_depths) == AsyncCrawler.FRONTIER_FACTOR * 5


async def test_start_url_listed_in_the_sitemap_is_fetched_once(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/c.html"), url("/site/c.html#top"))}
    async with make_crawler() as crawler:
        pages = await crawler.crawl([url("/site/c.html")], sitemap_urls=[url(SITEMAP), url(SITEMAP)])

    assert list(pages) == [url("/site/c.html")]
    assert site.hits["/site/c.html"] == 1
    assert site.hits[SITEMAP] == 1


async def test_filters_apply_to_sitemap_pages(url, site):
    site.sitemaps = {
        "sitemap.xml": urlset(
            url("/site/a.html"), url("/site/b.html"), url("/site/c.html"), url("/site/c.html", "localhost")
        )
    }
    async with make_crawler() as crawler:
        # The start URL is not filtered; the sitemap is on a start host.
        pages = await crawler.crawl(
            [url("/site/b.html")],
            sitemap_urls=[url(SITEMAP)],
            same_domain_only=True,
            include_patterns=[r"/[ac]\.html$"],
            exclude_patterns=[r"/a\.html$"],
        )

    assert set(pages) == {url("/site/b.html"), url("/site/c.html")}
    assert crawler.visited_urls == set(pages)


async def test_same_domain_only_keeps_the_host_of_the_sitemap(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/c.html"), url("/site/c.html", "localhost"))}
    async with make_crawler() as crawler:
        pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP)], same_domain_only=True)

    assert list(pages) == [url("/site/c.html")]


async def test_sitemap_pages_on_the_host_a_start_url_redirects_to(url, site):
    # The sitemaps are read before the start URL shows where it redirects to.
    site.sitemaps = {
        "sitemap.xml": urlset(
            url("/site/b.html"), url("/site/c.html", "localhost"), url("/site/a/deeper.html", "localhost")
        )
    }
    async with make_crawler() as crawler:
        pages = await crawler.crawl(
            [url("/site/to-other-host")],
            sitemap_urls=[url(SITEMAP)],
            same_domain_only=True,
            exclude_patterns=[r"/deeper\.html$"],
        )

    assert set(pages) == {url("/site/to-other-host"), url("/site/b.html"), url("/site/c.html", "localhost")}
    assert crawler.url_depths[url("/site/c.html", "localhost")] == 0
    assert site.hits["/site/a/deeper.html"] == 0


async def test_sitemap_pages_on_another_host_stay_out_without_a_redirect(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/c.html", "localhost"))}
    async with make_crawler() as crawler:
        pages = await crawler.crawl([url("/site/b.html")], sitemap_urls=[url(SITEMAP)], same_domain_only=True)

    assert list(pages) == [url("/site/b.html")]
    assert crawler.visited_urls == set(pages)


async def test_sitemap_page_redirecting_out_of_scope_is_skipped(url, site, server):
    # Unlike a start URL, a page from a sitemap does not bring the host it redirects to into the crawl.
    site.sitemaps = {"sitemap.xml": urlset(url("/site/to-other-host"))}
    async with make_crawler(max_depth=1) as crawler:
        pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP)], same_domain_only=True)

    assert pages == {}
    assert crawler.skipped_urls == {
        url("/site/to-other-host"): f"redirected out of scope: http://localhost:{server.port}/site/"
    }


async def test_robots_txt_applies_to_sitemap_pages_and_to_the_sitemap(url, site):
    site.robots = "User-agent: *\nDisallow: /site/b.html\nDisallow: /sitemaps/private.xml"
    site.sitemaps = {
        "sitemap.xml": urlset(url("/site/a.html"), url("/site/b.html")),
        "private.xml": urlset(url("/site/c.html")),
    }
    async with make_crawler(respect_robots=True) as crawler:
        pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP), url("/sitemaps/private.xml")])

    assert list(pages) == [url("/site/a.html")]
    assert crawler.blocked_urls == {url("/site/b.html"): "disallowed by robots.txt"}
    assert crawler.failed_sitemaps == {url("/sitemaps/private.xml"): "RobotsDisallowedError: disallowed by robots.txt"}
    assert site.hits["/site/b.html"] == site.hits["/sitemaps/private.xml"] == 0


async def test_sitemaps_named_in_robots_txt(url, site):
    site.robots = f"Sitemap: {url(SITEMAP)}\nUser-agent: *\nDisallow:"
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"), url("/site/c.html"))}
    async with make_crawler(respect_robots=True) as crawler:
        ignored = dict(await crawler.crawl([url("/site/b.html")]))
        # Named in robots.txt and given as well: read once.
        pages = await crawler.crawl([url("/site/b.html")], robots_sitemaps=True, sitemap_urls=[url(SITEMAP)])

    assert list(ignored) == [url("/site/b.html")]
    assert set(pages) == {url("/site/b.html"), url("/site/a.html"), url("/site/c.html")}
    assert site.hits[SITEMAP] == 1


async def test_site_without_sitemaps_in_robots_txt(url, site):
    async with make_crawler(respect_robots=True) as crawler:
        pages = await crawler.crawl([url("/site/b.html")], robots_sitemaps=True)

    assert list(pages) == [url("/site/b.html")]
    assert crawler.failed_sitemaps == {}


async def test_sitemap_waits_for_robots_txt_that_is_down_for_a_moment(url, site, caplog):
    # robots.txt answers 503 to the first download: a crawl fed by the
    # sitemap alone waits for it instead of ending with no pages.
    caplog.set_level(logging.INFO, logger="crawler.client")
    site.robots, site.robots_failures = "", 1
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"), url("/site/c.html"))}
    async with make_crawler(respect_robots=True) as crawler:
        crawler.robots.UNREACHABLE_TTL = 0.1
        pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP)])

    assert set(pages) == {url("/site/a.html"), url("/site/c.html")}
    assert crawler.failed_sitemaps == {}
    assert site.hits["/robots.txt"] == 2
    assert f"Sitemap {url(SITEMAP)} waits 0.1s: robots.txt is unreachable (HTTP 503)" in [
        r.getMessage() for r in caplog.records
    ]


async def test_sitemaps_named_in_robots_txt_wait_for_it_too(url, site, caplog):
    caplog.set_level(logging.INFO, logger="crawler.client")
    site.robots = f"Sitemap: {url(SITEMAP)}\nUser-agent: *\nDisallow:"
    site.robots_failures = 1
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"), url("/site/c.html"))}
    async with make_crawler(respect_robots=True) as crawler:
        crawler.robots.UNREACHABLE_TTL = 0.1
        pages = await crawler.crawl([url("/site/b.html")], robots_sitemaps=True)

    assert set(pages) == {url("/site/b.html"), url("/site/a.html"), url("/site/c.html")}
    assert site.hits["/robots.txt"] == 2
    assert f"Sitemaps of {url('/site/b.html')} wait 0.1s: robots.txt is unreachable (HTTP 503)" in [
        r.getMessage() for r in caplog.records
    ]


async def test_sitemap_of_a_site_whose_robots_txt_stays_down_is_left_out(url, site, caplog):
    site.robots, site.robots_status = "", 503
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"))}
    async with make_crawler(respect_robots=True) as crawler:
        crawler.robots.UNREACHABLE_TTL = 0.05
        with caplog.at_level(logging.WARNING, logger="crawler.client"):
            pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP)])

    assert pages == {}
    # Downloaded once more after each of the three waits.
    assert site.hits["/robots.txt"] == 1 + AsyncCrawler.MAX_WAITS_PER_PAGE
    assert crawler.failed_sitemaps == {url(SITEMAP): "RobotsUnreachableError: robots.txt is unreachable (HTTP 503)"}
    assert f"Sitemap {url(SITEMAP)} is left out" in caplog.text


async def test_no_sitemaps_from_robots_txt_that_stays_down(url, site, caplog):
    site.robots, site.robots_status = "", 503
    async with make_crawler(respect_robots=True) as crawler:
        crawler.robots.UNREACHABLE_TTL = 0.05
        with caplog.at_level(logging.WARNING, logger="crawler.client"):
            pages = await crawler.crawl([url("/site/b.html")], robots_sitemaps=True)

    assert pages == {}
    assert crawler.failed_sitemaps == {}
    assert f"No sitemaps from robots.txt of {url('/site/b.html')}: robots.txt is unreachable (HTTP 503)" in caplog.text
    assert list(crawler.unreachable_urls) == [url("/site/b.html")]


async def test_unreadable_sitemaps_do_not_stop_the_crawl(url, site, closed_port_url, caplog):
    site.sitemaps = {
        "sitemap.xml": urlset(url("/site/c.html")),
        "page.xml": b"<html><body>Not a sitemap</body></html>",
    }
    sitemap_urls = [
        url("/sitemaps/absent.xml"),
        url("/sitemaps/page.xml"),
        f"{closed_port_url}sitemap.xml",
        url(SITEMAP),
    ]
    async with make_crawler() as crawler:
        with caplog.at_level(logging.WARNING, logger="crawler.client"):
            pages = await crawler.crawl([url("/site/b.html")], sitemap_urls=sitemap_urls)
        failed = dict(crawler.failed_sitemaps)
        await crawler.crawl([url("/site/b.html")])

    assert set(pages) == {url("/site/b.html"), url("/site/c.html")}
    assert set(failed) == set(sitemap_urls[:3])
    assert failed[url("/sitemaps/absent.xml")].startswith("PermanentHTTPError: HTTP 404")
    assert failed[url("/sitemaps/page.xml")] == "SitemapError: not a sitemap: the root element is <html>"
    assert failed[f"{closed_port_url}sitemap.xml"].startswith("NetworkError")
    assert f"Sitemap {url('/sitemaps/absent.xml')} is left out" in caplog.text
    # Pages only: the sitemaps are in neither.
    assert crawler.failed_urls == {}
    assert crawler.error_stats().total == 0
    assert crawler.failed_sitemaps == {}  # reset by the second crawl


async def test_sitemap_download_is_retried(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/c.html"))}
    site.sitemap_failures = 2
    async with make_crawler(retry_strategy=RetryStrategy(max_retries=2, base_delay=0.01)) as crawler:
        pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP)])

    assert list(pages) == [url("/site/c.html")]
    assert site.hits[SITEMAP] == 3
    assert crawler.crawl_stats().retries == 2


@pytest.mark.parametrize("headers", [{}, {"Content-Encoding": "gzip"}], ids=["plain", "content-encoding"])
async def test_oversized_sitemap_is_not_downloaded_whole(url, site, monkeypatch, caplog, headers):
    monkeypatch.setattr(SitemapParser, "MAX_SIZE", 100_000)
    document = urlset(*[url(f"/site/{number}.html") for number in range(20_000)])
    # As a Content-Encoding the client undoes, the megabyte is a few kilobytes on the wire.
    site.sitemaps = {"sitemap.xml": gzip.compress(document) if headers else document}
    site.sitemap_headers = headers
    async with make_crawler() as crawler:
        with caplog.at_level(logging.INFO, logger="crawler.client"):
            pages = await crawler.crawl([url("/site/c.html")], sitemap_urls=[url(SITEMAP)])

    assert list(pages) == [url("/site/c.html")]
    assert crawler.failed_sitemaps == {url(SITEMAP): "SitemapError: larger than 100000 bytes"}
    assert site.hits[SITEMAP] == 1  # not retried
    # Given up while reading: the request did not end with the whole body in memory.
    assert f"Fetched {url(SITEMAP)}" not in caplog.text


async def test_sitemap_sent_with_content_encoding_is_read(url, site):
    site.sitemaps = {"sitemap.xml": gzip.compress(urlset(url("/site/c.html")))}
    site.sitemap_headers = {"Content-Encoding": "gzip"}
    async with make_crawler() as crawler:
        pages = await crawler.crawl([], sitemap_urls=[url(SITEMAP)])

    assert list(pages) == [url("/site/c.html")]


async def test_max_pages_caps_the_pages_of_a_sitemap(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"), url("/site/b.html"), url("/site/c.html"))}
    async with make_crawler(max_concurrent=1) as crawler:
        pages = await crawler.crawl([], max_pages=2, sitemap_urls=[url(SITEMAP)])

    assert list(pages) == [url("/site/a.html"), url("/site/b.html")]
    assert crawler.crawl_stats().queued == 1


async def test_invalid_sitemap_arguments_are_rejected(url, site):
    async with make_crawler() as crawler:
        with pytest.raises(ValueError, match="invalid sitemap URLs: 'sitemap.xml'"):
            await crawler.crawl([url("/site/")], sitemap_urls=[url(SITEMAP), "sitemap.xml"])
        with pytest.raises(TypeError, match="got a string"):
            await crawler.crawl([url("/site/")], sitemap_urls=url(SITEMAP))
        with pytest.raises(ValueError, match="robots_sitemaps needs"):
            await crawler.crawl([url("/site/")], robots_sitemaps=True)
    assert site.hits.total() == 0
