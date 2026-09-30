"""Integration tests: rate limits, robots.txt and retries against a local aiohttp server."""

import itertools

import pytest
from helpers import BOT, UNTHROTTLED

from crawler import AsyncCrawler, RobotsDisallowedError

# Gaps are measured where requests arrive, while the limiter controls when
# they are sent; opening a connection shifts an arrival by a millisecond or so.
EPSILON = 0.01


def polite(**options) -> AsyncCrawler:
    return AsyncCrawler(**{**UNTHROTTLED, "user_agent": BOT, **options})


async def open_session(crawler: AsyncCrawler, url, site) -> None:
    """Make a first request before measuring, then forget it.

    Creating the HTTP session loads the CA bundle and blocks the event loop
    for about 20 ms, which would delay the first measured request.
    """
    await crawler.fetch_url(url("/ok", "localhost"))
    site.log.clear()
    site.hits.clear()


def gaps(site) -> list[float]:
    times = [moment for _, moment in site.log]
    return [later - earlier for earlier, later in itertools.pairwise(times)]


class TestRateLimit:
    async def test_requests_to_one_host_are_spaced_out(self, url, site):
        paths = ["/site/", "/site/a.html", "/site/b.html", "/site/c.html", "/site/a/deeper.html"]
        async with polite(requests_per_second=10) as crawler:
            await open_session(crawler, url, site)
            await crawler.fetch_many([url(path) for path in paths])
        assert len(site.log) == len(paths)
        assert min(gaps(site)) >= 0.1 - EPSILON

    async def test_crawl_stats_report_the_rate(self, url, site):
        async with polite(requests_per_second=10, max_depth=1) as crawler:
            await crawler.crawl([url("/site/")], same_domain_only=True)
        stats = crawler.crawl_stats()

        assert stats.requests == len(site.log)
        assert stats.avg_delay >= 0.1 - EPSILON
        assert stats.avg_wait > 0
        assert stats.requests_per_second > 0
        assert crawler.rate_limiter.get_stats().domains["127.0.0.1"].requests == stats.requests


class TestRobots:
    async def test_disallowed_pages_are_blocked_and_never_requested(self, url, site):
        site.robots = "User-agent: *\nDisallow: /site/b.html\nDisallow: /site/a/"
        async with polite(respect_robots=True, max_concurrent=5) as crawler:
            pages = await crawler.crawl([url("/site/")], same_domain_only=True)

        assert crawler.blocked_urls == {
            url("/site/b.html"): "disallowed by robots.txt",
            url("/site/a/deeper.html"): "disallowed by robots.txt",
        }
        assert site.hits["/site/b.html"] == site.hits["/site/a/deeper.html"] == 0
        assert url("/site/a.html") in pages
        assert crawler.crawl_stats().blocked == 2
        # Five workers met the new site at once, yet robots.txt was fetched once.
        assert site.hits["/robots.txt"] == 1

    async def test_rules_for_the_crawlers_own_name(self, url, site):
        site.robots = "User-agent: testbot\nDisallow: /site/\n\nUser-agent: *\nDisallow:"
        async with polite(respect_robots=True) as crawler:
            with pytest.raises(RobotsDisallowedError):
                await crawler.fetch_url(url("/site/"))
        async with polite(respect_robots=True, user_agent="OtherBot/2.0") as crawler:
            assert await crawler.fetch_url(url("/site/"))

    async def test_unreachable_robots_txt_blocks_the_site(self, url, site):
        site.robots, site.robots_status = "", 503
        async with polite(respect_robots=True) as crawler:
            with pytest.raises(RobotsDisallowedError, match=r"robots.txt is unreachable \(HTTP 503\)"):
                await crawler.fetch_url(url("/site/"))
        assert site.hits["/site/"] == 0

    async def test_blocked_pages_do_not_count_toward_max_pages(self, url, site):
        # One worker, breadth-first: /site/, a.html, b.html (blocked), missing.html.
        site.robots = "User-agent: *\nDisallow: /site/b.html"
        async with polite(respect_robots=True, max_concurrent=1) as crawler:
            await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)

        assert list(crawler.blocked_urls) == [url("/site/b.html")]
        assert {path for path in site.hits if path.startswith("/site/")} == {
            "/site/",
            "/site/a.html",
            "/site/missing.html",
        }
        assert len(crawler.visited_urls) == 4

    async def test_crawl_delay_spaces_out_requests(self, url, site):
        site.robots = "User-agent: *\nCrawl-delay: 0.1"
        async with polite(respect_robots=True, max_depth=1) as crawler:
            await open_session(crawler, url, site)
            await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)

        # robots.txt included: the page after it was booked before the delay was known.
        assert [path for path, _ in site.log] == ["/robots.txt", "/site/", "/site/a.html", "/site/b.html"]
        assert min(gaps(site)) >= 0.1 - EPSILON


class TestRetries:
    async def test_crawl_counts_retries(self, url):
        async with polite(max_retries=2, backoff_base=0.01) as crawler:
            pages = await crawler.crawl([url("/flaky/1")])
        stats = crawler.crawl_stats()

        assert list(pages) == [url("/flaky/1")]
        assert stats.retries == 1
        assert stats.requests == 2
