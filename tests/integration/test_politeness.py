"""Integration tests: rate limits, robots.txt, retries and the circuit breaker against a local aiohttp server."""

import asyncio
import contextlib
import itertools
import time

import pytest
from helpers import BOT, UNTHROTTLED, FakeClock, MemoryStorage

from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    CircuitOpenError,
    RetryStrategy,
    RobotsDisallowedError,
    RobotsParser,
    RobotsUnreachableError,
)

# Starts are recorded right after the limiter reads its clock for them.
EPSILON = 0.005


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


def record_starts(crawler: AsyncCrawler) -> list[float]:
    """Record when the rate limiter lets each request of `crawler` start.

    That is the moment the limiter controls. Arrivals at the test server
    may come closer together: the server runs in the same event loop, and
    a loop busy for a moment delays one request but not the one after it.
    """
    starts: list[float] = []
    slot = crawler.rate_limiter.slot

    @contextlib.asynccontextmanager
    async def recording_slot(*args, **kwargs):
        async with slot(*args, **kwargs):
            starts.append(time.monotonic())
            yield

    crawler.rate_limiter.slot = recording_slot
    return starts


def gaps(times: list[float]) -> list[float]:
    return [later - earlier for earlier, later in itertools.pairwise(times)]


class TestRateLimit:
    async def test_requests_to_one_host_are_spaced_out(self, url, site):
        paths = ["/site/", "/site/a.html", "/site/b.html", "/site/c.html", "/site/a/deeper.html"]
        async with polite(requests_per_second=10) as crawler:
            await open_session(crawler, url, site)
            starts = record_starts(crawler)
            await crawler.fetch_many([url(path) for path in paths])
        assert len(site.log) == len(starts) == len(paths)
        assert min(gaps(starts)) >= 0.1 - EPSILON

    async def test_redirect_waits_for_its_turn(self, url, site):
        async with polite(requests_per_second=10) as crawler:
            await open_session(crawler, url, site)
            starts = record_starts(crawler)
            await crawler.fetch_url(url("/site/moved"))
        assert [path for path, _ in site.log] == ["/site/moved", "/site/c.html"]
        assert gaps(starts)[0] >= 0.1 - EPSILON

    async def test_waiting_host_does_not_hold_back_another(self, url, site):
        # Two slots, six pages of one host: the rest of them wait for their
        # turn without taking a slot, so the other host starts at once.
        async with polite(requests_per_second=5, max_concurrent=2) as crawler:
            await crawler.fetch_many(
                [url(f"/site/{number}") for number in range(6)] + [url("/site/other", "localhost")]
            )
        arrivals = dict(site.log)
        assert arrivals["/site/other"] - arrivals["/site/0"] < 0.1

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

    async def test_nofollow_and_noindex_are_respected(self, url, site):
        storage = MemoryStorage()
        async with polite(respect_robots=True, max_depth=2, storage=storage) as crawler:
            pages = await crawler.crawl([url("/site/robots.html")])

        # rel="nofollow" (a.html) and the links of pages that ask not to follow
        # them (c.html, a/deeper.html) are not requested; those of a page
        # that asks only not to be kept are (b.html).
        assert set(site.hits) == {
            "/robots.txt", "/site/robots.html", "/site/noindex.html", "/site/nofollow.html", "/site/tagged.html",
            "/site/b.html",
        }  # fmt: skip
        assert crawler.skipped_urls == {
            url("/site/noindex.html"): 'noindex in <meta name="robots">',
            url("/site/tagged.html"): "noindex in X-Robots-Tag",
        }
        crawled = {url("/site/robots.html"), url("/site/nofollow.html"), url("/site/b.html")}
        assert set(pages) == crawled
        assert {record["url"] for batch in storage.batches for record in batch} == crawled
        assert pages[url("/site/nofollow.html")]["metadata"]["robots"] == ["nofollow"]

    async def test_nofollow_and_noindex_are_ignored_without_robots_txt(self, url, site):
        async with polite(respect_robots=False, max_depth=2) as crawler:
            pages = await crawler.crawl([url("/site/robots.html")])

        assert not any(reason.startswith("noindex") for reason in crawler.skipped_urls.values())
        assert {url("/site/a.html"), url("/site/noindex.html"), url("/site/tagged.html"), url("/site/c.html")} <= set(
            pages
        )
        assert site.hits["/site/a/deeper.html"] == 1

    async def test_unreachable_robots_txt_keeps_the_site_unfetched(self, url, site):
        site.robots, site.robots_status = "", 503
        async with polite(respect_robots=True) as crawler:
            with pytest.raises(RobotsUnreachableError, match=r"robots.txt is unreachable \(HTTP 503\)"):
                await crawler.fetch_url(url("/site/"))
        assert site.hits["/site/"] == 0

    async def test_site_is_fetched_once_robots_txt_is_back(self, url, site):
        site.robots, site.robots_status = "", 503
        async with polite(respect_robots=True) as crawler:
            crawler.robots.UNREACHABLE_TTL = 0.05
            with pytest.raises(RobotsUnreachableError):
                await crawler.fetch_url(url("/site/"))
            site.robots_status = 200
            with pytest.raises(RobotsUnreachableError):
                await crawler.fetch_url(url("/site/"))  # still cached

            await asyncio.sleep(0.05)
            assert await crawler.fetch_url(url("/site/"))
        assert site.hits["/robots.txt"] == 2

    async def test_unreachable_pages_are_not_blocked_and_do_not_count_toward_max_pages(
        self, url, site, closed_port_url
    ):
        # One worker takes the page of the unreachable site first.
        async with polite(respect_robots=True, max_concurrent=1) as crawler:
            pages = await crawler.crawl([f"{closed_port_url}page", url("/site/")], max_pages=1)
        stats = crawler.crawl_stats()

        assert list(crawler.unreachable_urls) == [f"{closed_port_url}page"]
        assert crawler.unreachable_urls[f"{closed_port_url}page"].startswith("robots.txt is unreachable (NetworkError")
        assert (stats.unreachable, stats.blocked, stats.failed) == (1, 0, 0)
        assert list(pages) == [url("/site/")]

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

    async def test_redirect_to_a_disallowed_page_is_not_followed(self, url, site):
        site.robots = "User-agent: *\nDisallow: /site/private/"
        async with polite(respect_robots=True) as crawler:
            pages = await crawler.crawl([url("/site/go")])
            with pytest.raises(RobotsDisallowedError):
                await crawler.fetch_url(url("/site/go"))

        assert pages == {}
        assert crawler.blocked_urls == {
            url("/site/go"): f"redirects to {url('/site/private/secret')}, disallowed by robots.txt"
        }
        assert site.hits["/site/private/secret"] == 0

    async def test_redirect_to_another_host_follows_its_robots_txt(self, url, site):
        site.robots_by_host = {"localhost": "User-agent: *\nDisallow: /"}
        async with polite(respect_robots=True) as crawler:
            with pytest.raises(RobotsDisallowedError) as error:
                await crawler.fetch_url(url("/site/to-other-host"))

        assert error.value.url == url("/site/", "localhost")
        assert site.hits["/site/"] == 0
        assert site.hits["/robots.txt"] == 2  # of both hosts

    async def test_endless_robots_txt_is_cut_and_read(self, url, site, monkeypatch):
        monkeypatch.setattr(RobotsParser, "MAX_SIZE", 1000)
        site.robots, site.robots_endless = "User-agent: *\nDisallow: /site/b.html\n", True
        async with polite(respect_robots=True, total_timeout=5) as crawler:
            started = time.perf_counter()
            html = await crawler.fetch_url(url("/site/a.html"))
            with pytest.raises(RobotsDisallowedError):
                await crawler.fetch_url(url("/site/b.html"))

        assert "<title>A</title>" in html
        # Read up to the limit, not until the total timeout.
        assert time.perf_counter() - started < 3

    async def test_crawl_delay_spaces_out_requests(self, url, site):
        site.robots = "User-agent: *\nCrawl-delay: 0.1"
        async with polite(respect_robots=True, max_depth=1) as crawler:
            await open_session(crawler, url, site)
            starts = record_starts(crawler)
            await crawler.crawl([url("/site/")], max_pages=3, same_domain_only=True)

        # robots.txt included: the page after it was booked before the delay was known.
        assert [path for path, _ in site.log] == ["/robots.txt", "/site/", "/site/a.html", "/site/b.html"]
        assert len(starts) == 4
        assert min(gaps(starts)) >= 0.1 - EPSILON


class TestRetries:
    async def test_crawl_counts_retries(self, url):
        async with polite(retry_strategy=RetryStrategy(max_retries=2, base_delay=0.01)) as crawler:
            pages = await crawler.crawl([url("/flaky/1")])
        stats = crawler.crawl_stats()

        assert list(pages) == [url("/flaky/1")]
        assert stats.retries == 1
        assert stats.requests == 2

    async def test_backoff_holds_back_requests_already_waiting(self, url, site):
        # The retry of /flaky/1 waits 0.2..0.4 s; the pages had booked their
        # turns before the failure, and they wait for the retry too.
        async with polite(
            requests_per_second=10, retry_strategy=RetryStrategy(max_retries=1, base_delay=0.4, max_delay=0.4)
        ) as crawler:
            await open_session(crawler, url, site)
            starts = record_starts(crawler)
            await crawler.fetch_many([url("/flaky/1"), url("/site/"), url("/site/a.html"), url("/site/b.html")])

        # The first attempt fails at once; its retry and the pages wait out the 0.2..0.4 s pause.
        assert site.log[0][0] == "/flaky/1"
        assert len(starts) == 5
        assert all(start - starts[0] >= 0.2 - EPSILON for start in starts[1:])
        assert min(gaps(starts)) >= 0.1 - EPSILON


class TestRetryAfter:
    async def test_crawl_puts_off_the_pages_of_a_host_that_asked_to_wait(self, url, site):
        # The host asks for 2 s, longer than a retry may wait: the request is
        # not retried, yet the host is left alone for the whole 2 s, and the
        # crawl goes on to another host meanwhile.
        other_host = url("/site/b.html", "localhost")
        options = {"max_concurrent": 1, "max_depth": 0, "retry_strategy": RetryStrategy(max_retries=1, max_delay=0.5)}
        async with polite(**options) as crawler:
            await crawler.crawl([url("/busy/2"), url("/site/a.html"), other_host])

        (busy, asked), (other, _), (page, requested) = site.log
        assert [busy, other, page] == ["/busy/2", "/site/b.html", "/site/a.html"]
        assert requested - asked >= 2 - EPSILON
        assert list(crawler.failed_urls) == [url("/busy/2")]
        assert set(crawler.processed_urls) == {url("/site/a.html"), other_host}
        assert crawler.crawl_stats().requests == 3


class TestCircuitBreaker:
    async def test_failing_host_is_left_alone_until_the_cooldown(self, url, site):
        clock = FakeClock()
        breaker = CircuitBreaker(min_requests=3, cooldown=10, clock=clock)
        async with polite(circuit_breaker=breaker) as crawler:
            # 503 the first 3 times, then a page.
            for _ in range(5):
                result = await crawler.fetch_result(url("/flaky/3"))
            assert isinstance(result.error, CircuitOpenError)
            assert site.hits["/flaky/3"] == 3
            # Another host of the same server is not blocked.
            assert await crawler.fetch_url(url("/ok", "localhost"))

            clock.now += 10
            assert "Recovered" in await crawler.fetch_url(url("/flaky/3"))
            assert await crawler.fetch_url(url("/flaky/3"))
        assert site.hits["/flaky/3"] == 5

    async def test_crawl_puts_off_the_pages_of_a_blocked_host_until_it_recovers(self, url, site):
        # 503 the first 4 times, then pages.
        blocked = [url(f"/flaky/4?page={page}") for page in range(5)]
        other_host = url("/site/b.html", "localhost")
        options = {
            "max_concurrent": 1,
            "max_depth": 0,
            "retry_strategy": RetryStrategy(max_retries=3, base_delay=0.01),
            "circuit_breaker": CircuitBreaker(min_requests=3, cooldown=0.05),
        }
        async with polite(**options) as crawler:
            await crawler.crawl([*blocked, other_host], max_pages=6)

        # The first page opens the circuit on its third attempt; the others
        # wait, and the crawl goes on to the other host.
        assert [path for path, _ in site.log[:4]] == ["/flaky/4"] * 3 + ["/site/b.html"]
        # The first probe fails and opens the circuit again; the second one succeeds.
        assert list(crawler.failed_urls) == blocked[:2]
        assert set(crawler.failed_urls.values()) == {"TransientHTTPError: HTTP 503 Service Unavailable"}
        assert list(crawler.processed_urls) == [other_host, *blocked[2:]]
        assert site.hits["/flaky/4"] == 7
        assert crawler.circuit_breaker.get_stats()["127.0.0.1"].times_opened == 2
        assert crawler.crawl_stats().requests == 8
