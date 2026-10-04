"""Integration tests: rate limits, robots.txt, retries and the circuit breaker against a local aiohttp server."""

import asyncio
import contextlib
import itertools
import logging
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
            url("/site/noindex.html"): "noindex in a robots meta tag",
            url("/site/tagged.html"): "noindex in X-Robots-Tag",
        }
        crawled = {url("/site/robots.html"), url("/site/nofollow.html"), url("/site/b.html")}
        assert set(pages) == crawled
        assert {record["url"] for batch in storage.batches for record in batch} == crawled
        assert pages[url("/site/nofollow.html")]["metadata"]["robots"] == ["nofollow"]

    async def test_robots_meta_tag_named_after_the_crawler_is_respected(self, url, site):
        start = [url("/site/for-testbot.html"), url("/site/for-otherbot.html")]
        async with polite(respect_robots=True, max_depth=1) as crawler:
            pages = await crawler.crawl(start)

        # "none" for TestBot: not kept, links not followed; for another crawler: ignored.
        assert crawler.skipped_urls == {url("/site/for-testbot.html"): "noindex in a robots meta tag"}
        assert set(pages) == {url("/site/for-otherbot.html"), url("/site/b.html")}
        assert site.hits["/site/c.html"] == 0

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

    async def test_crawl_waits_for_robots_txt_that_is_down_for_a_moment(self, url, site, caplog):
        # robots.txt answers 503 to the first download (one attempt and three
        # retries), then it is back: the pages of the site wait out the TTL
        # instead of ending the crawl unreachable, with nothing fetched.
        caplog.set_level(logging.INFO, logger="crawler")
        site.robots, site.robots_failures = "", 4
        options = {
            "respect_robots": True,
            "max_depth": 1,
            "retry_strategy": RetryStrategy(max_retries=3, base_delay=0.01),
        }
        async with polite(**options) as crawler:
            crawler.robots.UNREACHABLE_TTL = 0.1
            pages = await crawler.crawl([url("/site/")], same_domain_only=True)
        stats = crawler.crawl_stats()

        assert site.hits["/robots.txt"] == 5
        assert url("/site/") in pages and len(pages) > 1
        assert stats.unreachable == 0
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred ")]
        assert deferred == [f"Deferred {url('/site/')} for 0.1s: robots.txt is unreachable (HTTP 503)"]

    async def test_crawl_gives_up_on_a_site_whose_robots_txt_stays_down(self, url, site, caplog):
        caplog.set_level(logging.INFO, logger="crawler")
        site.robots, site.robots_status = "", 503
        async with polite(respect_robots=True) as crawler:
            crawler.robots.UNREACHABLE_TTL = 0.05
            await crawler.crawl([url("/site/")])

        # Downloaded once more after each of the three waits.
        assert site.hits["/robots.txt"] == 1 + AsyncCrawler.MAX_ROBOTS_RETRIES
        assert crawler.unreachable_urls == {url("/site/"): "robots.txt is unreachable (HTTP 503)"}
        assert site.hits["/site/"] == 0
        assert f"Gave up on {url('/site/')}: robots.txt is unreachable (HTTP 503)" in [
            r.getMessage() for r in caplog.records
        ]

    async def test_crawl_waits_for_robots_txt_of_the_host_a_start_url_redirects_to(self, url, site, caplog):
        # /site/to-other-host on 127.0.0.1 redirects to /site/ on localhost,
        # whose robots.txt answers 503 once: the start URL waits for it and
        # is requested again, instead of ending the crawl unreachable.
        caplog.set_level(logging.INFO, logger="crawler")
        site.robots, site.robots_failures_by_host = "", {"localhost": 1}
        start = url("/site/to-other-host")
        async with polite(respect_robots=True) as crawler:
            crawler.robots.UNREACHABLE_TTL = 0.1
            # One page: the wait uncounts it, so the crawl does not end over max_pages.
            pages = await crawler.crawl([start], max_pages=1)
        stats = crawler.crawl_stats()

        assert list(pages) == [start]
        assert pages[start]["final_url"] == url("/site/", host="localhost")
        assert (stats.processed, stats.unreachable, stats.queued) == (1, 0, 0)
        assert site.hits["/site/to-other-host"] == 2
        assert site.robots_hits == {"127.0.0.1": 1, "localhost": 2}
        deferred = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Deferred ")]
        assert deferred == [f"Deferred {start} for 0.1s: robots.txt is unreachable (HTTP 503)"]

    async def test_crawl_gives_up_on_a_start_url_redirecting_to_a_site_whose_robots_txt_stays_down(
        self, url, site, caplog
    ):
        caplog.set_level(logging.INFO, logger="crawler")
        site.robots, site.robots_failures_by_host = "", {"localhost": 100}
        start = url("/site/to-other-host")
        async with polite(respect_robots=True) as crawler:
            crawler.robots.UNREACHABLE_TTL = 0.05
            pages = await crawler.crawl([start])

        reason = f"redirects to {url('/site/', host='localhost')}, robots.txt is unreachable (HTTP 503)"
        assert pages == {}
        assert crawler.unreachable_urls == {start: reason}
        # Requested once more after each of the three waits.
        assert site.hits["/site/to-other-host"] == 1 + AsyncCrawler.MAX_ROBOTS_RETRIES
        assert site.robots_hits["localhost"] == 1 + AsyncCrawler.MAX_ROBOTS_RETRIES
        assert f"Gave up on {start}: {reason}" in [r.getMessage() for r in caplog.records]

    async def test_unreachable_pages_are_not_blocked_and_do_not_count_toward_max_pages(
        self, url, site, closed_port_url
    ):
        # One worker takes the page of the unreachable site first; the crawl
        # does not wait for its robots.txt here, the test is about the limits.
        async with polite(respect_robots=True, max_concurrent=1) as crawler:
            crawler.MAX_ROBOTS_RETRIES = 0
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

    async def test_backoff_after_429_holds_back_requests_already_waiting(self, url, site):
        # HTTP 429 says the whole site is overloaded. The retry of /busy/0
        # waits 0.2..0.4 s; the pages had booked their turns before the
        # failure, and they wait for the retry too.
        async with polite(
            requests_per_second=10, retry_strategy=RetryStrategy(max_retries=1, base_delay=0.4, max_delay=0.4)
        ) as crawler:
            await open_session(crawler, url, site)
            starts = record_starts(crawler)
            await crawler.fetch_many([url("/busy/0"), url("/site/"), url("/site/a.html"), url("/site/b.html")])

        # The first attempt fails at once; its retry and the pages wait out the 0.2..0.4 s pause.
        assert site.log[0][0] == "/busy/0"
        assert len(starts) == 5
        assert all(start - starts[0] >= 0.2 - EPSILON for start in starts[1:])
        assert min(gaps(starts)) >= 0.1 - EPSILON

    async def test_backoff_after_a_server_error_holds_back_the_page_only(self, url, site):
        # HTTP 503 without a wait asked for (/flaky/1 sends Retry-After: 0) is
        # taken to be about the one page: its retry waits 0.2..0.4 s on its
        # own, and the pages take the next turns of the host meanwhile.
        async with polite(
            requests_per_second=10, retry_strategy=RetryStrategy(max_retries=1, base_delay=0.4, max_delay=0.4)
        ) as crawler:
            await open_session(crawler, url, site)
            starts = record_starts(crawler)
            await crawler.fetch_many([url("/flaky/1"), url("/site/"), url("/site/a.html"), url("/site/b.html")])

        assert [path for path, _ in site.log] == ["/flaky/1", "/site/", "/site/a.html", "/site/b.html", "/flaky/1"]
        assert len(starts) == 5
        assert starts[1] - starts[0] < 0.2 - EPSILON  # the first page did not wait for the pause
        assert starts[4] - starts[0] >= 0.2 - EPSILON  # the retry did
        assert min(gaps(starts)) >= 0.1 - EPSILON

    async def test_failing_pages_do_not_hold_back_the_crawl_of_their_host(self, url, site):
        # Two pages of the host answer HTTP 503 every time, and their retries
        # wait 0.2..0.4 s each. The other pages are crawled meanwhile by the
        # free workers, not after the pauses.
        broken = [url("/flaky/9?page=1"), url("/flaky/9?page=2")]
        pages = [url(f"/site/{name}") for name in ("", "a.html", "b.html", "c.html", "a/deeper.html", "a/deepest.html")]
        options = {"max_concurrent": 5, "max_depth": 0, "retry_strategy": RetryStrategy(max_retries=1, base_delay=0.4)}
        async with polite(**options) as crawler:
            await crawler.crawl([*broken, *pages])

        paths = [path for path, _ in site.log]
        assert paths.count("/flaky/9") == 4
        assert paths[-2:] == ["/flaky/9", "/flaky/9"]  # the retries come after every other page
        first = site.log[0][1]
        assert all(requested - first < 0.2 - EPSILON for path, requested in site.log if path != "/flaky/9")
        assert set(crawler.failed_urls) == set(broken)
        assert set(crawler.processed_urls) == set(pages)
        assert crawler.crawl_stats().retries == 2


class TestRetryAfter:
    async def test_crawl_puts_off_the_pages_of_a_host_that_asked_to_wait(self, url, site):
        # The host asks for 2 s, longer than a retry may wait: the request is
        # not retried, yet the host is left alone for the whole 2 s, and the
        # crawl goes on to another host meanwhile. The page that was asked
        # to wait comes back with the host instead of failing.
        other_host = url("/site/b.html", "localhost")
        options = {"max_concurrent": 1, "max_depth": 0, "retry_strategy": RetryStrategy(max_retries=1, max_delay=0.5)}
        async with polite(**options) as crawler:
            pages = await crawler.crawl([url("/overloaded/1/2"), url("/site/a.html"), other_host])

        (busy, asked), (other, _), *held_back = site.log
        assert [busy, other] == ["/overloaded/1/2", "/site/b.html"]
        assert {path for path, _ in held_back} == {"/overloaded/1/2", "/site/a.html"}
        assert all(requested - asked >= 2 - EPSILON for _, requested in held_back)
        assert crawler.failed_urls == {}
        assert set(pages) == {url("/overloaded/1/2"), url("/site/a.html"), other_host}
        assert crawler.crawl_stats().requests == 4

    async def test_crawl_gives_up_on_a_page_that_keeps_asking_to_wait(self, url, site):
        # Retry-After of 2 s is too long to retry and capped to 0.05 s of
        # holding the host back: the page comes back three times, then fails.
        options = {
            "max_depth": 0,
            "retry_strategy": RetryStrategy(max_retries=1, max_delay=0.5),
            "max_retry_after": 0.05,
        }
        async with polite(**options) as crawler:
            await crawler.crawl([url("/busy/2")])

        assert site.hits["/busy/2"] == 1 + AsyncCrawler.MAX_WAITS_PER_PAGE
        assert crawler.failed_urls == {url("/busy/2"): "TransientHTTPError: HTTP 429 Too Many Requests"}
        assert crawler.crawl_stats().requests == 4


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
        # 503 the first 4 times, then pages; no retries, so every page is one request.
        blocked = [url(f"/flaky/4?page={page}") for page in range(6)]
        other_host = url("/site/b.html", "localhost")
        options = {
            "max_concurrent": 1,
            "max_depth": 0,
            "circuit_breaker": CircuitBreaker(min_requests=3, cooldown=0.05),
        }
        async with polite(**options) as crawler:
            await crawler.crawl([*blocked, other_host], max_pages=7)

        # The third page opens the circuit and is put off with the others,
        # having lost its retries to the breaker; the crawl goes on to the
        # other host.
        assert [path for path, _ in site.log[:4]] == ["/flaky/4"] * 3 + ["/site/b.html"]
        # The first probe, the third page again, fails and opens the circuit
        # again; the second one succeeds.
        assert list(crawler.failed_urls) == blocked[:3]
        assert set(crawler.failed_urls.values()) == {"TransientHTTPError: HTTP 503 Service Unavailable"}
        assert list(crawler.processed_urls) == [other_host, *blocked[3:]]
        assert site.hits["/flaky/4"] == 7
        assert crawler.circuit_breaker.get_stats()["127.0.0.1"].times_opened == 2
        assert crawler.crawl_stats().requests == 8

    async def test_one_broken_page_does_not_open_the_circuit(self, url, site):
        # The review's probe: one page answers 503 every time and is retried
        # three times among the first pages of the crawl. It is one failed
        # request of the host, not four, so the breaker (half of at least 5
        # requests by default) stays closed and the other pages are not put off.
        pages = [url(f"/ok?n={n}") for n in range(9)]
        options = {
            "max_concurrent": 1,
            "max_depth": 0,
            "retry_strategy": RetryStrategy(max_retries=3, base_delay=0.01),
            "circuit_breaker": CircuitBreaker(cooldown=0.05),
        }
        async with polite(**options) as crawler:
            await crawler.crawl([url("/flaky/9"), *pages])

        assert site.hits["/flaky/9"] == 4
        assert list(crawler.failed_urls) == [url("/flaky/9")]
        assert list(crawler.processed_urls) == pages
        circuit = crawler.circuit_breaker.get_stats()["127.0.0.1"]
        assert (circuit.times_opened, circuit.requests, circuit.failures) == (0, 10, 1)
        assert crawler.crawl_stats().retries == 3
