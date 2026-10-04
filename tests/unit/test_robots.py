"""Unit tests for robots.txt parsing (RobotsRules) and fetching with a cache (RobotsParser)."""

import asyncio
import time

import pytest
from helpers import BOT, FakeClock

from crawler import (
    CertificateError,
    CircuitOpenError,
    CrawlerClosedError,
    DNSError,
    FetchTimeoutError,
    NetworkError,
    RobotsParser,
    RobotsRules,
    TooManyRedirectsError,
    product_token,
)
from crawler.robots import robots_tag_directives


def allowed(robots_txt: str, path: str, user_agent: str = BOT) -> bool:
    return RobotsRules.parse(robots_txt).can_fetch(f"https://site{path}", user_agent)


@pytest.mark.parametrize(
    ("user_agent", "token"),
    [
        ("TestBot/1.0 (+https://example.com/bot)", "testbot"),
        ("Googlebot-Image/1.0", "googlebot-image"),
        ("  my_bot", "my_bot"),
        ("(compatible)", ""),
    ],
)
def test_product_token(user_agent, token):
    assert product_token(user_agent) == token


@pytest.mark.parametrize(
    ("headers", "directives"),
    [
        ([], ()),
        (["NoIndex, nofollow"], ("noindex", "nofollow")),
        (["noindex", "noindex, noarchive"], ("noindex", "noarchive")),
        # For this crawler by name, and for another one.
        (["TestBot: none", "googlebot: noindex"], ("none",)),
        # A directive with a value is not a crawler name.
        (["unavailable_after: 25 Jun 2010 15:00:00 PST"], ("unavailable_after: 25 jun 2010 15:00:00 pst",)),
        (["max-snippet: 20, nofollow"], ("max-snippet: 20", "nofollow")),
    ],
)
def test_robots_tag_directives(headers, directives):
    assert robots_tag_directives(headers, BOT) == directives


class TestGroups:
    ROBOTS = """
        User-agent: *
        Disallow: /private/

        User-agent: testbot
        Disallow: /for-testbot/
    """

    def test_own_group_replaces_the_default_one(self):
        assert allowed(self.ROBOTS, "/private/page")
        assert not allowed(self.ROBOTS, "/for-testbot/page")

    def test_other_agents_use_the_default_group(self):
        assert not allowed(self.ROBOTS, "/private/page", "OtherBot/2.0")
        assert allowed(self.ROBOTS, "/for-testbot/page", "OtherBot/2.0")

    def test_agent_name_is_matched_case_insensitively(self):
        assert not allowed("User-agent: TESTBOT/9\nDisallow: /", "/page", "testBot")

    def test_consecutive_agents_share_a_group(self):
        robots = "User-agent: a-bot\nUser-agent: testbot\nDisallow: /shared/"
        assert not allowed(robots, "/shared/x")
        assert not allowed(robots, "/shared/x", "A-Bot")

    def test_agent_after_rules_starts_a_new_group(self):
        robots = "User-agent: other\nDisallow: /a/\nUser-agent: testbot\nDisallow: /b/"
        assert allowed(robots, "/a/x")
        assert not allowed(robots, "/b/x")

    def test_groups_for_one_agent_are_merged(self):
        robots = """
            User-agent: testbot
            Disallow: /a/

            User-agent: other
            Disallow: /

            User-agent: testbot
            Disallow: /b/
        """
        assert not allowed(robots, "/a/x")
        assert not allowed(robots, "/b/x")
        assert allowed(robots, "/c/x")

    def test_no_matching_group_allows_everything(self):
        assert allowed("User-agent: other\nDisallow: /", "/page")
        assert allowed("", "/page")

    def test_rules_before_any_agent_are_ignored(self):
        assert allowed("Disallow: /\nUser-agent: *\nDisallow: /x", "/page")


class TestMatching:
    def test_longest_match_wins_whatever_the_order(self):
        robots = "User-agent: *\nAllow: /shop/public\nDisallow: /shop"
        assert allowed(robots, "/shop/public/item")
        assert not allowed(robots, "/shop/private")

    def test_allow_wins_a_tie(self):
        robots = "User-agent: *\nDisallow: /page\nAllow: /page"
        assert allowed(robots, "/page")

    def test_empty_disallow_allows_everything(self):
        assert allowed("User-agent: *\nDisallow:", "/anything")

    @pytest.mark.parametrize(
        ("rule", "path", "expected"),
        [
            ("/a*b", "/axxb/c", False),
            ("/a*b", "/ac", True),
            ("/*.pdf$", "/docs/file.pdf", False),
            ("/*.pdf$", "/docs/file.pdf?download=1", True),
            ("/page$", "/page", False),
            ("/page$", "/pages", True),
            ("/*?", "/list?page=2", False),
            ("/*?", "/list", True),
            ("/a.b", "/axb", True),  # a dot is literal
            ("/a$b", "/a$b", False),  # "$" inside a pattern is literal
        ],
    )
    def test_wildcards(self, rule, path, expected):
        assert allowed(f"User-agent: *\nDisallow: {rule}", path) is expected

    def test_real_site_rules(self):
        # Rules published by webscraper.io for its test sites.
        robots = """
            User-agent: *
            Disallow: /test-sites/pagination/
            Disallow: /test-sites/pagination*?page=
        """
        assert allowed(robots, "/test-sites/pagination")
        assert not allowed(robots, "/test-sites/pagination/BMW")
        assert not allowed(robots, "/test-sites/pagination?page=10")

    def test_non_ascii_paths_match_in_either_spelling(self):
        robots = "User-agent: *\nDisallow: /café"
        assert not allowed(robots, "/café/menu")
        assert not allowed(robots, "/caf%C3%A9/menu")

    @pytest.mark.parametrize(("rule", "path"), [("/~joe/", "/%7Ejoe/page"), ("/%7ejoe/", "/~joe/page")])
    def test_escaped_unreserved_characters_match_the_characters(self, rule, path):
        assert not allowed(f"User-agent: *\nDisallow: {rule}", path)

    def test_escaped_wildcards_stay_literal(self):
        robots = "User-agent: *\nDisallow: /a%2Ab$"
        assert not allowed(robots, "/a%2Ab")
        assert allowed(robots, "/axb")

    @pytest.mark.parametrize("end", ["", "$"])
    def test_many_wildcards_on_a_long_url_answer_at_once(self, end):
        robots = "User-agent: *\nDisallow: /" + "*a" * 20 + "X" + end
        began = time.monotonic()
        assert allowed(robots, "/" + "a" * 5000)
        assert time.monotonic() - began < 0.1

    @pytest.mark.parametrize("path", ["/x/../private", "/./private/page", "/x/%2E%2E/private"])
    def test_dot_segments_do_not_get_around_a_rule(self, path):
        # The HTTP client resolves them and sends "/private...".
        assert not allowed("User-agent: *\nDisallow: /private", path)

    def test_robots_txt_itself_is_always_allowed(self):
        assert allowed("User-agent: *\nDisallow: /", "/robots.txt")

    def test_comments_blank_lines_and_unknown_keys_are_ignored(self):
        robots = """
            # The whole site is closed
            USER-AGENT: *   # every crawler
            Host: example.com
            DISALLOW: /     # everything
        """
        assert not allowed(robots, "/page")

    def test_invalid_url_is_not_allowed(self):
        assert RobotsRules.allow_all().can_fetch("ftp://site/", BOT) is False


class TestCrawlDelay:
    def test_crawl_delay_of_own_group_or_default(self):
        rules = RobotsRules.parse("User-agent: *\nCrawl-delay: 2\n\nUser-agent: testbot\nCrawl-delay: 0.5")
        assert rules.crawl_delay(BOT) == 0.5
        assert rules.crawl_delay("OtherBot") == 2.0

    def test_largest_delay_of_merged_groups(self):
        rules = RobotsRules.parse("User-agent: testbot\nCrawl-delay: 1\n\nUser-agent: testbot\nCrawl-delay: 3")
        assert rules.crawl_delay(BOT) == 3.0

    def test_largest_crawl_delay_of_one_group_wins(self):
        rules = RobotsRules.parse("User-agent: *\nCrawl-delay: 10\nCrawl-delay: 1")
        assert rules.crawl_delay(BOT) == 10.0

    @pytest.mark.parametrize("value", ["", "soon", "-1", "nan", "inf"])
    def test_invalid_crawl_delay_is_ignored(self, value):
        assert RobotsRules.parse(f"User-agent: *\nCrawl-delay: {value}").crawl_delay(BOT) is None

    def test_to_dict(self):
        rules = RobotsRules.parse(
            """
            User-agent: *
            Disallow: /private/
            Allow: /private/open
            Crawl-delay: 2
            """
        )
        assert rules.to_dict() == {
            "unreachable": None,
            "sitemaps": [],
            "groups": [
                {"user_agents": ["*"], "allow": ["/private/open"], "disallow": ["/private/"], "crawl_delay": 2.0}
            ],
        }


class TestSitemaps:
    def test_sitemap_lines_are_collected(self):
        rules = RobotsRules.parse(
            """
            Sitemap: https://site/sitemap.xml
            User-agent: *
            Disallow: /private/
            sitemap: HTTPS://Site/news.xml  # the latest posts
            """
        )
        assert rules.sitemaps == ["https://site/sitemap.xml", "https://site/news.xml"]
        assert rules.to_dict()["sitemaps"] == rules.sitemaps

    def test_sitemap_line_belongs_to_no_group(self):
        robots = "User-agent: other\nSitemap: https://site/sitemap.xml\nUser-agent: testbot\nDisallow: /shared/"
        assert not allowed(robots, "/shared/x", "Other/1.0")
        assert RobotsRules.parse(robots).sitemaps == ["https://site/sitemap.xml"]

    def test_repeated_and_invalid_sitemaps_are_dropped(self):
        robots = "Sitemap: https://site/sitemap.xml\nSitemap: /sitemap.xml\nSitemap:\nSitemap: https://site/sitemap.xml"
        assert RobotsRules.parse(robots).sitemaps == ["https://site/sitemap.xml"]

    def test_no_sitemaps_without_a_file(self):
        assert RobotsRules.allow_all().sitemaps == []
        assert RobotsRules.forbid_all("HTTP 503").sitemaps == []


class FakeFetcher:
    """Serves one robots.txt answer per URL and counts downloads."""

    def __init__(self, answer: tuple[int, str] | BaseException = (200, "")) -> None:
        self.answer = answer
        self.requested: list[str] = []
        self.latency = 0.0

    async def __call__(self, url: str) -> tuple[int, str]:
        self.requested.append(url)
        await asyncio.sleep(self.latency)
        if isinstance(self.answer, BaseException):
            raise self.answer
        return self.answer


class TestRobotsParser:
    async def test_fetches_robots_txt_of_the_origin_once(self):
        fetch = FakeFetcher((200, "User-agent: *\nDisallow: /private/\nCrawl-delay: 2"))
        robots = RobotsParser(fetch)

        rules = await robots.fetch_robots("https://user@Site:443/some/page?q=1")
        await robots.fetch_robots("https://site/other")

        assert fetch.requested == ["https://site/robots.txt"]
        assert rules["groups"][0]["disallow"] == ["/private/"]
        assert not robots.can_fetch("https://site/private/x", BOT)
        assert robots.can_fetch("https://site/public", BOT)
        assert robots.get_crawl_delay("https://site/", BOT) == 2.0

    async def test_sitemaps_of_a_site(self):
        robots = RobotsParser(FakeFetcher((200, "Sitemap: https://site/sitemap.xml\nUser-agent: *\nDisallow:")))
        rules = await robots.fetch_robots("https://site/")
        assert rules["sitemaps"] == ["https://site/sitemap.xml"]

    async def test_each_origin_has_its_own_rules(self):
        fetch = FakeFetcher()
        robots = RobotsParser(fetch)
        for url in ("http://site/", "https://site/", "https://site:8443/", "https://other/"):
            await robots.fetch_robots(url)
        assert fetch.requested == [
            "http://site/robots.txt",
            "https://site/robots.txt",
            "https://site:8443/robots.txt",
            "https://other/robots.txt",
        ]

    async def test_concurrent_requests_share_one_download(self):
        fetch = FakeFetcher((200, "User-agent: *\nDisallow: /x"))
        fetch.latency = 0.01
        robots = RobotsParser(fetch)

        verdicts = await asyncio.gather(*(robots.is_allowed(f"https://site/{name}", BOT) for name in "abxyz"))

        assert verdicts == [True, True, False, True, True]
        assert len(fetch.requested) == 1

    async def test_cancelled_caller_does_not_cancel_the_shared_download(self):
        fetch = FakeFetcher((200, "User-agent: *\nDisallow: /"))
        fetch.latency = 0.01
        robots = RobotsParser(fetch)
        first = asyncio.create_task(robots.is_allowed("https://site/a", BOT))
        second = asyncio.create_task(robots.is_allowed("https://site/b", BOT))
        await asyncio.sleep(0)
        first.cancel()

        assert await second is False
        assert first.cancelled()
        assert len(fetch.requested) == 1

    @pytest.mark.parametrize(
        ("answer", "unreachable"),
        [
            ((200, ""), None),
            ((404, ""), None),  # no robots.txt: no rules
            ((401, ""), None),
            ((403, ""), None),
            ((429, ""), "HTTP 429"),  # the site asks to back off: treated as unreachable
            ((500, ""), "HTTP 500"),
            ((503, ""), "HTTP 503"),
            (NetworkError("https://site/robots.txt", "connection refused"), "NetworkError: connection refused"),
            # A redirect loop counts as no robots.txt (RFC 9309 2.3.1.2).
            (TooManyRedirectsError("https://site/robots.txt", "too many redirects (10)"), None),
        ],
    )
    async def test_missing_or_unreachable_robots_txt(self, answer, unreachable):
        robots = RobotsParser(FakeFetcher(answer))
        assert await robots.is_allowed("https://site/page", BOT) is (unreachable is None)
        assert robots.unreachable_reason("https://site/page") == unreachable

    async def test_unreachable_robots_txt_is_fetched_again_after_a_while(self):
        fetch = FakeFetcher((503, ""))
        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)
        assert not await robots.is_allowed("https://site/page", BOT)

        clock.now += RobotsParser.UNREACHABLE_TTL - 1
        assert not await robots.is_allowed("https://site/page", BOT)
        assert len(fetch.requested) == 1

        clock.now += 1
        fetch.answer = (200, "")
        assert await robots.is_allowed("https://site/page", BOT)
        assert robots.unreachable_reason("https://site/page") is None
        assert len(fetch.requested) == 2

    async def test_unreachable_for_is_the_time_until_the_next_download(self):
        fetch = FakeFetcher((503, ""))
        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)
        assert robots.unreachable_for("https://site/page") == 0
        await robots.fetch_robots("https://site/")
        assert robots.unreachable_for("https://site/page") == RobotsParser.UNREACHABLE_TTL
        assert robots.unreachable_for("https://other/page") == 0

        clock.now += RobotsParser.UNREACHABLE_TTL + 5
        assert robots.unreachable_for("https://site/page") == 0
        fetch.answer = (200, "")
        await robots.fetch_robots("https://site/")
        assert robots.unreachable_for("https://site/page") == 0

    async def test_failed_downloads_counts_an_outage(self):
        fetch = FakeFetcher((503, ""))
        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)
        assert robots.failed_downloads("https://site/page") == 0
        await robots.fetch_robots("https://site/")
        assert robots.failed_downloads("https://site/page") == 1
        assert robots.failed_downloads("https://other/page") == 0

        clock.now += RobotsParser.UNREACHABLE_TTL
        await robots.fetch_robots("https://site/")
        assert robots.failed_downloads("https://site/page") == 2

        clock.now += RobotsParser.UNREACHABLE_TTL
        fetch.answer = (200, "")
        await robots.fetch_robots("https://site/")
        assert robots.failed_downloads("https://site/page") == 0

    @pytest.mark.parametrize(
        ("answer", "recoverable"),
        [
            ((503, ""), True),
            ((429, ""), True),
            (FetchTimeoutError("https://site/robots.txt", "read timeout (20.0s)"), True),
            (NetworkError("https://site/robots.txt", "connection refused"), True),
            (DNSError("https://site/robots.txt", "ClientConnectorDNSError: Cannot connect to host site:443"), False),
            (CertificateError("https://site/robots.txt", "certificate verify failed"), False),
            ((200, ""), True),
        ],
    )
    async def test_may_recover_tells_an_outage_from_a_failure_for_good(self, answer, recoverable):
        robots = RobotsParser(FakeFetcher(answer))
        await robots.fetch_robots("https://site/")
        assert robots.may_recover("https://site/page") is recoverable

    async def test_robots_txt_that_was_read_is_kept(self):
        fetch = FakeFetcher((200, "User-agent: *\nDisallow: /private/"))
        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)
        await robots.fetch_robots("https://site/")

        clock.now += 10 * RobotsParser.UNREACHABLE_TTL
        assert await robots.is_allowed("https://site/page", BOT)
        assert len(fetch.requested) == 1

    async def test_old_rules_answer_while_robots_txt_is_fetched_again(self):
        fetch = FakeFetcher((503, ""))
        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)
        await robots.fetch_robots("https://site/")
        clock.now += RobotsParser.UNREACHABLE_TTL
        fetch.answer, fetch.latency = (200, ""), 0.01

        downloads = [asyncio.create_task(robots.is_allowed(f"https://site/{page}", BOT)) for page in "ab"]
        await asyncio.sleep(0)
        assert robots.can_fetch("https://site/a", BOT) is False

        # The first caller made the download and waited for it; the second
        # one got the old rules at once instead of waiting too.
        assert await asyncio.gather(*downloads) == [True, False]
        assert len(fetch.requested) == 2

    @pytest.mark.parametrize(
        "error",
        [
            CrawlerClosedError("https://site/robots.txt", "crawler is closed"),
            CircuitOpenError("https://site/robots.txt", "circuit breaker of site is open"),
        ],
    )
    async def test_request_not_sent_is_reported_and_not_cached(self, error):
        fetch = FakeFetcher(error)
        robots = RobotsParser(fetch)
        with pytest.raises(type(error)):
            await robots.is_allowed("https://site/page", BOT)

        fetch.answer = (200, "")
        assert await robots.is_allowed("https://site/page", BOT)
        assert len(fetch.requested) == 2

    async def test_crawl_delay_is_capped(self):
        robots = RobotsParser(FakeFetcher((200, "User-agent: *\nCrawl-delay: 86400")))
        await robots.fetch_robots("https://site/")
        assert robots.get_crawl_delay("https://site/") == RobotsParser.MAX_CRAWL_DELAY

    async def test_no_crawl_delay_is_zero(self):
        robots = RobotsParser(FakeFetcher())
        await robots.fetch_robots("https://site/")
        assert robots.get_crawl_delay("https://site/") == 0.0

    async def test_oversized_file_is_cut(self):
        padding = "#" * RobotsParser.MAX_SIZE
        robots = RobotsParser(FakeFetcher((200, f"User-agent: *\n{padding}\nDisallow: /")))
        assert await robots.is_allowed("https://site/page", BOT)

    def test_rules_must_be_fetched_first(self):
        robots = RobotsParser(FakeFetcher())
        with pytest.raises(LookupError, match="https://site"):
            robots.can_fetch("https://site/page")
        with pytest.raises(LookupError):
            robots.get_crawl_delay("https://site/page")

    async def test_invalid_url_is_rejected(self):
        with pytest.raises(ValueError, match="not an absolute"):
            await RobotsParser(FakeFetcher()).fetch_robots("site/page")


class TestWaitingForDownloads:
    async def test_only_the_caller_that_downloads_again_waits_for_it(self):
        # While an unreachable robots.txt is downloaded again, the others
        # that ask get the stale rules at once: a site that is slow to fail
        # holds one task back, not every one that asks about it.
        clock = FakeClock()
        answered = asyncio.Event()
        requested: list[str] = []

        async def fetch(url: str) -> tuple[int, str]:
            requested.append(url)
            if len(requested) == 1:
                return 503, ""
            await answered.wait()
            return 200, ""

        robots = RobotsParser(fetch, clock=clock)
        assert await robots.is_allowed("https://site/a", BOT) is False
        clock.now += RobotsParser.UNREACHABLE_TTL

        first = asyncio.create_task(robots.is_allowed("https://site/a", BOT))
        await asyncio.sleep(0)  # the download is under way
        assert await asyncio.wait_for(robots.is_allowed("https://site/b", BOT), 1) is False
        assert robots.unreachable_reason("https://site/b") == "HTTP 503"
        assert not first.done()
        answered.set()

        assert await first is True
        assert await robots.is_allowed("https://site/b", BOT) is True
        assert requested == ["https://site/robots.txt"] * 2

    async def test_a_site_given_up_on_is_not_downloaded_again_until_the_outages_are_forgotten(self):
        fetch = FakeFetcher((503, ""))
        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)
        await robots.fetch_robots("https://site/")
        robots.give_up("https://site/page")
        clock.now += RobotsParser.UNREACHABLE_TTL * 10

        assert robots.unreachable_for("https://site/page") == 0
        assert await robots.is_allowed("https://site/page", BOT) is False
        assert len(fetch.requested) == 1

        robots.forget_outages()
        fetch.answer = (200, "")
        assert robots.failed_downloads("https://site/page") == 0
        assert await robots.is_allowed("https://site/page", BOT) is True
        assert len(fetch.requested) == 2

    async def test_giving_up_leaves_a_robots_txt_that_was_read_alone(self):
        fetch = FakeFetcher((200, "User-agent: *\nDisallow: /x"))
        robots = RobotsParser(fetch)
        await robots.fetch_robots("https://site/")
        robots.give_up("https://site/page")
        robots.give_up("https://other/page")  # not fetched yet: nothing to keep

        assert robots.can_fetch("https://site/x", BOT) is False
        assert await robots.is_allowed("https://other/y", BOT) is True
        assert fetch.requested == ["https://site/robots.txt", "https://other/robots.txt"]

    async def test_a_download_that_finished_as_the_wait_ran_out_answers(self, monkeypatch):
        # The timer of the wait may fire in the iteration of the event loop
        # in which the download finished: asyncio then reports a timeout
        # for a future that is done, and the rules are in the cache.
        wait_for = asyncio.wait_for

        async def wait_for_and_time_out(awaitable, timeout):
            result = await wait_for(awaitable, timeout)
            assert result is not None
            raise TimeoutError

        monkeypatch.setattr(asyncio, "wait_for", wait_for_and_time_out)
        requested: list[str] = []

        async def fetch(url: str) -> tuple[int, str]:
            requested.append(url)
            await asyncio.sleep(0)
            return (200, "User-agent: *\nDisallow: /x") if len(requested) == 1 else (503, "")

        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)

        assert await robots.is_allowed("https://site/x", BOT, wait=1) is False
        assert await robots.is_allowed("https://site/y", BOT, wait=1) is True
        # The same for a download that found the site unreachable: it is cached for a while.
        assert await robots.is_allowed("https://other/y", BOT, wait=1) is False
        assert robots.unreachable_reason("https://other/y") == "HTTP 503"
        assert requested == ["https://site/robots.txt", "https://other/robots.txt"]

    async def test_a_caller_may_wait_for_a_download_only_so_long(self):
        # The download goes on for the cache: the next caller finds it there.
        answered = asyncio.Event()
        requested: list[str] = []

        async def fetch(url: str) -> tuple[int, str]:
            requested.append(url)
            await answered.wait()
            return 200, "User-agent: *\nDisallow: /x"

        clock = FakeClock()
        robots = RobotsParser(fetch, clock=clock)
        with pytest.raises(TimeoutError):
            await robots.is_allowed("https://site/x", BOT, wait=0.01)
        assert robots.may_recover("https://site/x")  # not fetched yet: nothing says it is down
        # The time is the download's, not every caller's: once it is up,
        # the next caller is turned away at once, however long it would wait.
        clock.now += 1
        async with asyncio.timeout(1):
            with pytest.raises(TimeoutError, match="downloading for over 0.5s"):
                await robots.is_allowed("https://site/x", BOT, wait=0.5)
        answered.set()
        await asyncio.sleep(0)

        assert await robots.is_allowed("https://site/x", BOT, wait=0.01) is False
        assert await robots.is_allowed("https://site/y", BOT) is True
        assert requested == ["https://site/robots.txt"]
