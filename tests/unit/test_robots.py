"""Unit tests for robots.txt parsing (RobotsRules) and fetching with a cache (RobotsParser)."""

import asyncio

import pytest
from helpers import BOT

from crawler import CrawlerClosedError, NetworkError, RobotsParser, RobotsRules, product_token


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
            "groups": [
                {"user_agents": ["*"], "allow": ["/private/open"], "disallow": ["/private/"], "crawl_delay": 2.0}
            ],
        }


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
        ],
    )
    async def test_missing_or_unreachable_robots_txt(self, answer, unreachable):
        robots = RobotsParser(FakeFetcher(answer))
        assert await robots.is_allowed("https://site/page", BOT) is (unreachable is None)
        assert robots.unreachable_reason("https://site/page") == unreachable

    async def test_closed_fetcher_is_reported_and_not_cached(self):
        fetch = FakeFetcher(CrawlerClosedError("https://site/robots.txt", "crawler is closed"))
        robots = RobotsParser(fetch)
        with pytest.raises(CrawlerClosedError):
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
