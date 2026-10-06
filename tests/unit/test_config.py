"""Unit tests for the configuration: defaults, YAML and JSON files, overrides and validation."""

import inspect
import json
import sys
from pathlib import Path

import pytest
import yaml

from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    CompositeStorage,
    ConfigError,
    CrawlerConfig,
    CSVStorage,
    JSONStorage,
    ProxyPool,
    Rendering,
    RetryStrategy,
    load_config,
)
from crawler.config import (
    EXCLUDED_EXTENSIONS,
    CircuitBreakerOptions,
    CrawlOptions,
    FilterOptions,
    LoggingOptions,
    ProxyOptions,
    RenderingOptions,
    ReportOptions,
    RetryOptions,
    SessionOptions,
    SitemapOptions,
    StorageOptions,
)

EXAMPLE = Path(__file__).parents[2] / "config.example.yaml"

FULL = {
    "urls": ["https://example.com/", "https://example.org/docs"],
    "sitemaps": {"urls": ["https://example.com/sitemap.xml"], "from_robots": True, "max_urls": 200},
    "crawler": {
        "max_pages": 500,
        "max_pages_per_host": 50,
        "max_depth": 3,
        "max_concurrent": 20,
        "max_per_domain": 4,
        "rate_limit": 2.5,
        "per_domain_rate": False,
        "min_delay": 0.2,
        "jitter": 0.1,
        "respect_robots": True,
        "user_agent": "MyBot/1.0 (+https://example.com/bot)",
        "user_agents": ["MyBot/1.0 (a)", "MyBot/1.0 (b)"],
        "total_timeout": 60.0,
        "connect_timeout": 5.0,
        "read_timeout": 15.0,
        "timeout_growth": 2.0,
        "max_page_size": 1_000_000,
        "max_parsing": 4,
        "max_retry_after": 120.0,
        "keep_pages": False,
    },
    "retry": {"max_retries": 5, "backoff_factor": 3.0, "base_delay": 0.5, "max_delay": 10.0},
    "circuit_breaker": {"failure_threshold": 0.8, "min_requests": 10, "window": 120.0, "cooldown": 15.0},
    "filters": {
        "same_domain_only": True,
        "include": ["^https://example\\.com/blog/"],
        "exclude": ["\\.pdf$"],
        "exclude_extensions": ["zip", "mp4"],
    },
    "session": {
        "keep_cookies": True,
        "cookies": [{"name": "sid", "value": "s3cr3t", "domain": ".example.com", "path": "/app", "secure": True}],
        "cookies_file": "cookies.txt",
        "save_cookies": "saved-cookies.txt",
        "headers": {"Accept-Language": "en", "Authorization": "Bearer t0ken"},
    },
    "proxy": {
        "urls": ["http://user:pr0xyp4ss@proxy-1.example:3128", "https://proxy-2.example:8443"],
        "rotation": "per_request",
        "from_env": False,
        "max_failures": 5,
        "cooldown": 30.0,
    },
    "rendering": {
        "mode": "patterns",
        "include": ["^https://example\\.com/app/"],
        "wait_until": "networkidle",
        "wait_for": "#content",
        "timeout": 10.0,
        "max_open_pages": 4,
        "block_resources": ["image", "stylesheet"],
    },
    "storage": {
        "outputs": ["pages.jsonl", "pages.csv"],
        "batch_size": 50,
        "csv_encoding": "utf-8-sig",
        "overwrite": True,
    },
    "logging": {"level": "DEBUG", "file": "crawler.log", "max_bytes": 1000, "backup_count": 2},
    "report": {"stats_json": "stats.json", "html": "report.html", "title": "Blog crawl", "top_domains": 5},
}


def write(tmp_path: Path, text: str, name: str = "config.yaml") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def problems(data: object) -> list[str]:
    with pytest.raises(ConfigError) as error:
        CrawlerConfig.from_dict(data)
    return error.value.problems


class TestDefaults:
    def test_empty_mapping_gives_the_defaults(self):
        config = CrawlerConfig.from_dict({})

        assert config == CrawlerConfig()
        assert config.urls == ()
        assert config.crawler == CrawlOptions(
            max_pages=100,
            max_pages_per_host=None,
            max_depth=2,
            max_concurrent=10,
            max_per_domain=None,
            rate_limit=1.0,
            respect_robots=True,
        )
        assert config.crawler.user_agent == AsyncCrawler.DEFAULT_USER_AGENT
        assert config.sitemaps == SitemapOptions(urls=(), from_robots=False, max_urls=50_000)
        assert config.retry == RetryOptions(max_retries=3, backoff_factor=2.0, base_delay=1.0, max_delay=30.0)
        assert config.circuit_breaker == CircuitBreakerOptions(
            failure_threshold=0.5, min_requests=5, window=60.0, cooldown=30.0
        )
        assert config.filters == FilterOptions(
            same_domain_only=True, include=(), exclude=(), exclude_extensions=EXCLUDED_EXTENSIONS
        )
        assert {"pdf", "jpg", "zip", "mp4"} <= set(EXCLUDED_EXTENSIONS)
        assert config.storage == StorageOptions(outputs=(), batch_size=100, csv_encoding="utf-8", overwrite=False)
        assert config.logging == LoggingOptions(level="INFO", file=None, max_bytes=10 * 1024 * 1024, backup_count=5)
        assert config.report == ReportOptions(stats_json=None, html=None, title="Crawl report", top_domains=10)
        assert config.session == SessionOptions(
            keep_cookies=True, cookies=(), cookies_file=None, save_cookies=None, headers={}
        )
        assert config.proxy == ProxyOptions(urls=(), rotation="per_host", from_env=False, max_failures=3, cooldown=60.0)
        assert config.rendering == RenderingOptions(
            mode="off",
            include=(),
            wait_until="load",
            wait_for=None,
            timeout=30.0,
            max_open_pages=2,
            block_resources=("image", "font", "media"),
        )

    def test_defaults_are_those_of_the_components(self):
        """The crawler built without a configuration and with an empty one behave the same."""

        def defaults(function) -> dict:
            return {name: parameter.default for name, parameter in inspect.signature(function).parameters.items()}

        crawler, crawl = defaults(AsyncCrawler.__init__), defaults(AsyncCrawler.crawl)
        options = CrawlOptions()
        assert options.max_pages == crawl["max_pages"]
        assert options.max_pages_per_host == crawl["max_pages_per_host"]
        assert options.rate_limit == crawler["requests_per_second"]
        for name in ("max_depth", "max_concurrent", "max_per_domain", "per_domain_rate", "min_delay", "jitter"):
            assert getattr(options, name) == crawler[name], name
        for name in (
            "respect_robots",
            "total_timeout",
            "connect_timeout",
            "read_timeout",
            "timeout_growth",
            "max_page_size",
            "max_parsing",
            "max_retry_after",
            "keep_pages",
        ):
            assert getattr(options, name) == crawler[name], name
        retry, breaker = defaults(RetryStrategy.__init__), defaults(CircuitBreaker.__init__)
        assert RetryOptions() == RetryOptions(**{name: retry[name] for name in RetryOptions.__dataclass_fields__})
        assert CircuitBreakerOptions() == CircuitBreakerOptions(
            **{name: breaker[name] for name in CircuitBreakerOptions.__dataclass_fields__}
        )
        # Differ on purpose: a crawl by the configuration stays on the start hosts and leaves files alone,
        # the library follows every link.
        assert crawl["same_domain_only"] is False
        assert crawl["exclude_extensions"] == ()
        assert SitemapOptions().from_robots == crawl["robots_sitemaps"]
        pool = defaults(ProxyPool.__init__)
        for name in ("rotation", "max_failures", "cooldown"):
            assert getattr(ProxyOptions(), name) == pool[name], name
        assert RenderingOptions(mode="always").build() == Rendering()

    def test_configuration_cannot_be_changed(self):
        config = CrawlerConfig()
        with pytest.raises(AttributeError):
            config.crawler.max_pages = 5

    def test_example_file_lists_every_key_with_its_default(self):
        config = load_config(EXAMPLE)

        assert config.urls == ("https://example.com/",)
        assert config.to_dict() == CrawlerConfig().to_dict() | {"urls": ["https://example.com/"]}
        # Every key is written out, so the file doubles as the reference.
        written = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
        expected = CrawlerConfig().to_dict()
        assert written.keys() == expected.keys()
        for section, keys in expected.items():
            if isinstance(keys, dict):
                assert written[section].keys() == keys.keys(), section


class TestValues:
    def test_every_key_is_read(self):
        config = CrawlerConfig.from_dict(FULL)

        assert config.to_dict() == FULL
        assert config.urls == ("https://example.com/", "https://example.org/docs")
        assert config.crawler.max_per_domain == 4
        assert config.filters.exclude == ("\\.pdf$",)
        assert config.filters.exclude_extensions == ("zip", "mp4")

    def test_to_dict_can_be_read_back_and_written_as_json(self):
        config = CrawlerConfig.from_dict(FULL)

        assert CrawlerConfig.from_dict(json.loads(json.dumps(config.to_dict()))) == config

    def test_whole_number_is_taken_for_a_fraction(self):
        config = CrawlerConfig.from_dict({"crawler": {"rate_limit": 2, "total_timeout": 45}})

        assert (config.crawler.rate_limit, config.crawler.total_timeout) == (2.0, 45.0)
        assert isinstance(config.crawler.rate_limit, float)

    def test_null_turns_a_limit_off(self):
        config = CrawlerConfig.from_dict(
            {
                "crawler": {"rate_limit": None, "max_per_domain": None, "max_page_size": None},
                "circuit_breaker": {"failure_threshold": None},
            }
        )

        assert config.crawler.rate_limit is None
        assert config.crawler.max_page_size is None
        assert config.circuit_breaker.failure_threshold is None

    def test_section_without_keys_keeps_its_defaults(self):
        assert CrawlerConfig.from_dict({"crawler": None, "filters": {}}) == CrawlerConfig()

    def test_user_agent_loses_the_space_around_it(self):
        # A folded YAML scalar (`user_agent: >`) ends with a line break, which no header may hold.
        data = {
            "crawler": {"user_agent": "MyBot/1.0 (+https://example.com/bot)\n", "user_agents": [" MyBot/1.0 (a)\n"]}
        }

        options = CrawlerConfig.from_dict(data).crawler

        assert options.user_agent == "MyBot/1.0 (+https://example.com/bot)"
        assert options.user_agents == ("MyBot/1.0 (a)",)

    def test_log_level_in_any_case(self):
        assert CrawlerConfig.from_dict({"logging": {"level": "debug"}}).logging.level == "DEBUG"


class TestStorage:
    def test_no_outputs_no_storage(self):
        assert CrawlerConfig().storage.build() is None

    def test_one_output(self):
        storage = StorageOptions(outputs=("pages.json",), batch_size=7).build()

        assert isinstance(storage, JSONStorage)
        assert (storage.path, storage.indent, storage.batch_size) == (Path("pages.json"), 2, 7)

    def test_several_outputs_are_written_together(self):
        options = StorageOptions(outputs=("pages.jsonl", "pages.csv"), batch_size=7, csv_encoding="utf-8-sig")
        storage = options.build()

        assert isinstance(storage, CompositeStorage)
        first, second = storage.storages
        assert isinstance(first, JSONStorage) and isinstance(second, CSVStorage)
        assert (second.encoding, first.batch_size, second.batch_size) == ("utf-8-sig", 7, 7)

    def test_overwrite_reaches_the_files(self):
        storage = StorageOptions(outputs=("pages.jsonl", "pages.csv", "pages.db"), overwrite=True).build()

        first, second, _ = storage.storages
        assert first.overwrite is True and second.overwrite is True

    def test_validation_opens_nothing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        CrawlerConfig.from_dict({"storage": {"outputs": ["pages.jsonl", "pages.db", "sqlite:///other.db"]}})

        assert list(tmp_path.iterdir()) == []


class TestInvalid:
    def test_unknown_key_suggests_the_close_one(self):
        assert problems({"crawler": {"max_page": 5}}) == ['crawler.max_page: unknown key; did you mean "max_pages"?']

    def test_unknown_key_without_a_close_one_lists_the_keys(self):
        assert problems({"filters": {"zzz": 1}}) == [
            "filters.zzz: unknown key; expected one of same_domain_only, include, exclude, exclude_extensions"
        ]

    def test_unknown_section(self):
        assert problems({"crawlers": {}}) == ['crawlers: unknown key; did you mean "crawler"?']

    @pytest.mark.parametrize(
        ("data", "problem"),
        [
            ({"crawler": {"max_pages": "ten"}}, 'crawler.max_pages: expected a whole number, got "ten"'),
            ({"crawler": {"max_pages": 2.5}}, "crawler.max_pages: expected a whole number, got 2.5"),
            ({"crawler": {"max_pages": True}}, "crawler.max_pages: expected a whole number, got true"),
            ({"crawler": {"max_pages": None}}, "crawler.max_pages: expected a whole number, got null"),
            ({"crawler": {"rate_limit": "fast"}}, 'crawler.rate_limit: expected a number, got "fast"'),
            ({"crawler": {"rate_limit": False}}, "crawler.rate_limit: expected a number, got false"),
            ({"crawler": {"total_timeout": float("inf")}}, "crawler.total_timeout: expected a number, got inf"),
            ({"crawler": {"respect_robots": "yes"}}, 'crawler.respect_robots: expected true or false, got "yes"'),
            ({"crawler": {"respect_robots": 1}}, "crawler.respect_robots: expected true or false, got 1"),
            ({"crawler": {"user_agent": 5}}, "crawler.user_agent: expected a string, got 5"),
            ({"urls": "https://example.com"}, 'urls: expected a list, got "https://example.com"'),
            ({"urls": [5]}, "urls[0]: expected a string, got 5"),
            ({"crawler": [1]}, "crawler: expected a mapping of keys to values, got a list"),
            ({"filters": {"include": {"a": 1}}}, "filters.include: expected a list, got a mapping"),
        ],
    )
    def test_wrong_type(self, data, problem):
        assert problems(data) == [problem]

    @pytest.mark.parametrize(
        ("data", "problem"),
        [
            ({"crawler": {"max_pages": 0}}, "crawler.max_pages: must be >= 1, got 0"),
            ({"crawler": {"max_pages_per_host": 0}}, "crawler.max_pages_per_host: must be >= 1, got 0"),
            ({"crawler": {"max_depth": -1}}, "crawler.max_depth: must be >= 0, got -1"),
            ({"crawler": {"max_concurrent": 0}}, "crawler.max_concurrent: must be >= 1, got 0"),
            ({"crawler": {"max_per_domain": 0}}, "crawler.max_per_domain: must be >= 1, got 0"),
            ({"crawler": {"rate_limit": 0}}, "crawler.rate_limit: must be > 0, got 0.0"),
            ({"crawler": {"jitter": -0.5}}, "crawler.jitter: must be >= 0, got -0.5"),
            ({"crawler": {"read_timeout": 0}}, "crawler.read_timeout: must be > 0, got 0.0"),
            ({"crawler": {"timeout_growth": 0.5}}, "crawler.timeout_growth: must be >= 1, got 0.5"),
            ({"crawler": {"max_retry_after": 0}}, "crawler.max_retry_after: must be > 0, got 0.0"),
            ({"crawler": {"user_agent": "  "}}, 'crawler.user_agent: must not be empty, got ""'),
            ({"sitemaps": {"max_urls": 0}}, "sitemaps.max_urls: must be >= 1, got 0"),
            ({"retry": {"max_retries": -1}}, "retry.max_retries: must be >= 0, got -1"),
            ({"retry": {"backoff_factor": 0.9}}, "retry.backoff_factor: must be >= 1, got 0.9"),
            ({"circuit_breaker": {"failure_threshold": 0}}, "circuit_breaker.failure_threshold: must be > 0, got 0.0"),
            (
                {"circuit_breaker": {"failure_threshold": 1.5}},
                "circuit_breaker.failure_threshold: must be <= 1, got 1.5",
            ),
            ({"storage": {"batch_size": 0}}, "storage.batch_size: must be >= 1, got 0"),
            (
                {"logging": {"level": "LOUD"}},
                'logging.level: expected one of DEBUG, INFO, WARNING, ERROR, CRITICAL, got "LOUD"',
            ),
            ({"logging": {"backup_count": -1}}, "logging.backup_count: must be >= 0, got -1"),
            ({"report": {"top_domains": 0}}, "report.top_domains: must be >= 1, got 0"),
            ({"report": {"html": ""}}, 'report.html: must not be empty, got ""'),
        ],
    )
    def test_value_out_of_limits(self, data, problem):
        assert problems(data) == [problem]

    def test_invalid_urls_are_named_by_position(self):
        assert problems({"urls": ["https://ok.example/", "ftp://files.example/", "example.com"]}) == [
            'urls[1]: expected an http:// or https:// URL, got "ftp://files.example/"',
            'urls[2]: expected an http:// or https:// URL, got "example.com"',
        ]
        assert problems({"sitemaps": {"urls": ["sitemap.xml"]}}) == [
            'sitemaps.urls[0]: expected an http:// or https:// URL, got "sitemap.xml"'
        ]

    def test_urls_with_spaces_inside_are_invalid(self):
        # Valid for a client, which sends the space as %20, but not what was meant.
        assert problems({"urls": ["https://a.example/ # home", " https://b.example/ "]}) == [
            'urls[0]: a URL cannot contain spaces or control characters (a space is written %20), got "https://a.example/ # home"'
        ]
        assert problems({"sitemaps": {"urls": ["https://a.example/site\tmap.xml"]}}) == [
            'sitemaps.urls[0]: a URL cannot contain spaces or control characters (a space is written %20), got "https://a.example/site\\tmap.xml"'
        ]

    def test_invalid_pattern(self):
        (problem,) = problems({"filters": {"exclude": ["\\.pdf$", "("]}})

        assert problem.startswith("filters.exclude[1]: not a regular expression: ")
        assert problem.endswith(', got "("')

    def test_file_extensions_are_normalized(self):
        config = CrawlerConfig.from_dict({"filters": {"exclude_extensions": [".PDF", " Zip "]}})

        assert config.filters.exclude_extensions == ("pdf", "zip")
        assert CrawlerConfig.from_dict({"filters": {"exclude_extensions": []}}).filters.exclude_extensions == ()

    def test_invalid_file_extension(self):
        assert problems({"filters": {"exclude_extensions": ["pdf", "tar.gz", "", "a/b"]}}) == [
            "filters.exclude_extensions[1]: expected one extension without dots, such as 'pdf' or 'gz', got \"tar.gz\"",
            "filters.exclude_extensions[2]: expected one extension without dots, such as 'pdf' or 'gz', got \"\"",
            "filters.exclude_extensions[3]: expected one extension without dots, such as 'pdf' or 'gz', got \"a/b\"",
        ]

    def test_unknown_output_extension(self):
        (problem,) = problems({"storage": {"outputs": ["pages.jsonl", "pages.xml"]}})

        assert problem.startswith(
            'storage.outputs[1]: Cannot choose a storage for "pages.xml": unknown extension ".xml"'
        )

    def test_unknown_database_does_not_show_the_password(self):
        (problem,) = problems({"storage": {"outputs": ["mysql://crawler:secret@localhost/crawler"]}})

        assert problem.startswith('storage.outputs[0]: Cannot choose a database by the URL: unknown scheme "mysql"')
        assert "secret" not in problem

    def test_unknown_csv_encoding(self):
        assert problems({"storage": {"csv_encoding": "utf-99"}}) == [
            'storage.csv_encoding: unknown encoding, got "utf-99"'
        ]

    @pytest.mark.parametrize("encoding", ["undefined", "utf-8\0"])
    def test_csv_encoding_that_cannot_be_used(self, encoding):
        (problem,) = problems({"storage": {"csv_encoding": encoding}})

        assert problem.startswith("storage.csv_encoding: unknown encoding, got ")

    @pytest.mark.parametrize(
        "data, key",
        [
            ({"storage": {"outputs": ["~no-such-user-here/pages.jsonl"]}}, "storage.outputs[0]"),
            ({"logging": {"file": "~no-such-user-here/crawler.log"}}, "logging.file"),
            ({"report": {"html": "~no-such-user-here/report.html"}}, "report.html"),
            ({"report": {"stats_json": "~no-such-user-here/stats.json"}}, "report.stats_json"),
        ],
    )
    def test_path_in_the_home_of_an_unknown_user(self, data, key):
        (problem,) = problems(data)

        assert problem.startswith(f"{key}: the home directory of the user is unknown, got ")

    @pytest.mark.parametrize(
        "data, key",
        [
            ({"storage": {"outputs": ["pages\0.jsonl"]}}, "storage.outputs[0]"),
            ({"logging": {"file": "crawler\0.log"}}, "logging.file"),
            ({"report": {"html": "report\0.html"}}, "report.html"),
        ],
    )
    def test_path_with_a_null_character(self, data, key):
        (problem,) = problems(data)

        assert problem.startswith(f"{key}: must not contain a null character, got ")
        assert "\0" not in problem

    def test_whole_number_too_large_for_a_fraction(self):
        (problem,) = problems({"crawler": {"total_timeout": 10**400}})

        assert problem.startswith("crawler.total_timeout: expected a number, got 1000")

    @pytest.mark.parametrize("agent", ["MyBot/1.0\r\nX-Injected: 1", "MyBot/1.0 \x00", "My\x7fBot/1.0"])
    def test_user_agent_with_a_control_character(self, agent):
        found = problems({"crawler": {"user_agent": agent, "user_agents": [agent]}})

        assert [problem.partition(",")[0] for problem in found] == [
            "crawler.user_agent: must be one line without control characters",
            "crawler.user_agents[0]: must be one line without control characters",
        ]
        assert all("\n" not in problem and "\0" not in problem for problem in found)

    def test_robots_sitemaps_need_robots(self):
        assert problems({"sitemaps": {"from_robots": True}, "crawler": {"respect_robots": False}}) == [
            "sitemaps.from_robots: needs crawler.respect_robots, which is false"
        ]

    def test_rotated_user_agent_must_keep_the_robots_name(self):
        data = {"crawler": {"user_agent": "MyBot/1.0", "user_agents": ["MyBot/2.0 (x)", "OtherBot/1.0"]}}

        assert problems(data) == [
            'crawler.user_agents[1]: must use the robots.txt name "mybot" of crawler.user_agent, got "OtherBot/1.0"'
        ]

    def test_all_problems_are_reported_at_once(self):
        with pytest.raises(ConfigError) as error:
            CrawlerConfig.from_dict(
                {"urls": ["nope"], "crawler": {"max_pages": 0, "depth": 1}, "retry": "none"}, source="config.yaml"
            )

        assert error.value.problems == [
            'urls[0]: expected an http:// or https:// URL, got "nope"',
            "crawler.max_pages: must be >= 1, got 0",
            'crawler.depth: unknown key; did you mean "max_depth"?',
            'retry: expected a mapping of keys to values, got "none"',
        ]
        assert str(error.value).startswith("Invalid configuration: config.yaml: 4 problems\n  - urls[0]: ")
        assert isinstance(error.value, ValueError)

    def test_single_problem_is_one_line(self):
        with pytest.raises(ConfigError) as error:
            CrawlerConfig.from_dict({"crawler": {"max_pages": 0}})

        assert str(error.value) == "Invalid configuration: crawler.max_pages: must be >= 1, got 0"

    def test_message_shows_the_first_twenty_problems(self):
        with pytest.raises(ConfigError) as error:
            CrawlerConfig.from_dict({"urls": [f"bad{index}" for index in range(25)]})

        assert len(error.value.problems) == 25
        lines = str(error.value).splitlines()
        assert lines[0] == "Invalid configuration: 25 problems"
        assert lines[1:21] == [f"  - {problem}" for problem in error.value.problems[:20]]
        assert lines[21:] == ["  - ... and 5 more"]

    @pytest.mark.parametrize("data", [None, [], "urls", 5])
    def test_top_level_must_be_a_mapping(self, data):
        (problem,) = problems(data)

        assert problem.startswith("the top level: expected a mapping of keys to values, got ")


class TestFiles:
    def test_yaml_file(self, tmp_path):
        path = write(
            tmp_path,
            """
            # A comment.
            urls:
              - https://example.com/
            crawler:
              max_pages: 5
              rate_limit: null
            filters:
              exclude: ['\\.pdf$']
            """,
        )
        config = load_config(path)

        assert config.urls == ("https://example.com/",)
        assert (config.crawler.max_pages, config.crawler.rate_limit) == (5, None)
        assert config.filters.exclude == ("\\.pdf$",)
        assert config.crawler.max_depth == 2  # not in the file

    def test_yml_extension_and_a_string_path(self, tmp_path):
        path = write(tmp_path, "crawler: {max_depth: 0}", "config.YML")

        assert load_config(str(path)).crawler.max_depth == 0

    def test_json_file(self, tmp_path):
        path = write(tmp_path, json.dumps(FULL), "config.json")

        assert load_config(path).to_dict() == FULL

    @pytest.mark.parametrize(
        ("name", "text"),
        [
            ("config.yaml", ""),
            ("config.yaml", "  \n"),
            ("config.yaml", "# nothing yet\n"),
            ("config.json", ""),
            ("config.json", " \n"),
        ],
    )
    def test_empty_file_gives_the_defaults(self, tmp_path, name, text):
        assert load_config(write(tmp_path, text, name)) == CrawlerConfig()

    def test_overrides_win_over_the_file_key_by_key(self, tmp_path):
        path = write(tmp_path, "urls: [https://a.example/, https://b.example/]\ncrawler: {max_pages: 5, max_depth: 1}")
        config = load_config(
            path, {"urls": ["https://c.example/"], "crawler": {"max_pages": 9}, "retry": {"max_retries": 0}}
        )

        assert config.urls == ("https://c.example/",)  # a list is replaced, not extended
        assert (config.crawler.max_pages, config.crawler.max_depth) == (9, 1)
        assert config.retry.max_retries == 0

    def test_overrides_are_validated_too(self, tmp_path):
        with pytest.raises(ConfigError, match="crawler.max_pages: must be >= 1, got 0"):
            load_config(write(tmp_path, "crawler: {max_depth: 1}"), {"crawler": {"max_pages": 0}})

    def test_error_names_the_file(self, tmp_path):
        path = write(tmp_path, "crawler: {max_pages: 0}")

        with pytest.raises(ConfigError) as error:
            load_config(path)

        assert str(error.value) == f"Invalid configuration: {path}: crawler.max_pages: must be >= 1, got 0"
        assert error.value.source == str(path)

    def test_missing_file(self, tmp_path):
        with pytest.raises(ConfigError, match="cannot read the file"):
            load_config(tmp_path / "missing.yaml")

    @pytest.mark.parametrize("name", ["config.toml", "config", "config.yaml.bak"])
    def test_unknown_extension(self, tmp_path, name):
        with pytest.raises(ConfigError, match=r"expected \.yaml, \.yml or \.json"):
            load_config(write(tmp_path, "urls: []", name))

    def test_broken_yaml_names_the_line(self, tmp_path):
        with pytest.raises(ConfigError, match=r"not valid YAML: .*line 2") as error:
            load_config(write(tmp_path, "urls:\n  - [unclosed\n"))

        assert "\n" not in str(error.value)

    def test_broken_json(self, tmp_path):
        with pytest.raises(ConfigError, match="not valid JSON: .*line 1"):
            load_config(write(tmp_path, '{"urls": [}', "config.json"))

    def test_file_that_is_not_text(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_bytes(b"\xff\xfe\x00")

        with pytest.raises(ConfigError, match="cannot read the file"):
            load_config(path)

    def test_key_given_twice_is_rejected(self, tmp_path):
        # YAML itself would keep the last one without a word.
        path = write(tmp_path, "crawler:\n  max_pages: 5\n  max_depth: 1\n  max_pages: 50\n")

        with pytest.raises(ConfigError, match=r'the key "max_pages" is given twice.*line 4'):
            load_config(path)

    def test_yaml_is_loaded_safely(self, tmp_path):
        path = write(tmp_path, "urls: !!python/object/apply:os.getcwd []")

        with pytest.raises(ConfigError, match="not valid YAML"):
            load_config(path)

    def test_yaml_top_level_list(self, tmp_path):
        with pytest.raises(ConfigError, match="the top level: expected a mapping"):
            load_config(write(tmp_path, "- https://example.com/"), {"crawler": {"max_pages": 1}})

    def test_key_that_is_not_a_string(self, tmp_path):
        with pytest.raises(ConfigError, match="200: unknown key"):
            load_config(write(tmp_path, "200: ok"))


class TestSession:
    def test_cookies_and_headers_are_read(self):
        session = CrawlerConfig.from_dict(FULL).session

        (cookie,) = session.cookies
        assert (cookie.name, cookie.value, cookie.domain, cookie.path, cookie.secure) == (
            "sid",
            "s3cr3t",
            ".example.com",
            "/app",
            True,
        )
        assert session.headers == {"Accept-Language": "en", "Authorization": "Bearer t0ken"}

    def test_cookie_needs_a_name_a_value_and_a_domain(self):
        found = problems({"session": {"cookies": [{"name": "sid", "value": "x"}, {"domain": "example.com"}]}})

        assert found == [
            'session.cookies[0]: the key "domain" is required',
            'session.cookies[1]: the key "name" is required',
            'session.cookies[1]: the key "value" is required',
        ]

    def test_cookie_defaults(self):
        config = CrawlerConfig.from_dict(
            {"session": {"cookies": [{"name": "a", "value": "", "domain": "Example.COM"}]}}
        )

        (cookie,) = config.session.cookies
        assert (cookie.domain, cookie.path, cookie.secure) == ("example.com", "/", False)

    @pytest.mark.parametrize(
        "domain, problem",
        [
            ("127.0.0.1", "cookies of an IP address are not kept; use the host name, e.g. localhost"),
            (".10.0.0.1", "cookies of an IP address are not kept; use the host name, e.g. localhost"),
            ("[::1]", "cookies of an IP address are not kept; use the host name, e.g. localhost"),
            ("https://example.com", "expected a host name such as example.com or .example.com"),
            ("example.com/path", "expected a host name such as example.com or .example.com"),
            ("", "expected a host name such as example.com or .example.com"),
        ],
    )
    def test_invalid_cookie_domain(self, domain, problem):
        (found,) = problems({"session": {"cookies": [{"name": "a", "value": "b", "domain": domain}]}})

        assert found.startswith(f"session.cookies[0].domain: {problem}, got ")

    def test_invalid_cookie_name_and_path(self):
        found = problems(
            {"session": {"cookies": [{"name": "a b", "value": "x", "domain": "example.com", "path": "app"}]}}
        )

        assert [problem.partition(",")[0] for problem in found] == [
            "session.cookies[0].name: not a cookie name: letters",
            'session.cookies[0].path: expected a path that starts with "/"',
        ]

    def test_cookie_named_as_an_attribute(self):
        (found,) = problems({"session": {"cookies": [{"name": "Secure", "value": "x", "domain": "example.com"}]}})

        assert found.startswith("session.cookies[0].name: the name of a cookie attribute, such as Path or Secure, ")

    @pytest.mark.parametrize("value", ["two words", 'quo"te', "a;b", "line\nbreak", "caf\u00e9", "tab\t"])
    def test_invalid_cookie_value_is_not_shown(self, value):
        value = value.encode().decode("unicode_escape")
        (found,) = problems({"session": {"cookies": [{"name": "a", "value": value, "domain": "example.com"}]}})

        assert found == (
            "session.cookies[0].value: must be printable ASCII without spaces, quotes, commas, semicolons or backslashes"
        )

    def test_value_of_the_wrong_type_is_not_shown(self):
        found = problems(
            {
                "session": {
                    "cookies": [{"name": "a", "value": 12345, "domain": "example.com"}],
                    "headers": {"X-Key": 678},
                }
            }
        )

        assert found == ["session.cookies[0].value: expected a string", "session.headers.X-Key: expected a string"]

    @pytest.mark.parametrize("name", ["User-Agent", "user-agent", "Cookie", "HOST", "Proxy-Authorization"])
    def test_headers_with_keys_of_their_own_are_refused(self, name):
        (found,) = problems({"session": {"headers": {name: "value"}}})

        assert found.startswith(f"session.headers.{name}: this header is set by ")

    def test_invalid_header_name(self):
        (found,) = problems({"session": {"headers": {"X Key": "value"}}})

        assert found.startswith("session.headers.X Key: not a header name")

    @pytest.mark.parametrize("value", ["Bearer secret\r\nX-Injected: 1", "secret\u0000", ""])
    def test_invalid_header_value_is_not_shown(self, value):
        value = value.encode().decode("unicode_escape")
        (found,) = problems({"session": {"headers": {"Authorization": value}}})

        assert found in (
            "session.headers.Authorization: must be one line without control characters",
            "session.headers.Authorization: must not be empty",
        )

    @pytest.mark.parametrize("headers", [["Accept-Language: en"], "Authorization: Bearer t0ken"])
    def test_headers_must_be_a_mapping(self, headers):
        # The value is not shown: it may hold a secret.
        assert problems({"session": {"headers": headers}}) == ["session.headers: expected a mapping of names to values"]

    def test_header_given_twice_in_different_case(self):
        assert problems({"session": {"headers": {"Accept": "a", "accept": "b"}}}) == [
            'session.headers: the header "accept" is given twice, in different case'
        ]

    @pytest.mark.parametrize(
        "key, value",
        [
            ("cookies", [{"name": "a", "value": "b", "domain": "example.com"}]),
            ("cookies_file", "cookies.txt"),
            ("save_cookies", "cookies.txt"),
        ],
    )
    def test_cookies_need_keep_cookies(self, key, value):
        assert problems({"session": {"keep_cookies": False, key: value}}) == [
            f"session.{key}: needs session.keep_cookies, which is false"
        ]

    def test_repr_hides_the_secrets(self):
        text = repr(CrawlerConfig.from_dict(FULL))

        assert "s3cr3t" not in text
        assert "t0ken" not in text
        assert "name='sid'" in text

    def test_initial_cookies_come_from_the_file_then_the_section(self, tmp_path):
        path = tmp_path / "cookies.txt"
        path.write_text(
            "# Netscape HTTP Cookie File\nexample.com\tFALSE\t/\tFALSE\t0\tfrom_file\t1\n", encoding="utf-8"
        )
        session = CrawlerConfig.from_dict(
            {
                "session": {
                    "cookies_file": str(path),
                    "cookies": [{"name": "from_config", "value": "2", "domain": "example.com", "secure": True}],
                }
            }
        ).session

        cookies = session.initial_cookies()

        assert [(cookie.name, cookie.value, cookie.domain, cookie.secure) for cookie in cookies] == [
            ("from_file", "1", "example.com", False),
            ("from_config", "2", "example.com", True),
        ]

    @pytest.mark.parametrize("content", [None, "not a cookies file\n"])
    def test_cookies_file_that_cannot_be_read(self, tmp_path, content):
        path = tmp_path / "cookies.txt"
        if content is not None:
            path.write_text(content, encoding="utf-8")
        session = CrawlerConfig.from_dict({"session": {"cookies_file": str(path)}}).session

        with pytest.raises(ConfigError) as error:
            session.initial_cookies()

        (problem,) = error.value.problems
        assert problem.startswith("session.cookies_file: cannot read the cookies: ")


class TestProxy:
    @pytest.fixture(autouse=True)
    def clean_environment(self, monkeypatch):
        for name in ["http_proxy", "https_proxy", "no_proxy", "all_proxy", "REQUEST_METHOD"]:
            monkeypatch.delenv(name, raising=False)
            monkeypatch.delenv(name.upper(), raising=False)

    @pytest.mark.parametrize(
        "url, problem",
        [
            ("socks5://user:pr0xyp4ss@proxy.example:1080", "SOCKS proxies are not supported: use an http:// proxy"),
            ("http://user:pr0xyp4ss@proxy.example", "the proxy URL needs a port"),
            ("ftp://user:pr0xyp4ss@proxy.example:21", "expected an http:// or https:// proxy URL"),
            ("http://user:pr0xyp4ss@proxy.example:3128/path", "a proxy URL has no path, query or fragment"),
        ],
    )
    def test_invalid_url_is_not_shown(self, url, problem):
        (found,) = problems({"proxy": {"urls": ["http://proxy.example:3128", url]}})

        assert found.startswith(f"proxy.urls[1]: {problem}")
        assert "pr0xyp4ss" not in found
        assert "got" not in found

    def test_urls_given_as_a_string_are_not_shown(self):
        assert problems({"proxy": {"urls": "http://user:pr0xyp4ss@proxy.example:3128"}}) == [
            "proxy.urls: expected a list"
        ]

    def test_invalid_rotation(self):
        assert problems({"proxy": {"rotation": "random"}}) == [
            'proxy.rotation: expected per_host or per_request, got "random"'
        ]

    @pytest.mark.parametrize(
        "section, problem",
        [
            ({"max_failures": 0}, "proxy.max_failures: must be >= 1, got 0"),
            ({"cooldown": 0}, "proxy.cooldown: must be > 0, got 0.0"),
        ],
    )
    def test_limits(self, section, problem):
        assert problems({"proxy": section}) == [problem]

    def test_from_env_cannot_be_used_with_urls(self):
        assert problems({"proxy": {"urls": ["http://proxy.example:3128"], "from_env": True}}) == [
            "proxy.from_env: cannot be used with proxy.urls; give one of them"
        ]

    def test_proxy_listed_twice_is_named_without_its_password(self):
        found = problems(
            {
                "proxy": {
                    "urls": [
                        "http://user:pr0xyp4ss@proxy.example:3128",
                        "http://proxy.example:3128",
                        "http://user:other@Proxy.Example:3128",  # a host in any case is one proxy
                    ]
                }
            }
        )

        assert found == ["proxy.urls[2]: http://user:***@proxy.example:3128 is listed twice"]

    def test_repr_hides_the_passwords(self):
        text = repr(CrawlerConfig.from_dict(FULL))

        assert "pr0xyp4ss" not in text
        assert "rotation='per_request'" in text

    def test_no_proxies_build_no_pool(self):
        assert ProxyOptions().build() is None

    def test_urls_build_a_pool(self):
        pool = CrawlerConfig.from_dict(FULL).proxy.build()

        assert [proxy.label for proxy in pool.proxies] == [
            "http://user:***@proxy-1.example:3128",
            "https://proxy-2.example:8443",
        ]
        assert (pool.rotation, pool.max_failures, pool.cooldown) == ("per_request", 5, 30.0)

    def test_from_env_builds_a_pool_of_the_environment(self, monkeypatch):
        monkeypatch.setenv("HTTPS_PROXY", "http://user:pr0xyp4ss@proxy.example:3128")

        pool = ProxyOptions(from_env=True, max_failures=2, cooldown=5.0).build()

        assert [proxy.label for proxy in pool.proxies] == ["http://user:***@proxy.example:3128"]
        assert (pool.max_failures, pool.cooldown) == (2, 5.0)
        assert pool.pick("http://example.com/") is None  # no HTTP_PROXY

    def test_from_env_without_proxies_builds_no_pool(self):
        assert ProxyOptions(from_env=True).build() is None

    def test_invalid_variable_is_named_without_its_value(self, monkeypatch):
        monkeypatch.setenv("HTTPS_PROXY", "socks5://user:pr0xyp4ss@proxy.example:1080")

        with pytest.raises(ConfigError) as error:
            ProxyOptions(from_env=True).build()

        (problem,) = error.value.problems
        assert problem.startswith("proxy.from_env: HTTPS_PROXY: SOCKS proxies are not supported")
        assert "pr0xyp4ss" not in str(error.value)


class TestRendering:
    def test_off_builds_no_settings(self):
        assert RenderingOptions().build() is None

    def test_patterns_build_the_settings_of_the_browser(self):
        rendering = CrawlerConfig.from_dict(FULL).rendering.build()

        assert rendering == Rendering(
            include=("^https://example\\.com/app/",),
            wait_until="networkidle",
            wait_for="#content",
            timeout=10.0,
            max_open_pages=4,
            block_resources=frozenset({"image", "stylesheet"}),
        )
        assert rendering.renders("https://example.com/app/page")
        assert not rendering.renders("https://example.com/blog/")

    def test_always_renders_every_page(self):
        rendering = CrawlerConfig.from_dict({"rendering": {"mode": "always"}}).rendering.build()

        assert rendering.renders("https://example.com/any/page")

    @pytest.mark.parametrize(
        "section, problem",
        [
            ({"mode": "sometimes"}, 'rendering.mode: expected one of off, always, patterns, got "sometimes"'),
            ({"wait_until": "idle"}, "rendering.wait_until: expected one of load, domcontentloaded, networkidle"),
            ({"wait_for": " "}, 'rendering.wait_for: must not be empty, got " "'),
            ({"timeout": 0}, "rendering.timeout: must be > 0, got 0.0"),
            ({"max_open_pages": 0}, "rendering.max_open_pages: must be >= 1, got 0"),
            ({"block_resources": ["document"]}, "rendering.block_resources[0]: expected one of eventsource, fetch"),
            ({"block_resources": "image"}, 'rendering.block_resources: expected a list, got "image"'),
        ],
    )
    def test_invalid_values(self, section, problem):
        (found,) = problems({"rendering": section})

        assert found.startswith(problem)

    def test_invalid_pattern(self):
        (found,) = problems({"rendering": {"mode": "patterns", "include": ["/app/(", "/ok/"]}})

        assert found.startswith("rendering.include[0]: not a regular expression: missing ), unterminated subpattern")

    def test_off_written_bare_in_yaml_is_explained(self, tmp_path):
        path = write(tmp_path, "rendering:\n  mode: off\n")

        with pytest.raises(ConfigError) as error:
            load_config(path)

        assert error.value.problems == [
            (
                "rendering.mode: expected a string, got false; "
                "YAML reads off, on, no and yes as false or true: put the word in quotes"
            )
        ]
        assert load_config(write(tmp_path, 'rendering:\n  mode: "off"\n')).rendering.mode == "off"

    def test_patterns_need_include(self):
        assert problems({"rendering": {"mode": "patterns"}}) == [
            "rendering.mode: patterns needs rendering.include, which is empty"
        ]

    @pytest.mark.parametrize("mode", ["off", "always"])
    def test_include_needs_patterns(self, mode):
        assert problems({"rendering": {"mode": mode, "include": ["/app/"]}}) == [
            f'rendering.include: needs rendering.mode: patterns, got "{mode}"'
        ]

    @pytest.mark.parametrize("section", [{"mode": "always"}, {"mode": "patterns", "include": ["/app/"]}])
    def test_rendering_without_playwright_is_an_error_with_the_command_to_install_it(self, section, monkeypatch):
        monkeypatch.setitem(sys.modules, "playwright", None)  # find_spec() takes it for a missing package

        assert problems({"rendering": section}) == [
            "rendering.mode: Playwright is not installed; run: pip install -e ."
        ]

    def test_off_needs_no_playwright(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "playwright", None)

        assert CrawlerConfig.from_dict({"rendering": {"mode": "off", "timeout": 5}}).rendering.build() is None
