"""Unit tests for the configuration: defaults, YAML and JSON files, overrides and validation."""

import inspect
import json
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
    RetryStrategy,
    load_config,
)
from crawler.config import (
    CircuitBreakerOptions,
    CrawlOptions,
    FilterOptions,
    LoggingOptions,
    ReportOptions,
    RetryOptions,
    SitemapOptions,
    StorageOptions,
)

EXAMPLE = Path(__file__).parents[2] / "config.example.yaml"

FULL = {
    "urls": ["https://example.com/", "https://example.org/docs"],
    "sitemaps": {"urls": ["https://example.com/sitemap.xml"], "from_robots": True, "max_urls": 200},
    "crawler": {
        "max_pages": 500,
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
    },
    "retry": {"max_retries": 5, "backoff_factor": 3.0, "base_delay": 0.5, "max_delay": 10.0},
    "circuit_breaker": {"failure_threshold": 0.8, "min_requests": 10, "window": 120.0, "cooldown": 15.0},
    "filters": {"same_domain_only": True, "include": ["^https://example\\.com/blog/"], "exclude": ["\\.pdf$"]},
    "storage": {"outputs": ["pages.jsonl", "pages.csv"], "batch_size": 50, "csv_encoding": "utf-8-sig"},
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
            max_pages=100, max_depth=2, max_concurrent=10, max_per_domain=None, rate_limit=1.0, respect_robots=True
        )
        assert config.crawler.user_agent == AsyncCrawler.DEFAULT_USER_AGENT
        assert config.sitemaps == SitemapOptions(urls=(), from_robots=False, max_urls=50_000)
        assert config.retry == RetryOptions(max_retries=3, backoff_factor=2.0, base_delay=1.0, max_delay=30.0)
        assert config.circuit_breaker == CircuitBreakerOptions(
            failure_threshold=0.5, min_requests=5, window=60.0, cooldown=30.0
        )
        assert config.filters == FilterOptions(same_domain_only=False, include=(), exclude=())
        assert config.storage == StorageOptions(outputs=(), batch_size=100, csv_encoding="utf-8")
        assert config.logging == LoggingOptions(level="INFO", file=None, max_bytes=10 * 1024 * 1024, backup_count=5)
        assert config.report == ReportOptions(stats_json=None, html=None, title="Crawl report", top_domains=10)

    def test_defaults_are_those_of_the_components(self):
        """The crawler built without a configuration and with an empty one behave the same."""

        def defaults(function) -> dict:
            return {name: parameter.default for name, parameter in inspect.signature(function).parameters.items()}

        crawler, crawl = defaults(AsyncCrawler.__init__), defaults(AsyncCrawler.crawl)
        options = CrawlOptions()
        assert options.max_pages == crawl["max_pages"]
        assert options.rate_limit == crawler["requests_per_second"]
        for name in ("max_depth", "max_concurrent", "max_per_domain", "per_domain_rate", "min_delay", "jitter"):
            assert getattr(options, name) == crawler[name], name
        for name in ("respect_robots", "total_timeout", "connect_timeout", "read_timeout", "timeout_growth"):
            assert getattr(options, name) == crawler[name], name
        retry, breaker = defaults(RetryStrategy.__init__), defaults(CircuitBreaker.__init__)
        assert RetryOptions() == RetryOptions(**{name: retry[name] for name in RetryOptions.__dataclass_fields__})
        assert CircuitBreakerOptions() == CircuitBreakerOptions(
            **{name: breaker[name] for name in CircuitBreakerOptions.__dataclass_fields__}
        )
        assert FilterOptions().same_domain_only == crawl["same_domain_only"]
        assert SitemapOptions().from_robots == crawl["robots_sitemaps"]

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

    def test_to_dict_can_be_read_back_and_written_as_json(self):
        config = CrawlerConfig.from_dict(FULL)

        assert CrawlerConfig.from_dict(json.loads(json.dumps(config.to_dict()))) == config

    def test_whole_number_is_taken_for_a_fraction(self):
        config = CrawlerConfig.from_dict({"crawler": {"rate_limit": 2, "total_timeout": 45}})

        assert (config.crawler.rate_limit, config.crawler.total_timeout) == (2.0, 45.0)
        assert isinstance(config.crawler.rate_limit, float)

    def test_null_turns_a_limit_off(self):
        config = CrawlerConfig.from_dict(
            {"crawler": {"rate_limit": None, "max_per_domain": None}, "circuit_breaker": {"failure_threshold": None}}
        )

        assert config.crawler.rate_limit is None
        assert config.circuit_breaker.failure_threshold is None

    def test_section_without_keys_keeps_its_defaults(self):
        assert CrawlerConfig.from_dict({"crawler": None, "filters": {}}) == CrawlerConfig()

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

    def test_validation_opens_nothing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        CrawlerConfig.from_dict({"storage": {"outputs": ["pages.jsonl", "pages.db", "sqlite:///other.db"]}})

        assert list(tmp_path.iterdir()) == []


class TestInvalid:
    def test_unknown_key_suggests_the_close_one(self):
        assert problems({"crawler": {"max_page": 5}}) == ['crawler.max_page: unknown key; did you mean "max_pages"?']

    def test_unknown_key_without_a_close_one_lists_the_keys(self):
        assert problems({"filters": {"zzz": 1}}) == [
            "filters.zzz: unknown key; expected one of same_domain_only, include, exclude"
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
            ({"crawler": {"max_depth": -1}}, "crawler.max_depth: must be >= 0, got -1"),
            ({"crawler": {"max_concurrent": 0}}, "crawler.max_concurrent: must be >= 1, got 0"),
            ({"crawler": {"max_per_domain": 0}}, "crawler.max_per_domain: must be >= 1, got 0"),
            ({"crawler": {"rate_limit": 0}}, "crawler.rate_limit: must be > 0, got 0.0"),
            ({"crawler": {"jitter": -0.5}}, "crawler.jitter: must be >= 0, got -0.5"),
            ({"crawler": {"read_timeout": 0}}, "crawler.read_timeout: must be > 0, got 0.0"),
            ({"crawler": {"timeout_growth": 0.5}}, "crawler.timeout_growth: must be >= 1, got 0.5"),
            ({"crawler": {"user_agent": "  "}}, 'crawler.user_agent: must not be empty, got "  "'),
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

    def test_invalid_pattern(self):
        (problem,) = problems({"filters": {"exclude": ["\\.pdf$", "("]}})

        assert problem.startswith("filters.exclude[1]: not a regular expression: ")
        assert problem.endswith(', got "("')

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
