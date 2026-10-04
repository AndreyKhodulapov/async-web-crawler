"""Unit tests for the command line of the crawler: options, their priority over the configuration file, exit codes."""

import json

import pytest
import yaml

import main
from crawler import ConfigError, CrawlerConfig, StorageError
from main import build_config, config_overrides, parse_args


def write_config(tmp_path, data, name="config.yaml"):
    path = tmp_path / name
    path.write_text(json.dumps(data) if name.endswith(".json") else yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def test_no_options_override_nothing():
    assert config_overrides(parse_args([])) == {}


def test_options_are_shaped_like_the_configuration():
    args = parse_args(
        [
            "--urls", "https://one.example/", "https://two.example/",
            "--max-pages", "7",
            "--max-depth", "0",
            "--output", "pages.jsonl",
            "--output", "sqlite:///pages.db",
            "--overwrite",
            "--no-respect-robots",
            "--no-same-domain-only",
            "--rate-limit", "2.5",
            "--stats-json", "stats.json",
            "--report", "report.html",
            "--log-level", "debug",
            "--log-file", "crawler.log",
        ]
    )  # fmt: skip

    assert config_overrides(args) == {
        "urls": ["https://one.example/", "https://two.example/"],
        "crawler": {"max_pages": 7, "max_depth": 0, "respect_robots": False, "rate_limit": 2.5},
        "filters": {"same_domain_only": False},
        "storage": {"outputs": ["pages.jsonl", "sqlite:///pages.db"], "overwrite": True},
        "report": {"stats_json": "stats.json", "html": "report.html"},
        "logging": {"level": "DEBUG", "file": "crawler.log"},
    }


def test_pages_are_not_kept_in_memory_whatever_the_file_says(tmp_path):
    path = write_config(tmp_path, {"urls": ["https://example.com/"], "crawler": {"keep_pages": True}})

    assert build_config(parse_args(["--config", path])).crawler.keep_pages is False
    assert build_config(parse_args(["--urls", "https://example.com/"])).crawler.keep_pages is False


@pytest.mark.parametrize(("option", "expected"), [("--same-domain-only", True), ("--no-same-domain-only", False)])
def test_same_domain_only_wins_over_the_file(option, expected, tmp_path):
    config = write_config(tmp_path, {"urls": ["https://example.com/"], "filters": {"same_domain_only": not expected}})

    assert build_config(parse_args(["--config", config, option])).filters.same_domain_only is expected
    assert build_config(parse_args(["--config", config])).filters.same_domain_only is not expected


@pytest.mark.parametrize(("option", "expected"), [("--overwrite", True), ("--no-overwrite", False)])
def test_overwrite_wins_over_the_file(option, expected, tmp_path):
    config = write_config(tmp_path, {"urls": ["https://example.com/"], "storage": {"overwrite": not expected}})

    assert build_config(parse_args(["--config", config, option])).storage.overwrite is expected
    assert build_config(parse_args(["--config", config])).storage.overwrite is not expected


def test_crawl_stays_on_the_start_hosts_by_default():
    assert build_config(parse_args(["--urls", "https://example.com/"])).filters.same_domain_only is True


def test_rate_limit_of_zero_lifts_the_limit():
    args = parse_args(["--urls", "https://example.com/", "--rate-limit", "0"])

    assert config_overrides(args)["crawler"] == {"rate_limit": None}
    assert build_config(args).crawler.rate_limit is None


@pytest.mark.parametrize(("option", "expected"), [("--respect-robots", True), ("--no-respect-robots", False)])
def test_respect_robots_wins_over_the_file(option, expected, tmp_path):
    config = write_config(tmp_path, {"urls": ["https://example.com/"], "crawler": {"respect_robots": not expected}})

    assert build_config(parse_args(["--config", config, option])).crawler.respect_robots is expected
    assert build_config(parse_args(["--config", config])).crawler.respect_robots is not expected


@pytest.mark.parametrize(
    "argv",
    [
        ["--urls", "example.com"],
        ["--urls"],
        ["--max-pages", "0"],
        ["--max-pages", "1.5"],
        ["--max-depth", "-1"],
        ["--rate-limit", "-1"],
        ["--rate-limit", "nan"],
        ["--log-level", "LOUD"],
        ["https://example.com/"],
    ],
)
def test_invalid_options_are_usage_errors(argv, capsys):
    with pytest.raises(SystemExit) as exit_info:
        parse_args(argv)

    assert exit_info.value.code == 2
    assert "usage:" in capsys.readouterr().err


def test_options_alone_make_a_configuration():
    config = build_config(parse_args(["--urls", "https://example.com/", "--max-pages", "5"]))

    defaults = CrawlerConfig()
    assert config.urls == ("https://example.com/",)
    assert config.crawler.max_pages == 5
    assert (config.crawler.max_depth, config.storage, config.logging) == (
        defaults.crawler.max_depth,
        defaults.storage,
        defaults.logging,
    )


@pytest.mark.parametrize("name", ["config.yaml", "config.json"])
def test_options_win_over_the_file_and_the_rest_of_it_stays(name, tmp_path):
    path = write_config(
        tmp_path,
        {
            "urls": ["https://file.example/"],
            "crawler": {"max_pages": 50, "max_depth": 3, "max_concurrent": 4},
            "storage": {"outputs": ["file.jsonl", "file.csv"], "batch_size": 7},
            "filters": {"exclude": ["/login"]},
        },
        name,
    )

    config = build_config(parse_args(["--config", path, "--max-pages", "5", "--output", "flag.json"]))

    assert config.crawler.max_pages == 5
    assert config.storage.outputs == ("flag.json",)  # the list is replaced, not extended
    assert (config.urls, config.crawler.max_depth, config.crawler.max_concurrent) == (("https://file.example/",), 3, 4)
    assert (config.storage.batch_size, config.filters.exclude) == (7, ("/login",))


def test_urls_option_replaces_the_urls_of_the_file(tmp_path):
    path = write_config(tmp_path, {"urls": ["https://file.example/"]})

    config = build_config(parse_args(["--config", path, "--urls", "https://flag.example/"]))

    assert config.urls == ("https://flag.example/",)


def test_sitemaps_of_the_file_are_enough_to_crawl(tmp_path):
    path = write_config(tmp_path, {"sitemaps": {"urls": ["https://example.com/sitemap.xml"]}})

    assert build_config(parse_args(["--config", path])).urls == ()


@pytest.mark.parametrize("with_file", [False, True])
def test_nothing_to_crawl_is_an_error(with_file, tmp_path):
    argv = ["--config", write_config(tmp_path, {"crawler": {"max_pages": 5}})] if with_file else []

    with pytest.raises(ConfigError, match="nothing to crawl: give --urls"):
        build_config(parse_args(argv))


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        ([], "nothing to crawl"),
        (["--config", "missing.yaml"], "missing.yaml: cannot read the file"),
        (
            ["--urls", "https://example.com/", "--output", "pages.txt"],
            'storage.outputs[0]: Cannot choose a storage for "pages.txt"',
        ),
        (["--urls", "https://example.com/", "--log-file", " "], "logging.file: must not be empty"),
    ],
)
def test_invalid_configuration_exits_with_2_before_anything_runs(argv, message, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "run", None)  # would fail if called

    assert main.main(argv) == 2

    error = capsys.readouterr().err
    assert error.startswith("error: Invalid configuration: ")
    assert message in error
    assert list(tmp_path.iterdir()) == []


def test_problems_of_the_file_are_all_reported(tmp_path, capsys):
    path = write_config(tmp_path, {"urls": ["example.com"], "crawler": {"max_pagse": 5}})

    assert main.main(["--config", path]) == 2

    error = capsys.readouterr().err
    assert "2 problems" in error
    assert 'crawler.max_pagse: unknown key; did you mean "max_pages"?' in error
    assert "urls[0]: expected an http:// or https:// URL" in error


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        (0, 0),
        (1, 1),
        (OSError("cannot open the log"), 1),
        (StorageError("pages.jsonl is not JSON Lines of this storage"), 1),
        (KeyboardInterrupt(), 130),
    ],
)
def test_exit_code_follows_the_run(outcome, code, monkeypatch, capsys):
    seen = {}

    async def run(config, *, progress):
        seen.update(urls=config.urls, progress=progress)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(main, "run", run)

    assert main.main(["--urls", "https://example.com/", "--no-progress"]) == code
    assert seen == {"urls": ("https://example.com/",), "progress": False}
    assert capsys.readouterr().err == (f"error: {outcome}\n" if isinstance(outcome, OSError | StorageError) else "")
