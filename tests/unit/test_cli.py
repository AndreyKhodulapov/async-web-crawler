"""Unit tests for the command line of the crawler: options, their priority over the configuration file, exit codes."""

import asyncio
import io
import json
import sys
from pathlib import Path

import pytest
import yaml

import main
from crawler import ConfigError, CrawlerConfig, FrontierError, JobError, StorageError
from crawler.distributed import JobMode, JobProgress, format_job_progress
from crawler.stats import CrawlerStats
from main import build_config, config_overrides, parse_args, parse_command_args

DSN = "postgresql://crawler:secret@db.example/crawler"


def write_config(tmp_path, data, name="config.yaml"):
    path = tmp_path / name
    path.write_text(json.dumps(data) if name.endswith(".json") else yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def write_urls(tmp_path, *lines, name="urls.txt"):
    path = tmp_path / name
    path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
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
            "--cookies-file", "cookies.txt",
            "--save-cookies", "saved.txt",
            "--proxy", "http://user:secret@proxy-1.example:3128",
            "--proxy", "http://proxy-2.example:3128",
            "--render",
            "--no-respect-robots",
            "--no-same-domain-only",
            "--rate-limit", "2.5",
            "--stats-json", "stats.json",
            "--report", "report.html",
            "--pages-report", "not-saved.csv",
            "--log-level", "debug",
            "--log-file", "crawler.log",
        ]
    )  # fmt: skip

    assert config_overrides(args) == {
        "urls": ["https://one.example/", "https://two.example/"],
        "crawler": {"max_pages": 7, "max_depth": 0, "respect_robots": False, "rate_limit": 2.5},
        "filters": {"same_domain_only": False},
        "storage": {"outputs": ["pages.jsonl", "sqlite:///pages.db"], "overwrite": True},
        "session": {"cookies_file": "cookies.txt", "save_cookies": "saved.txt"},
        "proxy": {
            "urls": ["http://user:secret@proxy-1.example:3128", "http://proxy-2.example:3128"],
            "from_env": False,
        },
        "rendering": {"mode": "always", "include": []},
        "report": {"stats_json": "stats.json", "html": "report.html", "pages": "not-saved.csv"},
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


def test_cookie_options_keep_the_rest_of_the_session_of_the_file(tmp_path):
    config = write_config(
        tmp_path,
        {
            "urls": ["https://example.com/"],
            "session": {"save_cookies": "file.txt", "headers": {"Accept-Language": "en"}},
        },
    )

    session = build_config(parse_args(["--config", config, "--save-cookies", "flag.txt"])).session

    assert (session.save_cookies, session.headers) == ("flag.txt", {"Accept-Language": "en"})


@pytest.mark.parametrize("section", [{"urls": ["http://file.example:3128"]}, {"from_env": True}])
def test_proxy_option_replaces_the_proxies_of_the_file_and_keeps_the_rest(section, tmp_path):
    config = write_config(
        tmp_path, {"urls": ["https://example.com/"], "proxy": section | {"rotation": "per_request", "cooldown": 5}}
    )

    proxy = build_config(parse_args(["--config", config, "--proxy", "http://flag.example:3128"])).proxy

    assert (proxy.urls, proxy.from_env) == (("http://flag.example:3128",), False)
    assert (proxy.rotation, proxy.cooldown) == ("per_request", 5.0)


def test_render_renders_every_page_and_keeps_the_rest_of_the_section_of_the_file(tmp_path):
    config = write_config(
        tmp_path,
        {"urls": ["https://example.com/"], "rendering": {"mode": "patterns", "include": ["/app/"], "timeout": 5}},
    )

    rendering = build_config(parse_args(["--config", config, "--render"])).rendering

    assert (rendering.mode, rendering.include, rendering.timeout) == ("always", (), 5.0)
    assert build_config(parse_args(["--config", config])).rendering.mode == "patterns"


@pytest.mark.parametrize(
    "url, problem",
    [
        ("socks5://user:pr0xyp4ss@proxy.example:1080", "SOCKS proxies are not supported"),
        ("http://user:pr0xyp4ss@proxy.example", "the proxy URL needs a port"),
    ],
)
def test_invalid_proxy_is_an_error_of_the_option_without_the_password(url, problem, capsys):
    with pytest.raises(SystemExit) as exit_info:
        parse_args(["--proxy", url])

    assert exit_info.value.code == 2
    err = capsys.readouterr().err
    assert f"argument --proxy: {problem}" in err
    assert "pr0xyp4ss" not in err


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
        ["--urls-file"],
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


@pytest.mark.parametrize(
    "url", ["https://a.example/my page", "https://a.example/ # the home page", "https://a.example/a\tb"]
)
def test_a_url_with_a_space_is_an_error_of_the_option(url, capsys):
    # Not of `urls` in the configuration, which the user never wrote.
    with pytest.raises(SystemExit) as exit_info:
        parse_args(["--urls", url])

    assert exit_info.value.code == 2
    assert "argument --urls: a URL cannot contain spaces or control characters" in capsys.readouterr().err


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


def test_urls_file_gives_the_start_urls(tmp_path):
    path = write_urls(tmp_path, "# start pages", "https://one.example/", "", "https://two.example/")

    assert config_overrides(parse_args(["--urls-file", path])) == {
        "urls": ["https://one.example/", "https://two.example/"]
    }


def test_urls_option_comes_first_and_repeats_are_dropped(tmp_path):
    path = write_urls(tmp_path, "https://two.example/", "https://three.example/", "https://one.example/")

    args = parse_args(["--urls", "https://one.example/", "https://two.example/", "--urls-file", path])

    assert build_config(args).urls == ("https://one.example/", "https://two.example/", "https://three.example/")


def test_urls_file_replaces_the_urls_of_the_file_and_keeps_its_sitemaps(tmp_path):
    config = write_config(
        tmp_path,
        {"urls": ["https://file.example/"], "sitemaps": {"urls": ["https://file.example/sitemap.xml"]}},
    )

    built = build_config(parse_args(["--config", config, "--urls-file", write_urls(tmp_path, "https://list.example/")]))

    assert built.urls == ("https://list.example/",)
    assert built.sitemaps.urls == ("https://file.example/sitemap.xml",)


def test_empty_urls_file_with_sitemaps_of_the_file_is_enough_to_crawl(tmp_path):
    config = write_config(
        tmp_path,
        {"urls": ["https://file.example/"], "sitemaps": {"urls": ["https://file.example/sitemap.xml"]}},
    )

    built = build_config(parse_args(["--config", config, "--urls-file", write_urls(tmp_path, "# none yet")]))

    assert built.urls == ()


def test_urls_file_is_read_from_stdin(monkeypatch):
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"https://one.example/\r\n")))

    assert build_config(parse_args(["--urls-file", "-"])).urls == ("https://one.example/",)


def test_sitemaps_of_the_file_are_enough_to_crawl(tmp_path):
    path = write_config(tmp_path, {"sitemaps": {"urls": ["https://example.com/sitemap.xml"]}})

    assert build_config(parse_args(["--config", path])).urls == ()


@pytest.mark.parametrize(
    ("with_file", "hint"),
    [
        (False, "give --urls or --urls-file"),
        (True, "give --urls, --urls-file, or `urls` or `sitemaps.urls` in the configuration"),
    ],
)
def test_nothing_to_crawl_is_an_error(with_file, hint, tmp_path):
    argv = ["--config", write_config(tmp_path, {"crawler": {"max_pages": 5}})] if with_file else []

    with pytest.raises(ConfigError) as error:
        build_config(parse_args(argv))

    assert error.value.problems == [f"nothing to crawl: {hint}"]


def test_empty_urls_file_alone_is_nothing_to_crawl(tmp_path):
    path = write_urls(tmp_path)

    with pytest.raises(ConfigError) as error:
        build_config(parse_args(["--urls-file", path]))

    assert error.value.problems == [f"nothing to crawl: {path} lists no URLs"]


def test_empty_urls_file_is_said_to_replace_the_urls_of_the_file(tmp_path, monkeypatch):
    config = write_config(tmp_path, {"urls": ["https://file.example/"]})
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"# none yet\n")))

    with pytest.raises(ConfigError) as error:
        build_config(parse_args(["--config", config, "--urls-file", "-"]))

    assert error.value.problems == [
        "nothing to crawl: the standard input lists no URLs, and it replaces `urls` of the configuration"
    ]


def test_ctrl_c_while_reading_stdin_exits_with_130(monkeypatch, capsys):
    class Interrupted:
        def read(self):
            raise KeyboardInterrupt

    class Stdin:
        buffer = Interrupted()

    monkeypatch.setattr(sys, "stdin", Stdin())
    monkeypatch.setattr(main, "run", None)  # would fail if called

    assert main.main(["--urls-file", "-"]) == 130
    assert capsys.readouterr().err == ""


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
        (["--urls-file", "missing.txt"], "missing.txt: cannot read the file"),
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


def test_invalid_lines_of_the_urls_file_exit_with_2_before_anything_runs(tmp_path, monkeypatch, capsys):
    path = write_urls(tmp_path, "https://ok.example/", "example.com", "# fine", "ftp://files.example/")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "run", None)  # would fail if called

    assert main.main(["--urls-file", path, "--output", "pages.jsonl", "--report", "report.html"]) == 2

    assert capsys.readouterr().err == (
        f"error: Invalid configuration: {path}: 1 URL is valid, 2 lines are not\n"
        f'  - {path}:2: expected an http:// or https:// URL, got "example.com"\n'
        f'  - {path}:4: expected an http:// or https:// URL, got "ftp://files.example/"\n'
    )
    assert [item.name for item in tmp_path.iterdir()] == ["urls.txt"]


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        (0, 0),
        (1, 1),
        (OSError("cannot open the log"), 1),
        (StorageError("pages.jsonl is not JSON Lines of this storage"), 1),
        (KeyboardInterrupt(), 130),
        (asyncio.CancelledError(), 143),
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


def test_rendering_without_chromium_exits_with_2_before_anything_runs(tmp_path, monkeypatch, capsys):
    async def browser_problem():
        return "Chromium is not installed; run: playwright install chromium"

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "browser_problem", browser_problem)
    monkeypatch.setattr(main, "AdvancedCrawler", None)  # would fail if called

    assert main.main(["--urls", "https://example.com/", "--render", "--output", "pages.jsonl", "--no-progress"]) == 2

    assert capsys.readouterr().err == (
        "error: Invalid configuration: rendering.mode: Chromium is not installed; run: playwright install chromium\n"
    )
    assert list(tmp_path.iterdir()) == []


def test_chromium_is_not_looked_for_without_rendering(monkeypatch):
    async def browser_problem():
        raise AssertionError("looked for Chromium")

    class Crawler:
        def __init__(self, config):
            raise StorageError("stops the run here")

    monkeypatch.setattr(main, "browser_problem", browser_problem)
    monkeypatch.setattr(main, "AdvancedCrawler", Crawler)

    assert main.main(["--urls", "https://example.com/", "--no-progress"]) == 1


def test_help_of_a_crawl_names_the_commands_of_crawl_jobs(capsys):
    with pytest.raises(SystemExit):
        parse_args(["--help"])

    assert "job create --help, worker --help, report --help and status --help" in " ".join(
        capsys.readouterr().out.split()
    )


def test_job_create_takes_a_configuration_and_a_name():
    args = parse_command_args(["job", "create", "--config", "job.yaml", "--name", "books"])

    assert (args.command, args.action, args.config, args.name, args.mode) == (
        "job",
        "create",
        "job.yaml",
        "books",
        JobMode.NEW,
    )


@pytest.mark.parametrize(("option", "mode"), [("--resume", JobMode.RESUME), ("--restart", JobMode.RESTART)])
def test_job_create_resumes_or_restarts_a_job(option, mode):
    assert parse_command_args(["job", "create", "--config", "job.yaml", "--name", "books", option]).mode is mode


def test_worker_takes_a_job_and_optionally_the_rest():
    args = parse_command_args(["worker", "--job", "books"])
    assert (args.command, args.job, args.config, args.concurrency, args.name) == ("worker", "books", None, None, None)

    args = parse_command_args(["worker", "--job", "books", "--config", "w.yaml", "--concurrency", "4", "--name", "w-1"])
    assert (args.config, args.concurrency, args.name) == ("w.yaml", 4, "w-1")


@pytest.mark.parametrize(
    "argv",
    [
        ["job"],
        ["job", "delete", "--name", "books"],
        ["job", "create", "--name", "books"],
        ["job", "create", "--config", "job.yaml"],
        ["job", "create", "--config", "job.yaml", "--name", "books", "--resume", "--restart"],
        ["worker"],
        ["worker", "--job", "books", "--concurrency", "0"],
        ["worker", "--job", "books", "--name", "../w"],
        ["worker", "--job", "books", "--urls", "https://example.com/"],
        ["report"],
        ["report", "--job", "books", "--html", "report.html"],
        ["status"],
        ["status", "--job", "books", "--interval", "5"],
        ["status", "--job", "books", "--watch", "--interval", "0"],
    ],
)
def test_invalid_commands_are_usage_errors(argv, monkeypatch, capsys):
    monkeypatch.setattr(main, "run_command", None)  # would fail if called

    with pytest.raises(SystemExit) as exit_info:
        main.main(argv)

    assert exit_info.value.code == 2
    assert "usage:" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        ({}, 0),
        (JobError('A crawl job named "books" exists already: resume or restart it'), 1),
        (FrontierError("the database of crawl job books failed: OSError: refused"), 1),
        (OSError("cannot open the log"), 1),
        (ConfigError(["urls: nothing to crawl"]), 2),
        (KeyboardInterrupt(), 130),
        (asyncio.CancelledError(), 143),
    ],
)
def test_exit_code_of_job_create_follows_the_job(outcome, code, tmp_path, monkeypatch, capsys):
    seen = {}

    async def create_job(config, name, *, dsn, mode, configure_logging):
        seen.update(urls=config.urls, name=name, dsn=dsn, mode=mode, configure_logging=configure_logging)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(main, "create_job", create_job)
    path = write_config(tmp_path, {"urls": ["https://example.com/"], "distributed": {"database_url": DSN}})

    assert main.main(["job", "create", "--config", path, "--name", "books", "--resume"]) == code

    assert seen == {
        "urls": ("https://example.com/",),
        "name": "books",
        "dsn": DSN,
        "mode": JobMode.RESUME,
        "configure_logging": True,
    }
    captured = capsys.readouterr()
    assert captured.err == (f"error: {outcome}\n" if code in (1, 2) else "")
    assert captured.out == (
        "Crawl job books is ready: start its workers with worker --job books\n" if code == 0 else ""
    )


def test_job_create_names_the_sitemaps_it_could_not_read(tmp_path, monkeypatch, capsys):
    async def create_job(config, name, **options):
        return {"https://a.example/sitemap.xml": "HTTP 404", "https://b.example/sitemap.xml": "not XML"}

    monkeypatch.setattr(main, "create_job", create_job)
    path = write_config(tmp_path, {"urls": ["https://example.com/"], "distributed": {"database_url": DSN}})

    assert main.main(["job", "create", "--config", path, "--name", "books"]) == 0

    assert capsys.readouterr().out.splitlines()[0] == (
        "Sitemaps not read: https://a.example/sitemap.xml (HTTP 404), https://b.example/sitemap.xml (not XML)"
    )


@pytest.mark.parametrize(
    "command",
    [
        ["job", "create", "--config", "{path}", "--name", "books"],
        ["worker", "--job", "books", "--config", "{path}"],
        ["report", "--job", "books", "--config", "{path}", "--report", "report.html"],
        ["status", "--job", "books", "--config", "{path}"],
    ],
)
def test_command_without_a_database_exits_with_2_before_anything_runs(command, tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("CRAWLER_DATABASE_URL", raising=False)
    monkeypatch.setattr(main, "create_job", None)  # would fail if called
    monkeypatch.setattr(main, "AdvancedCrawler", None)
    monkeypatch.setattr(main, "job_stats", None)
    monkeypatch.setattr(main, "job_progress", None)
    path = write_config(tmp_path, {"urls": ["https://example.com/"]})

    assert main.main([part.replace("{path}", path) for part in command]) == 2

    assert capsys.readouterr().err.startswith(
        "error: Invalid configuration: distributed.database_url: a crawl job needs a PostgreSQL database"
    )


def test_job_create_with_an_invalid_configuration_exits_with_2(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(main, "create_job", None)  # would fail if called

    assert main.main(["job", "create", "--config", str(tmp_path / "missing.yaml"), "--name", "books"]) == 2

    assert "missing.yaml: cannot read the file" in capsys.readouterr().err


def worker_stats(**changes):
    return {**CrawlerStats().get_stats(), "worker": "w-1", "saved": 0, "save_failed": 0, **changes}


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        (worker_stats(), 0),
        (worker_stats(save_failed=1), 1),
        (JobError('There is no crawl job named "books"'), 1),
        (FrontierError("the database of crawl job books failed: OSError: refused"), 1),
        (StorageError("pages-w-1.jsonl is not JSON Lines of this storage"), 1),
        (OSError("cannot open the log"), 1),
        (ConfigError(["storage.outputs[0]: put {worker} in the name"]), 2),
        (KeyboardInterrupt(), 130),
        (asyncio.CancelledError(), 143),
    ],
)
def test_exit_code_of_a_worker_follows_its_run(outcome, code, monkeypatch, capsys):
    seen = {}

    async def run_worker(config, job, *, worker):
        seen.update(job=job, worker=worker)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(main, "run_worker", run_worker)

    assert main.main(["worker", "--job", "books", "--name", "w-1"]) == code

    assert seen == {"job": "books", "worker": "w-1"}
    assert capsys.readouterr().err == (f"error: {outcome}\n" if isinstance(outcome, Exception) else "")


def test_worker_takes_its_own_configuration_and_concurrency(tmp_path, monkeypatch):
    seen = {}

    async def run_worker(config, job, *, worker):
        seen.update(config=config, worker=worker)
        return worker_stats()

    monkeypatch.setattr(main, "run_worker", run_worker)
    path = write_config(tmp_path, {"storage": {"outputs": ["pages-{worker}.jsonl"]}, "crawler": {"max_concurrent": 3}})

    assert main.main(["worker", "--job", "books", "--config", path, "--concurrency", "7"]) == 0

    config = seen["config"]
    assert (config.storage.outputs, config.crawler.max_concurrent, config.urls) == (("pages-{worker}.jsonl",), 7, ())
    assert seen["worker"] is None


def test_worker_without_a_configuration_takes_the_defaults(monkeypatch):
    seen = {}

    async def run_worker(config, job, *, worker):
        seen["config"] = config
        return worker_stats()

    monkeypatch.setattr(main, "run_worker", run_worker)

    assert main.main(["worker", "--job", "books"]) == 0
    assert seen["config"] == CrawlerConfig()


def test_summary_of_a_worker_counts_its_own_pages(tmp_path, monkeypatch, capsys):
    stats = worker_stats(
        total_pages=5, successful=4, failed=1, elapsed_seconds=2.5, status_codes={200: 4, 500: 1}, saved=4
    )

    async def run_worker(config, job, *, worker):
        return stats

    monkeypatch.setattr(main, "run_worker", run_worker)
    path = write_config(tmp_path, {"storage": {"outputs": ["pages-{worker}.jsonl"]}})

    assert main.main(["worker", "--job", "books", "--config", path]) == 0

    summary = capsys.readouterr().out
    assert "=== Worker w-1 finished on crawl job books (2.50s) ===\n" in summary
    assert "Pages: 5 (4 successful, 1 failed, 0 skipped)" in summary
    assert "Status codes: 200: 4, 500: 1\n" in summary
    assert summary.endswith("Saved: 4 pages\n")


def job_stats_of(**changes):
    return {
        "job": "books",
        "state": "finished",
        **CrawlerStats().get_stats(),
        "queued": 0,
        "in_progress": 0,
        "workers": {},
        **changes,
    }


def test_report_takes_a_job_and_optionally_the_files():
    args = parse_command_args(["report", "--job", "books"])
    assert (args.command, args.job, args.config, args.stats_json, args.report) == ("report", "books", None, None, None)

    args = parse_command_args(
        ["report", "--job", "books", "--config", "c.yaml", "--stats-json", "s.json", "--report", "r.html"]
    )
    assert (args.config, args.stats_json, args.report, args.pages_report) == ("c.yaml", "s.json", "r.html", None)

    args = parse_command_args(["report", "--job", "books", "--pages-report", "p.csv"])
    assert args.pages_report == "p.csv"


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        (job_stats_of(), 0),
        (JobError('There is no crawl job named "books"'), 1),
        (FrontierError("the database of crawl job books failed: OSError: refused"), 1),
        (KeyboardInterrupt(), 130),
    ],
)
def test_exit_code_of_report_follows_the_job(outcome, code, tmp_path, monkeypatch, capsys):
    seen = {}

    async def job_stats(dsn, job, *, top_domains):
        seen.update(dsn=dsn, job=job, top_domains=top_domains)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(main, "job_stats", job_stats)
    monkeypatch.setenv("CRAWLER_DATABASE_URL", DSN)
    stats_json = tmp_path / "out" / "stats.json"

    assert main.main(["report", "--job", "books", "--stats-json", str(stats_json)]) == code

    assert seen == {"dsn": DSN, "job": "books", "top_domains": 10}
    assert stats_json.exists() is (code == 0)
    assert capsys.readouterr().err == (f"error: {outcome}\n" if code == 1 else "")


def test_report_that_cannot_be_written_exits_with_1(tmp_path, monkeypatch, capsys):
    async def job_stats(dsn, job, **options):
        return job_stats_of()

    monkeypatch.setattr(main, "job_stats", job_stats)
    monkeypatch.setenv("CRAWLER_DATABASE_URL", DSN)

    assert main.main(["report", "--job", "books", "--report", str(tmp_path)]) == 1  # a directory

    assert capsys.readouterr().err.startswith("error: [Errno")


def test_report_takes_its_files_title_and_domains_from_the_configuration(tmp_path, monkeypatch, capsys):
    seen = {}

    async def job_stats(dsn, job, *, top_domains):
        seen.update(dsn=dsn, top_domains=top_domains)
        return job_stats_of()

    monkeypatch.setattr(main, "job_stats", job_stats)
    html = tmp_path / "report.html"
    report = {"html": str(html), "stats_json": str(tmp_path / "unused.json"), "title": "Books", "top_domains": 3}
    path = write_config(tmp_path, {"distributed": {"database_url": DSN}, "report": report})
    stats_json = tmp_path / "stats.json"

    assert main.main(["report", "--job", "books", "--config", path, "--stats-json", str(stats_json)]) == 0

    assert seen == {"dsn": DSN, "top_domains": 3}
    assert "<title>Books: books</title>" in html.read_text(encoding="utf-8")
    assert stats_json.exists() and not (tmp_path / "unused.json").exists()
    assert capsys.readouterr().out.endswith(f"Reports: {stats_json}, {html}\n")


def test_report_lists_the_pages_of_the_job_not_saved_besides_its_statistics(tmp_path, monkeypatch, capsys):
    seen = {}

    async def job_stats(dsn, job, **options):
        return job_stats_of()

    async def export_job_pages(dsn, job, path):
        seen.update(dsn=dsn, job=job, path=path)
        return Path(path)

    monkeypatch.setattr(main, "job_stats", job_stats)
    monkeypatch.setattr(main, "export_job_pages", export_job_pages)
    monkeypatch.setenv("CRAWLER_DATABASE_URL", DSN)
    pages = str(tmp_path / "pages.csv")

    assert main.main(["report", "--job", "books", "--pages-report", pages]) == 0

    assert seen == {"dsn": DSN, "job": "books", "path": pages}
    assert capsys.readouterr().out.endswith(f"Reports: {pages}\n")


def test_report_by_the_configuration_of_a_worker_is_named_after_the_job(tmp_path, monkeypatch, capsys):
    async def job_stats(dsn, job, **options):
        return job_stats_of()

    monkeypatch.setattr(main, "job_stats", job_stats)
    report = {"stats_json": str(tmp_path / "stats-{worker}.json"), "html": str(tmp_path / "report-{worker}.html")}
    path = write_config(tmp_path, {"distributed": {"database_url": DSN}, "report": report})

    assert main.main(["report", "--job", "books", "--config", path]) == 0

    assert sorted(file.name for file in tmp_path.iterdir()) == ["config.yaml", "report-books.html", "stats-books.json"]


def test_report_with_no_file_to_write_exits_with_2(monkeypatch, capsys):
    monkeypatch.setattr(main, "job_stats", None)  # would fail if called
    monkeypatch.setenv("CRAWLER_DATABASE_URL", DSN)

    assert main.main(["report", "--job", "books"]) == 2

    assert "report: nothing to write, give --stats-json, --report or --pages-report" in capsys.readouterr().err


def test_summary_of_a_report_counts_the_pages_of_all_workers(tmp_path, monkeypatch, capsys):
    workers = {
        "w-1": {"state": "running", "pages": 4, "failed": 1, "pages_per_second": 2.0, "active_seconds": 2.0},
        "w-2": {"state": "lost", "pages": 1, "failed": 0, "pages_per_second": 0.5, "active_seconds": 2.0},
    }
    stats = job_stats_of(
        state="running", total_pages=5, successful=4, failed=1, elapsed_seconds=2.5, queued=7, in_progress=2
    )

    async def job_stats(dsn, job, **options):
        return {**stats, "workers": workers}

    monkeypatch.setattr(main, "job_stats", job_stats)
    monkeypatch.setenv("CRAWLER_DATABASE_URL", DSN)

    assert main.main(["report", "--job", "books", "--stats-json", str(tmp_path / "stats.json")]) == 0

    summary = capsys.readouterr().out
    assert summary.startswith("=== Crawl job books: running (2.50s) ===\n")
    assert "Pages: 5 (4 successful, 1 failed, 0 skipped)" in summary
    assert "Workers: w-1 (running, 4 pages, 2.0 pages/s), w-2 (lost, 1 pages, 0.5 pages/s)\n" in summary
    assert "Left: 7 pages queued, 2 in progress\n" in summary


def job_progress_of(**changes):
    fields = {
        "state": "running",
        "done": 30,
        "total": 100,
        "failed": 2,
        "percent": 30.0,
        "pages_per_second": 5.0,
        "eta": 14.0,
        "workers": 2,
        "lost": 0,
        "in_progress": 4,
        "queued": 66,
        "elapsed": 6.0,
    }
    return JobProgress(**fields | changes)


def test_status_takes_a_job_and_optionally_watches():
    args = parse_command_args(["status", "--job", "books"])
    assert (args.command, args.job, args.config, args.watch, args.interval) == ("status", "books", None, False, None)

    args = parse_command_args(["status", "--job", "books", "--config", "c.yaml", "--watch", "--interval", "0.5"])
    assert (args.config, args.watch, args.interval) == ("c.yaml", True, 0.5)


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        (job_progress_of(), 0),
        (JobError('There is no crawl job named "books"'), 1),
        (FrontierError("the database of crawl job books failed: OSError: refused"), 1),
        (KeyboardInterrupt(), 130),
    ],
)
def test_status_prints_the_progress_line_of_the_job(outcome, code, monkeypatch, capsys):
    seen = {}

    async def job_progress(dsn, job):
        seen.update(dsn=dsn, job=job)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(main, "job_progress", job_progress)
    monkeypatch.setattr(main, "watch_job", None)  # would fail if called
    monkeypatch.setenv("CRAWLER_DATABASE_URL", DSN)

    assert main.main(["status", "--job", "books"]) == code

    assert seen == {"dsn": DSN, "job": "books"}
    captured = capsys.readouterr()
    assert captured.out == (f"{format_job_progress(outcome)}\n" if code == 0 else "")
    assert captured.err == (f"error: {outcome}\n" if code == 1 else "")


@pytest.mark.parametrize(
    ("options", "interval", "outcome", "code"),
    [
        ([], 2.0, None, 0),
        (["--interval", "0.5"], 0.5, None, 0),
        ([], 2.0, JobError('There is no crawl job named "books"'), 1),
        ([], 2.0, KeyboardInterrupt(), 130),
        ([], 2.0, asyncio.CancelledError(), 143),
    ],
)
def test_status_watch_follows_the_job_until_it_is_finished(options, interval, outcome, code, tmp_path, monkeypatch):
    seen = {}

    async def watch_job(dsn, job, *, interval):
        seen.update(dsn=dsn, job=job, interval=interval)
        if outcome is not None:
            raise outcome

    monkeypatch.setattr(main, "watch_job", watch_job)
    monkeypatch.setattr(main, "job_progress", None)  # would fail if called
    path = write_config(tmp_path, {"distributed": {"database_url": DSN}})

    assert main.main(["status", "--job", "books", "--config", path, "--watch", *options]) == code

    assert seen == {"dsn": DSN, "job": "books", "interval": interval}
