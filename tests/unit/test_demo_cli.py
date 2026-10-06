"""Unit tests for command-line parsing and reports of the demo script."""

from pathlib import Path

import pytest
from helpers import FakeClock

import demo_main
from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    CSVStorage,
    FetchTimeoutError,
    JSONStorage,
    PostgresStorage,
    SQLiteStorage,
    is_valid_http_url,
)
from demo_main import format_size, make_crawler, open_storages, parse_args, print_error_report


@pytest.mark.parametrize(
    ("command", "retries", "log_level"),
    [
        ("benchmark", 0, "INFO"),
        ("parse", 2, "INFO"),
        ("crawl", 2, "WARNING"),
        ("errors", 3, "INFO"),
        ("save", 3, "INFO"),
    ],
)
def test_defaults_differ_by_command(command, retries, log_level):
    args = parse_args([command])
    assert (args.retries, args.log_level) == (retries, log_level)


@pytest.mark.parametrize(
    "option",
    [
        "--rps",
        "--min-delay",
        "--jitter",
        "--connect-timeout",
        "--read-timeout",
        "--timeout-growth",
        "--breaker-cooldown",
        "--retry-delay",
    ],
)
@pytest.mark.parametrize("value", ["inf", "nan"])
def test_rejects_non_finite_numbers(option, value):
    with pytest.raises(SystemExit):
        parse_args(["crawl", option, value])


def test_options_override_command_defaults():
    args = parse_args(["benchmark", "--retries", "3", "--log-level", "debug"])
    assert (args.retries, args.log_level) == (3, "DEBUG")


def test_timeout_options_configure_the_crawler():
    args = parse_args(
        ["crawl", "--connect-timeout", "1", "--read-timeout", "2", "--total-timeout", "3", "--timeout-growth", "2"]
    )
    crawler = make_crawler(args)
    timeout = crawler._fetcher._timeout_for(retries=1)
    assert (timeout.connect, timeout.sock_read, timeout.total) == (2, 4, 6)


@pytest.mark.parametrize("value", ["0.5", "0", "x"])
def test_rejects_timeout_growth_below_one(value):
    with pytest.raises(SystemExit):
        parse_args(["crawl", "--timeout-growth", value])


def test_breaker_options_configure_the_crawler():
    args = parse_args(["crawl", "--breaker-threshold", "0.8", "--breaker-cooldown", "5"])
    breaker = make_crawler(args).circuit_breaker
    assert (breaker.failure_threshold, breaker.cooldown) == (0.8, 5)
    assert not make_crawler(parse_args(["crawl", "--no-breaker"])).circuit_breaker.enabled


@pytest.mark.parametrize("value", ["0", "1.5", "nan", "x"])
def test_rejects_breaker_threshold_outside_zero_to_one(value):
    with pytest.raises(SystemExit):
        parse_args(["crawl", "--breaker-threshold", value])


def test_errors_defaults_keep_the_local_demo_fast():
    args = parse_args(["errors"])
    assert (args.retry_delay, args.read_timeout, args.rps, args.no_robots) == (0.2, 1.0, 0.0, True)
    assert args.breaker_cooldown == 1.0
    assert parse_args(["crawl"]).breaker_cooldown == 30.0
    assert args.json == Path("error_report.json")


def test_retry_delay_configures_the_retry_strategy():
    crawler = make_crawler(parse_args(["crawl", "--retry-delay", "0.5"]))
    assert crawler.retry_strategy.base_delay == 0.5
    assert make_crawler(parse_args(["crawl"])).retry_strategy.base_delay == 1.0


def test_rejects_zero_retry_delay():
    with pytest.raises(SystemExit):
        parse_args(["crawl", "--retry-delay", "0"])


def test_help_shows_the_defaults_of_the_command(capsys):
    with pytest.raises(SystemExit):
        parse_args(["errors", "--help"])
    help_text = " ".join(capsys.readouterr().out.split())
    assert "each chunk of the response, s (default: 1)" in help_text
    assert "0 = no limit (default: 0)" in help_text
    assert "up to 30 (default: 0.2)" in help_text


def test_errors_checks_robots_only_when_asked():
    assert parse_args(["errors", "--robots"]).no_robots is False
    with pytest.raises(SystemExit):
        parse_args(["errors", "--no-robots"])
    with pytest.raises(SystemExit):
        parse_args(["crawl", "--robots"])


def test_rejects_retry_delay_longer_than_the_longest_pause():
    assert parse_args(["crawl", "--retry-delay", "30"]).retry_delay == 30
    with pytest.raises(SystemExit):
        parse_args(["crawl", "--retry-delay", "31"])


def test_error_report_tells_open_and_half_open_circuits_apart(capsys):
    clock = FakeClock()
    breaker = CircuitBreaker(0.5, min_requests=1, cooldown=30.0, clock=clock)
    crawler = AsyncCrawler(circuit_breaker=breaker)

    def request(url, error=None):
        with breaker.call(url) as call:
            call.record(error)

    request("http://a.test/", FetchTimeoutError("http://a.test/", "timed out"))
    clock.now += breaker.cooldown  # a.test is half-open now
    request("http://b.test/", FetchTimeoutError("http://b.test/", "timed out"))
    request("http://c.test/")

    print_error_report(crawler)
    output = capsys.readouterr().out
    assert "=== Circuit breaker (3 hosts: 1 open, 1 half-open) ===" in output


def test_save_defaults(monkeypatch):
    monkeypatch.delenv("CRAWLER_DATABASE_URL", raising=False)

    args = parse_args(["save"])

    assert (args.json, args.csv, args.database_url) == (Path("pages.jsonl"), Path("pages.csv"), "sqlite:///crawler.db")
    assert (args.indent, args.csv_encoding, args.batch_size, args.append, args.preview) == (None, "utf-8", 10, False, 3)
    # The site is local, as in `errors`.
    assert (args.retry_delay, args.read_timeout, args.rps, args.no_robots) == (0.2, 1.0, 0.0, True)


def test_save_takes_the_database_from_the_environment(monkeypatch):
    monkeypatch.setenv("CRAWLER_DATABASE_URL", "postgresql://crawler:secret@db.example/pages")
    assert parse_args(["save"]).database_url == "postgresql://crawler:secret@db.example/pages"
    # The option wins over the variable.
    assert parse_args(["save", "--database-url", "sqlite:///other.db"]).database_url == "sqlite:///other.db"

    monkeypatch.setenv("CRAWLER_DATABASE_URL", "")
    assert parse_args(["save"]).database_url == "sqlite:///crawler.db"


@pytest.mark.parametrize("url", ["mysql://localhost/crawler", "crawler.db", "sqlite://crawler.db"])
def test_save_rejects_a_database_url_it_cannot_use(url, monkeypatch, capsys):
    with pytest.raises(SystemExit):
        parse_args(["save", "--database-url", url])
    assert "argument --database-url: " in capsys.readouterr().err

    monkeypatch.setenv("CRAWLER_DATABASE_URL", url)
    with pytest.raises(SystemExit):
        parse_args(["save"])
    # The variable is read by `save` alone.
    assert parse_args(["crawl"]).command == "crawl"


def test_save_help_does_not_show_the_database_password(monkeypatch, capsys):
    monkeypatch.setenv("CRAWLER_DATABASE_URL", "postgresql://crawler:secret@db.example/pages")
    with pytest.raises(SystemExit):
        parse_args(["save", "--help"])
    help_text = " ".join(capsys.readouterr().out.split())
    assert "(default: $CRAWLER_DATABASE_URL, or sqlite:///crawler.db)" in help_text
    assert "secret" not in help_text


@pytest.mark.parametrize(
    "options", [["--csv-encoding", "no-such-encoding"], ["--batch-size", "0"], ["--indent", "-1"], ["--preview", "0"]]
)
def test_save_rejects_bad_storage_options(options):
    with pytest.raises(SystemExit):
        parse_args(["save", *options])


def test_save_rejects_one_file_for_json_and_csv(tmp_path, capsys):
    with pytest.raises(SystemExit):
        parse_args(["save", "--json", str(tmp_path / "pages"), "--csv", str(tmp_path / "pages")])

    assert "--json and --csv must be different files" in capsys.readouterr().err


def test_save_options_configure_the_storages(tmp_path):
    database = tmp_path / "pages.db"
    options = ["--json", str(tmp_path / "p.json"), "--indent", "2", "--csv", str(tmp_path / "p.csv")]
    options += ["--csv-encoding", "cp1252", "--database-url", f"sqlite:///{database}", "--batch-size", "5"]

    storages = open_storages(parse_args(["save", *options]))

    assert list(storages) == [str(tmp_path / "p.json"), str(tmp_path / "p.csv"), f"sqlite:///{database}"]
    json_storage, csv_storage, database_storage = storages.values()
    assert type(json_storage) is JSONStorage
    assert (json_storage.path, json_storage.indent) == (tmp_path / "p.json", 2)
    assert type(csv_storage) is CSVStorage
    assert (csv_storage.path, csv_storage.encoding) == (tmp_path / "p.csv", "cp1252")
    assert type(database_storage) is SQLiteStorage
    assert database_storage.path == database
    assert {storage.batch_size for storage in storages.values()} == {5}
    # Nothing is opened until the first page is saved.
    assert list(tmp_path.iterdir()) == []


def test_save_shows_the_database_without_its_password():
    args = parse_args(["save", "--database-url", "postgresql://crawler:secret@db.example:5433/pages"])

    location, storage = list(open_storages(args).items())[2]

    assert location == "postgresql://crawler:***@db.example:5433/pages"
    assert type(storage) is PostgresStorage


@pytest.mark.parametrize(
    ("size", "shown"),
    [(0, "0 B"), (1023, "1023 B"), (1536, "1.5 KB"), (5 * 1024**2, "5.0 MB"), (3 * 1024**3, "3072.0 MB")],
)
def test_format_size(size, shown):
    assert format_size(size) == shown


@pytest.mark.parametrize(("command", "count"), [("benchmark", 10), ("parse", 4), ("crawl", 2)])
def test_default_urls_come_from_the_file_next_to_the_script(command, count, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # the file is found wherever the script is run from

    urls = parse_args([command]).urls

    assert len(urls) == count
    assert all(is_valid_http_url(url) for url in urls)


def test_default_urls_follow_the_file(tmp_path, monkeypatch):
    urls_file = tmp_path / "urls.yaml"
    urls_file.write_text("crawl:\n  - https://one.example/\n  - https://two.example/  # a comment\n")
    monkeypatch.setattr(demo_main, "DEMO_URLS_FILE", urls_file)

    assert parse_args(["crawl"]).urls == ["https://one.example/", "https://two.example/"]


@pytest.mark.parametrize(
    "argv",
    [["crawl", "https://example.com/"], ["benchmark", "https://example.com/"], ["errors"], ["save"]],
)
def test_default_urls_file_is_not_read_when_not_needed(argv, tmp_path, monkeypatch):
    monkeypatch.setattr(demo_main, "DEMO_URLS_FILE", tmp_path / "missing.yaml")

    assert parse_args(argv).urls == argv[1:]


@pytest.mark.parametrize("argv", [["--help"], ["crawl", "--help"]])
def test_help_does_not_need_the_default_urls_file(argv, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(demo_main, "DEMO_URLS_FILE", tmp_path / "missing.yaml")

    with pytest.raises(SystemExit) as exit_info:
        parse_args(argv)

    assert exit_info.value.code == 0
    assert "usage:" in capsys.readouterr().out


@pytest.mark.parametrize(
    "content",
    [
        None,  # no file
        "",
        "- https://example.com/\n",
        "parse:\n  - https://example.com/\n",
        "crawl: https://example.com/\n",
        "crawl: []\n",
        "crawl:\n  - example.com\n",
        "crawl:\n  - 42\n",
        "crawl: [unclosed\n",
    ],
)
def test_broken_default_urls_file_is_a_usage_error(content, tmp_path, monkeypatch, capsys):
    urls_file = tmp_path / "urls.yaml"
    if content is not None:
        urls_file.write_text(content)
    monkeypatch.setattr(demo_main, "DEMO_URLS_FILE", urls_file)

    with pytest.raises(SystemExit) as exit_info:
        parse_args(["crawl"])

    assert exit_info.value.code == 2
    assert "urls.yaml" in capsys.readouterr().err


def test_scale_defaults_and_options():
    args = parse_args(["scale"])
    assert (args.pages, args.delay, args.concurrency, args.no_memory, args.log_level) == (
        [100, 500, 1000],
        0.05,
        20,
        False,
        "WARNING",
    )

    args = parse_args(["scale", "10", "20", "--delay", "0", "--concurrency", "5", "--no-memory"])
    assert (args.pages, args.delay, args.concurrency, args.no_memory) == ([10, 20], 0.0, 5, True)


@pytest.mark.parametrize("options", [["0"], ["ten"], ["--delay", "-1"], ["--concurrency", "0"], ["--rps", "1"]])
def test_scale_rejects_invalid_options(options):
    with pytest.raises(SystemExit):
        parse_args(["scale", *options])
