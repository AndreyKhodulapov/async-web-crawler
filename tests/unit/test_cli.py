"""Unit tests for command-line parsing and reports of the demo script."""

from pathlib import Path

import pytest
from helpers import FakeClock

from crawler import CircuitBreaker, FetchTimeoutError
from main import make_crawler, parse_args, print_error_report


@pytest.mark.parametrize(
    ("command", "retries", "log_level"),
    [("benchmark", 0, "INFO"), ("parse", 2, "INFO"), ("crawl", 2, "WARNING"), ("errors", 3, "INFO")],
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
    timeout = crawler._timeout_for(retries=1)
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
    crawler = make_crawler(parse_args(["crawl"]))
    crawler.circuit_breaker = breaker

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
