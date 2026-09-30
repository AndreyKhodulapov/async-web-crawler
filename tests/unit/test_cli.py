"""Unit tests for command-line parsing of the demo script."""

import pytest

from main import parse_args


@pytest.mark.parametrize(
    ("command", "retries", "log_level"),
    [("benchmark", 0, "INFO"), ("parse", 2, "INFO"), ("crawl", 2, "WARNING")],
)
def test_defaults_differ_by_command(command, retries, log_level):
    args = parse_args([command])
    assert (args.retries, args.log_level) == (retries, log_level)


@pytest.mark.parametrize("option", ["--rps", "--min-delay", "--jitter"])
@pytest.mark.parametrize("value", ["inf", "nan"])
def test_rejects_non_finite_numbers(option, value):
    with pytest.raises(SystemExit):
        parse_args(["crawl", option, value])


def test_options_override_command_defaults():
    args = parse_args(["benchmark", "--retries", "3", "--log-level", "debug"])
    assert (args.retries, args.log_level) == (3, "DEBUG")
