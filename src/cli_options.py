"""What the crawler CLI and the demo commands share: checks of command-line values."""

import argparse
import codecs
import math
import re
from collections.abc import Callable

from crawler import is_valid_http_url, storage_from_url
from crawler.config import http_url_problem
from crawler.distributed.worker import check_worker_name
from crawler.proxy import proxy_url_problem


def number(raw: str, number_type: type[int] | type[float] = float) -> int | float:
    """`raw` as a finite number of `number_type`; the checks every numeric option shares."""
    try:
        value = number_type(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
    if not math.isfinite(value):
        raise argparse.ArgumentTypeError(f"must be a finite number, got {raw}")
    return value


def positive(number_type: type[int] | type[float], *, allow_zero: bool = False) -> Callable[[str], int | float]:
    def parse(raw: str) -> int | float:
        value = number(raw, number_type)
        if value < 0 or (value == 0 and not allow_zero):
            raise argparse.ArgumentTypeError(f"must be {'non-negative' if allow_zero else 'positive'}, got {raw}")
        return value

    return parse


def at_least_one(raw: str) -> float:
    if (value := number(raw)) < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {raw}")
    return value


def share(raw: str) -> float:
    if not 0 < (value := number(raw)) <= 1:
        raise argparse.ArgumentTypeError(f"must be in (0, 1], got {raw}")
    return value


def http_url(raw: str) -> str:
    if not is_valid_http_url(raw):
        raise argparse.ArgumentTypeError(f"not an absolute http(s) URL: {raw!r}")
    # Here rather than in the check of the configuration, which would name `urls`.
    if (problem := http_url_problem(raw)) is not None:
        raise argparse.ArgumentTypeError(f"{problem}, got {raw!r}")
    return raw


def proxy_url(raw: str) -> str:
    # The value is not repeated: it may hold a password.
    if (problem := proxy_url_problem(raw)) is not None:
        raise argparse.ArgumentTypeError(problem)
    return raw


def worker_name(raw: str) -> str:
    try:
        check_worker_name(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None
    return raw


def regex(raw: str) -> str:
    try:
        re.compile(raw)
    except re.error as exc:
        raise argparse.ArgumentTypeError(f"invalid regular expression {raw!r}: {exc}") from None
    return raw


def database_url(raw: str) -> str:
    try:
        storage_from_url(raw)  # opens nothing
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None
    return raw


def encoding(raw: str) -> str:
    try:
        codecs.lookup(raw)
    except LookupError:
        raise argparse.ArgumentTypeError(f"unknown encoding: {raw!r}") from None
    return raw
