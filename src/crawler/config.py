"""Configuration of a crawl: its sections, their defaults, loading from YAML or JSON and validation."""

import dataclasses
import difflib
import json
import math
import re
import types
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self, Union, get_args, get_origin, get_type_hints

import yaml

from crawler.client import AsyncCrawler
from crawler.exceptions import ConfigError
from crawler.robots import product_token
from crawler.storage import CompositeStorage, DataStorage, storage_from_output
from crawler.urls import is_valid_http_url

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

_INVALID = object()  # a value that was reported and is left at its default

# Says what is wrong with a value, None if nothing is.
Check = Callable[[Any], str | None]


def _option(
    default: Any,
    *,
    minimum: float | None = None,
    above: float | None = None,
    maximum: float | None = None,
    check: Check | None = None,
    normalize: Callable[[Any], Any] | None = None,
) -> Any:
    """A field of a section with the limits of its value: `minimum <= value <= maximum`, `value > above`.

    `check` looks at a value, or at every item of a list; `normalize`
    changes the value before the checks.
    """
    limits = {"minimum": minimum, "above": above, "maximum": maximum, "check": check, "normalize": normalize}
    return field(default=default, metadata={name: limit for name, limit in limits.items() if limit is not None})


def _http_url(value: str) -> str | None:
    return None if is_valid_http_url(value) else "expected an http:// or https:// URL"


def _pattern(value: str) -> str | None:
    try:
        re.compile(value)
    except re.error as error:
        return f"not a regular expression: {error}"
    return None


def _log_level(value: str) -> str | None:
    return None if value in LOG_LEVELS else f"expected one of {', '.join(LOG_LEVELS)}"


def _not_blank(value: str) -> str | None:
    return None if value.strip() else "must not be empty"


@dataclass(frozen=True)
class CrawlOptions:
    """Section `crawler`: how much to crawl and how fast. Times are in seconds."""

    max_pages: int = _option(100, minimum=1)
    max_depth: int = _option(2, minimum=0)
    max_concurrent: int = _option(10, minimum=1)
    max_per_domain: int | None = _option(None, minimum=1)
    rate_limit: float | None = _option(1.0, above=0)  # requests per second; null lifts the limit
    per_domain_rate: bool = True  # the limit is for each host, not for all of them together
    min_delay: float = _option(0.0, minimum=0)
    jitter: float = _option(0.0, minimum=0)
    respect_robots: bool = True
    user_agent: str = _option(AsyncCrawler.DEFAULT_USER_AGENT, check=_not_blank)
    user_agents: tuple[str, ...] = _option((), check=_not_blank)  # rotated; same robots.txt name as `user_agent`
    total_timeout: float = _option(30.0, above=0)
    connect_timeout: float = _option(10.0, above=0)
    read_timeout: float = _option(20.0, above=0)
    timeout_growth: float = _option(1.5, minimum=1)
    keep_pages: bool = True  # false lets a page go once it is saved: the memory of a large crawl stays flat


@dataclass(frozen=True)
class SitemapOptions:
    """Section `sitemaps`: sitemaps whose pages are crawled along with the start URLs."""

    urls: tuple[str, ...] = _option((), check=_http_url)
    from_robots: bool = False  # also the sitemaps that robots.txt of the start URLs' sites names
    max_urls: int = _option(50_000, minimum=1)  # pages taken from one sitemap, its index included


@dataclass(frozen=True)
class RetryOptions:
    """Section `retry`: repeated attempts of a failed request, see `RetryStrategy`."""

    max_retries: int = _option(3, minimum=0)
    backoff_factor: float = _option(2.0, minimum=1)
    base_delay: float = _option(1.0, above=0)
    max_delay: float = _option(30.0, above=0)


@dataclass(frozen=True)
class CircuitBreakerOptions:
    """Section `circuit_breaker`: when to stop asking a failing host, see `CircuitBreaker`."""

    failure_threshold: float | None = _option(0.5, above=0, maximum=1)  # null turns the breaker off
    min_requests: int = _option(5, minimum=1)
    window: float = _option(60.0, above=0)
    cooldown: float = _option(30.0, minimum=0)


@dataclass(frozen=True)
class FilterOptions:
    """Section `filters`: which links to follow, see `UrlFilter`."""

    same_domain_only: bool = False
    include: tuple[str, ...] = _option((), check=_pattern)
    exclude: tuple[str, ...] = _option((), check=_pattern)


@dataclass(frozen=True)
class StorageOptions:
    """Section `storage`: where the crawled pages are saved, see `storage_from_output`."""

    outputs: tuple[str, ...] = _option((), check=_not_blank)  # files by extension, or database URLs
    batch_size: int = _option(100, minimum=1)
    csv_encoding: str = "utf-8"

    def build(self) -> DataStorage | None:
        """The storage of the pages; None if there are no outputs.

        Raises:
            ValueError: an output has an unknown extension, or is a URL of an unknown database.
            LookupError: `csv_encoding` is unknown.
        """
        storages = [
            storage_from_output(output, csv_encoding=self.csv_encoding, batch_size=self.batch_size)
            for output in self.outputs
        ]
        if not storages:
            return None
        return storages[0] if len(storages) == 1 else CompositeStorage(*storages)


@dataclass(frozen=True)
class LoggingOptions:
    """Section `logging`: the level of the log and the file it is also written to."""

    level: str = _option("INFO", check=_log_level, normalize=str.upper)
    file: str | None = _option(None, check=_not_blank)
    max_bytes: int = _option(10 * 1024 * 1024, minimum=0)  # the file is rotated at this size; 0 never rotates it
    backup_count: int = _option(5, minimum=0)  # rotated files that are kept; 0 never rotates the file


@dataclass(frozen=True)
class ReportOptions:
    """Section `report`: files the statistics are written to after the crawl."""

    stats_json: str | None = _option(None, check=_not_blank)
    html: str | None = _option(None, check=_not_blank)
    title: str = "Crawl report"
    top_domains: int = _option(10, minimum=1)


@dataclass(frozen=True)
class CrawlerConfig:
    """Everything a crawl is set up with; every key is optional and has a default.

    Usage::

        config = load_config("config.yaml")
        config = load_config("config.yaml", {"crawler": {"max_pages": 500}})
        config = CrawlerConfig.from_dict({"urls": ["https://example.com"]})
        config.crawler.max_pages, config.filters.exclude

    `urls` are the start URLs; the rest are sections, each a class of this
    module with its keys. An instance is checked when it is made by
    `from_dict` or `load_config` and cannot be changed afterwards; to
    change a value, make another one with `overrides`.
    """

    urls: tuple[str, ...] = _option((), check=_http_url)
    sitemaps: SitemapOptions = field(default_factory=SitemapOptions)
    crawler: CrawlOptions = field(default_factory=CrawlOptions)
    retry: RetryOptions = field(default_factory=RetryOptions)
    circuit_breaker: CircuitBreakerOptions = field(default_factory=CircuitBreakerOptions)
    filters: FilterOptions = field(default_factory=FilterOptions)
    storage: StorageOptions = field(default_factory=StorageOptions)
    logging: LoggingOptions = field(default_factory=LoggingOptions)
    report: ReportOptions = field(default_factory=ReportOptions)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], *, source: str | None = None) -> Self:
        """The configuration for a mapping shaped like the file: `{"urls": [...], "crawler": {...}}`.

        `source` names where the mapping came from in the error message.

        Raises:
            ConfigError: a key is unknown or a value is invalid; all of them are listed.
        """
        problems: list[str] = []
        config = _build(cls, data, "", problems)
        if not problems:
            _check_together(config, problems)
        if problems:
            raise ConfigError(problems, source)
        return config

    def to_dict(self) -> dict[str, Any]:
        """The configuration as a mapping that `from_dict` takes and JSON or YAML can hold."""
        return _plain(dataclasses.asdict(self))


def load_config(path: str | Path, overrides: Mapping[str, Any] | None = None) -> CrawlerConfig:
    """Read a configuration from a YAML (".yaml", ".yml") or a JSON (".json") file.

    `overrides` is shaped like the file and wins over it, key by key:
    `{"crawler": {"max_pages": 5}}` replaces that one key, a list replaces
    the whole list. This is how command line flags are applied. An empty
    file gives the defaults. Paths in the file are relative to the working
    directory, not to the file.

    Raises:
        ConfigError: the file cannot be read, has another extension, is not
            valid YAML or JSON, or holds an unknown key or an invalid value.
    """
    path = Path(path)
    data = _read(path)
    if overrides and isinstance(data, Mapping):
        data = _merge(data, overrides)
    return CrawlerConfig.from_dict(data, source=str(path))


class _UniqueKeyLoader(yaml.SafeLoader):
    """Rejects a key written twice in a mapping; YAML itself keeps the last one silently."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=True)
            try:
                repeated = key in seen
            except TypeError:  # a key that is a list or a mapping; reported as an unknown key later
                continue
            if repeated:
                raise yaml.constructor.ConstructorError(
                    None, None, f'the key "{key}" is given twice', key_node.start_mark
                )
            seen.add(key)
        return super().construct_mapping(node, deep)


def _read(path: Path) -> Any:
    extension = path.suffix.lower()
    if extension not in (".yaml", ".yml", ".json"):
        raise ConfigError([f'unknown extension "{path.suffix}"; expected .yaml, .yml or .json'], str(path))
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ConfigError([f"cannot read the file: {error}"], str(path)) from error
    try:
        if extension == ".json":
            return json.loads(text) if text.strip() else {}
        data = yaml.load(text, Loader=_UniqueKeyLoader)  # a SafeLoader
    except (yaml.YAMLError, json.JSONDecodeError, RecursionError) as error:
        kind = "JSON" if extension == ".json" else "YAML"
        raise ConfigError([f"not valid {kind}: {' '.join(str(error).split())}"], str(path)) from error
    return {} if data is None else data


def _merge(base: Mapping[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _build(section: type, data: Any, path: str, problems: list[str]) -> Any:
    """An instance of a section for its mapping; what is wrong goes to `problems`, the defaults stay."""
    if data is None and path:  # a section with every key commented out
        return section()
    if not isinstance(data, Mapping):
        problems.append(f"{path or 'the top level'}: expected a mapping of keys to values, got {_show(data)}")
        return section()
    hints = get_type_hints(section)
    fields = {item.name: item for item in dataclasses.fields(section)}
    values = {}
    for key, value in data.items():
        where = f"{path}.{key}" if path else str(key)
        if key not in fields:
            close = difflib.get_close_matches(str(key), fields, n=1)
            hint = f'; did you mean "{close[0]}"?' if close else f"; expected one of {', '.join(fields)}"
            problems.append(f"{where}: unknown key{hint}")
        elif dataclasses.is_dataclass(hints[key]):
            values[key] = _build(hints[key], value, where, problems)
        else:
            converted = _convert(value, hints[key], fields[key].metadata, where, problems)
            if converted is not _INVALID:
                values[key] = converted
    return section(**values)


def _convert(value: Any, hint: Any, limits: Mapping[str, Any], path: str, problems: list[str]) -> Any:
    """The value as the type `hint` of its field, or `_INVALID` with the reason in `problems`."""
    if get_origin(hint) in (Union, types.UnionType):  # only `X | None` is used
        if value is None:
            return None
        hint = next(option for option in get_args(hint) if option is not type(None))
    if get_origin(hint) is tuple:
        if not isinstance(value, list):
            problems.append(f"{path}: expected a list, got {_show(value)}")
            return _INVALID
        items = [
            _convert(item, get_args(hint)[0], limits, f"{path}[{index}]", problems) for index, item in enumerate(value)
        ]
        return _INVALID if any(item is _INVALID for item in items) else tuple(items)
    expected = _TYPES[hint]
    # True is an int in Python, but "max_pages: yes" is a mistake.
    fits = isinstance(value, bool) if hint is bool else isinstance(value, hint) and not isinstance(value, bool)
    if hint is float and isinstance(value, int) and not isinstance(value, bool):
        value, fits = float(value), True
    if not fits or (hint is float and not math.isfinite(value)):
        problems.append(f"{path}: expected {expected}, got {_show(value)}")
        return _INVALID
    if "normalize" in limits:
        value = limits["normalize"](value)
    problem = _out_of_limits(value, limits)
    if problem:
        problems.append(f"{path}: {problem}, got {_show(value)}")
        return _INVALID
    return value


_TYPES = {int: "a whole number", float: "a number", bool: "true or false", str: "a string"}


def _out_of_limits(value: Any, limits: Mapping[str, Any]) -> str | None:
    if "minimum" in limits and value < limits["minimum"]:
        return f"must be >= {limits['minimum']}"
    if "above" in limits and value <= limits["above"]:
        return f"must be > {limits['above']}"
    if "maximum" in limits and value > limits["maximum"]:
        return f"must be <= {limits['maximum']}"
    return limits["check"](value) if "check" in limits else None


def _check_together(config: CrawlerConfig, problems: list[str]) -> None:
    """Rules that involve several keys, or that only the component a key is for can check."""
    if config.sitemaps.from_robots and not config.crawler.respect_robots:
        problems.append("sitemaps.from_robots: needs crawler.respect_robots, which is false")
    name = product_token(config.crawler.user_agent)
    for index, agent in enumerate(config.crawler.user_agents):
        if product_token(agent) != name:
            problems.append(
                f'crawler.user_agents[{index}]: must use the robots.txt name "{name}" of crawler.user_agent, '
                f"got {_show(agent)}"
            )
    try:
        "".encode(config.storage.csv_encoding)
    except LookupError:
        problems.append(f"storage.csv_encoding: unknown encoding, got {_show(config.storage.csv_encoding)}")
        return
    for index, output in enumerate(config.storage.outputs):
        try:
            storage_from_output(output)
        except ValueError as error:
            problems.append(f"storage.outputs[{index}]: {error}")


def _show(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str):
        return f'"{value}"'
    if isinstance(value, Mapping):
        return "a mapping"
    if isinstance(value, list):
        return "a list"
    return str(value)


def _plain(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return [_plain(item) for item in value] if isinstance(value, tuple) else value
