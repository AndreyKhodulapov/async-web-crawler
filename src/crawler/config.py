"""Configuration of a crawl: its sections, their defaults, loading from YAML or JSON and validation."""

import dataclasses
import difflib
import json
import math
import re
import sys
import types
from collections.abc import Callable, Mapping
from dataclasses import MISSING, dataclass, field
from http.cookiejar import Cookie
from pathlib import Path
from typing import Any, Self, Union, get_args, get_origin, get_type_hints

import yaml

from crawler.client import AsyncCrawler
from crawler.exceptions import ConfigError
from crawler.filters import extension_problem, normalize_extension
from crawler.proxy import Proxy, ProxyPool, proxy_url_problem
from crawler.rendering import RESOURCE_TYPES, WAIT_STATES, Rendering, playwright_problem
from crawler.robots import product_token
from crawler.session import (
    cookie_domain_problem,
    cookie_name_problem,
    cookie_value_problem,
    header_name_problem,
    header_value_problem,
    load_cookies_file,
    make_cookie,
)
from crawler.storage import CompositeStorage, DataStorage, storage_from_output
from crawler.urls import is_valid_http_url

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
# Which pages a headless browser renders: none, every HTML page, or those of `rendering.include`.
RENDER_MODES = ("off", "always", "patterns")

# Links to files a crawl of web pages has no use for: documents, images,
# archives, media, programs, styles and scripts.
# fmt: off
EXCLUDED_EXTENSIONS = (
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
    "jpg", "jpeg", "png", "gif", "webp", "svg", "ico",
    "zip", "gz", "tar", "rar", "7z",
    "mp3", "mp4", "avi", "mov", "webm",
    "exe", "dmg", "iso",
    "css", "js",
)
# fmt: on

_INVALID = object()  # a value that was reported and is left at its default

# Says what is wrong with a value, None if nothing is.
Check = Callable[[Any], str | None]


def _option(
    default: Any = MISSING,
    *,
    default_factory: Callable[[], Any] | None = None,
    minimum: float | None = None,
    above: float | None = None,
    maximum: float | None = None,
    check: Check | None = None,
    check_name: Check | None = None,
    normalize: Callable[[Any], Any] | None = None,
    secret: bool = False,
) -> Any:
    """A field of a section with the limits of its value: `minimum <= value <= maximum`, `value > above`.

    A field without a default or a `default_factory` is a key that must be
    given. `check` looks at a value, or at every item of a list or a
    mapping; `check_name` at every name of a mapping; `normalize` changes
    the value before the checks. A `secret` value, such as a token, is
    shown neither in the messages of the checks nor in the `repr()` of its
    section.
    """
    limits = {
        "minimum": minimum,
        "above": above,
        "maximum": maximum,
        "check": check,
        "check_name": check_name,
        "normalize": normalize,
        "secret": secret or None,
    }
    metadata = {name: limit for name, limit in limits.items() if limit is not None}
    if default_factory is not None:
        return field(default_factory=default_factory, metadata=metadata, repr=not secret)
    return field(default=default, metadata=metadata, repr=not secret)


# Whitespace, C0 and C1 controls. Inside a URL they are a mistake rather than
# a part of it, e.g. a comment after the URL on its line.
_SPACE_OR_CONTROL = re.compile(r"[\s\x00-\x1f\x7f-\x9f]")


def http_url_problem(value: str) -> str | None:
    """What is wrong with `value` as a URL to crawl, None if nothing.

    One check for `urls` and `sitemaps.urls` of the configuration, the
    lines of a URL list and the URLs of the command line.
    """
    if not is_valid_http_url(value):
        return "expected an http:// or https:// URL"
    # Valid all the same: a space would be sent as %20, a tab or a line break dropped.
    if _SPACE_OR_CONTROL.search(value.strip()):
        return "a URL cannot contain spaces or control characters (a space is written %20)"
    return None


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


def _rotation(value: str) -> str | None:
    return None if value in ("per_host", "per_request") else "expected per_host or per_request"


def _render_mode(value: str) -> str | None:
    return None if value in RENDER_MODES else f"expected one of {', '.join(RENDER_MODES)}"


def _wait_state(value: str) -> str | None:
    return None if value in WAIT_STATES else f"expected one of {', '.join(WAIT_STATES)}"


def _resource_type(value: str) -> str | None:
    return None if value in RESOURCE_TYPES else f"expected one of {', '.join(sorted(RESOURCE_TYPES))}"


def _cookie_path(value: str) -> str | None:
    return None if value.startswith("/") and value.isprintable() else 'expected a path that starts with "/"'


def _file_path(value: str) -> str | None:
    if "\0" in value:
        return "must not contain a null character"
    try:
        Path(value).expanduser()
    except RuntimeError:  # "~name/..." of a user the system does not know
        return "the home directory of the user is unknown"
    return _not_blank(value)


@dataclass(frozen=True)
class CrawlOptions:
    """Section `crawler`: how much to crawl and how fast. Times are in seconds."""

    max_pages: int = _option(100, minimum=1)
    max_pages_per_host: int | None = _option(None, minimum=1)  # null: as many as max_pages
    max_depth: int = _option(2, minimum=0)
    max_concurrent: int = _option(10, minimum=1)
    max_per_domain: int | None = _option(None, minimum=1)
    rate_limit: float | None = _option(1.0, above=0)  # requests per second; null lifts the limit
    per_domain_rate: bool = True  # the limit is for each host, not for all of them together
    min_delay: float = _option(0.0, minimum=0)
    jitter: float = _option(0.0, minimum=0)
    respect_robots: bool = True
    user_agent: str = _option(AsyncCrawler.DEFAULT_USER_AGENT, check=header_value_problem, normalize=str.strip)
    user_agents: tuple[str, ...] = _option(
        (), check=header_value_problem, normalize=str.strip
    )  # rotated; same robots.txt name as `user_agent`
    total_timeout: float = _option(30.0, above=0)
    connect_timeout: float = _option(10.0, above=0)
    read_timeout: float = _option(20.0, above=0)
    timeout_growth: float = _option(1.5, minimum=1)
    max_page_size: int | None = _option(AsyncCrawler.DEFAULT_MAX_PAGE_SIZE, minimum=1)  # bytes; null lifts the limit
    max_parsing: int = _option(
        2, minimum=1
    )  # pages parsed at once; parsing costs about 40 times the page size in memory
    max_retry_after: float = _option(AsyncCrawler.DEFAULT_MAX_RETRY_AFTER, above=0)  # the longest Retry-After obeyed
    keep_pages: bool = True  # false lets a page go once it is saved: the memory of a large crawl stays flat


@dataclass(frozen=True)
class SitemapOptions:
    """Section `sitemaps`: sitemaps whose pages are crawled along with the start URLs."""

    urls: tuple[str, ...] = _option((), check=http_url_problem)
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

    same_domain_only: bool = True  # links to other hosts are not followed
    include: tuple[str, ...] = _option((), check=_pattern)
    exclude: tuple[str, ...] = _option((), check=_pattern)
    exclude_extensions: tuple[str, ...] = _option(
        EXCLUDED_EXTENSIONS, check=extension_problem, normalize=normalize_extension
    )  # links to files with these extensions are not followed; [] follows them all


@dataclass(frozen=True)
class CookieOptions:
    """A cookie of `session.cookies`: sent from the first request to the hosts of its domain only.

    A `domain` such as "example.com" is that host only; ".example.com" is
    the host and its subdomains.
    """

    name: str = _option(check=cookie_name_problem)
    value: str = _option(check=cookie_value_problem, secret=True)
    domain: str = _option(check=cookie_domain_problem, normalize=lambda domain: domain.strip().lower())
    path: str = _option("/", check=_cookie_path)
    secure: bool = False  # sent over https only


@dataclass(frozen=True)
class SessionOptions:
    """Section `session`: the cookies and the headers of the requests, see `AsyncCrawler`."""

    keep_cookies: bool = True  # false sends no cookies and keeps none: a site cannot keep a session of the crawler
    cookies: tuple[CookieOptions, ...] = ()
    cookies_file: str | None = _option(None, check=_file_path)  # Netscape cookies.txt, as browsers and curl export it
    save_cookies: str | None = _option(None, check=_file_path)  # the cookies are written there after the crawl
    headers: Mapping[str, str] = _option(
        default_factory=dict, check=header_value_problem, check_name=header_name_problem, secret=True
    )  # sent with every request, to every host

    def initial_cookies(self) -> list[Cookie]:
        """The cookies of `cookies_file`, then those of `cookies`, which win over them.

        Raises:
            ConfigError: `cookies_file` cannot be read or is not a cookies.txt file.
        """
        cookies = []
        if self.cookies_file is not None:
            try:
                cookies = load_cookies_file(self.cookies_file)
            except (OSError, ValueError) as error:
                raise ConfigError([f"session.cookies_file: cannot read the cookies: {error}"]) from error
        cookies += [
            make_cookie(cookie.name, cookie.value, cookie.domain, path=cookie.path, secure=cookie.secure)
            for cookie in self.cookies
        ]
        return cookies


@dataclass(frozen=True)
class ProxyOptions:
    """Section `proxy`: the proxies requests go through, see `ProxyPool`. Without any, requests go directly."""

    urls: tuple[str, ...] = _option((), check=proxy_url_problem, secret=True)  # http://user:password@host:port
    rotation: str = _option("per_host", check=_rotation)  # per_host: a host keeps its proxy; per_request: in turn
    from_env: bool = False  # the proxies of HTTP_PROXY, HTTPS_PROXY and NO_PROXY instead of `urls`
    max_failures: int = _option(3, minimum=1)  # failures in a row that take a proxy out of rotation
    cooldown: float = _option(60.0, above=0)  # how long a proxy stays out

    def build(self) -> ProxyPool | None:
        """The pool of the proxies; None if there are none, `from_env` included.

        The environment is read here, once.

        Raises:
            ConfigError: with `from_env`, a variable is not the URL of a proxy.
        """
        if self.from_env:
            try:
                return ProxyPool.from_env(max_failures=self.max_failures, cooldown=self.cooldown)
            except ValueError as error:
                raise ConfigError([f"proxy.from_env: {error}"]) from None
        if not self.urls:
            return None
        return ProxyPool(self.urls, rotation=self.rotation, max_failures=self.max_failures, cooldown=self.cooldown)


@dataclass(frozen=True)
class RenderingOptions:
    """Section `rendering`: pages rendered in a headless browser, see `Rendering`. Off by default."""

    mode: str = _option("off", check=_render_mode)  # always: every HTML page; patterns: those that `include` names
    include: tuple[str, ...] = _option((), check=_pattern)  # with mode: patterns, searched in the URL as in `filters`
    wait_until: str = _option("load", check=_wait_state)  # load, domcontentloaded or networkidle
    wait_for: str | None = _option(None, check=_not_blank)  # a CSS selector to wait for after that
    timeout: float = _option(30.0, above=0)  # the browser's time for a page, the waits included
    max_open_pages: int = _option(2, minimum=1)  # pages rendered at once; a browser tab takes 50 to 100 MB
    block_resources: tuple[str, ...] = _option(
        ("image", "font", "media"), check=_resource_type
    )  # the types of requests the browser does not make

    def build(self) -> Rendering | None:
        """The settings of the rendering; None if it is off."""
        if self.mode == "off":
            return None
        return Rendering(
            include=self.include,
            wait_until=self.wait_until,
            wait_for=self.wait_for,
            timeout=self.timeout,
            max_open_pages=self.max_open_pages,
            block_resources=frozenset(self.block_resources),
        )


@dataclass(frozen=True)
class StorageOptions:
    """Section `storage`: where the crawled pages are saved, see `storage_from_output`."""

    outputs: tuple[str, ...] = _option((), check=_file_path)  # files by extension, or database URLs
    batch_size: int = _option(100, minimum=1)
    csv_encoding: str = "utf-8"
    overwrite: bool = False  # files are started anew instead of added to; databases keep a row per URL anyway

    def build(self) -> DataStorage | None:
        """The storage of the pages; None if there are no outputs.

        Raises:
            ValueError: an output has an unknown extension, or is a URL of an unknown database.
            LookupError: `csv_encoding` is unknown.
        """
        storages = [
            storage_from_output(
                output, csv_encoding=self.csv_encoding, overwrite=self.overwrite, batch_size=self.batch_size
            )
            for output in self.outputs
        ]
        if not storages:
            return None
        return storages[0] if len(storages) == 1 else CompositeStorage(*storages)


@dataclass(frozen=True)
class LoggingOptions:
    """Section `logging`: the level of the log and the file it is also written to."""

    level: str = _option("INFO", check=_log_level, normalize=str.upper)
    file: str | None = _option(None, check=_file_path)
    max_bytes: int = _option(10 * 1024 * 1024, minimum=0)  # the file is rotated at this size; 0 never rotates it
    backup_count: int = _option(5, minimum=0)  # rotated files that are kept; 0 never rotates the file


@dataclass(frozen=True)
class ReportOptions:
    """Section `report`: files the statistics are written to after the crawl."""

    stats_json: str | None = _option(None, check=_file_path)
    html: str | None = _option(None, check=_file_path)
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

    urls: tuple[str, ...] = _option((), check=http_url_problem)
    sitemaps: SitemapOptions = field(default_factory=SitemapOptions)
    crawler: CrawlOptions = field(default_factory=CrawlOptions)
    retry: RetryOptions = field(default_factory=RetryOptions)
    circuit_breaker: CircuitBreakerOptions = field(default_factory=CircuitBreakerOptions)
    filters: FilterOptions = field(default_factory=FilterOptions)
    session: SessionOptions = field(default_factory=SessionOptions)
    proxy: ProxyOptions = field(default_factory=ProxyOptions)
    rendering: RenderingOptions = field(default_factory=RenderingOptions)
    storage: StorageOptions = field(default_factory=StorageOptions)
    logging: LoggingOptions = field(default_factory=LoggingOptions)
    report: ReportOptions = field(default_factory=ReportOptions)

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any], *, source: str | None = None) -> Self:
        """The configuration for a mapping shaped like the file: `{"urls": [...], "crawler": {...}}`.

        `source` names where the mapping came from in the error message.

        Raises:
            ConfigError: a key is unknown or a value is invalid; all of them are listed.
        """
        problems: list[str] = []
        config = _build(cls, mapping, "", problems)
        if not problems:
            _check_together(config, problems)
        if problems:
            raise ConfigError(problems, source)
        return config

    def to_dict(self) -> dict[str, Any]:
        """The configuration as a mapping that `from_dict` takes and JSON or YAML can hold.

        It holds the secrets of the configuration, such as the values of
        cookies and headers or the passwords of proxies, which `repr()`
        leaves out: it is not for logs.
        """
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
    mapping = _read(path)
    if overrides and isinstance(mapping, Mapping):
        mapping = _merge(mapping, overrides)
    return CrawlerConfig.from_dict(mapping, source=str(path))


def load_urls(path: str | Path) -> list[str]:
    """Read start URLs from a text file, one per line; "-" reads them from stdin.

    The file is UTF-8, with or without a BOM, and its lines may end in
    "\n", "\r\n" or "\r". Blank lines and lines that start with "#" are
    skipped, spaces around a URL are dropped, and a URL given again is
    dropped too: the first one keeps its place. A comment takes a line of
    its own: spaces inside a URL are an error.

    Raises:
        ConfigError: the file cannot be read or is not UTF-8, there is no
            stdin to read, or lines of it are not http(s) URLs; every such
            line is listed by its number.
    """
    name = "<stdin>" if path == "-" else str(path)
    if path == "-" and sys.stdin is None:
        # Started with stdin closed, e.g. `<&-` in a shell.
        raise ConfigError(["there is no standard input to read URLs from"], name)
    try:
        content = sys.stdin.buffer.read() if path == "-" else Path(path).read_bytes()
        text = content.decode("utf-8-sig")
    except (OSError, UnicodeDecodeError) as error:
        raise ConfigError([f"cannot read the file: {error}"], name) from error
    urls: list[str] = []
    problems: list[str] = []
    # Not splitlines(): it also breaks at form feeds and Unicode separators,
    # and the line numbers would not be those an editor shows.
    for number, line in enumerate(re.split(r"\r\n|\r|\n", text), start=1):
        url = line.strip()
        if not url or url.startswith("#"):
            continue
        problem = http_url_problem(url)
        if problem is None:
            urls.append(url)
        else:
            shown = url if len(url) <= 100 else f"{url[:100]}..."
            problems.append(f"{name}:{number}: {problem}, got {_show(shown)}")
    if problems:
        valid = f"{len(urls)} URL is valid" if len(urls) == 1 else f"{len(urls)} URLs are valid"
        invalid = "1 line is not" if len(problems) == 1 else f"{len(problems)} lines are not"
        raise ConfigError(problems, name, summary=f"{valid}, {invalid}")
    return list(dict.fromkeys(urls))


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
        document = yaml.load(text, Loader=_UniqueKeyLoader)  # a SafeLoader
    except (yaml.YAMLError, json.JSONDecodeError, RecursionError) as error:
        kind = "JSON" if extension == ".json" else "YAML"
        raise ConfigError([f"not valid {kind}: {' '.join(str(error).split())}"], str(path)) from error
    return {} if document is None else document


def _merge(base: Mapping[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _build(section: type, mapping: Any, path: str, problems: list[str]) -> Any:
    """An instance of a section for its mapping; what is wrong goes to `problems`, the defaults stay.

    A section with keys that must be given, such as an item of a list, is
    `_INVALID` instead when one of them is missing or wrong.
    """
    fields = {item.name: item for item in dataclasses.fields(section)}
    required = [name for name, item in fields.items() if item.default is MISSING and item.default_factory is MISSING]
    if mapping is None and path and not required:  # a section with every key commented out
        return section()
    if not isinstance(mapping, Mapping):
        problems.append(f"{path or 'the top level'}: expected a mapping of keys to values{_got(mapping, {}, section)}")
        return _INVALID if required else section()
    hints = get_type_hints(section)
    values = {}
    for key, value in mapping.items():
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
    for name in required:
        if name not in mapping:
            problems.append(f'{path}: the key "{name}" is required')
    if any(name not in values for name in required):
        return _INVALID
    return section(**values)


def _convert(value: Any, hint: Any, limits: Mapping[str, Any], path: str, problems: list[str]) -> Any:
    """The value as the type `hint` of its field, or `_INVALID` with the reason in `problems`."""
    if dataclasses.is_dataclass(hint):  # an item of a list of sections
        return _build(hint, value, path, problems)
    if get_origin(hint) in (Union, types.UnionType):  # only `X | None` is used
        if value is None:
            return None
        hint = next(option for option in get_args(hint) if option is not type(None))
    if get_origin(hint) is Mapping:
        return _convert_mapping(value, hint, limits, path, problems)
    if get_origin(hint) is tuple:
        item_hint = get_args(hint)[0]
        if not isinstance(value, list):
            section = item_hint if dataclasses.is_dataclass(item_hint) else None
            problems.append(f"{path}: expected a list{_got(value, limits, section)}")
            return _INVALID
        items = [_convert(item, item_hint, limits, f"{path}[{index}]", problems) for index, item in enumerate(value)]
        return _INVALID if any(item is _INVALID for item in items) else tuple(items)
    expected = {int: "a whole number", float: "a number", bool: "true or false", str: "a string"}[hint]
    # True is an int in Python, but "max_pages: yes" is a mistake.
    fits = isinstance(value, bool) if hint is bool else isinstance(value, hint) and not isinstance(value, bool)
    if hint is float and isinstance(value, int) and not isinstance(value, bool):
        try:
            value, fits = float(value), True
        except OverflowError:  # a whole number above 1e308
            fits = False
    if not fits or (hint is float and not math.isfinite(value)):
        # `mode: off` is `mode: false` in YAML, as are no, yes and on.
        quote = (
            "; YAML reads off, on, no and yes as false or true: put the word in quotes"
            if hint is str and isinstance(value, bool)
            else ""
        )
        problems.append(f"{path}: expected {expected}{_got(value, limits)}{quote}")
        return _INVALID
    if "normalize" in limits:
        value = limits["normalize"](value)
    problem = _out_of_limits(value, limits)
    if problem:
        problems.append(f"{path}: {problem}{_got(value, limits)}")
        return _INVALID
    return value


def _got(value: Any, limits: Mapping[str, Any], section: type | None = None) -> str:
    """The end of a message about a value: the value, unless it is a secret.

    Neither is a value given in place of a `section` with a secret key,
    such as a cookie written as one string.
    """
    secret = "secret" in limits or (
        section is not None and any("secret" in item.metadata for item in dataclasses.fields(section))
    )
    return "" if secret else f", got {_show(value)}"


def _convert_mapping(value: Any, hint: Any, limits: Mapping[str, Any], path: str, problems: list[str]) -> Any:
    """A mapping of names to values, such as `session.headers`; `check_name` looks at the names."""
    if not isinstance(value, Mapping):
        problems.append(f"{path}: expected a mapping of names to values{_got(value, limits)}")
        return _INVALID
    name_hint, item_hint = get_args(hint)
    name_limits = {"check": limits["check_name"]} if "check_name" in limits else {}
    item_limits = {name: limit for name, limit in limits.items() if name != "check_name"}
    converted = {}
    for name, item in value.items():
        where = f"{path}.{name}"
        converted[_convert(name, name_hint, name_limits, where, problems)] = _convert(
            item, item_hint, item_limits, where, problems
        )
    return _INVALID if _INVALID in converted or _INVALID in converted.values() else converted


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
    session = config.session
    if not session.keep_cookies:
        for key in ("cookies", "cookies_file", "save_cookies"):
            if getattr(session, key):
                problems.append(f"session.{key}: needs session.keep_cookies, which is false")
    names = [name.lower() for name in session.headers]
    for name in sorted({name for name in names if names.count(name) > 1}):
        problems.append(f'session.headers: the header "{name}" is given twice, in different case')
    proxy = config.proxy
    if proxy.from_env and proxy.urls:
        problems.append("proxy.from_env: cannot be used with proxy.urls; give one of them")
    labels = [Proxy.from_url(url).label for url in proxy.urls]
    for index, label in enumerate(labels):
        if label in labels[:index]:
            problems.append(f"proxy.urls[{index}]: {label} is listed twice")
    rendering = config.rendering
    if rendering.mode == "patterns" and not rendering.include:
        problems.append("rendering.mode: patterns needs rendering.include, which is empty")
    if rendering.mode != "patterns" and rendering.include:
        problems.append(f'rendering.include: needs rendering.mode: patterns, got "{rendering.mode}"')
    missing = playwright_problem() if rendering.mode != "off" else None
    if missing is not None:
        problems.append(f"rendering.mode: {missing}")
    try:
        "".encode(config.storage.csv_encoding)
    except (LookupError, ValueError):  # ValueError: a name with a null character, or the codec "undefined"
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
        return f'"{value}"' if value.isprintable() else json.dumps(value)
    if isinstance(value, Mapping):
        return "a mapping"
    if isinstance(value, list):
        return "a list"
    return str(value)


def _plain(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return [_plain(item) for item in value] if isinstance(value, tuple) else value
