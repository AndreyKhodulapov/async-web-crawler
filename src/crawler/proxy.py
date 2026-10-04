"""Proxies of the crawler: the pool they rotate in, and the proxies taken out of it for a while."""

import base64
import logging
import math
import time
import urllib.request
import zlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import unquote, urlsplit, urlunsplit

from crawler.exceptions import FetchError, NoProxyError, ProxyNetworkError
from crawler.models import ProxyStats
from crawler.urls import get_host, hide_password, normalize_url

logger = logging.getLogger(__name__)

Rotation = Literal["per_host", "per_request"]


def proxy_url_problem(value: str) -> str | None:
    """What is wrong with `value` as the URL of a proxy, None if nothing; the value is not repeated."""
    try:
        parts = urlsplit(value.strip())
        port = parts.port  # raises ValueError for a non-numeric or out-of-range port
    except ValueError:
        return "not a URL of a proxy, such as http://proxy.example:3128"
    scheme = parts.scheme.lower()
    if scheme.startswith("socks"):
        return "SOCKS proxies are not supported: use an http:// proxy, or a local bridge from HTTP to SOCKS"
    if scheme not in ("http", "https") or not parts.hostname or normalize_url(urlunsplit(parts)) is None:
        return "expected an http:// or https:// proxy URL, such as http://proxy.example:3128"
    if any(char.isspace() or not char.isprintable() for char in value.strip()):
        return "a proxy URL cannot contain spaces or control characters"
    if port is None:
        # Clients disagree on the default: curl takes 1080, aiohttp 80 or 443.
        return "the proxy URL needs a port, such as http://proxy.example:3128"
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        return "a proxy URL has no path, query or fragment"
    if ":" in unquote(parts.username or ""):
        return "the user name of a proxy cannot contain a colon"
    return None


@dataclass(frozen=True, slots=True)
class Proxy:
    """A proxy to send requests through.

    `url` has no user name or password: they go in `authorization`, the
    value of the Proxy-Authorization header. `label` is the URL to show,
    with the password hidden; it names the proxy in the log and the stats.
    """

    url: str
    label: str
    authorization: str | None = field(default=None, repr=False)

    @classmethod
    def from_url(cls, value: str) -> "Proxy":
        """The proxy at `value`; `ValueError` if it is not the URL of one."""
        if (problem := proxy_url_problem(value)) is not None:
            raise ValueError(problem)
        parts = urlsplit(value.strip())
        user, at, address = parts.netloc.rpartition("@")
        address = address.lower()  # a host in any case is one proxy; the user and password keep theirs
        scheme = parts.scheme.lower()
        authorization = None
        if parts.username is not None:
            login, password = unquote(parts.username), unquote(parts.password or "")
            # Basic authentication (RFC 7617) in UTF-8; aiohttp.BasicAuth is deprecated since aiohttp 3.14.
            authorization = "Basic " + base64.b64encode(f"{login}:{password}".encode()).decode("ascii")
        return cls(
            url=f"{scheme}://{address}",
            label=hide_password(f"{scheme}://{user}{at}{address}"),
            authorization=authorization,
        )


@dataclass(slots=True)
class _ProxyState:
    failures_in_a_row: int = 0
    out_until: float | None = None  # while out of rotation
    requests: int = 0
    failures: int = 0
    times_removed: int = 0


class ProxyPool:
    """The proxies requests go through, and which of them is up.

    Usage::

        pool = ProxyPool(["http://user:secret@proxy-1:3128", "http://proxy-2:3128"])
        proxy = pool.pick(url)  # NoProxyError if every proxy is out of rotation
        error = ...  # send the request through `proxy`, or directly when it is None
        pool.record(proxy, url, error)

    `rotation` picks the proxy of a request:

    - "per_host": every host goes through a proxy of its own, chosen by a
      hash of the host, so a site sees one address and its session does
      not move between addresses. A `ProxyNetworkError` moves the host to
      the next proxy for good, so the retry of the request goes through
      another proxy at once, and the host does not come back later;
    - "per_request": the proxies take turns, one request each.

    A proxy that fails `max_failures` requests in a row is out of rotation
    for `cooldown` seconds: requests go through the other proxies. Once
    back, it is out again after one more failure, and any response through
    it clears the count. When every proxy is out, `pick` raises
    `NoProxyError`: the request is not sent.

    `from_env` makes a pool of the proxies of the environment variables.
    """

    def __init__(
        self,
        urls: Iterable[str],
        *,
        rotation: Rotation = "per_host",
        max_failures: int = 3,
        cooldown: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rotation not in ("per_host", "per_request"):
            raise ValueError(f"rotation must be per_host or per_request, got {rotation!r}")
        if max_failures < 1:
            raise ValueError(f"max_failures must be >= 1, got {max_failures}")
        if not (math.isfinite(cooldown) and cooldown > 0):
            raise ValueError(f"cooldown must be a positive number of seconds, got {cooldown}")
        self._proxies: list[Proxy] = []
        for number, url in enumerate(urls, start=1):
            try:
                proxy = Proxy.from_url(url)
            except ValueError as error:
                raise ValueError(f"proxy {number}: {error}") from None
            if any(other.label == proxy.label for other in self._proxies):
                raise ValueError(f"proxy {number}: {proxy.label} is listed twice")
            self._proxies.append(proxy)
        if not self._proxies:
            raise ValueError("a proxy pool needs at least one proxy")
        self.rotation = rotation
        self.max_failures = max_failures
        self.cooldown = cooldown
        self._clock = clock
        self._states = {proxy.label: _ProxyState() for proxy in self._proxies}
        self._shifts: dict[str, int] = {}  # per_host: how far each host has moved from the proxy of its hash
        self._turn = 0  # per_request: where the next search starts
        # From the environment: the proxy of each scheme, and the variables for NO_PROXY.
        self._schemes: dict[str, Proxy] | None = None
        self._environment: dict[str, str] = {}

    @classmethod
    def from_env(
        cls,
        *,
        max_failures: int = 3,
        cooldown: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> "ProxyPool | None":
        """A pool of the proxies of HTTP_PROXY and HTTPS_PROXY (either case); None if neither is set.

        A URL goes through the proxy of its scheme, or directly if that is
        not set or NO_PROXY names its host. A proxy without a scheme is an
        http:// one, as curl takes it. `ValueError` names the variable that
        is not a proxy URL, not its value.
        """
        environment = urllib.request.getproxies_environment()
        urls: dict[str, str] = {}  # by scheme
        for scheme in ("http", "https"):
            if not (value := environment.get(scheme, "").strip()):
                continue
            if "://" not in value:
                value = f"http://{value}"
            if (problem := proxy_url_problem(value)) is not None:
                raise ValueError(f"{scheme.upper()}_PROXY: {problem}")
            urls[scheme] = value
        if not urls:
            return None
        proxies = {scheme: Proxy.from_url(url) for scheme, url in urls.items()}
        if (
            len(proxies) == 2
            and proxies["http"].label == proxies["https"].label
            and proxies["http"] != proxies["https"]
        ):
            # The label hides the password: one proxy for both would send one of them with the other's.
            raise ValueError("HTTP_PROXY and HTTPS_PROXY name one proxy with different passwords")
        labels = {scheme: proxy.label for scheme, proxy in proxies.items()}
        # One proxy for both schemes is one proxy of the pool, with one state.
        unique = {label: urls[scheme] for scheme, label in labels.items()}
        pool = cls(unique.values(), max_failures=max_failures, cooldown=cooldown, clock=clock)
        by_label = {proxy.label: proxy for proxy in pool._proxies}
        pool._schemes = {scheme: by_label[label] for scheme, label in labels.items()}
        pool._environment = environment
        return pool

    @property
    def proxies(self) -> list[Proxy]:
        return list(self._proxies)

    def pick(self, url: str) -> Proxy | None:
        """The proxy for a request to `url`; None to send it directly (NO_PROXY, or no proxy for its scheme).

        `NoProxyError` when every proxy for the URL is out of rotation.
        """
        candidates = self._candidates(url)
        if not candidates:
            return None
        now = self._clock()
        start = self._start(url, candidates)
        for step in range(len(candidates)):
            index = (start + step) % len(candidates)
            if self._is_active(candidates[index], now):
                if self.rotation == "per_request":
                    self._turn = index + 1
                return candidates[index]
        back_in = min(self._states[proxy.label].out_until or now for proxy in candidates) - now
        if len(candidates) == 1:
            message = f"no proxy available: {candidates[0].label} is out of rotation, back in {back_in:.1f}s"
        else:
            message = (
                f"no proxy available: all {len(candidates)} proxies are out of rotation,"
                f" the first is back in {back_in:.1f}s"
            )
        raise NoProxyError(url, message)

    def record(self, proxy: Proxy, url: str, error: FetchError | None) -> None:
        """How a request to `url` through `proxy` went.

        None: a response came through, whatever its status; a
        `ProxyNetworkError`: the proxy failed. Any other error, such as a
        timeout, counts the request, but neither way: it may be the site's.
        """
        state = self._states[proxy.label]
        state.requests += 1
        if error is None:
            state.failures_in_a_row = 0
            return
        if not isinstance(error, ProxyNetworkError):
            return
        state.failures += 1
        state.failures_in_a_row += 1
        if self.rotation == "per_host" and (host := get_host(url)) is not None:
            # The host moves to the proxy after the one that failed, however
            # many of its requests fail through that one at once.
            candidates = self._candidates(url)
            if proxy in candidates:
                self._shifts[host] = (candidates.index(proxy) + 1 - _hash(host)) % len(candidates)
        now = self._clock()
        if state.failures_in_a_row >= self.max_failures and self._is_active(proxy, now):
            state.out_until = now + self.cooldown
            state.times_removed += 1
            logger.warning(
                "Proxy %s is out of rotation for %gs, failures in a row: %d, the last: %s",
                proxy.label,
                self.cooldown,
                state.failures_in_a_row,
                error.message,
            )

    def get_stats(self) -> dict[str, ProxyStats]:
        """The proxies of the pool, by label."""
        now = self._clock()
        return {
            proxy.label: ProxyStats(
                # Not _is_active: a proxy comes back when a request picks it, not when the stats are read.
                state="out" if self._is_out(proxy, now) else "active",
                requests=(state := self._states[proxy.label]).requests,
                failures=state.failures,
                times_removed=state.times_removed,
            )
            for proxy in self._proxies
        }

    def reset_stats(self) -> None:
        """Count `requests`, `failures` and `times_removed` from zero; the proxies out of rotation stay out."""
        for state in self._states.values():
            state.requests = state.failures = state.times_removed = 0

    def _candidates(self, url: str) -> list[Proxy]:
        if self._schemes is None:
            return self._proxies
        parts = urlsplit(url)
        proxy = self._schemes.get(parts.scheme.lower())
        if proxy is None:
            return []
        host = parts.hostname or ""
        if parts.port is not None:
            host = f"{host}:{parts.port}"  # NO_PROXY may name a host with its port
        return [] if urllib.request.proxy_bypass_environment(host, self._environment) else [proxy]

    def _start(self, url: str, candidates: list[Proxy]) -> int:
        """Where the search for an active proxy starts among `candidates`."""
        if self.rotation == "per_request":
            return self._turn % len(candidates)
        host = get_host(url) or ""
        return (_hash(host) + self._shifts.get(host, 0)) % len(candidates)

    def _is_out(self, proxy: Proxy, now: float) -> bool:
        """Whether `proxy` is out of rotation at `now`."""
        out_until = self._states[proxy.label].out_until
        return out_until is not None and now < out_until

    def _is_active(self, proxy: Proxy, now: float) -> bool:
        """Whether `proxy` is in rotation at `now`; one whose cooldown is over comes back."""
        state = self._states[proxy.label]
        if state.out_until is None:
            return True
        if self._is_out(proxy, now):
            return False
        state.out_until = None
        logger.info("Proxy %s is back in rotation", proxy.label)
        return True


def _hash(host: str) -> int:
    """A hash of `host` that stays the same between runs, unlike `hash()`."""
    return zlib.crc32(host.encode())
