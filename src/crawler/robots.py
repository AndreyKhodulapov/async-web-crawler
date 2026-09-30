"""robots.txt: downloading, parsing (RFC 9309) and caching per site."""

import asyncio
import functools
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from crawler.exceptions import CrawlerClosedError, FetchError, TooManyRedirectsError
from crawler.urls import normalize_url, percent_encode

logger = logging.getLogger(__name__)


def product_token(user_agent: str) -> str:
    """The name robots.txt knows a crawler by: "MyBot/1.0 (+https://...)" gives "mybot"."""
    match = re.match(r"[A-Za-z_-]+", user_agent.strip())
    return match.group().lower() if match else ""


@dataclass(frozen=True, slots=True)
class _Rule:
    allow: bool
    path: str  # percent-encoded like crawled URLs, "*" and "$" kept
    pattern: re.Pattern[str]

    @classmethod
    def parse(cls, allow: bool, value: str) -> "_Rule | None":
        path = percent_encode(value)
        if not path:  # "Disallow:" with no path means nothing is disallowed
            return None
        # "*" matches any sequence of characters, "$" at the end anchors the
        # pattern to the end of the URL; everything else is literal.
        anchored = path.endswith("$")
        first, *rest = (path[:-1] if anchored else path).split("*")
        regex = re.escape(first)
        if rest:
            # A plain ".*" per "*" backtracks through every split of the URL
            # between them: a few wildcards in a site's robots.txt would
            # freeze the crawl. Each "*" here takes the leftmost occurrence
            # of the part after it, in an atomic group that is never
            # re-entered; the leftmost one leaves the most room for the
            # parts that follow, so the answer is the same.
            *middle, last = rest
            regex += "".join(f"(?>.*?{re.escape(part)})" for part in middle)
            regex += f".*{re.escape(last)}" if anchored else f"(?>.*?{re.escape(last)})"
        return cls(allow, path, re.compile(regex + (r"\Z" if anchored else "")))


@dataclass(slots=True)
class _Group:
    agents: list[str]
    rules: list[_Rule] = field(default_factory=list)
    crawl_delay: float | None = None


class RobotsRules:
    """The rules of one site's robots.txt.

    Follows RFC 9309: the group for the crawler's product token (matched
    case-insensitively) applies, or the "*" group if there is none; several
    groups for the same agent are merged. Among the rules that match a URL
    the longest one wins, and Allow wins a tie. "*" and "$" work as
    wildcards. /robots.txt itself is always allowed.

    Crawl-delay is not part of the RFC, but many sites use it; when it is
    set several times, in one group or in several matching ones, the
    largest value is taken.

    `unreachable` tells why robots.txt could not be read, e.g. "HTTP 503";
    everything is disallowed then.
    """

    def __init__(self, groups: list[_Group], *, unreachable: str | None = None) -> None:
        self._groups = groups
        self.unreachable = unreachable

    @classmethod
    def parse(cls, text: str) -> "RobotsRules":
        groups: list[_Group] = []
        group: _Group | None = None
        # Consecutive User-agent lines share one group; any other rule line
        # closes the list of agents, so the next User-agent starts a new group.
        open_agents: list[str] | None = None
        for line in text.splitlines():
            key, _, value = line.split("#", 1)[0].partition(":")
            key, value = key.strip().lower(), value.strip()
            if key == "user-agent":
                agent = "*" if value == "*" else product_token(value)
                if open_agents is None:
                    group = _Group(agents=[agent])
                    groups.append(group)
                    open_agents = group.agents
                else:
                    open_agents.append(agent)
            elif key in ("allow", "disallow", "crawl-delay"):
                open_agents = None
                if group is None:
                    continue  # rules before the first User-agent apply to nobody
                if key == "crawl-delay":
                    group.crawl_delay = _parse_delay(value, group.crawl_delay)
                elif rule := _Rule.parse(key == "allow", value):
                    group.rules.append(rule)
        return cls(groups)

    @classmethod
    def allow_all(cls) -> "RobotsRules":
        return cls([])

    @classmethod
    def forbid_all(cls, reason: str) -> "RobotsRules":
        return cls([], unreachable=reason)

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Whether a crawler with this User-Agent may fetch `url`; False for an invalid URL."""
        normalized = normalize_url(url)
        if normalized is None:
            return False
        parts = urlsplit(normalized)
        target = parts.path + (f"?{parts.query}" if parts.query else "")
        if target == "/robots.txt":
            return True
        if self.unreachable is not None:
            return False
        best_length, allowed = -1, True
        for group in self._groups_for(user_agent):
            for rule in group.rules:
                if rule.pattern.match(target) and (
                    len(rule.path) > best_length or (len(rule.path) == best_length and rule.allow)
                ):
                    best_length, allowed = len(rule.path), rule.allow
        return allowed

    def crawl_delay(self, user_agent: str = "*") -> float | None:
        delays = [group.crawl_delay for group in self._groups_for(user_agent) if group.crawl_delay is not None]
        return max(delays, default=None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "unreachable": self.unreachable,
            "groups": [
                {
                    "user_agents": group.agents,
                    "allow": [rule.path for rule in group.rules if rule.allow],
                    "disallow": [rule.path for rule in group.rules if not rule.allow],
                    "crawl_delay": group.crawl_delay,
                }
                for group in self._groups
            ],
        }

    def _groups_for(self, user_agent: str) -> list[_Group]:
        token = product_token(user_agent)
        own = [group for group in self._groups if token and token in group.agents]
        return own or [group for group in self._groups if "*" in group.agents]


class RobotsParser:
    """Fetches robots.txt once per site and answers whether URLs may be crawled.

    Usage::

        robots = RobotsParser(fetch)                  # fetch(url) -> (status, body)
        await robots.fetch_robots("https://example.com/any/page")
        robots.can_fetch("https://example.com/private/", "MyBot/1.0")
        robots.get_crawl_delay("https://example.com/", "MyBot/1.0")

    `fetch` downloads a URL and returns the HTTP status and the body; it
    raises `FetchError` when no response arrives at all.

    Rules are cached per origin (scheme, host and port), as robots.txt
    applies to exactly one origin. Concurrent requests for a site that has
    not been fetched yet share a single download.

    The HTTP status decides what happens when there is no usable file
    (RFC 9309): 4xx means there are no rules and everything is allowed,
    and so does a redirect loop, which the RFC lets crawlers count as an
    unavailable file; 5xx, 429 and network errors mean the site is
    unreachable and everything is disallowed. 429 is treated as a server
    error, as major search engines do: the site is asking crawlers to back off.
    Unlike the rules of a file that was read, which are kept for good, an
    unreachable robots.txt is fetched again after `UNREACHABLE_TTL`
    seconds, so one timeout does not close the site for the whole crawl.
    Files over 500 KiB are cut to that size, the minimum the RFC requires
    crawlers to read.
    """

    MAX_SIZE = 500 * 1024
    MAX_CRAWL_DELAY = 30.0
    UNREACHABLE_TTL = 60.0

    def __init__(
        self,
        fetch: Callable[[str], Awaitable[tuple[int, str]]],
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetch = fetch
        self._clock = clock
        self._rules: dict[str, RobotsRules] = {}
        self._expires: dict[str, float] = {}  # origin -> when its unreachable robots.txt is fetched again
        self._downloads: dict[str, asyncio.Task[RobotsRules]] = {}

    async def fetch_robots(self, base_url: str) -> dict[str, Any]:
        """Download (or take from the cache) robots.txt of the site of `base_url`.

        An unreachable robots.txt is taken from the cache only for
        `UNREACHABLE_TTL` seconds, then it is downloaded again.

        Returns the parsed rules as a dict: "groups" (user agents, allow and
        disallow paths, crawl delay) and "unreachable" (why robots.txt could
        not be read, None if it could).

        Raises:
            ValueError: `base_url` is not a valid http(s) URL.
            CrawlerClosedError: the fetcher is closed; nothing is cached.
        """
        return (await self._rules_for(base_url)).to_dict()

    async def is_allowed(self, url: str, user_agent: str = "*") -> bool:
        """`can_fetch`, downloading the site's robots.txt first if needed."""
        return (await self._rules_for(url)).can_fetch(url, user_agent)

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Whether `user_agent` may fetch `url`. The site's rules must have been fetched.

        Answers from the cache as it is, even when the rules of an
        unreachable robots.txt are due to be fetched again.

        Raises:
            LookupError: robots.txt of this site has not been fetched yet.
        """
        return self._cached(url).can_fetch(url, user_agent)

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> float:
        """Crawl-delay of the site of `url` for `user_agent`, capped at `MAX_CRAWL_DELAY`; 0 if unset.

        The cap keeps a site asking for, say, one request a day from
        freezing the crawl. The site's rules must have been fetched.
        """
        delay = self._cached(url).crawl_delay(user_agent)
        return 0.0 if delay is None else min(delay, self.MAX_CRAWL_DELAY)

    def unreachable_reason(self, url: str) -> str | None:
        """Why robots.txt of the site of `url` could not be read, or None. The rules must have been fetched."""
        return self._cached(url).unreachable

    def _cached(self, url: str) -> RobotsRules:
        origin = _origin(url)
        if origin not in self._rules:
            raise LookupError(f"robots.txt of {origin} has not been fetched yet")
        return self._rules[origin]

    async def _rules_for(self, url: str) -> RobotsRules:
        origin = _origin(url)
        # The rules of an unreachable robots.txt stay in the cache while it
        # is downloaded again: the synchronous methods keep answering.
        if origin in self._rules and self._clock() < self._expires.get(origin, math.inf):
            return self._rules[origin]
        download = self._downloads.get(origin)
        if download is None:
            download = asyncio.create_task(self._download(origin))
            self._downloads[origin] = download
            download.add_done_callback(functools.partial(self._forget_download, origin))
        # A caller cancelled while waiting must not cancel the download
        # that other callers are waiting for too.
        return await asyncio.shield(download)

    def _forget_download(self, origin: str, download: asyncio.Task[RobotsRules]) -> None:
        del self._downloads[origin]
        if not download.cancelled():
            # Marks the exception as retrieved: when every caller was
            # cancelled, nobody else would, and asyncio would log it.
            download.exception()

    async def _download(self, origin: str) -> RobotsRules:
        url = f"{origin}/robots.txt"
        try:
            status, text = await self._fetch(url)
        except CrawlerClosedError:
            raise  # not an answer from the site: nothing to cache
        except TooManyRedirectsError as error:
            logger.info("robots.txt of %s: %s, everything is allowed", origin, error.message)
            rules = RobotsRules.allow_all()
        except FetchError as error:
            rules = RobotsRules.forbid_all(f"{type(error).__name__}: {error.message}")
        else:
            if 200 <= status < 300:
                rules = RobotsRules.parse(text[: self.MAX_SIZE])
            elif status == 429 or status >= 500:
                rules = RobotsRules.forbid_all(f"HTTP {status}")
            else:
                logger.info("robots.txt of %s answered HTTP %d, everything is allowed", origin, status)
                rules = RobotsRules.allow_all()
        if rules.unreachable is None:
            self._expires.pop(origin, None)
        else:
            logger.warning(
                "robots.txt of %s is unreachable, the site is disallowed for %gs: %s",
                origin,
                self.UNREACHABLE_TTL,
                rules.unreachable,
            )
            self._expires[origin] = self._clock() + self.UNREACHABLE_TTL
        self._rules[origin] = rules
        return rules


def _origin(url: str) -> str:
    normalized = normalize_url(url)
    if normalized is None:
        raise ValueError(f"not an absolute http(s) URL: {url!r}")
    parts = urlsplit(normalized)
    return f"{parts.scheme}://{parts.netloc.rpartition('@')[2]}"


def _parse_delay(value: str, current: float | None) -> float | None:
    try:
        delay = float(value)
    except ValueError:
        return current
    if not math.isfinite(delay) or delay < 0:
        return current
    return delay if current is None else max(current, delay)
