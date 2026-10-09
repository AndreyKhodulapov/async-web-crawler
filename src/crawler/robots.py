"""robots.txt: downloading, parsing (RFC 9309) and caching per site; robots directives of pages."""

import asyncio
import functools
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from crawler.exceptions import (
    CircuitOpenError,
    CrawlerClosedError,
    DNSError,
    FetchError,
    PermanentError,
    ProxyError,
    TooManyRedirectsError,
)
from crawler.urls import drop_userinfo, normalize_url, percent_encode

logger = logging.getLogger(__name__)


def product_token(user_agent: str) -> str:
    """The name robots.txt knows a crawler by: "MyBot/1.0 (+https://...)" gives "mybot"."""
    match = re.match(r"[A-Za-z_-]+", user_agent.strip())
    return match.group().lower() if match else ""


# X-Robots-Tag directives that carry a value after a colon. Any other word
# before a colon names the crawler the rest of the header is for.
_VALUED_DIRECTIVES = frozenset({"unavailable_after", "max-snippet", "max-image-preview", "max-video-preview"})


def robots_directives(value: str) -> list[str]:
    """The directives of a robots meta tag or X-Robots-Tag value: "NoIndex, follow" gives ["noindex", "follow"]."""
    return [directive for part in value.split(",") if (directive := part.strip().lower())]


def robots_tag_directives(values: Iterable[str], user_agent: str) -> tuple[str, ...]:
    """The directives of X-Robots-Tag headers that apply to the crawler named by `user_agent`.

    A header is for every crawler ("noindex, nofollow") or for the one it
    names ("mybot: noindex"); the headers for other crawlers are left out.
    """
    token = product_token(user_agent)
    directives: dict[str, None] = {}  # an ordered set
    for value in values:
        name, colon, rest = value.partition(":")
        name = name.strip().lower()
        if colon and re.fullmatch(r"[a-z0-9_-]+", name) and name not in _VALUED_DIRECTIVES:
            if name != token:
                continue
            value = rest
        directives.update(dict.fromkeys(robots_directives(value)))
    return tuple(directives)


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

    `sitemaps` lists the URLs of the Sitemap lines, which belong to no
    group: they are meant for every crawler.

    `unreachable` tells why robots.txt could not be read, e.g. "HTTP 503";
    everything is disallowed then. `recoverable` says whether a later
    download may read it: False after a failure that does not pass by
    itself, such as a bad certificate or a host name that does not resolve.
    """

    def __init__(
        self,
        groups: list[_Group],
        *,
        sitemaps: Sequence[str] = (),
        unreachable: str | None = None,
        recoverable: bool = True,
    ) -> None:
        self._groups = groups
        self.sitemaps = list(sitemaps)
        self.unreachable = unreachable
        self.recoverable = recoverable

    @classmethod
    def parse(cls, text: str) -> "RobotsRules":
        groups: list[_Group] = []
        sitemaps: dict[str, None] = {}
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
            elif key == "sitemap" and (sitemap := normalize_url(value)):
                sitemaps[drop_userinfo(sitemap)] = None  # credentials of the site's choosing are not sent
        return cls(groups, sitemaps=list(sitemaps))

    @classmethod
    def allow_all(cls) -> "RobotsRules":
        return cls([])

    @classmethod
    def forbid_all(cls, reason: str, *, recoverable: bool = True) -> "RobotsRules":
        return cls([], unreachable=reason, recoverable=recoverable)

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
            "sitemaps": list(self.sitemaps),
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
    The rules of a file that was read are kept for `RULES_TTL` seconds
    (24 hours, the longest RFC 9309 advises), then downloaded again in the
    background: every caller gets the old rules at once meanwhile, and
    keeps getting them if the site does not answer (they are tried again
    after `UNREACHABLE_TTL`); such a failure does not count as an outage.
    An unreachable robots.txt is fetched again after `UNREACHABLE_TTL`
    seconds, so one timeout does not close the site for the whole crawl.
    Only the caller that starts that download waits for it: the others
    get the stale rules, which still disallow everything, until it is
    over, so a site that is slow to fail holds one task back, not every
    one that asks about it. `failed_downloads` counts the downloads of an outage, and `may_recover`
    tells a failure that passes by itself (a 5xx, a timeout) from one that
    does not (a bad certificate, a host name that does not resolve); a
    caller that has waited enough says so with `give_up`, and the site is
    not downloaded again until `forget_outages`.
    A download the fetcher did not even start (`CrawlerClosedError`,
    `CircuitOpenError`), and one that failed in a proxy (`ProxyError`), is
    no answer from the site: the error is passed on and nothing is cached.
    Files over 500 KiB are cut to that size, the minimum the RFC requires
    crawlers to read; those over `PARSE_IN_THREAD` characters are parsed
    in a thread, so that the other requests do not stand still meanwhile.
    """

    MAX_SIZE = 500 * 1024
    MAX_CRAWL_DELAY = 30.0
    UNREACHABLE_TTL = 60.0
    RULES_TTL = 24 * 3600.0
    PARSE_IN_THREAD = 32 * 1024

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
        self._stale_at: dict[str, float] = {}  # origin -> when the rules of its robots.txt read are fetched again
        self._failed: dict[str, int] = {}  # origin -> downloads of its robots.txt failed in a row
        self._downloads: dict[str, asyncio.Task[RobotsRules]] = {}
        self._started: dict[str, float] = {}  # origin -> when its download under way started

    async def fetch_robots(self, base_url: str) -> dict[str, Any]:
        """Download (or take from the cache) robots.txt of the site of `base_url`.

        An unreachable robots.txt is taken from the cache only for
        `UNREACHABLE_TTL` seconds, then it is downloaded again.

        Returns the parsed rules as a dict: "groups" (user agents, allow and
        disallow paths, crawl delay), "sitemaps" (the URLs of the Sitemap
        lines) and "unreachable" (why robots.txt could not be read, None if
        it could).

        Raises:
            ValueError: `base_url` is not a valid http(s) URL.
            CrawlerClosedError: the fetcher is closed; nothing is cached.
            CircuitOpenError: the fetcher refused to request the site; nothing is cached.
            ProxyError: the request failed in a proxy, or no proxy was left for it; nothing is cached.
        """
        return (await self._rules_for(base_url)).to_dict()

    async def is_allowed(self, url: str, user_agent: str = "*", *, wait: float | None = None) -> bool:
        """`can_fetch`, downloading the site's robots.txt first if needed.

        With `wait`, a download that takes longer than that many seconds
        from its start is not waited for: `TimeoutError` is raised, at
        once if that long has passed already, and the download goes on
        for the callers that do wait for it, into the cache. A download
        that finished in time answers even when the wait ran out at the
        same moment.
        """
        return (await self._rules_for(url, wait)).can_fetch(url, user_agent)

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

    def unreachable_for(self, url: str) -> float:
        """Seconds until the unreachable robots.txt of the site of `url` is downloaded again; 0 if it is not unreachable."""
        return max(0.0, self._expires.get(_origin(url), 0.0) - self._clock())

    def failed_downloads(self, url: str) -> int:
        """How many downloads in a row of robots.txt of the site of `url` have failed; 0 once it has been read."""
        return self._failed.get(_origin(url), 0)

    def give_up(self, url: str) -> None:
        """Keep the unreachable robots.txt of the site of `url` as it is: it is not downloaded again until `forget_outages`.

        For a caller that has waited for the site enough: every later
        request for it would otherwise download robots.txt once more. Does
        nothing for a site whose robots.txt was read, or not fetched yet.
        """
        origin = _origin(url)
        if origin in self._rules and self._rules[origin].unreachable is not None:
            self._expires.pop(origin, None)

    def forget_outages(self) -> None:
        """Download the unreachable robots.txt of every site again on the next request, and count its failures from zero.

        For a new crawl: a site given up on an hour ago may be back.
        """
        now = self._clock()
        for origin, rules in self._rules.items():
            if rules.unreachable is not None:
                self._expires[origin] = now
        self._failed.clear()

    def may_recover(self, url: str) -> bool:
        """Whether robots.txt of the site of `url`, unreachable now, may be read by a later download.

        False after a failure that does not pass by itself: a bad
        certificate, a host name that does not resolve. True while it is
        not unreachable, or not fetched yet.
        """
        rules = self._rules.get(_origin(url))
        return rules is None or rules.recoverable

    def _cached(self, url: str) -> RobotsRules:
        origin = _origin(url)
        if origin not in self._rules:
            raise LookupError(f"robots.txt of {origin} has not been fetched yet")
        return self._rules[origin]

    async def _rules_for(self, url: str, wait: float | None = None) -> RobotsRules:
        origin = _origin(url)
        # The rules of an unreachable robots.txt stay in the cache while it
        # is downloaded again: the synchronous methods keep answering.
        cached = self._rules.get(origin)
        if cached is not None and self._clock() < self._expires.get(origin, math.inf):
            if self._clock() >= self._stale_at.get(origin, math.inf) and origin not in self._downloads:
                # Nobody waits for the rules read once to be downloaded again.
                self._start_download(origin, self._refresh(origin, cached))
            return cached
        download = self._downloads.get(origin)
        if download is None:
            download = self._start_download(origin, self._download(origin))
        elif cached is not None:
            # The unreachable robots.txt is being downloaded again: the
            # stale rules answer at once, so that nobody waits for a site
            # that may be slow to fail.
            return cached
        # A caller cancelled while waiting, or done waiting, must not cancel
        # the download that other callers are waiting for too.
        waiting = asyncio.shield(download)
        if wait is None:
            return await waiting
        # The time is given to the download, not to each caller: once it is
        # up, every caller is turned away at once.
        left = self._started[origin] + wait - self._clock()
        if left <= 0:
            waiting.cancel()
            raise TimeoutError(f"robots.txt of {origin} has been downloading for over {wait:g}s")
        try:
            return await asyncio.wait_for(waiting, left)
        except TimeoutError:
            # The download may have finished in the very iteration of the
            # event loop in which the wait ran out: the task is cancelled
            # with its future done. Then the rules are in the cache.
            rules = self._rules.get(origin)
            if rules is not None and self._clock() < self._expires.get(origin, math.inf):
                return rules
            raise

    def _start_download(self, origin: str, coroutine: Coroutine[Any, Any, RobotsRules]) -> asyncio.Task[RobotsRules]:
        download = asyncio.create_task(coroutine)
        self._downloads[origin] = download
        self._started[origin] = self._clock()
        download.add_done_callback(functools.partial(self._forget_download, origin))
        return download

    def _forget_download(self, origin: str, download: asyncio.Task[RobotsRules]) -> None:
        del self._downloads[origin]
        self._started.pop(origin, None)
        if not download.cancelled():
            # Marks the exception as retrieved: when every caller was
            # cancelled, nobody else would, and asyncio would log it.
            download.exception()

    async def _download(self, origin: str) -> RobotsRules:
        rules = await self._read(origin)
        if rules.unreachable is None:
            self._expires.pop(origin, None)
            self._failed.pop(origin, None)
            self._stale_at[origin] = self._clock() + self.RULES_TTL
        else:
            logger.warning(
                "robots.txt of %s is unreachable, the site is disallowed for %gs: %s",
                origin,
                self.UNREACHABLE_TTL,
                rules.unreachable,
            )
            self._expires[origin] = self._clock() + self.UNREACHABLE_TTL
            self._failed[origin] = self._failed.get(origin, 0) + 1
            self._stale_at.pop(origin, None)
        self._rules[origin] = rules
        return rules

    async def _refresh(self, origin: str, rules: RobotsRules) -> RobotsRules:
        """Download again robots.txt whose rules `RULES_TTL` has passed for; keep `rules` if it cannot be read."""
        # Set at once: an error nobody expects must not start a download on every call.
        self._stale_at[origin] = self._clock() + self.UNREACHABLE_TTL
        try:
            fresh = await self._read(origin)
        except (CrawlerClosedError, CircuitOpenError, ProxyError) as error:
            failure = f"{type(error).__name__}: {error.message}"
        else:
            failure = fresh.unreachable
        if failure is not None:
            logger.info(
                "robots.txt of %s could not be downloaded again, its old rules stay for %gs: %s",
                origin,
                self.UNREACHABLE_TTL,
                failure,
            )
            return rules
        self._stale_at[origin] = self._clock() + self.RULES_TTL
        self._rules[origin] = fresh
        return fresh

    async def _read(self, origin: str) -> RobotsRules:
        url = f"{origin}/robots.txt"
        try:
            status, text = await self._fetch(url)
        except (CrawlerClosedError, CircuitOpenError, ProxyError):
            raise  # not an answer from the site: nothing to cache
        except TooManyRedirectsError as error:
            logger.info("robots.txt of %s: %s, everything is allowed", origin, error.message)
            rules = RobotsRules.allow_all()
        except FetchError as error:
            # A bad certificate or a host name that does not resolve is not an outage.
            recoverable = not isinstance(error, PermanentError | DNSError)
            rules = RobotsRules.forbid_all(f"{type(error).__name__}: {error.message}", recoverable=recoverable)
        else:
            if 200 <= status < 300:
                text = text[: self.MAX_SIZE]
                if len(text) > self.PARSE_IN_THREAD:
                    rules = await asyncio.to_thread(RobotsRules.parse, text)
                else:
                    rules = RobotsRules.parse(text)
            elif status == 429 or status >= 500:
                rules = RobotsRules.forbid_all(f"HTTP {status}")
            else:
                logger.info("robots.txt of %s answered HTTP %d, everything is allowed", origin, status)
                rules = RobotsRules.allow_all()
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
