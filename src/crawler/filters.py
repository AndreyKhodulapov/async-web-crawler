"""Rules that decide which discovered links are worth crawling."""

import re
from collections.abc import Iterable
from urllib.parse import unquote

from crawler.urls import get_host


class UrlFilter:
    """Accepts or rejects URLs by host and by regular expressions.

    - `allowed_hosts`: if given, only URLs on these exact hosts pass
      ("www.example.com" and "example.com" are different hosts).
    - `include_patterns`: if given, a URL must match at least one of them.
    - `exclude_patterns`: a URL matching any of them is rejected, even if it
      also matches an include pattern.

    Patterns are searched anywhere in the normalized URL (`re.search`), so
    anchor them when needed: r"\\.pdf$", r"^https://example\\.com/blog/".
    The normalized URL is percent-encoded; a pattern also matches its
    decoded form, so both r"/café" and r"/caf%C3%A9" find "/caf%C3%A9".
    """

    def __init__(
        self,
        *,
        allowed_hosts: Iterable[str] | None = None,
        include_patterns: Iterable[str] = (),
        exclude_patterns: Iterable[str] = (),
    ) -> None:
        self.allowed_hosts = None if allowed_hosts is None else set(allowed_hosts)
        self._include = _compile(include_patterns)
        self._exclude = _compile(exclude_patterns)

    def allow_host_of(self, url: str) -> None:
        """Add the host of `url` to `allowed_hosts`; does nothing when hosts are not restricted."""
        host = get_host(url)
        if self.allowed_hosts is not None and host is not None:
            self.allowed_hosts.add(host)

    def allows(self, url: str) -> bool:
        if self.allowed_hosts is not None and get_host(url) not in self.allowed_hosts:
            return False
        forms = (url, unquote(url))
        if _matches(self._exclude, forms):
            return False
        return not self._include or _matches(self._include, forms)


def _matches(patterns: list[re.Pattern[str]], forms: tuple[str, ...]) -> bool:
    return any(pattern.search(form) for pattern in patterns for form in forms)


def _compile(patterns: Iterable[str]) -> list[re.Pattern[str]]:
    # A string is an iterable of characters: "pdf" would become three
    # one-letter patterns and reject almost every URL.
    if isinstance(patterns, str):
        raise TypeError(f"expected a list of patterns, got a string: {patterns!r}")
    compiled = []
    for pattern in patterns:
        try:
            compiled.append(re.compile(pattern))
        except re.error as exc:
            raise ValueError(f"invalid pattern {pattern!r}: {exc}") from exc
    return compiled
