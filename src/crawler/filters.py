"""Rules that decide which discovered links are worth crawling."""

import re
from collections.abc import Iterable

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

    def allow_host(self, host: str) -> None:
        """Add a host to `allowed_hosts`; does nothing when hosts are not restricted."""
        if self.allowed_hosts is not None:
            self.allowed_hosts.add(host)

    def allows(self, url: str) -> bool:
        if self.allowed_hosts is not None and get_host(url) not in self.allowed_hosts:
            return False
        if any(pattern.search(url) for pattern in self._exclude):
            return False
        return not self._include or any(pattern.search(url) for pattern in self._include)


def _compile(patterns: Iterable[str]) -> list[re.Pattern[str]]:
    compiled = []
    for pattern in patterns:
        try:
            compiled.append(re.compile(pattern))
        except re.error as exc:
            raise ValueError(f"invalid pattern {pattern!r}: {exc}") from exc
    return compiled
