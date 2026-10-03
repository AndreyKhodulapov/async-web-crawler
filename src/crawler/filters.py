"""Rules that decide which discovered links are worth crawling."""

import posixpath
import re
from collections.abc import Iterable
from urllib.parse import unquote, urlsplit

from crawler.urls import get_host


class UrlFilter:
    """Accepts or rejects URLs by host and by regular expressions.

    - `allowed_hosts`: if given, only URLs on these exact hosts pass
      ("www.example.com" and "example.com" are different hosts).
    - `include_patterns`: if given, a URL must match at least one of them.
    - `exclude_patterns`: a URL matching any of them is rejected, even if it
      also matches an include pattern.
    - `exclude_extensions`: a URL whose path ends in a file with one of these
      extensions ("pdf" or ".pdf", any case) is rejected; the query is not
      looked at, so "/report.pdf?v=2" is rejected and "/view?file=a.pdf" is not.
    - `max_url_length`: if given, a normalized URL longer than this many
      characters is rejected: such URLs are mostly generated ones, such as
      a filter or a session piled up in the query.

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
        exclude_extensions: Iterable[str] = (),
        max_url_length: int | None = None,
    ) -> None:
        if max_url_length is not None and max_url_length < 1:
            raise ValueError(f"max_url_length must be >= 1 or None, got {max_url_length}")
        self.allowed_hosts = None if allowed_hosts is None else set(allowed_hosts)
        self._include = _compile(include_patterns)
        self._exclude = _compile(exclude_patterns)
        self._extensions = _extensions(exclude_extensions)
        self.max_url_length = max_url_length

    def allow_host_of(self, url: str) -> None:
        """Add the host of `url` to `allowed_hosts`; does nothing when hosts are not restricted."""
        host = get_host(url)
        if self.allowed_hosts is not None and host is not None:
            self.allowed_hosts.add(host)

    def allows(self, url: str) -> bool:
        if self.max_url_length is not None and len(url) > self.max_url_length:
            return False
        if self.allowed_hosts is not None and get_host(url) not in self.allowed_hosts:
            return False
        if self._extensions and _file_extension(url) in self._extensions:
            return False
        forms = (url, unquote(url))
        if _matches(self._exclude, forms):
            return False
        return not self._include or _matches(self._include, forms)


def _file_extension(url: str) -> str:
    """The extension of the file the path of `url` names, lowercase and without the dot; "" if none."""
    name = posixpath.basename(unquote(urlsplit(url).path))
    return posixpath.splitext(name)[1][1:].lower()


def normalize_extension(extension: str) -> str:
    """The form in which extensions are compared: ".PDF" becomes "pdf"."""
    return extension.strip().lower().removeprefix(".")


def extension_problem(extension: str) -> str | None:
    """What is wrong with a normalized extension; None if nothing is."""
    if not extension or "/" in extension or "." in extension:
        # Only the last extension is compared: "tar.gz" would never match, "gz" does.
        return "expected one extension without dots, such as 'pdf' or 'gz'"
    return None


def _matches(patterns: list[re.Pattern[str]], forms: tuple[str, ...]) -> bool:
    return any(pattern.search(form) for pattern in patterns for form in forms)


def _extensions(extensions: Iterable[str]) -> frozenset[str]:
    if isinstance(extensions, str):
        raise TypeError(f"expected a list of extensions, got a string: {extensions!r}")
    normalized = set()
    for extension in extensions:
        value = normalize_extension(extension)
        problem = extension_problem(value)
        if problem:
            raise ValueError(f"invalid file extension {extension!r}: {problem}")
        normalized.add(value)
    return frozenset(normalized)


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
