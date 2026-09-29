"""URL helpers: validation, normalization and resolution of relative links."""

from urllib.parse import urljoin, urlsplit, urlunsplit

import idna

_DEFAULT_PORTS = {"http": 80, "https": 443}


def is_valid_http_url(url: str) -> bool:
    """Return True for an absolute http(s) URL with a host."""
    try:
        parts = urlsplit(url)
        return parts.scheme in ("http", "https") and bool(parts.hostname)
    except ValueError:  # e.g. an unclosed IPv6 bracket: "http://[::1"
        return False


def normalize_url(url: str) -> str | None:
    """Return a canonical form of an absolute http(s) URL, or None if invalid.

    Two spellings of the same address map to one string, so links can be
    deduplicated: the scheme and host are lowercased, an internationalized
    host is converted to punycode, the default port and the fragment are
    dropped, and an empty path becomes "/".
    """
    try:
        parts = urlsplit(url.strip())
        port = parts.port  # raises ValueError for a non-numeric port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS or not parts.hostname:
        return None
    host = _encode_host(parts.hostname)
    if host is None:
        return None

    # `hostname` is already lowercased but loses IPv6 brackets.
    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        netloc += f":{port}"
    if parts.username is not None:
        userinfo = parts.netloc.rpartition("@")[0]
        netloc = f"{userinfo}@{netloc}"
    return urlunsplit((scheme, netloc, parts.path or "/", parts.query, ""))


def resolve_url(href: str, base_url: str) -> str | None:
    """Turn an href found on a page into an absolute, normalized URL.

    Returns None for values that do not point to another crawlable page:
    empty or fragment-only hrefs ("#top") and non-http schemes such as
    "mailto:", "tel:", "javascript:" or "data:".
    """
    href = href.strip()
    if not href or href.startswith("#"):
        return None
    try:
        absolute = urljoin(base_url, href)
    except ValueError:
        return None
    return normalize_url(absolute)


def is_same_host(url: str, other: str) -> bool:
    """Return True if both URLs point to the same host (ports are ignored).

    Hosts are compared after normalization, so "bücher.de" and its punycode
    form "xn--bcher-kva.de" are the same host.
    """
    first, second = normalize_url(url), normalize_url(other)
    if first is None or second is None:
        return False
    return urlsplit(first).hostname == urlsplit(second).hostname


def _encode_host(host: str) -> str | None:
    """Return the ASCII (punycode) form of a host, or None if it is invalid.

    Mirrors yarl, which aiohttp uses for the final URL of a response: UTS #46
    mapping first, then the stdlib IDNA 2003 codec for hosts it rejects.
    """
    if host.isascii():
        return host
    try:
        return idna.encode(host, uts46=True).decode("ascii")
    except UnicodeError:
        pass
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError:
        return None
