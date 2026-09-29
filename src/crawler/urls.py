"""URL helpers: validation, normalization and resolution of relative links."""

from urllib.parse import urljoin, urlsplit, urlunsplit

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
    deduplicated: the scheme and host are lowercased, the default port and the
    fragment are dropped, and an empty path becomes "/".
    """
    try:
        parts = urlsplit(url.strip())
        port = parts.port  # raises ValueError for a non-numeric port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS or not parts.hostname:
        return None

    # Rebuild netloc by hand: `hostname` loses IPv6 brackets and userinfo.
    userinfo, _, host_port = parts.netloc.rpartition("@")
    host = host_port.lower()
    if port == _DEFAULT_PORTS[scheme]:
        host = host.removesuffix(f":{port}")
    netloc = f"{userinfo}@{host}" if userinfo else host
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
    """Return True if both URLs point to the same host (ports are ignored)."""
    try:
        host = urlsplit(url).hostname
        return host is not None and host == urlsplit(other).hostname
    except ValueError:
        return False
