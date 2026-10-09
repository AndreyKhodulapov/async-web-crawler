"""URL helpers: validation, normalization and resolution of relative links."""

import functools
import re
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import idna


def is_valid_http_url(url: str) -> bool:
    """Return True for an absolute http(s) URL with a valid host and port."""
    return normalize_url(url) is not None


# A crawl asks about the same URL many times over: a link when it is found,
# filtered and queued, a page at every step of its request, a link to the
# site's menu on every page. The answers for the latest URLs are remembered,
# here and in `get_host`.
@functools.lru_cache(maxsize=4096)
def normalize_url(url: str) -> str | None:
    """Return a canonical form of an absolute http(s) URL, or None if invalid.

    Two spellings of the same address map to one string, so links can be
    deduplicated: the scheme and host are lowercased, an internationalized
    host is converted to punycode, a trailing dot in the host, the default
    port and the fragment are dropped, and an empty path becomes "/".
    The path and query are percent-encoded the way an HTTP client sends
    them: "/café" and "/caf%C3%A9" are the same address, and so are
    "/~joe" and "/%7Ejoe". Dot segments are resolved as the client does
    before sending: "/a/../b" and "/a/%2E%2E/b" are "/b".
    """
    try:
        parts = urlsplit(url.strip())
        port = parts.port  # raises ValueError for a non-numeric or out-of-range port
    except ValueError:  # also an unclosed IPv6 bracket: "http://[::1"
        return None
    default_ports = {"http": 80, "https": 443}
    scheme = parts.scheme.lower()
    if scheme not in default_ports or not parts.hostname:
        return None
    # "example.com." (a fully qualified name) is the same host as "example.com".
    host = _encode_host(parts.hostname.rstrip("."))
    if not host:
        return None

    # `hostname` is already lowercased but loses IPv6 brackets.
    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != default_ports[scheme]:
        netloc += f":{port}"
    if parts.username is not None:
        userinfo = parts.netloc.rpartition("@")[0]
        netloc = f"{userinfo}@{netloc}"
    path, query = percent_encode(parts.path or "/"), percent_encode(parts.query)
    if path is None or query is None:
        return None
    return urlunsplit((scheme, netloc, _remove_dot_segments(path), query, ""))


def resolve_url(href: str, base_url: str) -> str | None:
    """Turn an href found on a page into an absolute, normalized URL.

    Returns None for values that do not point to another crawlable page:
    empty or fragment-only hrefs ("#top") and non-http schemes such as
    "mailto:", "tel:", "javascript:" or "data:".

    A user name and password written in the href itself
    ("http://user:pass@host/") are dropped: the crawler would send them in
    an Authorization header and keep them in its records and log. A link
    without a host of its own keeps those of `base_url`.
    """
    href = href.strip()
    if not href or href.startswith("#"):
        return None
    try:
        absolute = urljoin(base_url, href)
        scheme = urlsplit(absolute).scheme
        own_host = bool(urlsplit(href).netloc)
    except ValueError:
        return None
    # Turned away before `normalize_url`, which would remember the whole of
    # an inline image ("data:" with tens of kilobytes) as a key of its cache.
    if scheme not in ("http", "https"):
        return None
    normalized = normalize_url(absolute)
    return drop_userinfo(normalized) if own_host and normalized else normalized


def drop_userinfo(url: str) -> str:
    """`url` without the user name and password in it, if it has them."""
    parts = urlsplit(url)
    if parts.username is None:
        return url
    return urlunsplit(parts._replace(netloc=parts.netloc.rpartition("@")[2]))


# Click IDs of ad networks; parameters starting with "utm_" go too.
TRACKING_PARAMS = frozenset({"fbclid", "gclid", "dclid", "msclkid", "yclid"})


def strip_tracking_params(url: str) -> str:
    """Drop tracking parameters ("utm_*", "fbclid", "gclid", ...) from the query of a normalized URL.

    They tell the site where a visitor came from, not which page to show:
    "/a?utm_source=x&id=1" and "/a?id=1" are the same page. The other
    parameters keep their order and spelling; a query left empty is dropped.
    """
    parts = urlsplit(url)
    if not parts.query:
        return url
    kept = [param for param in parts.query.split("&") if not _is_tracking_param(param)]
    return urlunsplit(parts._replace(query="&".join(kept)))


def _is_tracking_param(param: str) -> bool:
    name = param.partition("=")[0]
    return name.startswith("utm_") or name in TRACKING_PARAMS


@functools.lru_cache(maxsize=4096)
def get_host(url: str) -> str | None:
    """Return the normalized host of an http(s) URL, or None if the URL is invalid.

    "bücher.de" and its punycode form "xn--bcher-kva.de" give the same host.
    """
    normalized = normalize_url(url)
    return None if normalized is None else urlsplit(normalized).hostname


def is_same_host(url: str, other: str) -> bool:
    """Return True if both URLs point to the same host (ports are ignored)."""
    host = get_host(url)
    return host is not None and host == get_host(other)


def hide_password(url: str) -> str:
    """A URL fit to be shown, such as that of a database or a proxy: its password is replaced with ***."""
    password = urlsplit(url).password
    if password is not None:
        url = url.replace(f":{password}@", ":***@", 1)
    # PostgreSQL takes the password as a parameter of the URL too.
    return re.sub(r"(?<=[?&]password=)[^&#]*", "***", url)


def percent_encode(component: str) -> str | None:
    """Percent-encode non-ASCII characters, spaces and the like; None if impossible.

    Characters that RFC 3986 allows in a path or query are kept, and so are
    existing escapes, whose hex digits are uppercased: "%d0" and "%D0" are
    the same byte. An escaped unreserved character (letter, digit, "-",
    ".", "_", "~") is decoded, as RFC 3986 says it means the same:
    "%7E" is "~". Other escapes, such as "%2F" or "%2A", keep their meaning
    apart from the character. A lone surrogate cannot be encoded as UTF-8.
    """
    try:
        encoded = quote(component, safe="/?:@!$&'()*+,;=-._~%")
    except UnicodeEncodeError:
        return None
    return re.sub(r"%[0-9a-fA-F]{2}", _normalize_escape, encoded)


def _normalize_escape(escape: re.Match[str]) -> str:
    char = chr(int(escape.group()[1:], 16))
    return char if char.isascii() and (char.isalnum() or char in "-._~") else escape.group().upper()


def _remove_dot_segments(path: str) -> str:
    """Resolve "." and ".." in an absolute path (RFC 3986 5.2.4): "/a/./b/../c" gives "/a/c".

    A path that ends in a dot segment keeps its trailing slash: "/a/b/.." is
    "/a/". ".." never goes above the root.
    """
    segments = path.split("/")[1:]
    output: list[str] = []
    for index, segment in enumerate(segments):
        if segment in (".", ".."):
            if segment == ".." and output:
                output.pop()
            if index == len(segments) - 1:
                output.append("")
        else:
            output.append(segment)
    return "/" + "/".join(output)


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
