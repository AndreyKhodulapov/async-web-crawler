"""Cookies and headers of the HTTP session: the cookie jar, Netscape cookies.txt files and the checks of names."""

import contextlib
import ipaddress
import logging
import os
import re
import tempfile
import time
import warnings
from collections.abc import Iterable, Mapping
from email.utils import formatdate
from http.cookiejar import HTTPONLY_ATTR, Cookie, LoadError, MozillaCookieJar, http2time
from http.cookies import CookieError, Morsel, SimpleCookie
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
from aiohttp.typedefs import LooseCookies
from yarl import URL

from crawler.urls import is_valid_http_url

logger = logging.getLogger(__name__)

# A header name or a cookie name (RFC 9110 and RFC 6265).
_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")


def header_name_problem(name: str) -> str | None:
    """What is wrong with `name` as a header of every request, None if nothing."""
    if not _TOKEN.fullmatch(name):
        return "not a header name: letters, digits and !#$%&'*+-.^_`|~ only"
    # They have keys of their own, and Host is that of the URL. Proxy-Authorization
    # would reach the sites behind https proxies: it goes to the proxy only.
    reserved = {
        "user-agent": "crawler.user_agent",
        "cookie": "session.cookies",
        "host": "the URL",
        "proxy-authorization": "the user and password of proxy.urls",
    }
    if name.lower() in reserved:
        return f"this header is set by {reserved[name.lower()]}"
    return None


def header_value_problem(value: str) -> str | None:
    if not value:
        return "must not be empty"
    # A line break would end the header and start another one; aiohttp refuses to send it.
    return None if value.isprintable() else "must be one line without control characters"


def cookie_name_problem(name: str) -> str | None:
    if not _TOKEN.fullmatch(name):
        return "not a cookie name: letters, digits and !#$%&'*+-.^_`|~ only"
    try:
        Morsel().set(name, "", "")
    except CookieError:
        return "the name of a cookie attribute, such as Path or Secure, cannot name a cookie"
    return None


def cookie_value_problem(value: str) -> str | None:
    # Other characters would be sent quoted and escaped, which most sites do not undo (RFC 6265, cookie-octet).
    if not value.isascii() or not value.isprintable() or any(char in value for char in ' ",;\\'):
        return "must be printable ASCII without spaces, quotes, commas, semicolons or backslashes"
    return None


def cookie_domain_problem(domain: str) -> str | None:
    """What is wrong with `domain` as the domain of a cookie, None if nothing; "." in front is for subdomains too."""
    host = domain.removeprefix(".")
    if _is_ip_address(host):
        # aiohttp keeps no cookies of IP addresses (RFC 6265 has no domain cookies for them).
        return "cookies of an IP address are not kept; use the host name, e.g. localhost"
    # A host with nothing else: no scheme, port, user or path.
    url = f"http://{host}/"
    if not host or not is_valid_http_url(url) or urlsplit(url).hostname != host.lower():
        return "expected a host name such as example.com or .example.com"
    return None


def make_cookie(
    name: str,
    value: str,
    domain: str,
    *,
    path: str = "/",
    secure: bool = False,
    expires: int | None = None,
    http_only: bool = False,
) -> Cookie:
    """A cookie as `http.cookiejar` has it; one without `expires` lasts for the session.

    A `domain` that starts with "." is for its subdomains too, as in a
    cookies.txt file; "example.com" is for that host only.
    """
    return Cookie(
        version=0,
        name=name,
        value=value,
        port=None,
        port_specified=False,
        domain=domain,
        domain_specified=domain.startswith("."),
        domain_initial_dot=domain.startswith("."),
        path=path,
        path_specified=True,
        secure=secure,
        expires=expires,
        discard=expires is None,
        comment=None,
        comment_url=None,
        rest={HTTPONLY_ATTR: ""} if http_only else {},
    )


def load_cookies_file(path: str | Path) -> list[Cookie]:
    """The cookies of a Netscape cookies.txt file, session ones included, expired ones left out.

    Such a file is exported by browser extensions and `curl -c`. Cookies
    the crawler cannot send are left out and logged by their host and
    name: those of IP addresses and those with an invalid name.

    Raises:
        OSError: the file cannot be read.
        ValueError: it is not a cookies.txt file. The message does not
            quote the file: its lines hold the values of the cookies.
    """
    jar = MozillaCookieJar()
    # On a malformed line, http.cookiejar warns of a bug of its own with a traceback.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            # Expired cookies are dropped below: curl writes 0 as the expiry
            # of a session cookie, which http.cookiejar takes for 1970.
            jar.load(os.fspath(Path(path).expanduser()), ignore_discard=True, ignore_expires=True)
        except (LoadError, UnicodeDecodeError):
            raise ValueError("not a Netscape cookies.txt file, or a line of it is malformed") from None
    now = time.time()
    cookies = []
    for cookie in jar:
        if cookie.expires == 0:
            cookie.expires, cookie.discard = None, True
        if cookie.is_expired(now):
            continue
        if cookie.value is None:
            # A line without a name: http.cookiejar takes its value for the name, so it is not logged.
            logger.warning("Left out a cookie without a name of %s from %s", cookie.domain, path)
            continue
        problem = cookie_name_problem(cookie.name) or cookie_domain_problem(cookie.domain)
        if problem is not None:
            logger.warning("Left out the cookie %r of %s from %s: %s", cookie.name, cookie.domain, path, problem)
            continue
        cookies.append(cookie)
    return cookies


def save_cookies_file(cookies: Iterable[Cookie], path: str | Path) -> None:
    """Write cookies to a Netscape cookies.txt file that only its owner can read, session cookies included.

    Raises:
        OSError: the file cannot be written; an existing one is left as it was.
    """
    path = Path(path).expanduser()
    jar = MozillaCookieJar()
    for cookie in cookies:
        jar.set_cookie(cookie)
    # Written next to the file and moved over it: mkstemp() creates it with
    # mode 0600, which an existing file would not get by being overwritten,
    # and a failed save leaves the old file whole.
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    os.close(descriptor)
    try:
        jar.save(temporary, ignore_discard=True)
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


class CookieJar(aiohttp.CookieJar):
    """aiohttp's cookie jar that can give its cookies back as `http.cookiejar` cookies.

    A cookies.txt file says whether a cookie is for its host only and when
    it expires, and aiohttp keeps neither where it can be read. So the jar
    remembers the cookies set without a Domain, and turns a Max-Age into
    an Expires date as the cookie arrives.
    """

    def __init__(self) -> None:
        super().__init__()
        self._for_host_only: set[tuple[str, str]] = set()  # (host, name)

    def update_cookies(self, cookies: LooseCookies, response_url: URL = URL()) -> None:  # noqa: B008, as aiohttp has it
        host = response_url.raw_host
        received = []
        for name, cookie in cookies.items() if isinstance(cookies, Mapping) else cookies:
            if isinstance(cookie, Morsel):
                cookie = cookie.copy()
            else:
                parsed = SimpleCookie()
                parsed[name] = cookie
                cookie = parsed[name]
            max_age = cookie["max-age"]
            if max_age.isdigit() and int(max_age) > 0:
                cookie["expires"] = formatdate(min(time.time() + int(max_age), self.MAX_TIME), usegmt=True)
                cookie["max-age"] = ""
            if host is not None:
                # aiohttp ignores a domain with a trailing dot, and one that does not match the host.
                domain = cookie["domain"].removeprefix(".")
                if not domain or domain.endswith("."):
                    self._for_host_only.add((host, name))
                elif host == domain or host.endswith(f".{domain}"):
                    self._for_host_only.discard((host, name))
            received.append((name, cookie))
        super().update_cookies(received, response_url)

    def add(self, cookies: Iterable[Cookie]) -> None:
        """Add cookies of `http.cookiejar`, such as those of `load_cookies_file()`."""
        for cookie in cookies:
            parsed = SimpleCookie()
            parsed[cookie.name] = cookie.value
            morsel = parsed[cookie.name]
            morsel["path"] = cookie.path
            if cookie.domain.startswith("."):
                morsel["domain"] = cookie.domain
            if cookie.secure:
                morsel["secure"] = True
            if cookie.has_nonstandard_attr(HTTPONLY_ATTR):
                morsel["httponly"] = True
            if cookie.expires is not None:
                morsel["expires"] = formatdate(cookie.expires, usegmt=True)
            self.update_cookies(
                [(cookie.name, morsel)], URL.build(scheme="https", host=cookie.domain.removeprefix("."))
            )

    def export(self) -> list[Cookie]:
        """The cookies kept, expired ones left out, as `http.cookiejar` cookies."""
        cookies = []
        for morsel in self:
            host = morsel["domain"]
            cookies.append(
                make_cookie(
                    morsel.key,
                    morsel.value,
                    host if (host, morsel.key) in self._for_host_only else f".{host}",
                    path=morsel["path"] or "/",
                    secure=bool(morsel["secure"]),
                    expires=http2time(morsel["expires"]) if morsel["expires"] else None,
                    http_only=bool(morsel["httponly"]),
                )
            )
        return cookies


def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    return True
