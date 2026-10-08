"""HTTP layer of the crawler: the contract of a transport, and single GET requests over a shared aiohttp session."""

import codecs
import contextlib
import itertools
import logging
import socket
import ssl
from collections.abc import Iterable, Mapping, Sequence
from http.cookiejar import Cookie
from typing import NamedTuple, Protocol, runtime_checkable
from urllib.parse import urlsplit

import aiohttp
import certifi
from bs4.dammit import EncodingDetector
from yarl import URL

from crawler.exceptions import (
    CertificateError,
    CrawlerClosedError,
    DNSError,
    FetchError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    PageTooLargeError,
    ProxyNetworkError,
    SitemapError,
)
from crawler.parser import is_html_content_type
from crawler.proxy import Proxy, ProxyPool
from crawler.retry import parse_retry_after
from crawler.session import CookieJar
from crawler.urls import is_valid_http_url

logger = logging.getLogger(__name__)

# The getaddrinfo() codes for a host name that has no address. The others,
# such as EAI_AGAIN, say that the lookup failed for now.
_NO_SUCH_HOST = frozenset(getattr(socket, name) for name in ("EAI_NONAME", "EAI_NODATA") if hasattr(socket, name))


class Response(NamedTuple):
    """A response to a GET request; the body is read, or left unread, as asked."""

    status: int
    content: str
    size: int
    final_url: str
    content_type: str | None
    redirected: bool = False  # a redirect, not followed; `final_url` is its Location header
    body: bytes | None = None  # the bytes as sent, when asked for instead of the text
    robots_tag: tuple[str, ...] = ()  # the X-Robots-Tag headers as sent
    proxy: Proxy | None = None  # the proxy the request went through


@runtime_checkable
class Transport(Protocol):
    """The contract of the HTTP layer, which the request layer sends its requests through.

    `get()` makes a single GET request and returns a `Response`; a
    redirect comes back as one, with `redirected` set and its target as
    `final_url`, for the caller to check and follow. Every failure is
    raised as a `FetchError`, `CrawlerClosedError` after `close()`.
    """

    async def get(
        self,
        url: str,
        *,
        html_only: bool,
        raw_limit: int | None,
        truncate_at: int | None,
        timeout: aiohttp.ClientTimeout,
    ) -> Response:
        """Perform the GET request and read the body up to its size limit.

        With `html_only`, the body of a response whose Content-Type is not
        HTML is not read: the content is empty and the size is 0. With
        `raw_limit`, the body is returned as bytes and the content is empty;
        over `raw_limit` bytes it fails with `SitemapError`. With
        `truncate_at`, the body is cut to that many bytes. Otherwise a body
        over the size limit of the transport fails with `PageTooLargeError`.
        """
        ...

    async def close(self) -> None:
        """Release what the transport holds. Safe to call more than once; later requests fail."""
        ...

    def reset_stats(self) -> None:
        """Count the requests anew."""
        ...

    def cookies(self) -> list[Cookie]:
        """The cookies the transport keeps, those sites have set included."""
        ...

    def update_cookies(self, changed: Iterable[Cookie], removed: Iterable[Cookie]) -> None:
        """Keep the `changed` cookies, in place of those of their domain, path and name, and drop the `removed` ones."""
        ...


class HttpTransport:
    """Sends GET requests over one aiohttp session, without following redirects: a `Transport`.

    `get()` makes a single request and returns a `Response`; a redirect
    comes back as one, with its Location header as `final_url`, for the
    caller to check and follow. Every failure is raised as a `FetchError`:
    `CrawlerClosedError` after `close()`, `InvalidURLError`,
    `HTTPStatusError` (with the Retry-After of the response),
    `FetchTimeoutError`, `CertificateError`, `DNSError`, `NetworkError`,
    `PageTooLargeError`, or `SitemapError` for a raw body over its limit.
    Requests carry `user_agent`, or the `user_agents` in turn when there
    are any, and the `headers`.

    With `proxies`, a request goes through the proxy the pool picks for it
    (see `ProxyPool`), and its outcome goes back to the pool. A proxy that
    cannot be reached, or that refuses the request with HTTP 407, fails it
    with `ProxyNetworkError`; when every proxy is out of rotation, the
    request is not sent and fails with `NoProxyError`. A proxy that cannot
    reach the site (CONNECT answered with another error) fails it with a
    `NetworkError` of the site, and so does a timeout: the connection
    through a proxy times out the same way whether the proxy or the site
    is slow. The password of a proxy is sent in the Proxy-Authorization
    header, never as part of a URL, and errors name the proxy by its label.
    A response names the proxy it came through, as `Response.proxy`.

    The session keeps the cookies that sites set, along with the starting
    `cookies`, and sends them back as a browser would; `cookies()` gives
    them all. With `keep_cookies=False` it sends and keeps none.
    """

    REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
    # The constants the crawler sets on its transport.
    SETTINGS = ("REDIRECT_STATUSES",)

    def __init__(
        self,
        *,
        max_concurrent: int,
        timeout: aiohttp.ClientTimeout,
        user_agent: str,
        user_agents: Sequence[str] = (),
        max_page_size: int | None,
        headers: Mapping[str, str] | None = None,
        cookies: Iterable[Cookie] = (),
        keep_cookies: bool = True,
        proxies: ProxyPool | None = None,
    ) -> None:
        self._max_concurrent = max_concurrent
        self._timeout = timeout
        self._headers = {"User-Agent": user_agent, **(headers or {})}
        self._initial_cookies = list(cookies)
        self._keep_cookies = keep_cookies
        # Made with the session, and kept after it is closed for cookies().
        self._cookie_jar: CookieJar | None = None
        self._rotated_agents = itertools.cycle(user_agents) if user_agents else None
        self._max_page_size = max_page_size
        self.proxies = proxies
        self._session: aiohttp.ClientSession | None = None
        self._closed = False

    async def close(self) -> None:
        """Close the session. Safe to call more than once; later requests fail with `CrawlerClosedError`."""
        self._closed = True
        if self._session is not None:
            await self._session.close()
            self._session = None
            logger.debug("HTTP session closed")

    def _get_session(self) -> aiohttp.ClientSession:
        # The session is created lazily because aiohttp requires a running
        # event loop.
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self) -> aiohttp.ClientSession:
        # Filled before the connector is made, so that a cookie it fails on leaves no connector open.
        cookie_jar: aiohttp.abc.AbstractCookieJar = aiohttp.DummyCookieJar()
        if self._keep_cookies:
            cookie_jar = self._cookie_jar = CookieJar()
            cookie_jar.add(self._initial_cookies)
        # certifi's CA bundle is added on top of the system store: TLS then
        # works on Python builds without system certificates, and locally
        # installed CAs (corporate proxies) stay trusted.
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(cafile=certifi.where())
        connector = aiohttp.TCPConnector(limit=self._max_concurrent, ttl_dns_cache=300, ssl=ssl_context)
        return aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout,
            headers=self._headers,
            cookie_jar=cookie_jar,
        )

    def reset_stats(self) -> None:
        """Count the requests through the proxies anew."""
        if self.proxies is not None:
            self.proxies.reset_stats()

    def cookies(self) -> list[Cookie]:
        """The cookies the session keeps, the starting ones before the first request; none without `keep_cookies`."""
        if not self._keep_cookies:
            return []
        return list(self._initial_cookies) if self._cookie_jar is None else self._cookie_jar.export()

    def update_cookies(self, changed: Iterable[Cookie], removed: Iterable[Cookie]) -> None:
        """Keep the `changed` cookies and drop the `removed` ones, as `Transport.update_cookies` says; none without `keep_cookies`."""
        if not self._keep_cookies:
            return
        changed, removed = list(changed), list(removed)
        if self._cookie_jar is not None:
            self._cookie_jar.remove(removed)
            self._cookie_jar.add(changed)
            return
        # Before the first request: the session takes the starting cookies when it is made.
        replaced = {(cookie.domain, cookie.path, cookie.name) for cookie in [*changed, *removed]}
        self._initial_cookies = [
            cookie for cookie in self._initial_cookies if (cookie.domain, cookie.path, cookie.name) not in replaced
        ] + changed

    async def get(
        self,
        url: str,
        *,
        html_only: bool,
        raw_limit: int | None,
        truncate_at: int | None,
        timeout: aiohttp.ClientTimeout,
    ) -> Response:
        """Perform the GET request and read the body up to its size limit, as `Transport.get` says.

        The size limit is `max_page_size`. The size is measured after
        content decoding (gzip, deflate, ...), so it may be larger than the
        number of bytes sent over the network.
        """
        # Checked on every request: close() may run while the caller waits
        # for its turn, and the failure is that of one URL only.
        if self._closed:
            raise CrawlerClosedError(url, "crawler is closed")
        _validate_url(url)
        pool = self.proxies
        # NoProxyError when every proxy is out: the request is not sent.
        proxy = None if pool is None else pool.pick(url)
        try:
            response = await self._request(
                url, proxy, html_only=html_only, raw_limit=raw_limit, truncate_at=truncate_at, timeout=timeout
            )
        except FetchError as error:
            if pool is not None and proxy is not None:
                # A response came through the proxy, whatever it says of the page.
                answered = isinstance(error, HTTPStatusError | PageTooLargeError | SitemapError)
                pool.record(proxy, url, None if answered else error)
            raise
        if pool is not None and proxy is not None:
            pool.record(proxy, url, None)
            response = response._replace(proxy=proxy)
        return response

    async def _request(
        self,
        url: str,
        proxy: Proxy | None,
        *,
        html_only: bool,
        raw_limit: int | None,
        truncate_at: int | None,
        timeout: aiohttp.ClientTimeout,
    ) -> Response:
        """The request of `get()`, through `proxy` if there is one."""
        session = self._get_session()
        headers = {} if self._rotated_agents is None else {"User-Agent": next(self._rotated_agents)}
        proxy_headers = None
        if proxy is not None and proxy.authorization is not None:
            if urlsplit(url).scheme.lower() == "https":
                # Sent with CONNECT: the request inside the tunnel goes to the site, which must not see it.
                proxy_headers = {aiohttp.hdrs.PROXY_AUTHORIZATION: proxy.authorization}
            else:
                # The request itself goes to the proxy, which takes the header out.
                headers[aiohttp.hdrs.PROXY_AUTHORIZATION] = proxy.authorization
        try:
            # Redirects are followed by the caller, one request at a time, so
            # that each one is checked as a link to its target would be.
            async with session.get(
                url,
                headers=headers or None,
                timeout=timeout,
                allow_redirects=False,
                proxy=None if proxy is None else proxy.url,
                proxy_headers=proxy_headers,
            ) as response:
                # Inside the tunnel of an https URL the site answers; the proxy refuses CONNECT itself.
                if proxy is not None and response.status == 407 and urlsplit(url).scheme.lower() == "http":
                    raise ProxyNetworkError(url, f"proxy {proxy.label} refused the request: HTTP 407 {response.reason}")
                response.raise_for_status()
                # aiohttp reports "application/octet-stream" when the header
                # is missing; None lets callers tell the two cases apart.
                content_type = response.content_type if aiohttp.hdrs.CONTENT_TYPE in response.headers else None
                location = response.headers.get(aiohttp.hdrs.LOCATION)
                robots_tag = tuple(response.headers.getall("X-Robots-Tag", ()))
                if response.status in self.REDIRECT_STATUSES and location is not None:
                    # The body of a redirect is not wanted.
                    return Response(
                        status=response.status,
                        content="",
                        size=0,
                        final_url=location,
                        content_type=content_type,
                        redirected=True,
                    )
                if html_only and not is_html_content_type(content_type):
                    # A link to an archive or a video must not be downloaded
                    # just to be rejected by the parser. Leaving the block
                    # without reading closes the connection mid-transfer.
                    logger.info("Skipping body of %s: %s is not HTML", url, content_type)
                    return Response(
                        status=response.status,
                        content="",
                        size=0,
                        final_url=str(response.url),
                        content_type=content_type,
                        robots_tag=robots_tag,
                    )
                body = await self._read_body(response, url, raw_limit=raw_limit, truncate_at=truncate_at)
                raw = raw_limit is not None
                return Response(
                    status=response.status,
                    content="" if raw else _decode(body, _encoding(response, body)),
                    size=len(body),
                    final_url=str(response.url),
                    content_type=content_type,
                    body=body if raw else None,
                    robots_tag=robots_tag,
                )
        # Order matters: aiohttp's ServerTimeoutError is both a ClientError and
        # a TimeoutError; InvalidURL and the certificate error are ClientErrors too,
        # and the error of CONNECT is a ClientResponseError.
        except aiohttp.ClientHttpProxyError as exc:
            assert proxy is not None
            message = f"proxy {proxy.label} answered CONNECT with HTTP {exc.status} {exc.message}"
            # Any other answer is the proxy's word on the site: it cannot reach it, or may not.
            raise (ProxyNetworkError if exc.status == 407 else NetworkError)(url, message) from exc
        except aiohttp.ClientResponseError as exc:
            retry_after = parse_retry_after(exc.headers.get(aiohttp.hdrs.RETRY_AFTER) if exc.headers else None)
            raise HTTPStatusError(url, exc.status, exc.message, retry_after=retry_after) from exc
        except TimeoutError as exc:
            # Through a proxy the connect phase is the connection to the proxy
            # and, for an https URL, its answer to CONNECT: a silent proxy, not
            # a slow site. Past it a slow proxy and a slow site look the same,
            # and the timeout stays the site's.
            if proxy is not None and isinstance(exc, aiohttp.ConnectionTimeoutError):
                raise ProxyNetworkError(url, f"proxy {proxy.label}: {_describe_timeout(exc, timeout)}") from exc
            raise FetchTimeoutError(url, _describe_timeout(exc, timeout)) from exc
        # UnicodeError comes from IDNA encoding of the host, e.g. a domain
        # label longer than 63 characters; aiohttp does not wrap it.
        except (aiohttp.InvalidURL, UnicodeError) as exc:
            raise InvalidURLError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientProxyConnectionError as exc:
            assert proxy is not None
            raise ProxyNetworkError(url, f"proxy {proxy.label}: {type(exc).__name__}: {exc}") from exc
        except (aiohttp.ClientConnectorCertificateError, aiohttp.ClientConnectorSSLError) as exc:
            if proxy is not None and _is_proxy_address(proxy, exc.host, exc.port):
                # TLS with an https proxy, not with the site inside the tunnel.
                raise ProxyNetworkError(url, f"proxy {proxy.label}: {type(exc).__name__}: {exc}") from exc
            if isinstance(exc, aiohttp.ClientConnectorCertificateError):
                raise CertificateError(url, f"{type(exc).__name__}: {exc}") from exc
            raise NetworkError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientConnectorDNSError as exc:
            if proxy is not None:
                # The client looks up the proxy alone; the proxy looks up the site.
                raise ProxyNetworkError(url, f"proxy {proxy.label}: {type(exc).__name__}: {exc}") from exc
            error = DNSError if _no_such_host(exc.os_error) else NetworkError
            raise error(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientError as exc:
            raise NetworkError(url, f"{type(exc).__name__}: {exc}") from exc

    async def _read_body(
        self, response: aiohttp.ClientResponse, url: str, *, raw_limit: int | None, truncate_at: int | None
    ) -> bytes:
        """Read the body, giving up once it is over its size limit; the rest is not downloaded.

        A response sent with Content-Encoding: gzip is unpacked as it is
        read, so a few hundred kilobytes may turn into gigabytes: the limit
        is on the unpacked body.
        """
        limit = truncate_at or raw_limit or self._max_page_size
        if limit is None:
            return await response.read()
        too_large = SitemapError if raw_limit is not None else PageTooLargeError
        # Content-Length counts the packed bytes, never more than the unpacked ones.
        if truncate_at is None and response.content_length is not None and response.content_length > limit:
            raise too_large(url, f"larger than {limit} bytes")
        chunks, size = [], 0
        async for chunk in response.content.iter_chunked(64 * 1024):
            if size + len(chunk) > limit:
                if truncate_at is None:
                    raise too_large(url, f"larger than {limit} bytes")
                chunks.append(chunk[: limit - size])
                logger.info("Cut the body of %s to %d bytes", url, limit)
                break
            chunks.append(chunk)
            size += len(chunk)
        return b"".join(chunks)


def _is_proxy_address(proxy: Proxy, host: str, port: int | None) -> bool:
    """Whether a connection that failed, by its host and port, was the one to `proxy`."""
    address = URL(proxy.url)
    return (address.raw_host, address.port) == (host, port)


def _no_such_host(exc: OSError) -> bool:
    """Whether a failed lookup says the host name does not exist, not that the resolver failed for now.

    A resolver that gives no code, such as aiodns, is taken at its word:
    the name does not resolve.
    """
    return exc.errno is None or exc.errno in _NO_SUCH_HOST


def _describe_timeout(exc: TimeoutError, timeout: aiohttp.ClientTimeout) -> str:
    # aiohttp raises its own subclasses for the connect and read timeouts
    # and a plain TimeoutError for the total one.
    if isinstance(exc, aiohttp.ConnectionTimeoutError):
        return f"connect timeout ({timeout.connect:.1f}s)"
    if isinstance(exc, aiohttp.SocketTimeoutError):
        return f"read timeout ({timeout.sock_read:.1f}s)"
    return f"total timeout ({timeout.total:.1f}s)"


def _encoding(response: aiohttp.ClientResponse, body: bytes) -> str:
    """The charset of the Content-Type header if Python knows it, else the one the markup declares.

    `response.get_encoding()` does the same with a resolver, but only for a
    body read whole with `read()`, not in chunks.
    """
    if response.charset:
        with contextlib.suppress(LookupError, ValueError):
            return codecs.lookup(response.charset).name
    return _sniff_charset(response, body)


def _sniff_charset(response: aiohttp.ClientResponse, body: bytes) -> str:
    """Pick an encoding when the Content-Type header has no charset.

    Without one, UTF-8 would be assumed, but many pages declare their
    encoding in the markup instead:
    <meta charset="..."> or <meta http-equiv="Content-Type" content="...">.
    """
    declared = EncodingDetector.find_declared_encoding(body, is_html=True)
    if declared is None:
        return "utf-8"
    if not _is_ascii_compatible(declared):
        # The declaration was found by reading the bytes as ASCII, so they
        # cannot be UTF-16 and the like; the HTML spec says to use UTF-8.
        # This also rejects unknown names and codecs such as "undefined",
        # "idna" or "base64" that cannot decode a page at all.
        logger.debug("Ignoring declared charset %r for %s", declared, response.url)
        return "utf-8"
    return declared


def _is_ascii_compatible(encoding: str) -> bool:
    # A charset that decodes printable ASCII unchanged can read the markup.
    probe = bytes(range(0x20, 0x7F)) + b"\t\n\r"
    try:
        return probe.decode(encoding, errors="replace") == probe.decode("ascii")
    except (LookupError, UnicodeError):
        return False


def _decode(body: bytes, encoding: str) -> str:
    # A byte order mark overrides any declared charset (HTML spec) and is not
    # part of the text. A wrong charset should not drop the whole page:
    # undecodable bytes are replaced, and a charset naming a codec that cannot
    # decode text (e.g. "base64" or "undefined") falls back to UTF-8.
    body, bom_encoding = EncodingDetector.strip_byte_order_mark(body)
    try:
        return body.decode(bom_encoding or encoding, errors="replace")
    except (LookupError, UnicodeError):
        return body.decode("utf-8", errors="replace")


def _validate_url(url: str) -> None:
    """Reject URLs without an http(s) scheme, a host or a valid port before sending them.

    aiohttp does not wrap every malformed URL: "//host" fails on an internal
    assert, so such input is caught here instead.
    """
    if not is_valid_http_url(url):
        raise InvalidURLError(url, "expected an absolute http(s) URL with a valid host and port")
