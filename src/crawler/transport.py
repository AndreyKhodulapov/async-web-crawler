"""HTTP layer of the crawler: single GET requests over a shared aiohttp session."""

import codecs
import contextlib
import itertools
import logging
import ssl
from collections.abc import Sequence
from typing import NamedTuple

import aiohttp
import certifi
from bs4.dammit import EncodingDetector

from crawler.exceptions import (
    CertificateError,
    CrawlerClosedError,
    DNSError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    PageTooLargeError,
    SitemapError,
)
from crawler.parser import is_html_content_type
from crawler.retry import parse_retry_after
from crawler.urls import is_valid_http_url

logger = logging.getLogger(__name__)


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


class HttpTransport:
    """Sends GET requests over one aiohttp session, without following redirects.

    `get()` makes a single request and returns a `Response`; a redirect
    comes back as one, with its Location header as `final_url`, for the
    caller to check and follow. Every failure is raised as a `FetchError`:
    `CrawlerClosedError` after `close()`, `InvalidURLError`,
    `HTTPStatusError` (with the Retry-After of the response),
    `FetchTimeoutError`, `CertificateError`, `DNSError`, `NetworkError`,
    `PageTooLargeError`, or `SitemapError` for a raw body over
    `max_raw_size`. Requests carry `user_agent`, or the `user_agents` in
    turn when there are any.
    """

    REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})

    def __init__(
        self,
        *,
        max_concurrent: int,
        timeout: aiohttp.ClientTimeout,
        user_agent: str,
        user_agents: Sequence[str] = (),
        max_page_size: int | None,
        max_raw_size: int,
    ) -> None:
        self._max_concurrent = max_concurrent
        self._timeout = timeout
        self._user_agent = user_agent
        self._rotated_agents = itertools.cycle(user_agents) if user_agents else None
        self._max_page_size = max_page_size
        self._max_raw_size = max_raw_size
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
        # certifi's CA bundle is added on top of the system store: TLS then
        # works on Python builds without system certificates, and locally
        # installed CAs (corporate proxies) stay trusted.
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(cafile=certifi.where())
        connector = aiohttp.TCPConnector(limit=self._max_concurrent, ttl_dns_cache=300, ssl=ssl_context)
        return aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent},
        )

    async def get(
        self, url: str, *, html_only: bool, raw: bool, truncate_at: int | None, timeout: aiohttp.ClientTimeout
    ) -> Response:
        """Perform the GET request and read the body up to its size limit.

        With `html_only`, the body of a response whose Content-Type is not
        HTML is not read: the content is empty and the size is 0. With
        `raw`, the body is returned as bytes and the content is empty; over
        `max_raw_size` it fails with `SitemapError`. With
        `truncate_at`, the body is cut to that many bytes. Otherwise a body
        over `max_page_size` fails with `PageTooLargeError`.
        The size is measured after content decoding (gzip, deflate, ...),
        so it may be larger than the number of bytes sent over the network.
        """
        # Checked on every request: close() may run while the caller waits
        # for its turn, and the failure is that of one URL only.
        if self._closed:
            raise CrawlerClosedError(url, "crawler is closed")
        _validate_url(url)
        session = self._get_session()
        headers = None if self._rotated_agents is None else {"User-Agent": next(self._rotated_agents)}
        try:
            # Redirects are followed by the caller, one request at a time, so
            # that each one is checked as a link to its target would be.
            async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as response:
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
                body = await self._read_body(response, url, raw=raw, truncate_at=truncate_at)
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
        # a TimeoutError; InvalidURL and the certificate error are ClientErrors too.
        except aiohttp.ClientResponseError as exc:
            retry_after = parse_retry_after(exc.headers.get(aiohttp.hdrs.RETRY_AFTER) if exc.headers else None)
            raise HTTPStatusError(url, exc.status, exc.message, retry_after=retry_after) from exc
        except TimeoutError as exc:
            raise FetchTimeoutError(url, _describe_timeout(exc, timeout)) from exc
        # UnicodeError comes from IDNA encoding of the host, e.g. a domain
        # label longer than 63 characters; aiohttp does not wrap it.
        except (aiohttp.InvalidURL, UnicodeError) as exc:
            raise InvalidURLError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientConnectorCertificateError as exc:
            raise CertificateError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientConnectorDNSError as exc:
            raise DNSError(url, f"{type(exc).__name__}: {exc}") from exc
        except aiohttp.ClientError as exc:
            raise NetworkError(url, f"{type(exc).__name__}: {exc}") from exc

    async def _read_body(
        self, response: aiohttp.ClientResponse, url: str, *, raw: bool, truncate_at: int | None
    ) -> bytes:
        """Read the body, giving up once it is over its size limit; the rest is not downloaded.

        A response sent with Content-Encoding: gzip is unpacked as it is
        read, so a few hundred kilobytes may turn into gigabytes: the limit
        is on the unpacked body.
        """
        limit = truncate_at or (self._max_raw_size if raw else self._max_page_size)
        if limit is None:
            return await response.read()
        too_large = SitemapError if raw else PageTooLargeError
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
