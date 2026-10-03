"""A local site that fails in the ways real sites do, for the `errors` demo.

The start page links to pages that answer normally and to pages that fail:

    /articles/N     ordinary pages
    /flaky          HTTP 503 twice, then the page
    /rate-limited   HTTP 429 with Retry-After: 1 once, then the page
    /server-error   always HTTP 500
    /slow           answers after 1.2 s, longer than a 1 s read timeout
    /missing        HTTP 404
    /private        HTTP 403
    /empty          an HTML page with nothing in it
    /data.json      JSON instead of HTML

It also links to a server that is down: pages on `localhost` at a port
nothing listens on. The crawler tells hosts apart by name, not port, so
their failures open the circuit breaker of `localhost` and leave the
site itself, served on 127.0.0.1, alone. The last link is a domain that
does not resolve.

The ordinary pages come first in the list, so that the site's own
failures stay under the breaker's threshold whatever order the retries
take.
"""

import asyncio
import html
import socket
from collections import Counter
from typing import Self

from aiohttp import web


def free_port() -> int:
    """A local port that nothing listens on right now."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def page(title: str, body: str = "") -> web.Response:
    text = f"<!doctype html><html><head><title>{html.escape(title)}</title></head><body>{body}</body></html>"
    return web.Response(text=text, content_type="text/html")


class DemoSite:
    """Serves the site on 127.0.0.1 at a free port while in `async with`.

    `extra_links` are added to the start page, so that real URLs are
    fetched along with the demo pages, without following their links.
    """

    ARTICLES = 8

    def __init__(self, extra_links: list[str] | None = None) -> None:
        self.extra_links = extra_links or []
        self.hits: Counter[str] = Counter()
        self._runner: web.AppRunner | None = None
        self._port = 0
        self._down_port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}/"

    async def __aenter__(self) -> Self:
        app = web.Application()
        app.router.add_get("/", self._index)
        app.router.add_get("/articles/{number}", self._article)
        app.router.add_get("/flaky", self._flaky)
        app.router.add_get("/rate-limited", self._rate_limited)
        app.router.add_get("/server-error", self._server_error)
        app.router.add_get("/slow", self._slow)
        app.router.add_get("/private", self._private)
        app.router.add_get("/empty", self._empty)
        app.router.add_get("/data.json", self._data)
        # /missing is not routed: aiohttp answers 404.
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            await web.TCPSite(runner, "127.0.0.1", 0).start()
        except BaseException:
            await runner.cleanup()
            raise
        self._runner = runner
        self._port = runner.addresses[0][1]
        # Chosen once the site holds its own port, so the system cannot
        # give the site the port of the server that is down.
        self._down_port = free_port()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    def links(self) -> list[str]:
        """Every link of the start page, in order."""
        own = [f"articles/{number}" for number in range(1, self.ARTICLES + 1)]
        own += ["flaky", "rate-limited", "server-error", "slow", "missing", "private", "empty", "data.json"]
        down = [f"http://localhost:{self._down_port}/page/{number}" for number in range(1, 8 + 1)]
        return [self.url + path for path in own] + down + ["http://unreachable.invalid/", *self.extra_links]

    async def _index(self, request: web.Request) -> web.Response:
        items = "".join(f'<li><a href="{html.escape(link)}">{html.escape(link)}</a></li>' for link in self.links())
        return page("Unreliable site", f"<h1>Unreliable site</h1><ul>{items}</ul>")

    async def _article(self, request: web.Request) -> web.Response:
        return page(f"Article {request.match_info['number']}", "<p>Nothing goes wrong here.</p>")

    def _count(self, request: web.Request) -> int:
        self.hits[request.path] += 1
        return self.hits[request.path]

    async def _flaky(self, request: web.Request) -> web.Response:
        if self._count(request) <= 2:
            raise web.HTTPServiceUnavailable()
        return page("Flaky page", "<p>Answered after a few failures.</p>")

    async def _rate_limited(self, request: web.Request) -> web.Response:
        if self._count(request) == 1:
            raise web.HTTPTooManyRequests(headers={"Retry-After": "1"})
        return page("Rate-limited page", "<p>Answered after the wait the server asked for.</p>")

    async def _server_error(self, request: web.Request) -> web.Response:
        raise web.HTTPInternalServerError()

    async def _slow(self, request: web.Request) -> web.Response:
        await asyncio.sleep(1.2)
        return page("Slow page", "<p>Answered once the read timeout grew.</p>")

    async def _private(self, request: web.Request) -> web.Response:
        raise web.HTTPForbidden()

    async def _empty(self, request: web.Request) -> web.Response:
        return web.Response(text="", content_type="text/html")

    async def _data(self, request: web.Request) -> web.Response:
        return web.json_response({"pages": self.ARTICLES})
