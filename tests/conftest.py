import asyncio
import time
from collections import Counter

import pytest
from aiohttp import web
from pages import ENCODING_PAGES, SITE_PAGES, fixture_html

from demo_site import free_port


class SiteState:
    """What the crawl-test site has served, and its robots.txt.

    `log` lists (path, time) of every request to /site/ pages, /flaky/,
    sitemaps and robots.txt, in the order they arrived. `robots` is the
    body of /robots.txt, served with `robots_status`; None means 404.
    `sitemaps` maps the names of the files under /sitemaps/ to their
    bodies; the first `sitemap_failures` requests for them answer 503.
    """

    def __init__(self) -> None:
        self.hits: Counter[str] = Counter()
        self.log: list[tuple[str, float]] = []
        self.latency = 0.0
        self.in_flight = 0
        self.peak_in_flight = 0
        self.robots: str | None = None
        self.robots_status = 200
        self.sitemaps: dict[str, bytes] = {}
        self.sitemap_failures = 0

    def record(self, request: web.Request) -> None:
        self.hits[request.path] += 1
        self.log.append((request.path, time.monotonic()))


SITE_STATE = web.AppKey("site_state", SiteState)


async def ok(request: web.Request) -> web.Response:
    return web.Response(text="<html><body>hello</body></html>", content_type="text/html")


async def status(request: web.Request) -> web.Response:
    return web.Response(status=int(request.match_info["code"]))


async def delay(request: web.Request) -> web.Response:
    await asyncio.sleep(float(request.match_info["seconds"]))
    return web.Response(text="done")


async def redirect_loop(request: web.Request) -> web.Response:
    raise web.HTTPFound("/redirect-loop")


async def catalog(request: web.Request) -> web.Response:
    return web.Response(text=fixture_html("valid_page.html"), content_type="text/html")


async def moved(request: web.Request) -> web.Response:
    raise web.HTTPMovedPermanently("/catalog/tools/")


async def json_data(request: web.Request) -> web.Response:
    return web.json_response({"html": "<p>not a page</p>"})


async def encoding_page(request: web.Request) -> web.Response:
    body, charset, _ = ENCODING_PAGES[request.match_info["name"]]
    content_type = "text/html" if charset is None else f"text/html; charset={charset}"
    return web.Response(body=body, headers={"Content-Type": content_type})


async def robots_txt(request: web.Request) -> web.Response:
    state = request.app[SITE_STATE]
    state.record(request)
    if state.robots is None:
        raise web.HTTPNotFound()
    return web.Response(text=state.robots, status=state.robots_status)


async def sitemap(request: web.Request) -> web.Response:
    state = request.app[SITE_STATE]
    state.record(request)
    if state.sitemap_failures > 0:
        state.sitemap_failures -= 1
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "0"})
    name = request.match_info["name"]
    if name not in state.sitemaps:
        raise web.HTTPNotFound()
    # Gzipped files are sent as they are, not as a Content-Encoding the client would undo.
    content_type = "application/gzip" if name.endswith(".gz") else "application/xml"
    return web.Response(body=state.sitemaps[name], content_type=content_type)


async def flaky(request: web.Request) -> web.Response:
    """Answers 503 with Retry-After: 0 the first `fails` times, then a page."""
    state = request.app[SITE_STATE]
    state.record(request)
    if state.hits[request.path] <= int(request.match_info["fails"]):
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "0"})
    return web.Response(text="<title>Recovered</title>", content_type="text/html")


async def site_page(request: web.Request) -> web.Response:
    state = request.app[SITE_STATE]
    state.record(request)
    state.in_flight += 1
    state.peak_in_flight = max(state.peak_in_flight, state.in_flight)
    try:
        await asyncio.sleep(state.latency)
    finally:
        state.in_flight -= 1
    if request.path == "/site/moved":
        raise web.HTTPFound("/site/c.html")
    if request.path == "/site/to-other-host":
        raise web.HTTPFound(f"http://localhost:{request.url.port}/site/")
    if request.path not in SITE_PAGES:
        raise web.HTTPNotFound()
    html = SITE_PAGES[request.path].replace("{other_host}", f"http://localhost:{request.url.port}")
    return web.Response(text=html, content_type="text/html")


@pytest.fixture
async def server(aiohttp_server):
    """Local HTTP server with predictable endpoints; no internet required."""
    app = web.Application()
    app[SITE_STATE] = SiteState()
    app.router.add_get("/ok", ok)
    app.router.add_get("/status/{code}", status)
    app.router.add_get("/delay/{seconds}", delay)
    app.router.add_get("/redirect-loop", redirect_loop)
    app.router.add_get("/catalog/tools/", catalog)
    app.router.add_get("/moved", moved)
    app.router.add_get("/data.json", json_data)
    app.router.add_get("/encoding/{name}", encoding_page)
    app.router.add_get("/site/{path:.*}", site_page)
    app.router.add_get("/robots.txt", robots_txt)
    app.router.add_get("/sitemaps/{name}", sitemap)
    app.router.add_get("/flaky/{fails}", flaky)
    return await aiohttp_server(app)


@pytest.fixture
def url(server):
    def make(path: str, host: str = "127.0.0.1") -> str:
        return f"http://{host}:{server.port}{path}"

    return make


@pytest.fixture
def site(server) -> SiteState:
    return server.app[SITE_STATE]


@pytest.fixture
def closed_port_url() -> str:
    """URL pointing to a local port that nothing listens on."""
    return f"http://127.0.0.1:{free_port()}/"
