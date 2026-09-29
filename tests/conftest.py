import asyncio
import socket
from collections import Counter

import pytest
from aiohttp import web
from pages import ENCODING_PAGES, SITE_PAGES, fixture_html


class SiteState:
    """What the crawl-test site has served: hits per path and peak concurrency."""

    def __init__(self) -> None:
        self.hits: Counter[str] = Counter()
        self.latency = 0.0
        self.in_flight = 0
        self.peak_in_flight = 0


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


async def site_page(request: web.Request) -> web.Response:
    state = request.app[SITE_STATE]
    state.hits[request.path] += 1
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
    return await aiohttp_server(app)


@pytest.fixture
def site(server) -> SiteState:
    return server.app[SITE_STATE]


@pytest.fixture
def closed_port_url() -> str:
    """URL pointing to a local port that nothing listens on."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}/"
