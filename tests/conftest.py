import asyncio
import socket
from collections.abc import Callable
from pathlib import Path

import pytest
from aiohttp import web

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def read_fixture() -> Callable[[str], str]:
    """Return a function that reads an HTML file from tests/fixtures."""

    def read(name: str) -> str:
        return (FIXTURES_DIR / name).read_text(encoding="utf-8")

    return read


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
    html = (FIXTURES_DIR / "valid_page.html").read_text(encoding="utf-8")
    return web.Response(text=html, content_type="text/html")


async def moved(request: web.Request) -> web.Response:
    raise web.HTTPMovedPermanently("/catalog/tools/")


async def json_data(request: web.Request) -> web.Response:
    return web.json_response({"html": "<p>not a page</p>"})


async def cp1252_page(request: web.Request) -> web.Response:
    # Not valid UTF-8: decoding only works if the declared charset is used.
    body = "<title>Café</title><p>Crème brûlée</p>".encode("cp1252")
    return web.Response(body=body, content_type="text/html", charset="windows-1252")


@pytest.fixture
async def server(aiohttp_server):
    """Local HTTP server with predictable endpoints; no internet required."""
    app = web.Application()
    app.router.add_get("/ok", ok)
    app.router.add_get("/status/{code}", status)
    app.router.add_get("/delay/{seconds}", delay)
    app.router.add_get("/redirect-loop", redirect_loop)
    app.router.add_get("/catalog/tools/", catalog)
    app.router.add_get("/moved", moved)
    app.router.add_get("/data.json", json_data)
    app.router.add_get("/cp1252", cp1252_page)
    return await aiohttp_server(app)


@pytest.fixture
def closed_port_url() -> str:
    """URL pointing to a local port that nothing listens on."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}/"
