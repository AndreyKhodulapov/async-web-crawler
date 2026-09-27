import asyncio
import socket

import pytest
from aiohttp import web


async def ok(request: web.Request) -> web.Response:
    return web.Response(
        text="<html><body>hello</body></html>", content_type="text/html"
    )


async def status(request: web.Request) -> web.Response:
    return web.Response(status=int(request.match_info["code"]))


async def delay(request: web.Request) -> web.Response:
    await asyncio.sleep(float(request.match_info["seconds"]))
    return web.Response(text="done")


@pytest.fixture
async def server(aiohttp_server):
    """Local HTTP server with predictable endpoints; no internet required."""
    app = web.Application()
    app.router.add_get("/ok", ok)
    app.router.add_get("/status/{code}", status)
    app.router.add_get("/delay/{seconds}", delay)
    return await aiohttp_server(app)


@pytest.fixture
def closed_port_url() -> str:
    """URL pointing to a local port that nothing listens on."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}/"
