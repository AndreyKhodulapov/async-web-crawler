import asyncio
import codecs
import socket
from pathlib import Path

import pytest
from aiohttp import web


def fixture_html(name: str) -> str:
    return (Path(__file__).parent / "fixtures" / name).read_text(encoding="utf-8")


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


async def cp1252_page(request: web.Request) -> web.Response:
    # Not valid UTF-8: decoding only works if the declared charset is used.
    body = "<title>Café</title><p>Crème brûlée</p>".encode("cp1252")
    return web.Response(body=body, content_type="text/html", charset="windows-1252")


# Pages whose encoding must be worked out from the body, or from a header
# charset that cannot decode text: (body, header charset, expected title).
CAFE = "<title>Café</title>"
ENCODING_PAGES: dict[str, tuple[bytes, str | None, str]] = {
    # The encoding is declared only in the markup, not in the header.
    "meta-charset": (('<meta charset="windows-1252">' + CAFE).encode("cp1252"), None, "Café"),
    "meta-http-equiv": (
        '<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-2"><title>Łódź</title>'.encode(
            "iso-8859-2"
        ),
        None,
        "Łódź",  # "£ódŸ" under windows-1252
    ),
    "meta-shift-jis": ('<meta charset="shift_jis"><title>カフェ</title>'.encode("shift_jis"), None, "カフェ"),
    # The header charset wins over the markup.
    "header-charset-wins": (('<meta charset="utf-8">' + CAFE).encode("cp1252"), "windows-1252", "Café"),
    # A byte order mark wins over everything else.
    "utf8-bom": (codecs.BOM_UTF8 + CAFE.encode(), "windows-1252", "Café"),
    "utf16le-bom": (codecs.BOM_UTF16_LE + CAFE.encode("utf-16-le"), None, "Café"),
    "utf16be-bom": (codecs.BOM_UTF16_BE + CAFE.encode("utf-16-be"), None, "Café"),
    # A <meta> found in ASCII bytes cannot really mean UTF-16/32 (HTML spec):
    # the page is read as UTF-8.
    "meta-utf16": (('<meta charset="utf-16">' + CAFE).encode(), None, "Café"),
    "meta-utf32": (('<meta charset="utf-32">' + CAFE).encode(), None, "Café"),
    # Charsets that name no codec, or a codec that cannot decode a page.
    "meta-unknown": (('<meta charset="no-such-charset">' + CAFE).encode(), None, "Café"),
    "meta-undefined": (('<meta charset="undefined">' + CAFE).encode(), None, "Café"),
    "meta-idna": (('<meta charset="idna">' + CAFE).encode(), None, "Café"),
    "meta-base64": (('<meta charset="base64">' + CAFE).encode(), None, "Café"),
    "header-undefined": (CAFE.encode(), "undefined", "Café"),
    "header-idna": (CAFE.encode(), "idna", "Café"),
    "header-base64": (CAFE.encode(), "base64", "Café"),
    "header-unknown": (CAFE.encode(), "no-such-charset", "Café"),
}


async def encoding_page(request: web.Request) -> web.Response:
    body, charset, _ = ENCODING_PAGES[request.match_info["name"]]
    content_type = "text/html" if charset is None else f"text/html; charset={charset}"
    return web.Response(body=body, headers={"Content-Type": content_type})


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
    app.router.add_get("/encoding/{name}", encoding_page)
    return await aiohttp_server(app)


@pytest.fixture
def closed_port_url() -> str:
    """URL pointing to a local port that nothing listens on."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}/"
