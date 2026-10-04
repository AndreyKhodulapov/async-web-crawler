import asyncio
import contextlib
import gzip
import logging
import time
from collections import Counter

import pytest
from aiohttp import web
from pages import ENCODING_PAGES, SITE_HEADERS, SITE_PAGES, fixture_html

from crawler.logging_setup import reset_logging
from demo_site import free_port


class SiteState:
    """What the crawl-test site has served, and its robots.txt.

    `log` lists (path, time) of every request to /site/ pages, /flaky/,
    /busy/, /shop/, sitemaps and robots.txt, in the order they arrived. `robots` is the
    body of /robots.txt, served with `robots_status`; None means 404; the
    first `robots_failures` requests for it answer 503 whatever it is, or
    the first `robots_failures_by_host[host]` requests from the hosts named
    there; `robots_hits` counts its requests by host.
    With `robots_endless`, comment lines follow the body for as long as
    the client reads them. `robots_by_host` gives other hosts of the server
    a robots.txt of their own.
    `sitemaps` maps the names of the files under /sitemaps/ to their
    bodies; the first `sitemap_failures` requests for them answer 503.
    `sitemap_headers` are added to the responses with them.
    `headers` keeps the request headers of the latest request for each
    path recorded, /cookies/ pages included.
    """

    def __init__(self) -> None:
        self.hits: Counter[str] = Counter()
        self.log: list[tuple[str, float]] = []
        self.latency = 0.0
        self.in_flight = 0
        self.peak_in_flight = 0
        self.robots: str | None = None
        self.robots_status = 200
        self.robots_failures = 0
        self.robots_failures_by_host: dict[str, int] = {}
        self.robots_hits: Counter[str] = Counter()
        self.robots_endless = False
        self.robots_by_host: dict[str, str] = {}
        self.sitemaps: dict[str, bytes] = {}
        self.sitemap_failures = 0
        self.sitemap_headers: dict[str, str] = {}
        self.headers: dict[str, dict[str, str]] = {}

    def record(self, request: web.Request) -> None:
        self.hits[request.path] += 1
        self.log.append((request.path, time.monotonic()))
        self.headers[request.path] = dict(request.headers)


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


async def set_cookies(request: web.Request) -> web.Response:
    """Sets a cookie for every parameter of the query but `redirect`, which redirects to /cookies/echo instead.

    `/cookies/set?sid=abc` answers a page that links to /cookies/echo; a
    value may carry attributes after ";", such as "abc; Max-Age=60".
    """
    request.app[SITE_STATE].record(request)
    cookies = {name: value for name, value in request.query.items() if name != "redirect"}
    if "redirect" in request.query:
        response = web.Response(status=302, headers={"Location": "/cookies/echo"})
    else:
        response = web.Response(
            text='<html><body><a href="/cookies/echo">echo</a></body></html>', content_type="text/html"
        )
    for name, value in cookies.items():
        response.headers.add("Set-Cookie", f"{name}={value}")
    return response


async def echo_cookies(request: web.Request) -> web.Response:
    """A page that lists the cookies of the request, as "cookie:name=value", one per paragraph."""
    request.app[SITE_STATE].record(request)
    items = "".join(f"<p>cookie:{name}={value}</p>" for name, value in sorted(request.cookies.items()))
    return web.Response(text=f"<html><body>{items}</body></html>", content_type="text/html")


async def robots_txt(request: web.Request) -> web.StreamResponse:
    state = request.app[SITE_STATE]
    state.record(request)
    host = request.url.host or ""
    state.robots_hits[host] += 1
    if state.robots_hits[host] <= state.robots_failures_by_host.get(host, state.robots_failures):
        raise web.HTTPServiceUnavailable()
    if request.url.host in state.robots_by_host:
        return web.Response(text=state.robots_by_host[request.url.host])
    if state.robots is None:
        raise web.HTTPNotFound()
    if not state.robots_endless:
        return web.Response(text=state.robots, status=state.robots_status)
    response = web.StreamResponse(status=state.robots_status, headers={"Content-Type": "text/plain"})
    await response.prepare(request)
    await response.write(state.robots.encode())
    with contextlib.suppress(ConnectionResetError):  # the client stops reading
        while True:
            await response.write(b"# padding\n" * 1000)
    return response


async def endless_page(request: web.Request) -> web.StreamResponse:
    """An HTML page that never ends, sent without Content-Length."""
    response = web.StreamResponse(headers={"Content-Type": "text/html"})
    await response.prepare(request)
    await response.write(b"<title>Endless</title>")
    with contextlib.suppress(ConnectionResetError):  # the client stops reading
        while True:
            await response.write(b"<p>" + b"x" * 65536 + b"</p>")
    return response


async def large_page(request: web.Request) -> web.Response:
    """A 2 MB page; its Content-Length tells the size before the body."""
    return web.Response(body=b"<title>Large</title>" + b"x" * 2_000_000, content_type="text/html")


async def gzip_bomb(request: web.Request) -> web.Response:
    """A few kilobytes on the wire that the client unpacks into 20 MB of HTML."""
    body = gzip.compress(b"<title>Bomb</title>" + b" " * 20_000_000)
    return web.Response(body=body, headers={"Content-Type": "text/html", "Content-Encoding": "gzip"})


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
    return web.Response(body=state.sitemaps[name], content_type=content_type, headers=state.sitemap_headers)


async def flaky(request: web.Request) -> web.Response:
    """Answers 503 with Retry-After: 0 the first `fails` times, then a page."""
    state = request.app[SITE_STATE]
    state.record(request)
    if state.hits[request.path] <= int(request.match_info["fails"]):
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "0"})
    return web.Response(text="<title>Recovered</title>", content_type="text/html")


async def busy(request: web.Request) -> web.Response:
    """Answers 429 with Retry-After of `seconds`."""
    request.app[SITE_STATE].record(request)
    raise web.HTTPTooManyRequests(headers={"Retry-After": request.match_info["seconds"]})


async def overloaded(request: web.Request) -> web.Response:
    """Answers 429 with Retry-After of `seconds` the first `fails` times, then a page."""
    state = request.app[SITE_STATE]
    state.record(request)
    if state.hits[request.path] <= int(request.match_info["fails"]):
        raise web.HTTPTooManyRequests(headers={"Retry-After": request.match_info["seconds"]})
    return web.Response(text="<title>Recovered</title>", content_type="text/html")


SHOP_PAGES = 20
SHOP_SORTS = ("price", "name", "date")


async def shop_list(request: web.Request) -> web.Response:
    """Page N of a listing, under every sort order: an endless-looking URL space.

    /shop/list?page=N, with or without &sort=S, links to every sort order of
    the page, to the next page in the same order, to item N and, as a share
    button, to itself with utm_source. Its canonical URL is /shop/list?page=N.
    """
    request.app[SITE_STATE].record(request)
    page = int(request.query.get("page", "1"))
    sort = request.query.get("sort")
    order = "" if sort is None else f"&sort={sort}"
    links = [f"list?page={page}&sort={each}" for each in SHOP_SORTS]
    links += [f"item/{page}", f"list?page={page}{order}&utm_source=share"]
    if page < SHOP_PAGES:
        links.append(f"list?page={page + 1}{order}")
    html = f'<link rel="canonical" href="/shop/list?page={page}"><title>Page {page}</title>'
    html += " ".join(f'<a href="{link}">{link}</a>' for link in links)
    return web.Response(text=html, content_type="text/html")


async def shop_item(request: web.Request) -> web.Response:
    request.app[SITE_STATE].record(request)
    return web.Response(text=f"<title>Item {request.match_info['n']}</title>", content_type="text/html")


WIDE_LINKS = 50


async def wide_page(request: web.Request) -> web.Response:
    """Page N links to WIDE_LINKS pages of its own: a site far larger than any crawl of it."""
    request.app[SITE_STATE].record(request)
    first = int(request.match_info["n"]) * WIDE_LINKS + 1
    html = " ".join(f'<a href="{n}">{n}</a>' for n in range(first, first + WIDE_LINKS))
    return web.Response(text=f"<title>Wide</title>{html}", content_type="text/html")


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
    if request.path == "/site/to-missing":
        raise web.HTTPFound("/site/missing.html")
    if request.path == "/site/to-other-host":
        raise web.HTTPFound(f"http://localhost:{request.url.port}/site/")
    if request.path == "/site/bounce":
        # Through a page of another host, like a consent page, and back.
        raise web.HTTPFound(f"http://localhost:{request.url.port}/site/bounce-back")
    if request.path == "/site/bounce-back":
        raise web.HTTPFound(f"http://127.0.0.1:{request.url.port}/site/")
    if request.path == "/site/go":
        raise web.HTTPFound("/site/private/secret")
    if request.path == "/site/lang" and "hl" not in request.query:
        raise web.HTTPFound("/site/lang?hl=en")
    if request.path == "/site/cookie-check" and "checked" not in request.cookies:
        # Sends the client back to the same page with a cookie.
        response = web.HTTPFound("/site/cookie-check")
        response.set_cookie("checked", "1")
        raise response
    if request.path not in SITE_PAGES:
        raise web.HTTPNotFound()
    html = SITE_PAGES[request.path].replace("{other_host}", f"http://localhost:{request.url.port}")
    return web.Response(text=html, content_type="text/html", headers=SITE_HEADERS.get(request.path))


@pytest.fixture
def restore_logging():
    """Undoes `configure_logging` after the test: its handlers are removed and closed, the level is put back."""
    level = logging.getLogger().level
    yield
    reset_logging()
    logging.getLogger().setLevel(level)


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
    app.router.add_get("/huge/endless", endless_page)
    app.router.add_get("/huge/large", large_page)
    app.router.add_get("/huge/gzip", gzip_bomb)
    app.router.add_get("/encoding/{name}", encoding_page)
    app.router.add_get("/site/{path:.*}", site_page)
    app.router.add_get("/robots.txt", robots_txt)
    app.router.add_get("/sitemaps/{name}", sitemap)
    app.router.add_get("/flaky/{fails}", flaky)
    app.router.add_get("/busy/{seconds}", busy)
    app.router.add_get("/overloaded/{fails}/{seconds}", overloaded)
    app.router.add_get("/shop/list", shop_list)
    app.router.add_get("/shop/item/{n}", shop_item)
    app.router.add_get("/wide/{n}", wide_page)
    app.router.add_get("/cookies/set", set_cookies)
    app.router.add_get("/cookies/echo", echo_cookies)
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
