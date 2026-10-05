"""Rendering of pages in a headless browser: a transport that runs the JavaScript of HTML pages.

Playwright is an optional dependency (`pip install -e ".[js]"`, then
`playwright install chromium`): it is imported when the first page is
rendered, so the crawler works without it as long as nothing is.
"""

import asyncio
import contextlib
import importlib.util
import logging
import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from http.cookiejar import HTTPONLY_ATTR, Cookie
from typing import TYPE_CHECKING, Any, NamedTuple

import aiohttp

from crawler.exceptions import CrawlerClosedError, FetchTimeoutError, PageTooLargeError, RenderError
from crawler.filters import UrlFilter
from crawler.models import RenderStats
from crawler.parser import is_html_content_type
from crawler.proxy import Proxy
from crawler.session import cookie_domain_problem, cookie_name_problem, cookie_value_problem, make_cookie
from crawler.transport import Response, Transport

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserContext, Page, Playwright, Request, Route

logger = logging.getLogger(__name__)

INSTALL_PACKAGE = 'pip install -e ".[js]"'
INSTALL_BROWSER = "playwright install chromium"
_NO_PLAYWRIGHT = f"Playwright is not installed; run: {INSTALL_PACKAGE}"
_NO_CHROMIUM = f"Chromium is not installed; run: {INSTALL_BROWSER}"
# When the page has fired its event: "networkidle" is no request for 500 ms.
WAIT_STATES = ("load", "domcontentloaded", "networkidle")
# The types of the requests a page makes, as Playwright names them, but
# that of the page itself ("document"), which cannot be left out.
RESOURCE_TYPES = frozenset(
    {
        "stylesheet",
        "image",
        "media",
        "font",
        "script",
        "texttrack",
        "xhr",
        "fetch",
        "eventsource",
        "websocket",
        "manifest",
        "other",
    }
)


@dataclass(frozen=True)
class Rendering:
    """Which pages a headless browser renders, and how long it waits for them; see `BrowserTransport`.

    - `include`: regular expressions, searched as `UrlFilter` does; when
      given, only the URLs that match one of them are rendered, the
      others are fetched as without a browser. Empty: every HTML page is.
    - `wait_until`: the event of the page to wait for, one of
      `WAIT_STATES`: "load", "domcontentloaded" or "networkidle" (no
      request for half a second).
    - `wait_for`: a CSS selector to wait for after that, if any.
    - `timeout`: seconds the browser has for a page, the waits included;
      the download of the page itself has the timeouts of the crawler.
    - `max_open_pages`: pages rendered at once; each is a browser tab of
      50 to 100 MB.
    - `block_resources`: the types of requests the browser does not make
      (see `RESOURCE_TYPES`); by default images, fonts and media, which
      take time and add nothing to the HTML.
    """

    include: tuple[str, ...] = ()
    wait_until: str = "load"
    wait_for: str | None = None
    timeout: float = 30.0
    max_open_pages: int = 2
    block_resources: frozenset[str] = frozenset({"image", "font", "media"})
    _filter: UrlFilter = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for name in ("include", "block_resources"):
            if isinstance(getattr(self, name), str):
                raise TypeError(f"expected a list for {name}, got a string: {getattr(self, name)!r}")
        object.__setattr__(self, "include", tuple(self.include))
        object.__setattr__(self, "block_resources", frozenset(self.block_resources))
        if self.wait_until not in WAIT_STATES:
            raise ValueError(f"wait_until must be one of {', '.join(WAIT_STATES)}, got {self.wait_until!r}")
        if self.wait_for is not None and not self.wait_for.strip():
            raise ValueError("wait_for must be a CSS selector, got an empty string")
        if not self.timeout > 0:
            raise ValueError(f"timeout must be positive, got {self.timeout}")
        if self.max_open_pages < 1:
            raise ValueError(f"max_open_pages must be >= 1, got {self.max_open_pages}")
        unknown = sorted(self.block_resources - RESOURCE_TYPES)
        if unknown:
            raise ValueError(
                f"unknown resource types {', '.join(map(repr, unknown))} in block_resources,"
                f" expected some of {', '.join(sorted(RESOURCE_TYPES))}"
            )
        # Raises ValueError for a pattern that is not a regular expression.
        object.__setattr__(self, "_filter", UrlFilter(include_patterns=self.include))

    def renders(self, url: str) -> bool:
        """Whether the page at `url` is rendered, if it is HTML."""
        return self._filter.allows(url)


def playwright_problem() -> str | None:
    """Why pages cannot be rendered if the Playwright package is missing, with the command to install it; else None.

    The package is looked for, not imported.
    """
    return _NO_PLAYWRIGHT if importlib.util.find_spec("playwright") is None else None


async def browser_problem() -> str | None:
    """Why pages cannot be rendered: Playwright or its Chromium is not installed; None if both are.

    The message holds the command to install what is missing. The driver
    of Playwright is started for a moment to ask where Chromium is, the
    browser is not.
    """
    problem = playwright_problem()
    if problem is not None:
        return problem
    from playwright.async_api import Error, async_playwright

    try:
        async with async_playwright() as playwright:
            executable = playwright.chromium.executable_path
    except (Error, OSError) as exc:  # OSError: the driver cannot be run
        return f"Playwright could not start: {_first_line(exc)}"
    return None if os.path.exists(executable) else _NO_CHROMIUM


def bypass_rules(no_proxy: str | None) -> str | None:
    """The hosts of NO_PROXY as Chromium's rules for the hosts it reaches without a proxy; None if there are none.

    As the crawler reads NO_PROXY, a name is that host and its subdomains,
    a dot in front of it changes nothing, and a port is part of the name.
    A range of addresses ("10.0.0.0/8") is left out: the crawler does not
    read one, and sends its requests through the proxy.
    """
    rules = []
    for entry in (no_proxy or "").split(","):
        name = entry.strip().lstrip(".").lower()
        if name and "/" not in name:
            rules += [name, f"*.{name}"]
    if not rules:
        return None
    # Chromium reaches every loopback address directly, and Playwright stops that only while the
    # rules name none of them: with "localhost", 127.0.0.1 would bypass the proxy, unlike for the crawler.
    return ", ".join(["<-loopback>", *rules])


_Key = tuple[str, str, str]  # the domain, path and name of a cookie


class CookieSync:
    """Keeps the cookies of a browser context in step with those of the jar of the crawler, both ways.

    Before a page, what the jar has changed since the last time goes to
    the browser (`to_browser`); after it, what the browser has changed
    goes to the jar (`to_jar`). Each side is compared with how it was the
    last time, so neither undoes what the other changed meanwhile: a
    page that opens while another one runs does not overwrite with the
    jar the cookies the other one's JavaScript set, and the cookies the
    crawler gets while a page runs are not lost. When both sides change
    a cookie, the browser wins: what the browser changed is taken before
    what the jar changed is given.

    `sent` and `written` record how a side kept what it was given,
    which may differ from what it was given (Chromium refuses some
    cookies and shortens long lives): it is not taken for a change.

    Cookies the crawler would not keep or send stay in the browser:
    those of IP addresses, and those with a name or a value it cannot
    send (see `syncable`). One per context; `lock` keeps the steps of
    two pages apart.
    """

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self._jar: dict[_Key, Cookie] = {}  # how each side was the last time
        self._browser: dict[_Key, Cookie] = {}
        self._giving: dict[_Key, Cookie] = {}  # the jar of `to_browser`, until `sent`

    def to_browser(self, jar: Iterable[Cookie]) -> tuple[list[Cookie], list[Cookie]]:
        """The cookies the jar has changed and those it has dropped since the last time.

        They count as given once `sent` says how the browser keeps them:
        if giving them fails, they are given again the next time.
        """
        self._giving = _by_key(jar)
        return _changes(self._jar, self._giving)

    def sent(self, cookies: Iterable[Cookie], browser: Iterable[Cookie]) -> None:
        """How the browser keeps `cookies`, just sent to it: as it has them in `browser`, or not at all."""
        self._jar = self._giving
        _record(self._browser, cookies, _by_key(browser))

    def to_jar(self, browser: Iterable[Cookie]) -> tuple[list[Cookie], list[Cookie]]:
        """The cookies the browser has changed and those it has dropped since the last time.

        They count as taken at once: one the jar fails to take is not
        taken again before every page.
        """
        now = _by_key(browser)
        changes = _changes(self._browser, now)
        self._browser = now
        return changes

    def written(self, cookies: Iterable[Cookie], jar: Iterable[Cookie]) -> None:
        """How the jar keeps `cookies`, just written to it: as it has them in `jar`, or not at all."""
        _record(self._jar, cookies, _by_key(jar))


def syncable(cookie: Cookie) -> bool:
    """Whether the crawler keeps and sends `cookie`, so that it goes between the browser and the jar."""
    return (
        cookie_domain_problem(cookie.domain) is None
        and cookie_name_problem(cookie.name) is None
        and cookie_value_problem(cookie.value or "") is None
    )


def _key(cookie: Cookie) -> _Key:
    return cookie.domain, cookie.path, cookie.name


def _state(cookie: Cookie) -> tuple[object, ...]:
    return cookie.value, cookie.expires, cookie.secure, cookie.has_nonstandard_attr(HTTPONLY_ATTR)


def _by_key(cookies: Iterable[Cookie]) -> dict[_Key, Cookie]:
    return {_key(cookie): cookie for cookie in cookies if syncable(cookie)}


def _changes(before: dict[_Key, Cookie], now: dict[_Key, Cookie]) -> tuple[list[Cookie], list[Cookie]]:
    changed = [cookie for key, cookie in now.items() if key not in before or _state(before[key]) != _state(cookie)]
    removed = [cookie for key, cookie in before.items() if key not in now]
    return changed, removed


def _record(side: dict[_Key, Cookie], cookies: Iterable[Cookie], now: dict[_Key, Cookie]) -> None:
    for key in map(_key, cookies):
        if key in now:
            side[key] = now[key]
        else:
            side.pop(key, None)


def to_playwright(cookie: Cookie) -> dict[str, Any]:
    """`cookie` as Playwright adds it to a context; a domain without "." in front is for its host only."""
    result: dict[str, Any] = {
        "name": cookie.name,
        "value": cookie.value or "",
        "domain": cookie.domain,
        "path": cookie.path,
        "secure": cookie.secure,
        "httpOnly": cookie.has_nonstandard_attr(HTTPONLY_ATTR),
    }
    if cookie.expires is not None:
        result["expires"] = cookie.expires
    return result


def from_playwright(cookie: Mapping[str, Any]) -> Cookie:
    """A cookie of a context, as Playwright gives it, as `http.cookiejar` has it; -1 as the expiry is for the session."""
    expires = cookie.get("expires", -1)
    return make_cookie(
        cookie.get("name", ""),
        cookie.get("value", ""),
        cookie.get("domain", ""),
        path=cookie.get("path", "/"),
        secure=cookie.get("secure", False),
        expires=int(expires) if expires > 0 else None,
        http_only=cookie.get("httpOnly", False),
    )


async def _add_cookies(context: "BrowserContext", cookies: list[Cookie]) -> None:
    """Add `cookies` to `context`; one Chromium refuses is left out, not the others with it.

    Chromium refuses a whole batch for one cookie it takes for invalid,
    such as a `__Host-` cookie with a path, so after a refusal they are
    added one by one. A cookie refused takes the one of its name the
    browser had out too: the browser would go on with a value the
    crawler no longer has.
    """
    from playwright.async_api import Error

    try:
        await context.add_cookies([to_playwright(cookie) for cookie in cookies])  # type: ignore[arg-type]
        return
    except Error:
        # Unless the context is gone, which fails the page: one cookie was refused.
        await context.cookies()
    for cookie in cookies:
        try:
            await context.add_cookies([to_playwright(cookie)])  # type: ignore[arg-type]
        except Error:
            # The value is a secret, and so may be the message.
            logger.warning("The browser refused the cookie %s of %s", cookie.name, cookie.domain)
            await context.clear_cookies(name=cookie.name, domain=cookie.domain, path=cookie.path)


class Rendered(NamedTuple):
    """What the browser made of a page: its HTML, or the URL it went to on its own instead."""

    content: str
    location: str | None = None


class BrowserTransport:
    """A `Transport` that renders HTML pages in a headless browser, as `rendering` says.

    Every request is made by `http` first, as without a browser, and only
    a page that came back as HTML, and that `rendering` names, goes to the
    browser: the browser gets the document as it was downloaded, runs its
    JavaScript and loads what it asks for, but the `block_resources`. So
    the document goes through the proxies, the cookies and the size limit
    of `http`, and a redirect, robots.txt, a sitemap, a page that is not
    HTML come back as without a browser.

    A page that goes to another URL on its own (JavaScript setting
    `location`, `<meta http-equiv="refresh">`, a form sent) does not get
    there: the browser is stopped, and the page comes back as a redirect
    to that URL, for the caller to check and follow as any other.

    The browser shares the cookies of `http`: it gets those `http` keeps
    before every page, and they get those the page set (by JavaScript or
    in the responses to its requests) after it, see `CookieSync`. With
    `keep_cookies=False` nothing goes between them, and each page has a
    browser of its own. The requests of the browser carry `user_agent`
    and the `headers`, and go through the proxy the document of their
    page came through (`Response.proxy`), or directly if it came so; the
    hosts `no_proxy` names (as the NO_PROXY variable does) are reached
    directly. They are not requests of the crawler: their failures are
    neither those of the proxy nor of the site.

    A rendered page keeps the status, the headers and the final URL of
    its download; its content is the HTML of the page once rendered,
    which fails with `PageTooLargeError` over `max_page_size` bytes. A
    page that takes the browser longer than `rendering.timeout` fails
    with `FetchTimeoutError`; a browser that is not installed, cannot
    start or crashes fails it with `RenderError` (see `Renderer`).
    """

    def __init__(
        self,
        http: Transport,
        rendering: Rendering,
        *,
        user_agent: str,
        max_page_size: int | None,
        headers: Mapping[str, str] | None = None,
        keep_cookies: bool = True,
        no_proxy: str | None = None,
    ) -> None:
        self.http = http
        self.rendering = rendering
        self.renderer = Renderer(
            rendering,
            user_agent=user_agent,
            headers=headers,
            cookies=http if keep_cookies else None,
            no_proxy=no_proxy,
        )
        self._max_page_size = max_page_size

    async def get(
        self,
        url: str,
        *,
        html_only: bool,
        raw_limit: int | None,
        truncate_at: int | None,
        timeout: aiohttp.ClientTimeout,
    ) -> Response:
        """Perform the GET request as `Transport.get` says, and render the page if it is one to render."""
        response = await self.http.get(
            url, html_only=html_only, raw_limit=raw_limit, truncate_at=truncate_at, timeout=timeout
        )
        # robots.txt is truncated and a sitemap is raw: neither is a page.
        if raw_limit is not None or truncate_at is not None or response.redirected:
            return response
        if not is_html_content_type(response.content_type) or not self.rendering.renders(url):
            return response
        rendered = await self.renderer.render(url, response)
        if rendered.location is not None:
            logger.info("%s went to %s in the browser", url, rendered.location)
            return Response(
                status=response.status,
                content="",
                size=0,
                final_url=rendered.location,
                content_type=response.content_type,
                redirected=True,
                proxy=response.proxy,
            )
        size = len(rendered.content.encode("utf-8", "replace"))
        if self._max_page_size is not None and size > self._max_page_size:
            raise PageTooLargeError(url, f"larger than {self._max_page_size} bytes once rendered")
        return response._replace(content=rendered.content, size=size)

    async def close(self) -> None:
        """Close the browser and the session of `http`. Safe to call more than once."""
        try:
            await self.renderer.close()
        finally:
            await self.http.close()

    def reset_stats(self) -> None:
        self.http.reset_stats()
        self.renderer.reset_stats()

    def render_stats(self) -> RenderStats:
        """The pages rendered since the stats were last reset, see `RenderStats`."""
        return self.renderer.stats()

    def cookies(self) -> list[Cookie]:
        return self.http.cookies()

    def update_cookies(self, changed: Iterable[Cookie], removed: Iterable[Cookie]) -> None:
        self.http.update_cookies(changed, removed)


class _LaunchError(Exception):
    """The browser could not start; the message says why, for every page that needed it."""


class _Tab:
    """A page in the browser: the document it is to get, and where it went on its own."""

    def __init__(self, document: Response) -> None:
        self.document = document
        self.served = False
        self.crashed = False
        self.navigated: asyncio.Future[str] = asyncio.get_running_loop().create_future()


class Renderer:
    """Renders pages in one headless Chromium, at most `rendering.max_open_pages` at once.

    The browser is started for the first page and closed by `close()`.
    A page is rendered in the context (cookies, cache) of the proxy its
    document came through, so all its requests go through that proxy;
    the pages of a proxy share one, those without one share another. The
    cookies of a context are kept in step with those of `cookies`, the
    transport of the crawler, around every page (see `CookieSync`), and
    so go from one context to the others. Without `cookies`, each page
    has a context of its own, closed after it: nothing goes from one page
    to the next. The requests of the browser carry the `user_agent` and
    the `headers` of the crawler; the hosts of `no_proxy` (see
    `bypass_rules`) are reached without a proxy. Every request of a page
    goes through `_route`: the page gets its document as downloaded, its
    own navigations are stopped and reported, frames and pop-ups get
    nothing, and the `block_resources` are not requested.

    A browser that crashes fails the pages being rendered with
    `RenderError` and is started again for the next one, once: after
    that, and after a browser that could not start, every page fails
    with `RenderError` at once.

    `stats()` counts the pages rendered and failed, and the time the
    browser took for them, see `RenderStats`.
    """

    MAX_LAUNCHES = 2

    def __init__(
        self,
        rendering: Rendering,
        *,
        user_agent: str,
        headers: Mapping[str, str] | None = None,
        cookies: Transport | None = None,
        no_proxy: str | None = None,
    ) -> None:
        self.rendering = rendering
        self._user_agent = user_agent
        self._headers = dict(headers or {})
        self._cookies = cookies
        self._bypass = bypass_rules(no_proxy)
        self._pages = asyncio.Semaphore(rendering.max_open_pages)
        self._lock = asyncio.Lock()  # one launch at a time
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        # Shared by the pages of a proxy, by its label (None: without a proxy), with what keeps their cookies in step.
        self._contexts: dict[str | None, tuple[BrowserContext, CookieSync]] = {}
        self._tabs: dict[Page, _Tab] = {}
        self._launches = 0
        self._failure: str | None = None  # why the browser is not used any more
        self._closed = False
        self.reset_stats()

    def stats(self) -> RenderStats:
        """The pages rendered since the stats were last reset."""
        average = self._render_time / self._rendered if self._rendered else 0.0
        return RenderStats(rendered=self._rendered, failed=self._failed, avg_render_time=average)

    def reset_stats(self) -> None:
        """Count the pages anew."""
        self._rendered = self._failed = 0
        self._render_time = 0.0

    @property
    def running(self) -> bool:
        """Whether a browser is running."""
        return self._browser is not None and self._browser.is_connected()

    async def render(self, url: str, document: Response) -> Rendered:
        """Load `document`, downloaded from `url`, in a browser tab and return what it became.

        Raises:
            FetchTimeoutError: the page took longer than `rendering.timeout`.
            RenderError: the browser is not installed, could not start or crashed.
            CrawlerClosedError: the renderer is closed.
        """
        try:
            rendered, elapsed = await self._render(url, document)
        except (FetchTimeoutError, RenderError):
            self._failed += 1
            raise
        self._rendered += 1
        self._render_time += elapsed
        return rendered

    async def _render(self, url: str, document: Response) -> tuple[Rendered, float]:
        """Render the page as `render` says; with the seconds it took once the browser was ready for it."""
        async with self._pages:
            context, sync = await self._get_context(url, document.proxy)
            started = time.monotonic()
            from playwright.async_api import Error as PlaywrightError
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError

            tab = _Tab(document)
            page: Page | None = None
            try:
                if sync is not None:
                    await self._send_cookies(context, sync)
                page = await context.new_page()
                self._tabs[page] = tab
                page.on("crash", lambda _: setattr(tab, "crashed", True))
                page.on("popup", self._close_popup)
                rendered = await self._load(page, url, tab)
                return rendered, time.monotonic() - started
            except PlaywrightTimeoutError as exc:
                raise FetchTimeoutError(url, f"rendering timeout ({self.rendering.timeout:.1f}s)") from exc
            except PlaywrightError as exc:
                raise self._error(url, tab, exc) from exc
            finally:
                if page is not None:
                    self._tabs.pop(page, None)
                    with contextlib.suppress(Exception):
                        await page.close()
                # The cookies a page set before it failed are kept as well. A browser
                # that crashed meanwhile has failed the page already.
                with contextlib.suppress(Exception):
                    await (context.close() if sync is None else self._take_cookies(context, sync))

    async def _send_cookies(self, context: "BrowserContext", sync: CookieSync) -> None:
        """Give the context the cookies the crawler has changed since the last time.

        What the context has changed meanwhile, by the scripts of a page
        still open, goes to the crawler first: when both changed a cookie,
        that of the browser wins.
        """
        assert self._cookies is not None
        async with sync.lock:
            changed, removed = sync.to_browser(self._cookies.cookies())
            if not changed and not removed:
                return  # what the browser changed is taken when its page ends
            if await self._collect_cookies(context, sync):
                changed, removed = sync.to_browser(self._cookies.cookies())
                if not changed and not removed:
                    return  # the browser had changed them too, and won
            if changed:
                await _add_cookies(context, changed)
            for cookie in removed:
                await context.clear_cookies(name=cookie.name, domain=cookie.domain, path=cookie.path)
            sync.sent([*changed, *removed], map(from_playwright, await context.cookies()))
            logger.debug("Gave the browser %d cookies, took %d away", len(changed), len(removed))

    async def _take_cookies(self, context: "BrowserContext", sync: CookieSync) -> None:
        """Give the crawler the cookies the context has changed since the last time."""
        async with sync.lock:
            await self._collect_cookies(context, sync)

    async def _collect_cookies(self, context: "BrowserContext", sync: CookieSync) -> bool:
        """As `_take_cookies`, with `sync.lock` held by the caller; whether the browser had changed any."""
        assert self._cookies is not None
        changed, removed = sync.to_jar(map(from_playwright, await context.cookies()))
        if not changed and not removed:
            return False
        self._cookies.update_cookies(changed, removed)
        sync.written([*changed, *removed], self._cookies.cookies())
        logger.debug("Took %d cookies from the browser, %d removed", len(changed), len(removed))
        return True

    async def _load(self, page: "Page", url: str, tab: _Tab) -> Rendered:
        """Load the page and wait for it, unless it goes to another URL meanwhile."""
        from playwright.async_api import Error

        load = asyncio.ensure_future(self._wait(page, url))
        await asyncio.wait({load, tab.navigated}, return_when=asyncio.FIRST_COMPLETED)
        if tab.navigated.done():
            load.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await load
            return Rendered("", tab.navigated.result())
        load.result()
        try:
            content = await page.content()
        except Error:
            # The page started to go elsewhere while its HTML was read.
            if tab.navigated.done():
                return Rendered("", tab.navigated.result())
            raise
        return Rendered(content, tab.navigated.result() if tab.navigated.done() else None)

    async def _wait(self, page: "Page", url: str) -> None:
        """Open the page and wait for it as `rendering` says, all within its timeout."""
        deadline = time.monotonic() + self.rendering.timeout

        def remaining() -> float:
            # In milliseconds; 0 would mean no timeout to Playwright.
            return max((deadline - time.monotonic()) * 1000, 1.0)

        await page.goto(url, wait_until=self.rendering.wait_until, timeout=remaining())  # type: ignore[arg-type]
        if self.rendering.wait_for is not None:
            await page.wait_for_selector(self.rendering.wait_for, state="attached", timeout=remaining())

    async def _route(self, route: "Route", request: "Request") -> None:
        """Decide what becomes of a request of the browser."""
        from playwright.async_api import Error

        with contextlib.suppress(Error):  # the page may be closed meanwhile
            try:
                frame = request.frame
            except Error:
                # A request of a service worker: they are blocked, it is not expected.
                await route.abort()
                return
            tab = self._tabs.get(frame.page)
            if request.is_navigation_request():
                if tab is None or frame.parent_frame is not None:
                    # A pop-up or a frame: what it shows is not in the HTML of the page.
                    await route.abort()
                elif not tab.served:
                    # The first navigation of a tab is the page itself, already downloaded.
                    tab.served = True
                    await route.fulfill(
                        status=tab.document.status,
                        headers={"Content-Type": "text/html; charset=utf-8"},
                        body=tab.document.content,
                    )
                else:
                    if not tab.navigated.done():
                        tab.navigated.set_result(request.url)
                    await route.abort()
                return
            if tab is None or request.resource_type in self.rendering.block_resources:
                await route.abort()
                return
            await route.continue_()

    async def _close_popup(self, popup: "Page") -> None:
        with contextlib.suppress(Exception):
            await popup.close()

    def _error(self, url: str, tab: _Tab | None, exc: Exception) -> Exception:
        """The error to fail `url` with after a failure of the browser."""
        if self._closed:
            return CrawlerClosedError(url, "crawler is closed")
        if tab is not None and tab.crashed:
            return RenderError(url, "the page crashed in the browser")
        if not self.running:
            return RenderError(url, "the browser crashed")
        return RenderError(url, f"browser: {_first_line(exc)}")

    async def _get_context(self, url: str, proxy: Proxy | None) -> tuple["BrowserContext", CookieSync | None]:
        """The context to render a page through `proxy` in, and what keeps its cookies in step.

        The browser is started now if it is not running. Without cookies,
        it is a context of the page's own, with no `CookieSync`, to close
        after the page.
        """
        async with self._lock:
            if self._closed:
                raise CrawlerClosedError(url, "crawler is closed")
            if not self.running:
                await self._start(url)
            browser = self._browser
            assert browser is not None
            from playwright.async_api import Error

            try:
                if self._cookies is None:
                    return await self._new_context(browser, proxy), None
                key = None if proxy is None else proxy.label
                if key not in self._contexts:
                    self._contexts[key] = await self._new_context(browser, proxy), CookieSync()
                return self._contexts[key]
            except Error as exc:
                raise self._error(url, None, exc) from exc

    async def _start(self, url: str) -> None:
        """Start the browser, for the first time or after it crashed, unless it may not be any more."""
        if self._failure is None and self._launches == self.MAX_LAUNCHES:
            self._failure = f"the browser crashed {self._launches} times; pages are not rendered any more"
        if self._failure is not None:
            raise RenderError(url, self._failure)
        await self._shutdown()  # what is left of a crashed browser
        self._launches += 1
        try:
            await self._launch()
        except _LaunchError as error:
            self._failure = str(error)
            await self._shutdown()
            raise RenderError(url, self._failure) from error.__cause__

    async def _launch(self) -> None:
        try:
            from playwright.async_api import Error, async_playwright
        except ImportError as exc:
            raise _LaunchError(_NO_PLAYWRIGHT) from exc
        try:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch()
        except Error as exc:
            if "Executable doesn't exist" in str(exc):
                raise _LaunchError(_NO_CHROMIUM) from exc
            raise _LaunchError(f"the browser could not start: {_first_line(exc)}") from exc
        self._browser.on("disconnected", self._on_disconnected)
        logger.info("Started Chromium %s to render pages", self._browser.version)

    async def _new_context(self, browser: "Browser", proxy: Proxy | None) -> "BrowserContext":
        """A context of `browser` whose requests are those of the crawler, go through `proxy` and through `_route`."""
        settings: dict[str, Any] = {}
        if proxy is not None:
            settings["proxy"] = {"server": proxy.url}
            if proxy.credentials is not None:
                # Given to the browser as they are, sent when the proxy asks for them (HTTP 407).
                settings["proxy"]["username"], settings["proxy"]["password"] = proxy.credentials
            if self._bypass is not None:
                settings["proxy"]["bypass"] = self._bypass
        # Service workers would take requests past the routing of the context.
        context = await browser.new_context(
            user_agent=self._user_agent, extra_http_headers=self._headers, service_workers="block", **settings
        )
        try:
            await context.route("**/*", self._route)
        except BaseException:
            with contextlib.suppress(Exception):
                await context.close()
            raise
        if proxy is not None:
            logger.debug("Opened a browser context for proxy %s", proxy.label)
        return context

    def _on_disconnected(self, browser: "Browser") -> None:
        if self._closed or browser is not self._browser:
            return
        if self._launches < self.MAX_LAUNCHES:
            logger.error("The browser crashed; it is started again for the next page")
        else:
            logger.error("The browser crashed again; pages are not rendered any more")

    async def close(self) -> None:
        """Close the browser. Safe to call more than once; later pages fail with `CrawlerClosedError`."""
        self._closed = True
        async with self._lock:  # a launch in progress ends first
            running = self._playwright is not None
            await self._shutdown()
        if running:
            logger.debug("Browser closed")

    async def _shutdown(self) -> None:
        """Close the contexts, the browser and Playwright, whatever state they are in."""
        for close in (
            *(context.close for context, _ in self._contexts.values()),
            self._browser.close if self._browser is not None else None,
            self._playwright.stop if self._playwright is not None else None,
        ):
            if close is not None:
                with contextlib.suppress(Exception):
                    await close()
        self._browser = self._playwright = None
        self._contexts.clear()


def _first_line(exc: BaseException) -> str:
    """The message of a Playwright error without the log of the call that follows it."""
    return str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
