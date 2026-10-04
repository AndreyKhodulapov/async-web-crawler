"""Rendering of pages in a headless browser: a transport that runs the JavaScript of HTML pages.

Playwright is an optional dependency (`pip install -e ".[js]"`, then
`playwright install chromium`): it is imported when the first page is
rendered, so the crawler works without it as long as nothing is.
"""

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from http.cookiejar import Cookie
from typing import TYPE_CHECKING, NamedTuple

import aiohttp

from crawler.exceptions import CrawlerClosedError, FetchTimeoutError, PageTooLargeError, RenderError
from crawler.filters import UrlFilter
from crawler.parser import is_html_content_type
from crawler.transport import Response, Transport

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserContext, Page, Playwright, Request, Route

logger = logging.getLogger(__name__)

INSTALL_PACKAGE = 'pip install -e ".[js]"'
INSTALL_BROWSER = "playwright install chromium"
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

    A rendered page keeps the status, the headers and the final URL of
    its download; its content is the HTML of the page once rendered,
    which fails with `PageTooLargeError` over `max_page_size` bytes. A
    page that takes the browser longer than `rendering.timeout` fails
    with `FetchTimeoutError`; a browser that is not installed, cannot
    start or crashes fails it with `RenderError` (see `Renderer`).
    """

    def __init__(self, http: Transport, rendering: Rendering, *, user_agent: str, max_page_size: int | None) -> None:
        self.http = http
        self.rendering = rendering
        self.renderer = Renderer(rendering, user_agent=user_agent)
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

    def cookies(self) -> list[Cookie]:
        return self.http.cookies()


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
    Its pages share one context (cookies, cache), with the `user_agent`
    of the crawler. Every request of a page goes through `_route`: the
    page gets its document as downloaded, its own navigations are
    stopped and reported, frames and pop-ups get nothing, and the
    `block_resources` are not requested.

    A browser that crashes fails the pages being rendered with
    `RenderError` and is started again for the next one, once: after
    that, and after a browser that could not start, every page fails
    with `RenderError` at once.
    """

    MAX_LAUNCHES = 2

    def __init__(self, rendering: Rendering, *, user_agent: str) -> None:
        self.rendering = rendering
        self._user_agent = user_agent
        self._pages = asyncio.Semaphore(rendering.max_open_pages)
        self._lock = asyncio.Lock()  # one launch at a time
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._tabs: dict[Page, _Tab] = {}
        self._launches = 0
        self._failure: str | None = None  # why the browser is not used any more
        self._closed = False

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
        async with self._pages:
            context = await self._get_context(url)
            from playwright.async_api import Error as PlaywrightError
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError

            tab = _Tab(document)
            page: Page | None = None
            try:
                page = await context.new_page()
                self._tabs[page] = tab
                page.on("crash", lambda _: setattr(tab, "crashed", True))
                page.on("popup", self._close_popup)
                return await self._load(page, url, tab)
            except PlaywrightTimeoutError as exc:
                raise FetchTimeoutError(url, f"rendering timeout ({self.rendering.timeout:.1f}s)") from exc
            except PlaywrightError as exc:
                raise self._error(url, tab, exc) from exc
            finally:
                if page is not None:
                    self._tabs.pop(page, None)
                    with contextlib.suppress(Exception):
                        await page.close()

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

    def _error(self, url: str, tab: _Tab, exc: Exception) -> Exception:
        """The error to fail `url` with after a failure of the browser."""
        if self._closed:
            return CrawlerClosedError(url, "crawler is closed")
        if tab.crashed:
            return RenderError(url, "the page crashed in the browser")
        if not self.running:
            return RenderError(url, "the browser crashed")
        return RenderError(url, f"browser: {_first_line(exc)}")

    async def _get_context(self, url: str) -> "BrowserContext":
        """The context of the running browser, started now if there is none."""
        async with self._lock:
            if self._closed:
                raise CrawlerClosedError(url, "crawler is closed")
            if self._context is not None and self.running:
                return self._context
            if self._failure is None and self._launches == self.MAX_LAUNCHES:
                self._failure = f"the browser crashed {self._launches} times; pages are not rendered any more"
            if self._failure is not None:
                raise RenderError(url, self._failure)
            await self._shutdown()  # what is left of a crashed browser
            self._launches += 1
            try:
                return await self._launch()
            except _LaunchError as error:
                self._failure = str(error)
                await self._shutdown()
                raise RenderError(url, self._failure) from error.__cause__

    async def _launch(self) -> "BrowserContext":
        try:
            from playwright.async_api import Error, async_playwright
        except ImportError as exc:
            raise _LaunchError(f"Playwright is not installed; run: {INSTALL_PACKAGE}") from exc
        try:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch()
            self._browser.on("disconnected", self._on_disconnected)
            # Service workers would take requests past the routing of the context.
            context = await self._browser.new_context(user_agent=self._user_agent, service_workers="block")
            await context.route("**/*", self._route)
        except Error as exc:
            if "Executable doesn't exist" in str(exc):
                raise _LaunchError(f"Chromium is not installed; run: {INSTALL_BROWSER}") from exc
            raise _LaunchError(f"the browser could not start: {_first_line(exc)}") from exc
        self._context = context
        logger.info("Started Chromium %s to render pages", self._browser.version)
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
        """Close the context, the browser and Playwright, whatever state they are in."""
        for close in (
            self._context.close if self._context is not None else None,
            self._browser.close if self._browser is not None else None,
            self._playwright.stop if self._playwright is not None else None,
        ):
            if close is not None:
                with contextlib.suppress(Exception):
                    await close()
        self._context = self._browser = self._playwright = None


def _first_line(exc: BaseException) -> str:
    """The message of a Playwright error without the log of the call that follows it."""
    return str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
