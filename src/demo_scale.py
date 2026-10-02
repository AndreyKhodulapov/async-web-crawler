"""Measures how the crawler scales, for the `scale` demo: a site of any size, a synchronous crawler to compare with.

`ScaleSite` serves a site of N pages that answers every request after a
fixed delay, as a remote server would. `SyncCrawler` crawls it the plain
way, one request after another; `AsyncCrawler` crawls the same pages
concurrently. `measure` times both and takes their peak memory.
"""

import asyncio
import threading
import time
import tracemalloc
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Self

from aiohttp import web

from crawler import AsyncCrawler, CircuitBreaker, HTMLParser, ParsedPage, ParseError, RetryStrategy
from crawler.urls import is_same_host, normalize_url

# Links of a page to the pages below it: the site is a tree this wide.
FANOUT = 10
PARAGRAPHS = 12
PARAGRAPH = (
    "A crawler spends most of its time waiting for servers to answer, which is why "
    "fetching many pages at once pays off so much more than fetching them faster. "
)


class ScaleSite:
    """A site of `pages` pages on 127.0.0.1 at a free port, served while in `with`.

    Page 0 is the start page; page N links to pages N * FANOUT + 1 and on,
    so every page is reached from the start and the depth grows as the
    logarithm of the size. Every response takes `delay` seconds.

    The server has a thread and an event loop of its own: a synchronous
    client blocks the thread it runs in, and a server in the same thread
    could never answer it.
    """

    def __init__(self, pages: int, delay: float = 0.0) -> None:
        if pages < 1:
            raise ValueError(f"pages must be >= 1, got {pages}")
        if delay < 0:
            raise ValueError(f"delay must be >= 0, got {delay}")
        self.pages = pages
        self.delay = delay
        self.requests = 0
        self._port = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop: asyncio.Event | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}/"

    def page_url(self, number: int) -> str:
        return self.url if number == 0 else f"{self.url}pages/{number}"

    def urls(self) -> set[str]:
        """The URL of every page of the site."""
        return {self.page_url(number) for number in range(self.pages)}

    def __enter__(self) -> Self:
        started = threading.Event()
        failure: list[BaseException] = []
        self._thread = threading.Thread(target=self._run, args=(started, failure), name="scale-site", daemon=True)
        self._thread.start()
        started.wait()
        if failure:
            self._thread.join()
            raise failure[0]
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._loop is not None and self._stop is not None:
            self._loop.call_soon_threadsafe(self._stop.set)
        if self._thread is not None:
            self._thread.join()

    def _run(self, started: threading.Event, failure: list[BaseException]) -> None:
        try:
            asyncio.run(self._serve(started))
        except Exception as error:  # noqa: BLE001 - handed to the thread that waits in __enter__
            failure.append(error)
        finally:
            started.set()

    async def _serve(self, started: threading.Event) -> None:
        app = web.Application()
        app.router.add_get("/", self._page)
        app.router.add_get("/pages/{number:\\d+}", self._page)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            await web.TCPSite(runner, "127.0.0.1", 0).start()
            self._port = runner.addresses[0][1]
            self._loop = asyncio.get_running_loop()
            self._stop = asyncio.Event()
            started.set()
            await self._stop.wait()
        finally:
            await runner.cleanup()

    async def _page(self, request: web.Request) -> web.Response:
        self.requests += 1
        number = int(request.match_info.get("number", 0))
        if number >= self.pages:
            raise web.HTTPNotFound()
        await asyncio.sleep(self.delay)
        return web.Response(text=self._html(number), content_type="text/html")

    def _html(self, number: int) -> str:
        children = range(number * FANOUT + 1, min(number * FANOUT + FANOUT, self.pages - 1) + 1)
        links = "".join(f'<li><a href="/pages/{child}">Page {child}</a></li>' for child in children)
        text = "".join(f"<p>{PARAGRAPH * 3}</p>" for _ in range(PARAGRAPHS))
        return (
            f'<!doctype html><html lang="en"><head><title>Page {number}</title>'
            f'<meta name="description" content="Page {number} of {self.pages}"></head>'
            f'<body><main><h1>Page {number}</h1>{text}<img src="/images/{number}.png" alt="Figure {number}">'
            f"<h2>Next pages</h2><ul>{links}</ul></main></body></html>"
        )


class SyncCrawler:
    """A crawler that fetches one page at a time, with the standard library alone.

    The baseline `AsyncCrawler` is compared with: the same parser, the same
    normalized URLs, the same breadth-first order and limits, and nothing
    else, no rate limit, robots.txt or retries. Links are followed on the
    hosts of the start URLs only.
    """

    def __init__(self, *, max_depth: int = 2, timeout: float = 10.0, parser: HTMLParser | None = None) -> None:
        self.max_depth = max_depth
        self.timeout = timeout
        self.processed_urls: dict[str, ParsedPage] = {}
        self.failed_urls: dict[str, str] = {}
        self._parser = parser or HTMLParser()

    def crawl(self, start_urls: list[str], max_pages: int = 100) -> dict[str, ParsedPage]:
        """Crawl from `start_urls`; return the parsed pages by normalized URL.

        `max_pages` counts the pages requested, failed ones included, as
        `AsyncCrawler.crawl` does. Failed pages are listed in `failed_urls`.
        """
        self.processed_urls, self.failed_urls = {}, {}
        starts = [url for url in map(normalize_url, start_urls) if url is not None]
        queue = deque((url, 0) for url in dict.fromkeys(starts))
        seen = set(starts)
        requested = 0
        while queue and requested < max_pages:
            url, depth = queue.popleft()
            requested += 1
            try:
                page = self._fetch_and_parse(url)
            except (OSError, ParseError) as error:  # URLError and HTTPError are OSError
                self.failed_urls[url] = f"{type(error).__name__}: {error}"
                continue
            self.processed_urls[url] = page
            if depth >= self.max_depth:
                continue
            for link in page["links"]:
                normalized = normalize_url(link)
                if normalized is None or normalized in seen or not any(is_same_host(normalized, s) for s in starts):
                    continue
                seen.add(normalized)
                queue.append((normalized, depth + 1))
        return self.processed_urls

    def _fetch_and_parse(self, url: str) -> ParsedPage:
        request = urllib.request.Request(url, headers={"User-Agent": AsyncCrawler.DEFAULT_USER_AGENT})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            body = response.read()
            content_type = response.headers.get_content_type()
            charset = response.headers.get_content_charset() or "utf-8"
            final_url = response.geturl()
        return self._parser.parse(
            body.decode(charset, errors="replace"), url, final_url=final_url, content_type=content_type
        )


@dataclass(frozen=True)
class Run:
    """One crawl of a site: what it fetched, how long it took, how much memory it held at its peak."""

    pages: int
    failed: int
    elapsed: float
    peak_memory: int | None  # bytes; None if memory was not measured

    @property
    def pages_per_second(self) -> float:
        return self.pages / self.elapsed if self.elapsed > 0 else 0.0


@dataclass(frozen=True)
class Comparison:
    """Both crawlers on a site of `pages` pages."""

    pages: int
    sync: Run
    concurrent: Run
    lean_memory: int | None = None  # peak of the concurrent crawl that does not keep its pages, bytes

    @property
    def speedup(self) -> float:
        return self.sync.elapsed / self.concurrent.elapsed if self.concurrent.elapsed > 0 else 0.0


def crawl_sync(site: ScaleSite) -> tuple[int, int]:
    """Crawl the whole site one page at a time; return the pages fetched and failed."""
    crawler = SyncCrawler(max_depth=site.pages)
    crawler.crawl([site.url], max_pages=site.pages)
    return len(crawler.processed_urls), len(crawler.failed_urls)


def crawl_async(site: ScaleSite, concurrency: int, *, keep_pages: bool = True) -> tuple[int, int]:
    """Crawl the whole site with `concurrency` requests in flight; return the pages fetched and failed."""

    async def crawl() -> tuple[int, int]:
        # As bare as the synchronous crawler: no rate limit, robots.txt,
        # retries or circuit breaker.
        async with AsyncCrawler(
            max_concurrent=concurrency,
            max_depth=site.pages,
            requests_per_second=None,
            respect_robots=False,
            retry_strategy=RetryStrategy(max_retries=0),
            circuit_breaker=CircuitBreaker(None),
            keep_pages=keep_pages,
        ) as crawler:
            await crawler.crawl([site.url], max_pages=site.pages, same_domain_only=True)
            return crawler.crawl_stats().processed, len(crawler.failed_urls)

    return asyncio.run(crawl())


def _timed(crawl: Callable[[], tuple[int, int]]) -> tuple[int, int, float]:
    started = time.perf_counter()
    pages, failed = crawl()
    return pages, failed, time.perf_counter() - started


def _peak_memory(crawl: Callable[[], tuple[int, int]]) -> int:
    """The most memory Python held at once during `crawl`, over what it held before, in bytes."""
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        crawl()
        return tracemalloc.get_traced_memory()[1] - before
    finally:
        tracemalloc.stop()


def measure(crawl: Callable[[ScaleSite], tuple[int, int]], pages: int, delay: float, *, memory: bool = True) -> Run:
    """Run `crawl` on a site of `pages` pages: once for the time, once more for the memory.

    Tracing allocations slows Python down several times, so the timed run
    goes without it. The memory run needs no delay: what a crawler holds
    does not depend on how long the server takes, and without the delay
    the second run is short.
    """
    with ScaleSite(pages, delay) as site:
        fetched, failed, elapsed = _timed(lambda: crawl(site))
    peak = None
    if memory:
        with ScaleSite(pages) as site:
            peak = _peak_memory(lambda: crawl(site))
    return Run(pages=fetched, failed=failed, elapsed=elapsed, peak_memory=peak)


def compare(pages: int, delay: float, concurrency: int, *, memory: bool = True) -> Comparison:
    """Crawl a site of `pages` pages with both crawlers.

    With `memory`, the concurrent crawler is also run with
    `keep_pages=False`, to show what holding the pages costs.
    """
    lean = None
    if memory:
        with ScaleSite(pages) as site:
            lean = _peak_memory(lambda: crawl_async(site, concurrency, keep_pages=False))
    return Comparison(
        pages=pages,
        sync=measure(crawl_sync, pages, delay, memory=memory),
        concurrent=measure(lambda site: crawl_async(site, concurrency), pages, delay, memory=memory),
        lean_memory=lean,
    )
