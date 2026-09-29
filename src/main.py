"""Command-line demo of the crawler.

Usage:
    python src/main.py benchmark [options] [URL ...]   # sequential vs concurrent fetching
    python src/main.py parse [options] [URL ...]       # fetch pages and extract data
    python src/main.py crawl [options] [URL ...]       # follow links from start pages
"""

import argparse
import asyncio
import json
import logging
import re
import sys
import textwrap
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from crawler import (
    AsyncCrawler,
    CrawlStats,
    FetchError,
    FetchResult,
    HTMLParser,
    HTTPStatusError,
    ParsedPage,
    is_same_host,
    is_valid_http_url,
)


def positive(number_type: type[int] | type[float], *, allow_zero: bool = False) -> Callable[[str], int | float]:
    def parse(raw: str) -> int | float:
        try:
            value = number_type(raw)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
        if value < 0 or (value == 0 and not allow_zero):
            raise argparse.ArgumentTypeError(f"must be {'non-negative' if allow_zero else 'positive'}, got {raw}")
        return value

    return parse


def http_url(raw: str) -> str:
    if not is_valid_http_url(raw):
        raise argparse.ArgumentTypeError(f"not an absolute http(s) URL: {raw!r}")
    return raw


def regex(raw: str) -> str:
    try:
        re.compile(raw)
    except re.error as exc:
        raise argparse.ArgumentTypeError(f"invalid regular expression {raw!r}: {exc}") from None
    return raw


def parse_args() -> argparse.Namespace:
    # A mix of fast pages, slow endpoints and deliberate failures.
    benchmark_urls = [
        "https://example.com",
        "https://www.python.org",
        "https://docs.aiohttp.org/en/stable/",
        "https://httpbin.org/html",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/status/404",
        "https://httpbin.org/status/500",
        "https://httpbin.org/delay/10",  # slower than the default --timeout
        "https://nonexistent-domain.invalid",
    ]
    # Real sites of different kinds.
    parse_urls = [
        "https://en.wikipedia.org/wiki/Main_Page",  # large server-rendered page
        "https://apilearn.tukas.dev/",  # scraping sandbox; answers HTTP 429 to bursts
        "https://apilearn.tukas.dev/exercises/",  # same site, a page with tables
        "https://httpbin.org/status/403",  # an access denied response, as anti-bot protection gives
    ]

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--concurrency", type=positive(int), default=10, help="max parallel requests")
    common.add_argument(
        "--timeout",
        type=positive(float),
        default=5.0,
        help="connect, read and total timeout per request, s",
    )
    common.add_argument(
        "--log-level",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    benchmark = commands.add_parser(
        "benchmark", parents=[common], help="fetch URLs sequentially and concurrently, compare time"
    )
    benchmark.add_argument("urls", nargs="*", default=benchmark_urls, help="URLs to fetch")

    parse = commands.add_parser("parse", parents=[common], help="fetch pages and extract structured data")
    parse.add_argument("urls", nargs="*", default=parse_urls, help="URLs to parse")
    parse.add_argument("--same-host", action="store_true", help="keep only links to the page's own host")
    parse.add_argument("--preview", type=positive(int), default=5, help="links and headings shown per page")
    parse.add_argument("--json", type=Path, metavar="PATH", help="save full results to a JSON file")

    crawl = commands.add_parser("crawl", parents=[common], help="follow links from start pages, show live progress")
    # A sandbox made for crawling practice. robots.txt is not checked yet,
    # so the default must be a site that welcomes crawlers.
    crawl.add_argument("urls", nargs="*", type=http_url, default=["https://books.toscrape.com/"], help="start URLs")
    crawl.add_argument("--max-depth", type=positive(int, allow_zero=True), default=2, help="0 = start pages only")
    crawl.add_argument("--max-pages", type=positive(int), default=50, help="pages to fetch, failed ones included")
    crawl.add_argument("--per-domain", type=positive(int), default=2, help="max parallel requests to one host")
    crawl.add_argument("--same-domain", action="store_true", help="follow links on the start hosts only")
    crawl.add_argument(
        "--include", type=regex, action="append", default=[], metavar="REGEX", help="follow matching links only"
    )
    crawl.add_argument(
        "--exclude", type=regex, action="append", default=[], metavar="REGEX", help="skip matching links"
    )
    crawl.add_argument("--json", type=Path, metavar="PATH", help="save pages, errors and stats to a JSON file")
    # A log line per request would bury the progress line; --log-level INFO shows them.
    crawl.set_defaults(log_level="WARNING")
    return parser.parse_args()


def make_crawler(
    concurrency: int,
    timeout: float,
    parser: HTMLParser | None = None,
    *,
    max_depth: int = 2,
    max_per_domain: int | None = None,
) -> AsyncCrawler:
    return AsyncCrawler(
        max_concurrent=concurrency,
        max_depth=max_depth,
        max_per_domain=max_per_domain,
        total_timeout=timeout,
        connect_timeout=timeout,
        read_timeout=timeout,
        parser=parser,
    )


def describe_error(error: FetchError) -> str:
    """Short form for table cells: the error class and, for HTTP errors, the status."""
    name = type(error).__name__
    return f"{name} {error.status}" if isinstance(error, HTTPStatusError) else name


def error_record(url: str, error: FetchError) -> dict[str, object]:
    """Full form for JSON output, in place of a parsed page."""
    return {"url": url, "error": type(error).__name__, "message": error.message}


async def run_sequential(crawler: AsyncCrawler, urls: list[str]) -> list[FetchResult]:
    return [await crawler.fetch_result(url) for url in urls]


async def timed(
    runner: Callable[[AsyncCrawler, list[str]], Awaitable[list[FetchResult]]],
    urls: list[str],
    concurrency: int,
    timeout: float,
) -> tuple[list[FetchResult], float]:
    # A fresh crawler per run, so the second run does not benefit from
    # connections and DNS entries already cached by the first one.
    async with make_crawler(concurrency, timeout) as crawler:
        started = time.perf_counter()
        results = await runner(crawler, urls)
        return results, time.perf_counter() - started


def print_benchmark_report(title: str, results: list[FetchResult], total: float) -> None:
    url_width = max(len(result.url) for result in results)
    print(f"\n=== {title} ===")
    print(f"{'URL':<{url_width}}  {'STATUS':<22}  {'SIZE':>9}  {'TIME':>6}")
    for result in results:
        status = str(result.status) if result.error is None else describe_error(result.error)
        print(f"{result.url:<{url_width}}  {status:<22}  {result.size:>8}B  {result.elapsed:>5.2f}s")
    succeeded = sum(result.ok for result in results)
    print(f"Succeeded: {succeeded}/{len(results)}, total time: {total:.2f}s")


async def run_benchmark(args: argparse.Namespace) -> None:
    sequential, sequential_time = await timed(run_sequential, args.urls, args.concurrency, args.timeout)
    concurrent, concurrent_time = await timed(AsyncCrawler.fetch_many, args.urls, args.concurrency, args.timeout)

    print_benchmark_report("Sequential", sequential, sequential_time)
    print_benchmark_report(f"Concurrent (max_concurrent={args.concurrency})", concurrent, concurrent_time)
    print(f"\nSpeedup: {sequential_time / concurrent_time:.1f}x")


async def parse_one(crawler: AsyncCrawler, url: str) -> ParsedPage | FetchError:
    try:
        return await crawler.fetch_and_parse(url)
    except FetchError as error:
        return error


def summarize(page: ParsedPage, preview: int) -> dict[str, object]:
    """Condense a parsed page into the fields worth showing on screen."""
    links = page["links"]
    internal = sum(is_same_host(link, page["final_url"]) for link in links)
    summary: dict[str, object] = {"url": page["url"]}
    if page["final_url"] != page["url"]:
        summary["final_url"] = page["final_url"]
    summary |= {
        "title": page["title"],
        "description": page["metadata"]["description"],
        "text_length": len(page["text"]),
        "text_preview": textwrap.shorten(page["text"], width=120, placeholder="..."),
        "links_count": len(links),
        "internal_links": internal,
        "external_links": len(links) - internal,
        "links": links[:preview] + ([f"... and {len(links) - preview} more"] if len(links) > preview else []),
        "images_count": len(page["images"]),
        "headings": [f"h{heading['level']}: {heading['text']}" for heading in page["headings"][:preview]],
        "tables_count": len(page["tables"]),
        "lists_count": len(page["lists"]),
        "errors": page["errors"],
    }
    return summary


def print_parse_summary(urls: list[str], outcomes: list[ParsedPage | FetchError], total: float) -> None:
    url_width = max(len(url) for url in urls)
    columns = ("TEXT", "LINKS", "IMAGES", "HEADINGS", "TABLES", "LISTS")
    print(f"\n=== Summary ({len(urls)} pages, {total:.2f}s) ===")
    print(f"{'URL':<{url_width}}  {'RESULT':<22}" + "".join(f"  {column:>8}" for column in columns))
    for url, outcome in zip(urls, outcomes, strict=True):
        if isinstance(outcome, FetchError):
            print(f"{url:<{url_width}}  {describe_error(outcome)}")
            continue
        status = f"ok, {len(outcome['errors'])} warning(s)" if outcome["errors"] else "ok"
        counts = (
            len(outcome["text"]),
            len(outcome["links"]),
            len(outcome["images"]),
            len(outcome["headings"]),
            len(outcome["tables"]),
            len(outcome["lists"]),
        )
        print(f"{url:<{url_width}}  {status:<22}" + "".join(f"  {count:>8}" for count in counts))

    pages = [outcome for outcome in outcomes if not isinstance(outcome, FetchError)]
    print(
        f"Parsed: {len(pages)}/{len(urls)} pages, "
        f"links: {sum(len(page['links']) for page in pages)}, "
        f"text: {sum(len(page['text']) for page in pages)} chars"
    )


def save_json(path: Path, urls: list[str], outcomes: list[ParsedPage | FetchError]) -> None:
    records = [
        error_record(url, outcome) if isinstance(outcome, FetchError) else outcome
        for url, outcome in zip(urls, outcomes, strict=True)
    ]
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nFull results saved to {path}")


async def run_parse(args: argparse.Namespace) -> None:
    urls = list(dict.fromkeys(args.urls))
    parser = HTMLParser(same_host_only=args.same_host)
    async with make_crawler(args.concurrency, args.timeout, parser) as crawler:
        started = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(parse_one(crawler, url)) for url in urls]
        total = time.perf_counter() - started
    outcomes = [task.result() for task in tasks]

    for url, outcome in zip(urls, outcomes, strict=True):
        print(f"\n=== {url} ===")
        summary = error_record(url, outcome) if isinstance(outcome, FetchError) else summarize(outcome, args.preview)
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    print_parse_summary(urls, outcomes, total)
    if args.json is not None:
        save_json(args.json, urls, outcomes)


def format_progress(stats: CrawlStats) -> str:
    return (
        f"pages {stats.processed} | failed {stats.failed} | skipped {stats.skipped} | queued {stats.queued} | "
        f"in progress {stats.in_progress} | requests {stats.active_requests} | "
        f"{stats.pages_per_second:.1f} pages/s | {stats.elapsed:.1f}s"
    )


async def show_progress(crawler: AsyncCrawler, crawl_task: asyncio.Task[object], interval: float = 1.0) -> None:
    """Print crawl progress to stderr every `interval` seconds until the crawl ends."""
    # In a terminal the line is redrawn in place; in a file or pipe every
    # update goes on a line of its own.
    live = sys.stderr.isatty()
    while not crawl_task.done():
        await asyncio.wait({crawl_task}, timeout=interval)
        line = format_progress(crawler.crawl_stats())
        if live:
            print(f"\r\033[K{line}", end="", file=sys.stderr, flush=True)
        else:
            print(line, file=sys.stderr, flush=True)
    if live:
        print(file=sys.stderr)


def print_crawl_report(crawler: AsyncCrawler) -> None:
    stats = crawler.crawl_stats()
    print(f"\n=== Crawl ({len(crawler.visited_urls)} pages, {stats.elapsed:.2f}s) ===")
    print(f"{'DEPTH':>5}  {'RESULT':<36}  {'LINKS':>5}  URL")
    # url_depths keeps the order in which pages were found: breadth-first.
    for url, depth in crawler.url_depths.items():
        if url in crawler.processed_urls:
            page = crawler.processed_urls[url]
            result = f"ok, {len(page['errors'])} warning(s)" if page["errors"] else "ok"
            print(f"{depth:>5}  {result:<36}  {len(page['links']):>5}  {url}")
        elif url in crawler.failed_urls or url in crawler.skipped_urls:
            reason = crawler.failed_urls.get(url) or f"skipped, {crawler.skipped_urls[url]}"
            result = textwrap.shorten(reason, width=36, placeholder="...")
            print(f"{depth:>5}  {result:<36}  {'':>5}  {url}")
    print(
        f"Crawled: {stats.processed} pages, failed: {stats.failed}, skipped: {stats.skipped}, "
        f"left in queue: {stats.queued}, speed: {stats.pages_per_second:.1f} pages/s"
    )


def save_crawl_json(path: Path, crawler: AsyncCrawler) -> None:
    stats = crawler.crawl_stats()
    depths = crawler.url_depths
    report = {
        "stats": {
            "processed": stats.processed,
            "failed": stats.failed,
            "skipped": stats.skipped,
            "queued": stats.queued,
            "elapsed": round(stats.elapsed, 3),
            "pages_per_second": round(stats.pages_per_second, 2),
        },
        "pages": [{"depth": depths[url], **page} for url, page in crawler.processed_urls.items()],
        "failed": [{"url": url, "depth": depths[url], "error": error} for url, error in crawler.failed_urls.items()],
        "skipped": [
            {"url": url, "depth": depths[url], "reason": reason} for url, reason in crawler.skipped_urls.items()
        ],
    }
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nFull results saved to {path}")


async def run_crawl(args: argparse.Namespace) -> None:
    crawler = make_crawler(args.concurrency, args.timeout, max_depth=args.max_depth, max_per_domain=args.per_domain)
    async with crawler:
        crawl_task = asyncio.create_task(
            crawler.crawl(
                args.urls,
                max_pages=args.max_pages,
                same_domain_only=args.same_domain,
                include_patterns=args.include,
                exclude_patterns=args.exclude,
            )
        )
        await show_progress(crawler, crawl_task)
        await crawl_task

    print_crawl_report(crawler)
    if args.json is not None:
        save_crawl_json(args.json, crawler)


class ProgressAwareHandler(logging.StreamHandler):
    """Writes log records to stderr, erasing the live progress line first.

    The progress line ends with "\r" instead of a newline, so a record would
    otherwise be glued to its end. The next progress update redraws it below.
    """

    def __init__(self) -> None:
        super().__init__(sys.stderr)
        self._live = sys.stderr.isatty()

    def emit(self, record: logging.LogRecord) -> None:
        if self._live:
            # Like StreamHandler.emit: a failed write (closed terminal, broken
            # pipe) must not raise into the code that logged the record.
            try:
                self.stream.write("\r\033[K")
            except (OSError, ValueError):
                self.handleError(record)
                return
        super().emit(record)


async def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[ProgressAwareHandler()],
    )
    commands = {"benchmark": run_benchmark, "parse": run_parse, "crawl": run_crawl}
    await commands[args.command](args)


if __name__ == "__main__":
    asyncio.run(main())
