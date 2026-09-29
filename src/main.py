"""Command-line demo of the crawler.

Usage:
    python src/main.py benchmark [options] [URL ...]   # sequential vs concurrent fetching
    python src/main.py parse [options] [URL ...]       # fetch pages and extract data
"""

import argparse
import asyncio
import json
import logging
import textwrap
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from crawler import AsyncCrawler, FetchError, FetchResult, HTMLParser, HTTPStatusError, ParsedPage
from crawler.urls import is_same_host

# A mix of fast pages, slow endpoints and deliberate failures.
BENCHMARK_URLS = [
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
PARSE_URLS = [
    "https://en.wikipedia.org/wiki/Main_Page",  # large server-rendered page
    "https://apilearn.tukas.dev/",  # scraping sandbox; answers HTTP 429 to bursts
    "https://apilearn.tukas.dev/exercises/",  # same site, a page with tables
    "https://stepik.org/",  # JavaScript app: the HTML is only a shell
    "https://www.ozon.ru/",  # anti-bot protection: HTTP 403
]


def positive(number_type: type[int] | type[float]) -> Callable[[str], int | float]:
    def parse(raw: str) -> int | float:
        try:
            value = number_type(raw)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
        if value <= 0:
            raise argparse.ArgumentTypeError(f"must be positive, got {raw}")
        return value

    return parse


def parse_args() -> argparse.Namespace:
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
    benchmark.add_argument("urls", nargs="*", default=BENCHMARK_URLS, help="URLs to fetch")

    parse = commands.add_parser("parse", parents=[common], help="fetch pages and extract structured data")
    parse.add_argument("urls", nargs="*", default=PARSE_URLS, help="URLs to parse")
    parse.add_argument("--same-host", action="store_true", help="keep only links to the page's own host")
    parse.add_argument("--preview", type=positive(int), default=5, help="links and headings shown per page")
    parse.add_argument("--json", type=Path, metavar="PATH", help="save full results to a JSON file")
    return parser.parse_args()


def make_crawler(args: argparse.Namespace, parser: HTMLParser | None = None) -> AsyncCrawler:
    return AsyncCrawler(
        max_concurrent=args.concurrency,
        total_timeout=args.timeout,
        connect_timeout=args.timeout,
        read_timeout=args.timeout,
        parser=parser,
    )


def describe_error(error: FetchError) -> str:
    name = type(error).__name__
    return f"{name} {error.status}" if isinstance(error, HTTPStatusError) else name


# --- benchmark -------------------------------------------------------------


async def run_sequential(crawler: AsyncCrawler, urls: list[str]) -> list[FetchResult]:
    return [await crawler.fetch_result(url) for url in urls]


async def timed(
    runner: Callable[[AsyncCrawler, list[str]], Awaitable[list[FetchResult]]],
    args: argparse.Namespace,
) -> tuple[list[FetchResult], float]:
    # A fresh crawler per run, so the second run does not benefit from
    # connections and DNS entries already cached by the first one.
    async with make_crawler(args) as crawler:
        started = time.perf_counter()
        results = await runner(crawler, args.urls)
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
    sequential, sequential_time = await timed(run_sequential, args)
    concurrent, concurrent_time = await timed(AsyncCrawler.fetch_many, args)

    print_benchmark_report("Sequential", sequential, sequential_time)
    print_benchmark_report(f"Concurrent (max_concurrent={args.concurrency})", concurrent, concurrent_time)
    print(f"\nSpeedup: {sequential_time / concurrent_time:.1f}x")


# --- parse -----------------------------------------------------------------


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
        result = f"ok, {len(outcome['errors'])} warning(s)" if outcome["errors"] else "ok"
        counts = (
            len(outcome["text"]),
            len(outcome["links"]),
            len(outcome["images"]),
            len(outcome["headings"]),
            len(outcome["tables"]),
            len(outcome["lists"]),
        )
        print(f"{url:<{url_width}}  {result:<22}" + "".join(f"  {count:>8}" for count in counts))

    pages = [outcome for outcome in outcomes if not isinstance(outcome, FetchError)]
    print(
        f"Parsed: {len(pages)}/{len(urls)} pages, "
        f"links: {sum(len(page['links']) for page in pages)}, "
        f"text: {sum(len(page['text']) for page in pages)} chars"
    )


def save_json(path: Path, urls: list[str], outcomes: list[ParsedPage | FetchError]) -> None:
    records = [
        {"url": url, "error": str(outcome)} if isinstance(outcome, FetchError) else outcome
        for url, outcome in zip(urls, outcomes, strict=True)
    ]
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nFull results saved to {path}")


async def run_parse(args: argparse.Namespace) -> None:
    urls = list(dict.fromkeys(args.urls))
    async with make_crawler(args, parser=HTMLParser(same_host_only=args.same_host)) as crawler:
        started = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(parse_one(crawler, url)) for url in urls]
        total = time.perf_counter() - started
    outcomes = [task.result() for task in tasks]

    for url, outcome in zip(urls, outcomes, strict=True):
        print(f"\n=== {url} ===")
        if isinstance(outcome, FetchError):
            summary: dict[str, object] = {"url": url, "error": f"{type(outcome).__name__}: {outcome.message}"}
        else:
            summary = summarize(outcome, args.preview)
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    print_parse_summary(urls, outcomes, total)
    if args.json is not None:
        save_json(args.json, urls, outcomes)


async def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    args.urls = list(args.urls)
    if args.command == "benchmark":
        await run_benchmark(args)
    else:
        await run_parse(args)


if __name__ == "__main__":
    asyncio.run(main())
