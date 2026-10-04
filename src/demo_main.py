"""Command-line demo of the crawler.

Usage:
    python src/demo_main.py benchmark [options] [URL ...]   # sequential vs concurrent fetching
    python src/demo_main.py parse [options] [URL ...]       # fetch pages and extract data
    python src/demo_main.py crawl [options] [URL ...]       # follow links from start pages
    python src/demo_main.py errors [options] [URL ...]      # crawl a local site that fails on purpose
    python src/demo_main.py save [options] [URL ...]        # save crawled pages to JSON, CSV and a database
    python src/demo_main.py scale [options] [PAGES ...]     # synchronous vs asynchronous crawl of 100, 500, 1000 pages
"""

import argparse
import asyncio
import contextlib
import dataclasses
import json
import os
import sys
import textwrap
import threading
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

import yaml

from cli_options import (
    at_least_one,
    database_url,
    encoding,
    hide_password,
    http_url,
    positive,
    regex,
    share,
)
from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    CircuitState,
    CompositeStorage,
    CrawlStats,
    CSVStorage,
    DatabaseStorage,
    DataStorage,
    FetchError,
    FetchResult,
    HTMLParser,
    HTTPStatusError,
    JSONStorage,
    PageRecord,
    ParsedPage,
    RetryStrategy,
    StorageError,
    get_host,
    is_same_host,
    is_valid_http_url,
    product_token,
    storage_from_url,
)
from crawler.logging_setup import configure_logging
from crawler.progress import show_progress
from crawler.storage import DATABASE_URL_VARIABLE, DEFAULT_DATABASE_URL
from demo_scale import Comparison, compare
from demo_site import DemoSite

# The longest pause between retries; a longer --retry-delay would not be doubled.
MAX_RETRY_DELAY = 30.0
# The URLs a command uses when none are given, a list per command. Found by
# the location of this file, so the working directory does not matter.
DEMO_URLS_FILE = Path(__file__).with_name("demo_urls.yaml")


def default_urls(command: str) -> list[str]:
    """The URLs `command` uses when none are given: its list in the file next to this script.

    Raises ValueError if the file cannot be read or the list is missing, empty
    or has anything but absolute http(s) URLs.
    """
    try:
        lists = yaml.safe_load(DEMO_URLS_FILE.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as error:
        raise ValueError(f"cannot read the default URLs from {DEMO_URLS_FILE}: {error}") from None
    urls = lists.get(command) if isinstance(lists, dict) else None
    if (
        not isinstance(urls, list)
        or not urls
        or not all(isinstance(url, str) and is_valid_http_url(url) for url in urls)
    ):
        raise ValueError(f"{DEMO_URLS_FILE}: `{command}` must be a non-empty list of absolute http(s) URLs")
    return urls


def add_common_options(
    parser: argparse.ArgumentParser,
    *,
    retries: int = 2,
    retry_delay: float = 1.0,
    log_level: str = "INFO",
    robots: bool = True,
) -> None:
    """Options every command has; the defaults that differ by command are arguments.

    With `robots=False`, the command does not check robots.txt unless given
    `--robots`, in place of `--no-robots`.

    Each command gets its own copy: a parent parser shared through `parents=`
    shares its option objects too, so `set_defaults` on one command would
    change the default of all of them.
    """
    parser.add_argument("--concurrency", type=positive(int), default=10, help="max parallel requests")
    timeouts = parser.add_argument_group("timeouts")
    timeouts.add_argument(
        "--connect-timeout",
        type=positive(float),
        default=5.0,
        metavar="S",
        help="DNS, TCP and TLS, s (default: %(default)g)",
    )
    timeouts.add_argument(
        "--read-timeout",
        type=positive(float),
        default=5.0,
        metavar="S",
        help="each chunk of the response, s (default: %(default)g)",
    )
    timeouts.add_argument(
        "--total-timeout",
        type=positive(float),
        default=10.0,
        metavar="S",
        help="whole request, s (default: %(default)g)",
    )
    timeouts.add_argument(
        "--timeout-growth",
        type=at_least_one,
        default=1.5,
        metavar="X",
        help=f"multiply the timeouts by X on every retry, at most {AsyncCrawler.MAX_TIMEOUT_GROWTH:g}x (default: %(default)g)",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=log_level,
    )
    politeness = parser.add_argument_group("politeness")
    politeness.add_argument(
        "--rps",
        type=positive(float, allow_zero=True),
        default=1.0,
        help="max requests per second to one host, 0 = no limit (default: %(default)g)",
    )
    politeness.add_argument(
        "--min-delay", type=positive(float, allow_zero=True), default=0.0, help="min seconds between requests to a host"
    )
    politeness.add_argument(
        "--jitter", type=positive(float, allow_zero=True), default=0.0, help="random extra delay up to this, s"
    )
    if robots:
        politeness.add_argument("--no-robots", action="store_true", help="do not check robots.txt")
    else:
        politeness.add_argument(
            "--robots", action="store_false", dest="no_robots", help="check robots.txt; for real URLs, add --rps 1"
        )
    politeness.add_argument(
        "--retries",
        type=positive(int, allow_zero=True),
        default=retries,
        help="retries of timeouts, network errors, HTTP 408, 429, 500, 502-504 and 520-524",
    )
    politeness.add_argument(
        "--retry-delay",
        type=positive(float),
        default=retry_delay,
        metavar="S",
        help=f"seconds before the first retry, doubled for every next one up to {MAX_RETRY_DELAY:g} (default: %(default)g)",
    )
    breaker = parser.add_argument_group("circuit breaker")
    breaker.add_argument(
        "--breaker-threshold",
        type=share,
        default=0.5,
        metavar="SHARE",
        help="block a host once this share of its requests in the last minute (5 at least) "
        "failed with a timeout, a network error, HTTP 408, 429 or 5xx (default: %(default)g)",
    )
    breaker.add_argument(
        "--breaker-cooldown",
        type=positive(float, allow_zero=True),
        default=30.0,
        metavar="S",
        help="how long a blocked host is left alone before a probe request, s (default: %(default)g)",
    )
    breaker.add_argument("--no-breaker", action="store_true", help="never block a host")
    politeness.add_argument(
        "--user-agent",
        action="append",
        metavar="STRING",
        help="User-Agent; repeat to rotate several, all with the same bot name",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    benchmark = commands.add_parser("benchmark", help="fetch URLs sequentially and concurrently, compare time")
    benchmark.add_argument(
        "urls", nargs="*", help=f"URLs to fetch (default: the `benchmark` list of {DEMO_URLS_FILE.name})"
    )
    # Retries would blur the comparison: the list fails on purpose.
    add_common_options(benchmark, retries=0)

    parse = commands.add_parser("parse", help="fetch pages and extract structured data")
    parse.add_argument("urls", nargs="*", help=f"URLs to parse (default: the `parse` list of {DEMO_URLS_FILE.name})")
    parse.add_argument("--same-host", action="store_true", help="keep only links to the page's own host")
    parse.add_argument("--preview", type=positive(int), default=5, help="links and headings shown per page")
    parse.add_argument("--json", type=Path, metavar="PATH", help="save full results to a JSON file")
    add_common_options(parse)

    crawl = commands.add_parser("crawl", help="follow links from start pages, show live progress")
    crawl.add_argument(
        "urls",
        nargs="*",
        type=http_url,
        help=f"start URLs (default: the `crawl` list of {DEMO_URLS_FILE.name})",
    )
    crawl.add_argument("--max-depth", type=positive(int, allow_zero=True), default=2, help="0 = start pages only")
    crawl.add_argument("--max-pages", type=positive(int), default=30, help="pages to fetch, failed ones included")
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
    add_common_options(crawl, log_level="WARNING")

    errors = commands.add_parser(
        "errors", help="crawl a local site that fails in every way, show retries and error statistics"
    )
    errors.add_argument(
        "urls",
        nargs="*",
        type=http_url,
        help="real URLs to fetch along with the local site; ones on localhost or 127.0.0.1 "
        "share the circuit breaker with the site's hosts",
    )
    errors.add_argument(
        "--json",
        type=Path,
        default=Path("error_report.json"),
        metavar="PATH",
        help="where to save the error report (default: %(default)s)",
    )
    # Fast retries, a short read timeout and a short cooldown of the breaker
    # (the crawl waits for the probes of the server that is down) keep the
    # demo within seconds; the site is local, so no rate limit or robots.txt.
    add_common_options(errors, retries=3, retry_delay=0.2, robots=False)
    errors.set_defaults(read_timeout=1.0, rps=0.0, breaker_cooldown=1.0)

    save = commands.add_parser(
        "save", help="crawl a local site, save its pages to JSON, CSV and a database, read them back"
    )
    save.add_argument("urls", nargs="*", type=http_url, help="real URLs to fetch and save along with the local site")
    storages = save.add_argument_group("storages")
    storages.add_argument(
        "--json",
        type=Path,
        default=Path("pages.jsonl"),
        metavar="PATH",
        help="JSON file, a record per line (default: %(default)s)",
    )
    storages.add_argument(
        "--indent",
        type=positive(int, allow_zero=True),
        metavar="N",
        help="write --json as one JSON array indented by N spaces instead",
    )
    storages.add_argument(
        "--csv", type=Path, default=Path("pages.csv"), metavar="PATH", help="CSV file (default: %(default)s)"
    )
    storages.add_argument(
        "--csv-encoding",
        type=encoding,
        default="utf-8",
        metavar="NAME",
        help="encoding of --csv (default: %(default)s)",
    )
    storages.add_argument(
        "--database-url",
        type=database_url,
        # Checked like a value given on the command line.
        default=os.environ.get(DATABASE_URL_VARIABLE) or DEFAULT_DATABASE_URL,
        metavar="URL",
        help="sqlite:///path or postgresql://user:password@host:port/database "
        f"(default: ${DATABASE_URL_VARIABLE}, or {DEFAULT_DATABASE_URL})",
    )
    storages.add_argument(
        "--batch-size",
        type=positive(int),
        default=10,
        metavar="N",
        help="pages written to a storage at once (default: %(default)s)",
    )
    storages.add_argument(
        "--append",
        action="store_true",
        help="add to the files of an earlier run instead of replacing them; the database is never emptied",
    )
    save.add_argument(
        "--preview", type=positive(int), default=3, help="records read back from each storage (default: %(default)s)"
    )
    # The site is the one of `errors`, and so are the defaults.
    add_common_options(save, retries=3, retry_delay=0.2, robots=False)
    save.set_defaults(read_timeout=1.0, rps=0.0, breaker_cooldown=1.0)

    scale = commands.add_parser(
        "scale",
        help="crawl local sites of growing size one request at a time and concurrently, compare time and memory",
    )
    scale.add_argument(
        "pages",
        nargs="*",
        type=positive(int),
        default=[100, 500, 1000],
        help="sizes of the sites (default: 100 500 1000)",
    )
    scale.add_argument(
        "--delay",
        type=positive(float, allow_zero=True),
        default=0.05,
        metavar="S",
        help="how long the site takes to answer a request, s (default: %(default)g)",
    )
    scale.add_argument(
        "--concurrency", type=positive(int), default=20, help="parallel requests of the asynchronous crawler"
    )
    scale.add_argument("--no-memory", action="store_true", help="skip the runs that measure peak memory")
    scale.add_argument("--json", type=Path, metavar="PATH", help="save the results to a JSON file")
    # A log line per page would cost time that is not the crawler's.
    scale.add_argument("--log-level", type=str.upper, choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="WARNING")

    args = parser.parse_args(argv)
    if args.command == "scale":
        return args
    # The other commands crawl a local site.
    if not args.urls and args.command in ("benchmark", "parse", "crawl"):
        try:
            args.urls = default_urls(args.command)
        except ValueError as error:
            parser.error(str(error))
    if args.retry_delay > MAX_RETRY_DELAY:
        parser.error(f"--retry-delay must be at most {MAX_RETRY_DELAY:g}, got {args.retry_delay:g}")
    if args.user_agent and len({product_token(agent) for agent in args.user_agent}) > 1:
        parser.error("every --user-agent must start with the same bot name, e.g. MyBot/1.0 (...)")
    if args.command == "save" and args.json.resolve() == args.csv.resolve():
        parser.error(f"--json and --csv must be different files, got {args.json} for both")
    return args


def make_crawler(args: argparse.Namespace, parser: HTMLParser | None = None, **options: Any) -> AsyncCrawler:
    """A crawler configured by the command-line options; `options` adds crawl limits."""
    if args.user_agent:
        # The first one names the bot for robots.txt; all of them rotate.
        options |= {"user_agent": args.user_agent[0], "user_agents": args.user_agent}
    return AsyncCrawler(
        max_concurrent=args.concurrency,
        total_timeout=args.total_timeout,
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        timeout_growth=args.timeout_growth,
        requests_per_second=args.rps or None,
        min_delay=args.min_delay,
        jitter=args.jitter,
        respect_robots=not args.no_robots,
        retry_strategy=RetryStrategy(max_retries=args.retries, base_delay=args.retry_delay, max_delay=MAX_RETRY_DELAY),
        circuit_breaker=CircuitBreaker(
            None if args.no_breaker else args.breaker_threshold, cooldown=args.breaker_cooldown
        ),
        parser=parser,
        **options,
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
    args: argparse.Namespace,
) -> tuple[list[FetchResult], float]:
    # A fresh crawler per run, so the second run does not benefit from
    # connections, DNS entries and robots.txt already cached by the first one.
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
    async with make_crawler(args, parser) as crawler:
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


def print_crawl_report(crawler: AsyncCrawler) -> None:
    stats = crawler.crawl_stats()
    print(f"\n=== Crawl ({len(crawler.visited_urls)} pages, {stats.elapsed:.2f}s) ===")
    print(f"{'DEPTH':>5}  {'RESULT':<36}  {'LINKS':>5}  URL")
    # url_depths keeps the order in which pages were found: breadth-first.
    for url, depth in crawler.url_depths.items():
        if url in crawler.processed_urls:
            page = crawler.processed_urls[url]
            outcome = f"ok, {len(page['errors'])} warning(s)" if page["errors"] else "ok"
            print(f"{depth:>5}  {outcome:<36}  {len(page['links']):>5}  {url}")
            continue
        if url in crawler.failed_urls:
            reason = crawler.failed_urls[url]
        elif url in crawler.skipped_urls:
            reason = f"skipped, {crawler.skipped_urls[url]}"
        elif url in crawler.blocked_urls:
            reason = f"blocked, {crawler.blocked_urls[url]}"
        elif url in crawler.unreachable_urls:
            reason = crawler.unreachable_urls[url]
        else:
            continue  # still in the queue
        outcome = textwrap.shorten(reason, width=36, placeholder="...")
        print(f"{depth:>5}  {outcome:<36}  {'':>5}  {url}")
    print(
        f"Crawled: {stats.processed} pages, failed: {stats.failed}, skipped: {stats.skipped}, "
        f"blocked: {stats.blocked}, unreachable: {stats.unreachable}, left in queue: {stats.queued}, speed: {stats.pages_per_second:.1f} pages/s"
    )


def host_stats(crawler: AsyncCrawler) -> list[dict[str, object]]:
    """Requests, enforced interval, average gap, blocked and unreachable pages per host, busiest first."""
    blocked = Counter(get_host(url) for url in crawler.blocked_urls)
    unreachable = Counter(get_host(url) for url in crawler.unreachable_urls)
    domains = crawler.rate_limiter.get_stats().domains
    return [
        {
            "host": host,
            "requests": rate.requests,
            "interval": round(rate.interval, 3),
            "avg_gap": None if rate.avg_gap is None else round(rate.avg_gap, 3),
            "blocked": blocked[host],
            "unreachable": unreachable[host],
        }
        for host, rate in sorted(domains.items(), key=lambda item: -item[1].requests)
    ]


def print_politeness_report(crawler: AsyncCrawler) -> None:
    stats = crawler.crawl_stats()
    rows = host_stats(crawler)
    host_width = max([len("HOST")] + [len(str(row["host"])) for row in rows])
    print(f"\n=== Requests by host ({stats.requests} requests, {stats.requests_per_second:.2f} req/s) ===")
    print(
        f"{'HOST':<{host_width}}  {'REQUESTS':>8}  {'INTERVAL':>8}  {'AVG GAP':>8}  {'BLOCKED':>7}  {'UNREACHABLE':>11}"
    )
    for row in rows:
        avg_gap = "-" if row["avg_gap"] is None else f"{row['avg_gap']:.2f}s"
        print(
            f"{row['host']:<{host_width}}  {row['requests']:>8}  {row['interval']:>7.2f}s  "
            f"{avg_gap:>8}  {row['blocked']:>7}  {row['unreachable']:>11}"
        )
    print(
        f"Average gap between requests to a host: {stats.avg_delay:.2f}s, "
        f"average wait for the rate limit: {stats.avg_wait:.2f}s, "
        f"retries (robots.txt included): {stats.retries}, blocked by robots.txt: {stats.blocked}, "
        f"not fetched as robots.txt was unreachable: {stats.unreachable}"
    )


def error_report(crawler: AsyncCrawler) -> dict[str, object]:
    """Error statistics and the circuit breaker of every host, for JSON output."""
    errors = crawler.error_stats()
    return {
        "errors": {
            "total": errors.total,
            "by_kind": dict(errors.by_kind),
            "by_class": dict(errors.by_class),
            "retries": errors.retries,
            "successful_retries": errors.successful_retries,
            "avg_retry_time": round(errors.avg_retry_time, 3),
            "permanent_errors": dict(errors.permanent_errors),
        },
        "circuit_breaker": {
            host: dataclasses.asdict(circuit) for host, circuit in crawler.circuit_breaker.get_stats().items()
        },
    }


def format_counts(counts: Mapping[str, int]) -> str:
    return ", ".join(f"{name} {count}" for name, count in counts.items()) or "none"


def print_error_report(crawler: AsyncCrawler) -> None:
    errors = crawler.error_stats()
    print(f"\n=== Errors ({errors.total} failed attempts) ===")
    print(f"By kind:  {format_counts(errors.by_kind)}")
    by_class = dict(sorted(errors.by_class.items(), key=lambda item: -item[1]))
    print(f"By class: {format_counts(by_class)}")
    print(
        f"Retries: {errors.retries}, pages recovered by a retry: {errors.successful_retries}, "
        f"average time per retry: {errors.avg_retry_time:.2f}s"
    )
    if errors.permanent_errors:
        print(f"Permanent errors ({len(errors.permanent_errors)}):")
        for url, error in errors.permanent_errors.items():
            print(f"  {url}  {error}")

    breaker = crawler.circuit_breaker
    if not breaker.enabled:
        print("\nCircuit breaker: off")
        return
    circuits = breaker.get_stats()
    host_width = max([len("HOST")] + [len(host) for host in circuits])
    states = Counter(circuit.state for circuit in circuits.values())
    print(
        f"\n=== Circuit breaker ({len(circuits)} hosts: {states[CircuitState.OPEN]} open, "
        f"{states[CircuitState.HALF_OPEN]} half-open) ==="
    )
    print(f"{'HOST':<{host_width}}  {'STATE':<9}  {'REQUESTS':>8}  {'FAILURES':>8}  {'OPENED':>6}  {'REJECTED':>8}")
    for host, circuit in circuits.items():
        print(
            f"{host:<{host_width}}  {circuit.state:<9}  {circuit.requests:>8}  {circuit.failures:>8}  "
            f"{circuit.times_opened:>6}  {circuit.rejected:>8}"
        )


def save_crawl_json(path: Path, crawler: AsyncCrawler) -> None:
    stats = crawler.crawl_stats()
    depths = crawler.url_depths
    report = {
        "stats": {
            "processed": stats.processed,
            "failed": stats.failed,
            "skipped": stats.skipped,
            "blocked": stats.blocked,
            "unreachable": stats.unreachable,
            "queued": stats.queued,
            "elapsed": round(stats.elapsed, 3),
            "pages_per_second": round(stats.pages_per_second, 2),
            "requests": stats.requests,
            "requests_per_second": round(stats.requests_per_second, 2),
            "retries": stats.retries,
            "avg_delay": round(stats.avg_delay, 3),
            "avg_wait": round(stats.avg_wait, 3),
        },
        "hosts": host_stats(crawler),
        **error_report(crawler),
        "pages": [{"depth": depths[url], **page} for url, page in crawler.processed_urls.items()],
        "failed": [{"url": url, "depth": depths[url], "error": error} for url, error in crawler.failed_urls.items()],
        "skipped": [
            {"url": url, "depth": depths[url], "reason": reason} for url, reason in crawler.skipped_urls.items()
        ],
        "blocked": [
            {"url": url, "depth": depths[url], "reason": reason} for url, reason in crawler.blocked_urls.items()
        ],
        "unreachable": [
            {"url": url, "depth": depths[url], "reason": reason} for url, reason in crawler.unreachable_urls.items()
        ],
    }
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nFull results saved to {path}")


async def run_crawl(args: argparse.Namespace) -> None:
    crawler = make_crawler(args, max_depth=args.max_depth, max_per_domain=args.per_domain)
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
        await show_progress(crawler, crawl_task, args.max_pages)
        await crawl_task

    print_crawl_report(crawler)
    print_politeness_report(crawler)
    print_error_report(crawler)
    if args.json is not None:
        save_crawl_json(args.json, crawler)


def save_error_report(path: Path, crawler: AsyncCrawler) -> None:
    report = {
        **error_report(crawler),
        "failed": [{"url": url, "error": error} for url, error in crawler.failed_urls.items()],
        "skipped": [{"url": url, "reason": reason} for url, reason in crawler.skipped_urls.items()],
        "fetched": list(crawler.processed_urls),
    }
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nError report saved to {path}")


async def run_errors(args: argparse.Namespace) -> None:
    async with DemoSite(extra_links=args.urls) as site:
        print(f"Crawling {site.url}: ordinary pages, HTTP 503, 429, 500, 404 and 403, a slow page,", file=sys.stderr)
        print("an empty page, a JSON file, a server that is down and a domain that does not exist\n", file=sys.stderr)
        # Depth 1: the start page and its links, none of theirs. Two requests
        # to a host at a time keep the pages in the order of the links.
        async with make_crawler(args, max_depth=1, max_per_domain=2) as crawler:
            await crawler.crawl([site.url], max_pages=len(site.links()) + 1)

    print_crawl_report(crawler)
    print_error_report(crawler)
    save_error_report(args.json, crawler)


def open_storages(args: argparse.Namespace) -> dict[str, DataStorage]:
    """The storages of the `save` command by the name shown for each; nothing is opened yet."""
    return {
        str(args.json): JSONStorage(args.json, indent=args.indent, batch_size=args.batch_size),
        str(args.csv): CSVStorage(args.csv, encoding=args.csv_encoding, batch_size=args.batch_size),
        hide_password(args.database_url): storage_from_url(args.database_url, batch_size=args.batch_size),
    }


async def count_records(storage: DataStorage) -> tuple[int, dict[int, int]]:
    """The number of records in a storage, in all and by HTTP status."""
    if isinstance(storage, DatabaseStorage):
        # The database counts them itself.
        return await storage.count(), await storage.status_counts()
    statuses: Counter[int] = Counter()
    async with contextlib.aclosing(storage.read()) as records:
        async for record in records:
            statuses[record["status_code"]] += 1
    return statuses.total(), dict(sorted(statuses.items()))


def format_size(size: int) -> str:
    for unit in ("B", "KB", "MB"):
        if size < 1024 or unit == "MB":
            return f"{size} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


async def print_storage_report(storages: Mapping[str, DataStorage], stats: CrawlStats) -> None:
    print(f"\n=== Saved pages (this crawl: {stats.saved} saved, {stats.save_failed} not saved) ===")
    name_width = max(len(type(storage).__name__) for storage in storages.values())
    print(f"{'STORAGE':<{name_width}}  {'RECORDS':>7}  {'SIZE':>9}  {'BY STATUS':<16}  LOCATION")
    earlier_runs = False
    for location, storage in storages.items():
        name = type(storage).__name__
        try:
            records, statuses = await count_records(storage)
        except (StorageError, *storage.WRITE_ERRORS) as error:
            # A file that cannot be read, a database that is down.
            print(f"{name:<{name_width}}  cannot be read: {type(error).__name__}: {error}")
            continue
        # A database on a server has no file to measure.
        path = getattr(storage, "path", None)
        size = format_size(path.stat().st_size) if path is not None and path.exists() else "-"
        by_status = ", ".join(f"{status}: {pages}" for status, pages in statuses.items()) or "-"
        print(f"{name:<{name_width}}  {records:>7}  {size:>9}  {by_status:<16}  {location}")
        earlier_runs = earlier_runs or records > stats.saved + stats.save_failed
    if earlier_runs:
        print("A storage with more records than this crawl had pages also keeps those of earlier runs.")


def format_record(record: PageRecord) -> str:
    return (
        f"{record['crawled_at']:%Y-%m-%d %H:%M:%S}  {record['status_code']:>6}  "
        f"{record['content_type']:<9}  {len(record['text']):>6}  {len(record['links']):>5}  "
        f"{record['metadata'].get('depth', ''):>5}  {record['url']}  {record['title']!r}"
    )


async def print_saved_records(location: str, storage: DataStorage, urls: list[str]) -> None:
    """Read records back from a storage and show them: as many as there are `urls`.

    A file is read from its start. A database finds the pages by URL, so it
    shows those of this crawl, not the oldest it keeps.
    """
    by_url = isinstance(storage, DatabaseStorage)
    print(f"\n=== {'Pages found by URL' if by_url else 'First records'} in {location} ({type(storage).__name__}) ===")
    print(f"{'CRAWLED AT (UTC)':<19}  {'STATUS':>6}  {'TYPE':<9}  {'TEXT':>6}  {'LINKS':>5}  {'DEPTH':>5}  URL  TITLE")
    try:
        if by_url:
            for url in urls:
                record = await storage.get(url)
                print(f"not saved: {url}" if record is None else format_record(record))
            return
        # Closed explicitly: the reader stops before the storage runs out of records.
        async with contextlib.aclosing(storage.read()) as records:
            shown = 0
            async for record in records:
                if shown >= len(urls):
                    break
                print(format_record(record))
                shown += 1
    except (StorageError, *storage.WRITE_ERRORS) as error:
        print(f"cannot be read: {type(error).__name__}: {error}")


async def run_save(args: argparse.Namespace) -> None:
    if not args.append:
        for path in (args.json, args.csv):
            path.unlink(missing_ok=True)
    storage = CompositeStorage(*open_storages(args).values())
    async with DemoSite(extra_links=args.urls) as site:
        print(f"Crawling {site.url} and saving its pages to JSON, CSV and a database\n", file=sys.stderr)
        # As in `errors`: the start page and its links.
        async with make_crawler(args, max_depth=1, max_per_domain=2, storage=storage) as crawler:
            try:
                await crawler.crawl([site.url], max_pages=len(site.links()) + 1)
            except StorageError as error:
                # Found out before the first request: nothing was crawled, so there is nothing to report.
                print(f"error: {error}", file=sys.stderr)
                return

    print_crawl_report(crawler)
    # The crawler has closed its storages: the pages are read from new ones.
    storages = open_storages(args)
    try:
        await print_storage_report(storages, crawler.crawl_stats())
        for location, saved in storages.items():
            await print_saved_records(location, saved, list(crawler.processed_urls)[: args.preview])
    finally:
        for saved in storages.values():
            await saved.close()


def scale_row(comparison: Comparison) -> str:
    def memory(size: int | None) -> str:
        return "-" if size is None else format_size(size)

    sync, concurrent = comparison.sync, comparison.concurrent
    return (
        f"{comparison.pages:>5}  {sync.elapsed:>8.2f}s  {sync.pages_per_second:>12.1f}  "
        f"{concurrent.elapsed:>9.2f}s  {concurrent.pages_per_second:>13.1f}  {comparison.speedup:>6.1f}x  "
        f"{memory(sync.peak_memory):>11}  {memory(concurrent.peak_memory):>12}  {memory(comparison.lean_memory):>14}"
    )


def save_scale_json(path: Path, args: argparse.Namespace, results: list[Comparison]) -> None:
    report = {
        "delay": args.delay,
        "concurrency": args.concurrency,
        "results": [
            {**dataclasses.asdict(comparison), "speedup": round(comparison.speedup, 2)} for comparison in results
        ],
    }
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nResults saved to {path}")


async def run_scale(args: argparse.Namespace) -> None:
    print(
        f"\n=== Scale: one request at a time vs {args.concurrency} at once "
        f"(the site answers in {args.delay * 1000:g} ms) ==="
    )
    print(
        f"{'PAGES':>5}  {'SYNC TIME':>9}  {'SYNC PAGES/S':>12}  {'ASYNC TIME':>10}  {'ASYNC PAGES/S':>13}  "
        f"{'SPEEDUP':>7}  {'SYNC MEMORY':>11}  {'ASYNC MEMORY':>12}  {'PAGES NOT KEPT':>14}"
    )
    results = []
    for pages in args.pages:
        print(f"Crawling a site of {pages} pages with both crawlers...", file=sys.stderr)
        # In a thread: the synchronous crawler blocks, and the asynchronous
        # one is given an event loop of its own, as in a program that runs it.
        stop = threading.Event()
        try:
            comparison = await asyncio.to_thread(
                compare, pages, args.delay, args.concurrency, memory=not args.no_memory, stop=stop
            )
        except asyncio.CancelledError:
            # Ctrl-C cancels the wait, not the thread, and the program
            # would not exit until the crawls in it are over.
            stop.set()
            raise
        results.append(comparison)
        print(scale_row(comparison), flush=True)
        for name, run in (("synchronous", comparison.sync), ("asynchronous", comparison.concurrent)):
            if run.pages != pages:
                print(f"  the {name} crawler fetched {run.pages} of {pages} pages, {run.failed} failed")
    if not args.no_memory:
        print(
            "Memory is the peak of what Python allocated during a crawl; "
            "PAGES NOT KEPT is the asynchronous crawler with keep_pages=False."
        )
    if args.json is not None:
        save_scale_json(args.json, args, results)


async def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    commands = {
        "benchmark": run_benchmark,
        "parse": run_parse,
        "crawl": run_crawl,
        "errors": run_errors,
        "save": run_save,
        "scale": run_scale,
    }
    await commands[args.command](args)


if __name__ == "__main__":
    asyncio.run(main())
