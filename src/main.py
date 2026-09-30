"""Command-line demo of the crawler.

Usage:
    python src/main.py benchmark [options] [URL ...]   # sequential vs concurrent fetching
    python src/main.py parse [options] [URL ...]       # fetch pages and extract data
    python src/main.py crawl [options] [URL ...]       # follow links from start pages
    python src/main.py errors [options] [URL ...]      # crawl a local site that fails on purpose
"""

import argparse
import asyncio
import dataclasses
import json
import logging
import math
import re
import sys
import textwrap
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    CircuitState,
    CrawlStats,
    FetchError,
    FetchResult,
    HTMLParser,
    HTTPStatusError,
    ParsedPage,
    RetryStrategy,
    get_host,
    is_same_host,
    is_valid_http_url,
    product_token,
)
from demo_site import DemoSite

# The longest pause between retries; a longer --retry-delay would not be doubled.
MAX_RETRY_DELAY = 30.0


def positive(number_type: type[int] | type[float], *, allow_zero: bool = False) -> Callable[[str], int | float]:
    def parse(raw: str) -> int | float:
        try:
            value = number_type(raw)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
        if not math.isfinite(value):
            raise argparse.ArgumentTypeError(f"must be a finite number, got {raw}")
        if value < 0 or (value == 0 and not allow_zero):
            raise argparse.ArgumentTypeError(f"must be {'non-negative' if allow_zero else 'positive'}, got {raw}")
        return value

    return parse


def at_least_one(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
    if not (math.isfinite(value) and value >= 1):
        raise argparse.ArgumentTypeError(f"must be >= 1, got {raw}")
    return value


def share(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
    if not 0 < value <= 1:
        raise argparse.ArgumentTypeError(f"must be in (0, 1], got {raw}")
    return value


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
        help="retries of timeouts, network errors, HTTP 408, 429 and 5xx",
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
        "failed with a timeout, a network error, HTTP 429 or 5xx (default: %(default)g)",
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
        "https://httpbin.org/delay/10",  # slower than the default --read-timeout
        "https://nonexistent-domain.invalid",
    ]
    # Real sites of different kinds.
    parse_urls = [
        "https://en.wikipedia.org/wiki/Main_Page",  # large server-rendered page
        "https://apilearn.tukas.dev/",  # scraping sandbox; answers HTTP 429 to bursts
        "https://apilearn.tukas.dev/exercises/",  # same site, a page with tables
        "https://httpbin.org/status/403",  # an access denied response, as anti-bot protection gives
    ]

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    benchmark = commands.add_parser("benchmark", help="fetch URLs sequentially and concurrently, compare time")
    benchmark.add_argument("urls", nargs="*", default=benchmark_urls, help="URLs to fetch")
    # Retries would blur the comparison: the list fails on purpose.
    add_common_options(benchmark, retries=0)

    parse = commands.add_parser("parse", help="fetch pages and extract structured data")
    parse.add_argument("urls", nargs="*", default=parse_urls, help="URLs to parse")
    parse.add_argument("--same-host", action="store_true", help="keep only links to the page's own host")
    parse.add_argument("--preview", type=positive(int), default=5, help="links and headings shown per page")
    parse.add_argument("--json", type=Path, metavar="PATH", help="save full results to a JSON file")
    add_common_options(parse)

    crawl = commands.add_parser("crawl", help="follow links from start pages, show live progress")
    # Sandboxes made for crawling practice whose robots.txt shows the rules
    # at work: the first disallows its pagination and product pages (with a
    # wildcard rule), the second sets Crawl-delay: 2.
    crawl_urls = ["https://webscraper.io/test-sites/pagination", "https://web-scraping.dev/products"]
    crawl.add_argument("urls", nargs="*", type=http_url, default=crawl_urls, help="start URLs")
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
    # Fast retries and a short read timeout keep the demo within seconds; the
    # site is local, so no rate limit or robots.txt.
    add_common_options(errors, retries=3, retry_delay=0.2, robots=False)
    errors.set_defaults(read_timeout=1.0, rps=0.0)

    args = parser.parse_args(argv)
    if args.retry_delay > MAX_RETRY_DELAY:
        parser.error(f"--retry-delay must be at most {MAX_RETRY_DELAY:g}, got {args.retry_delay:g}")
    if args.user_agent and len({product_token(agent) for agent in args.user_agent}) > 1:
        parser.error("every --user-agent must start with the same bot name, e.g. MyBot/1.0 (...)")
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


def format_progress(stats: CrawlStats) -> str:
    return (
        f"pages {stats.processed} | failed {stats.failed} | skipped {stats.skipped} | blocked {stats.blocked} | "
        f"unreachable {stats.unreachable} | queued {stats.queued} | in progress {stats.in_progress} | in flight {stats.active_requests} | "
        f"{stats.current_rps:.1f} req/s | gap {stats.avg_delay:.2f}s | {stats.elapsed:.1f}s"
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
        result = textwrap.shorten(reason, width=36, placeholder="...")
        print(f"{depth:>5}  {result:<36}  {'':>5}  {url}")
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
        await show_progress(crawler, crawl_task)
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
        "fetched": list(crawler.processed_urls),
    }
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nError report saved to {path}")


async def run_errors(args: argparse.Namespace) -> None:
    async with DemoSite(extra_links=args.urls) as site:
        print(f"Crawling {site.url}: ordinary pages, HTTP 503, 429, 500, 404 and 403, a slow page,", file=sys.stderr)
        print("a JSON file, a server that is down and a domain that does not exist\n", file=sys.stderr)
        # Depth 1: the start page and its links, none of theirs. Two requests
        # to a host at a time keep the pages in the order of the links.
        async with make_crawler(args, max_depth=1, max_per_domain=2) as crawler:
            await crawler.crawl([site.url], max_pages=len(site.links()) + 1)

    print_crawl_report(crawler)
    print_error_report(crawler)
    save_error_report(args.json, crawler)


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
    commands = {"benchmark": run_benchmark, "parse": run_parse, "crawl": run_crawl, "errors": run_errors}
    await commands[args.command](args)


if __name__ == "__main__":
    asyncio.run(main())
