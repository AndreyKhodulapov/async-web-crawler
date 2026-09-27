"""Demo: fetch a set of URLs sequentially and concurrently, then compare.

Usage:
    python src/main.py [--concurrency N] [--timeout SECONDS] [URL ...]
"""

import argparse
import asyncio
import time
from collections.abc import Awaitable, Callable

from crawler import AsyncCrawler, FetchResult
from crawler.logging_config import setup_logging

# A mix of fast pages, slow endpoints and deliberate failures.
DEFAULT_URLS = [
    "https://example.com",
    "https://www.python.org",
    "https://docs.aiohttp.org/en/stable/",
    "https://httpbin.org/html",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
    "https://httpbin.org/status/500",
    "https://httpbin.org/delay/10",  # exceeds the default demo timeout
    "https://nonexistent-domain.invalid",
]


LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR"]


def positive(number_type: type[int] | type[float]) -> Callable[[str], int | float]:
    """Build an argparse type that accepts only numbers greater than zero."""

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
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("urls", nargs="*", default=DEFAULT_URLS, help="URLs to fetch")
    parser.add_argument(
        "--concurrency", type=positive(int), default=10, help="max parallel requests"
    )
    parser.add_argument(
        "--timeout",
        type=positive(float),
        default=5.0,
        help="total timeout per request, s",
    )
    parser.add_argument(
        "--log-level", type=str.upper, choices=LOG_LEVELS, default="INFO"
    )
    return parser.parse_args()


async def run_sequential(crawler: AsyncCrawler, urls: list[str]) -> list[FetchResult]:
    # Each request starts only after the previous one has finished.
    return [await crawler.fetch_result(url) for url in urls]


async def run_concurrent(crawler: AsyncCrawler, urls: list[str]) -> list[FetchResult]:
    return await crawler.fetch_many(urls)


Runner = Callable[[AsyncCrawler, list[str]], Awaitable[list[FetchResult]]]


async def timed(
    runner: Runner, urls: list[str], args: argparse.Namespace
) -> tuple[list[FetchResult], float]:
    # A fresh crawler per run, so the second run does not benefit from
    # connections and DNS entries already cached by the first one.
    async with AsyncCrawler(
        max_concurrent=args.concurrency, total_timeout=args.timeout
    ) as crawler:
        started = time.perf_counter()
        results = await runner(crawler, urls)
        return results, time.perf_counter() - started


def print_report(title: str, results: list[FetchResult], total: float) -> None:
    url_width = max(len(result.url) for result in results)
    print(f"\n=== {title} ===")
    print(f"{'URL':<{url_width}}  {'STATUS':<22}  {'SIZE':>9}  {'TIME':>6}")
    for result in results:
        if result.ok:
            status = str(result.status)
        else:
            status = type(result.error).__name__
            if result.status is not None:
                status += f" {result.status}"
        print(
            f"{result.url:<{url_width}}  {status:<22}  "
            f"{result.size:>8}B  {result.elapsed:>5.2f}s"
        )
    succeeded = sum(result.ok for result in results)
    print(f"Succeeded: {succeeded}/{len(results)}, total time: {total:.2f}s")


async def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    urls = list(args.urls)

    sequential, sequential_time = await timed(run_sequential, urls, args)
    concurrent, concurrent_time = await timed(run_concurrent, urls, args)

    print_report("Sequential", sequential, sequential_time)
    print_report(
        f"Concurrent (max_concurrent={args.concurrency})", concurrent, concurrent_time
    )
    print(f"\nSpeedup: {sequential_time / concurrent_time:.1f}x")


if __name__ == "__main__":
    asyncio.run(main())
