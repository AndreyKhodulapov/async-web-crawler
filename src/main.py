"""Demo: fetch a set of URLs sequentially and concurrently, then compare.

Usage:
    python src/main.py [--concurrency N] [--timeout SECONDS] [URL ...]
"""

import argparse
import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from crawler import AsyncCrawler, FetchResult


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
    # A mix of fast pages, slow endpoints and deliberate failures.
    default_urls = [
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
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("urls", nargs="*", default=default_urls, help="URLs to fetch")
    parser.add_argument("--concurrency", type=positive(int), default=10, help="max parallel requests")
    parser.add_argument(
        "--timeout",
        type=positive(float),
        default=5.0,
        help="connect, read and total timeout per request, s",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser.parse_args()


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
    async with AsyncCrawler(
        max_concurrent=concurrency,
        total_timeout=timeout,
        connect_timeout=timeout,
        read_timeout=timeout,
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
        print(f"{result.url:<{url_width}}  {status:<22}  {result.size:>8}B  {result.elapsed:>5.2f}s")
    succeeded = sum(result.ok for result in results)
    print(f"Succeeded: {succeeded}/{len(results)}, total time: {total:.2f}s")


async def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    urls = list(args.urls)

    sequential, sequential_time = await timed(run_sequential, urls, args.concurrency, args.timeout)
    concurrent, concurrent_time = await timed(AsyncCrawler.fetch_many, urls, args.concurrency, args.timeout)

    print_report("Sequential", sequential, sequential_time)
    print_report(f"Concurrent (max_concurrent={args.concurrency})", concurrent, concurrent_time)
    print(f"\nSpeedup: {sequential_time / concurrent_time:.1f}x")


if __name__ == "__main__":
    asyncio.run(main())
