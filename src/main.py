"""Command-line interface of the crawler.

Usage:
    python src/main.py --config config.yaml
    python src/main.py --urls https://example.com --max-pages 100 --output results.json
    python src/main.py --config config.yaml --max-pages 500 --report report.html
    python src/main.py --config config.yaml --urls-file urls.txt
    cat urls.txt | python src/main.py --config config.yaml --urls-file -

A crawl is set up by a configuration file (see config.example.yaml), by
options, or by both: an option wins over the file. Logs and progress go to
stderr, the summary to stdout.

Exit codes: 0 - the crawl ran, fetched pages and saved every page it should,
1 - no page was fetched, some could not be saved, or a file or the database
could not be opened, 2 - wrong options or configuration, 130 - interrupted
(Ctrl-C); the pages fetched by then are saved and reported.
"""

import argparse
import asyncio
import sys
from typing import Any

from cli_options import http_url, positive
from crawler import AdvancedCrawler, ConfigError, CrawlerConfig, StorageError, load_config, load_urls, show_progress
from crawler.config import LOG_LEVELS
from crawler.urls import hide_password


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl websites: follow links from start URLs, save the pages, report statistics.",
        epilog="An option left out keeps the value of the configuration file, or the default without a file.",
    )
    parser.add_argument("--config", metavar="PATH", help="configuration file, YAML or JSON; see config.example.yaml")
    parser.add_argument(
        "--urls", nargs="+", type=http_url, metavar="URL", help="start URLs, in place of those of the configuration"
    )
    parser.add_argument(
        "--urls-file",
        metavar="PATH",
        help='start URLs from a text file, one per line, "#" for comments; "-" reads them from stdin. '
        "With --urls, both are crawled",
    )
    parser.add_argument("--max-pages", type=positive(int), metavar="N", help="pages to request, failed ones included")
    parser.add_argument(
        "--max-depth",
        type=positive(int, allow_zero=True),
        metavar="N",
        help="links followed from a start URL; 0 = start URLs only",
    )
    parser.add_argument(
        "--output",
        action="append",
        metavar="PATH",
        help="where to save the pages: a .jsonl, .json, .csv or .db file, or a database URL; "
        "repeat for several, in place of those of the configuration",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        help="start output files anew, or add to them; databases keep a row per URL either way",
    )
    parser.add_argument(
        "--cookies-file",
        metavar="PATH",
        help="send the cookies of a Netscape cookies.txt file, as a browser extension or curl -c exports it",
    )
    parser.add_argument(
        "--save-cookies", metavar="PATH", help="write the cookies to a cookies.txt file after the crawl"
    )
    parser.add_argument(
        "--respect-robots",
        action=argparse.BooleanOptionalAction,
        help="follow robots.txt, nofollow and noindex, or do not",
    )
    parser.add_argument(
        "--same-domain-only",
        action=argparse.BooleanOptionalAction,
        help="follow links on the hosts of the start URLs only, or on any host",
    )
    parser.add_argument(
        "--rate-limit",
        type=positive(float, allow_zero=True),
        metavar="RPS",
        help="max requests per second to one host; 0 = no limit",
    )
    reports = parser.add_argument_group("reports and log")
    reports.add_argument("--stats-json", metavar="PATH", help="write the statistics of the crawl to a JSON file")
    reports.add_argument("--report", metavar="PATH", help="write an HTML report with charts")
    reports.add_argument("--log-level", type=str.upper, choices=LOG_LEVELS, help="level of the log")
    reports.add_argument("--log-file", metavar="PATH", help="also write the log to a file, as JSON Lines")
    reports.add_argument("--no-progress", action="store_true", help="do not show the progress line")
    return parser.parse_args(argv)


def start_urls(args: argparse.Namespace) -> list[str] | None:
    """The URLs of --urls, then those of --urls-file, each once; None if neither is given.

    Raises:
        ConfigError: the file cannot be read, or lines of it are not URLs.
    """
    if args.urls_file is None:
        return args.urls
    return list(dict.fromkeys([*(args.urls or ()), *load_urls(args.urls_file)]))


def config_overrides(args: argparse.Namespace) -> dict[str, Any]:
    """The options that were given, shaped like the configuration file.

    Raises:
        ConfigError: the file of --urls-file cannot be read, or lines of it are not URLs.
    """
    # (section, key, value); a value of None is an option that was not given.
    options = [
        (None, "urls", start_urls(args)),
        ("crawler", "max_pages", args.max_pages),
        ("crawler", "max_depth", args.max_depth),
        ("crawler", "respect_robots", args.respect_robots),
        ("filters", "same_domain_only", args.same_domain_only),
        ("storage", "outputs", args.output),
        ("storage", "overwrite", args.overwrite),
        ("session", "cookies_file", args.cookies_file),
        ("session", "save_cookies", args.save_cookies),
        ("report", "stats_json", args.stats_json),
        ("report", "html", args.report),
        ("logging", "level", args.log_level),
        ("logging", "file", args.log_file),
    ]
    overrides: dict[str, Any] = {}
    for section, key, value in options:
        if value is not None:
            target = overrides if section is None else overrides.setdefault(section, {})
            target[key] = value
    if args.rate_limit is not None:
        # The configuration spells "no limit" as null.
        overrides.setdefault("crawler", {})["rate_limit"] = args.rate_limit or None
    return overrides


def build_config(args: argparse.Namespace) -> CrawlerConfig:
    """The configuration of the crawl: the file, if one is given, with the options over it.

    Raises:
        ConfigError: the file or an option is invalid, or there is nothing to crawl.
    """
    overrides = config_overrides(args)
    # The pages go to the storage and the counts to the statistics; nothing
    # here reads them from memory, so a large crawl need not hold them.
    overrides.setdefault("crawler", {})["keep_pages"] = False
    config = CrawlerConfig.from_dict(overrides) if args.config is None else load_config(args.config, overrides)
    if not config.urls and not config.sitemaps.urls:
        if args.urls_file is not None:
            # An empty list is not an error by itself: sitemaps may give the pages.
            name = "the standard input" if args.urls_file == "-" else args.urls_file
            replaced = "" if args.config is None else ", and it replaces `urls` of the configuration"
            raise ConfigError([f"nothing to crawl: {name} lists no URLs{replaced}"])
        where = (
            "--urls or --urls-file"
            if args.config is None
            else "--urls, --urls-file, or `urls` or `sitemaps.urls` in the configuration"
        )
        raise ConfigError([f"nothing to crawl: give {where}"])
    return config


def print_summary(crawler: AdvancedCrawler, *, interrupted: bool = False) -> None:
    stats = crawler.get_stats()
    state = "interrupted" if interrupted else "finished"
    print(f"\n=== Crawl {state} ({stats['elapsed_seconds']:.2f}s) ===")
    print(
        f"Pages: {stats['total_pages']} ({stats['successful']} successful, {stats['failed']} failed, "
        f"{stats['skipped']} skipped), {stats['pages_per_second']:.1f} pages/s, "
        f"average response time {stats['avg_response_time']:.2f}s"
    )
    for title, counts in (
        ("Status codes", stats["status_codes"]),
        ("Top domains", stats["top_domains"]),
        ("Errors", stats["errors"]),
    ):
        if counts:
            print(f"{title}: {', '.join(f'{name}: {pages}' for name, pages in counts.items())}")
    outputs = crawler.config.storage.outputs
    if outputs:
        saving = crawler.crawler.crawl_stats()
        not_saved = f", {saving.save_failed} not saved" if saving.save_failed else ""
        print(f"Saved: {saving.saved} pages to {', '.join(map(hide_password, outputs))}{not_saved}")
    if crawler.reports:
        print(f"Reports: {', '.join(map(str, crawler.reports))}")
    if crawler.cookie_file is not None:
        print(f"Cookies: {crawler.cookie_file}")
    if crawler.config.logging.file is not None:
        print(f"Log: {crawler.config.logging.file}")


async def run(config: CrawlerConfig, *, progress: bool = True) -> int:
    """Crawl by the configuration, print the summary; return the exit code.

    Cancelled (Ctrl-C), it stops the crawl, writes the reports of the pages
    fetched so far, the cookies and those pages before the cancellation goes on.
    So it does when the progress cannot be shown, e.g. stderr is a pipe
    that was closed.

    Raises:
        ConfigError: the file of `session.cookies_file` cannot be read.
        OSError: a directory cannot be created, or the log file cannot be opened.
        StorageError: an output file or the database cannot be opened; nothing is requested.
    """
    async with AdvancedCrawler(config) as crawler:
        crawl = asyncio.create_task(crawler.crawl())
        try:
            if progress:
                await show_progress(crawler.crawler, crawl, config.crawler.max_pages)
            await crawl
        except BaseException as error:
            # The crawl must stop before the crawler is closed under it.
            crawl.cancel()
            await asyncio.gather(crawl, return_exceptions=True)
            if not isinstance(error, StorageError):
                # A storage that could not be opened stopped the crawl before
                # it requested anything: there is nothing to report.
                crawler.write_reports()
                crawler.save_cookies()
            if isinstance(error, asyncio.CancelledError):
                await crawler.close()  # writes the pages the storage still holds, so the summary counts them
                print_summary(crawler, interrupted=True)
            raise
        print_summary(crawler)
        # A page that could not be saved is a failure of the run too.
        return 0 if crawler.get_stats()["successful"] and not crawler.crawler.crawl_stats().save_failed else 1


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = build_config(args)
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2  # as argparse exits for a wrong flag
    except KeyboardInterrupt:  # while the URLs are typed into stdin
        return 130
    try:
        return asyncio.run(run(config, progress=not args.no_progress))
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except (OSError, StorageError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130  # 128 + SIGINT, as a shell reports a program stopped by Ctrl-C


if __name__ == "__main__":
    sys.exit(main())
