"""Command-line interface of the crawler.

Usage:
    python src/main.py --config config.yaml
    python src/main.py --urls https://example.com --max-pages 100 --output results.json
    python src/main.py --config config.yaml --max-pages 500 --report report.html
    python src/main.py --config config.yaml --urls-file urls.txt
    python src/main.py --urls https://quotes.toscrape.com/js/ --render
    cat urls.txt | python src/main.py --config config.yaml --urls-file -
    python src/main.py job create --config config.yaml --name books
    python src/main.py worker --job books --config worker.yaml
    python src/main.py report --job books --stats-json stats.json --report report.html
    python src/main.py status --job books --watch

A crawl is set up by a configuration file (see config.example.yaml), by
options, or by both: an option wins over the file. Logs and progress go to
stderr, the summary to stdout.

A crawl job is a crawl that workers share through PostgreSQL. `job create`
creates it by a configuration file, reads its sitemaps and queues its start
URLs; `worker`, started in as many processes or containers as wanted, crawls
its pages until none is left. `report` writes the statistics of the job, of
all its workers together, to a JSON file and an HTML report, at any time;
`status` prints a line of its progress, updated until it is finished with
`--watch`.
The database is `distributed.database_url` of the configuration, or the
CRAWLER_DATABASE_URL variable.

Exit codes: 0 - the crawl ran, fetched pages and saved every page it should;
a job was created; a worker crawled until no page of its job was left and
saved every page it should; the reports of a job were written; its
progress was shown, 1 - no page was fetched, some could not be
saved, a file or the database could not be opened or failed, or the crawl
job is not as the command expects (the name is taken, there is none), 2 -
wrong options or configuration, 130 - interrupted (Ctrl-C), 143 - stopped
with SIGTERM (docker stop, systemd); the pages fetched by then are saved and
reported, and those a worker had in progress are queued again.
"""

import argparse
import asyncio
import contextlib
import signal
import sys
from collections.abc import Coroutine, Iterator
from typing import Any

from cli_options import http_url, positive, proxy_url, worker_name
from crawler import (
    AdvancedCrawler,
    ConfigError,
    CrawlerConfig,
    FrontierError,
    JobError,
    StorageError,
    load_config,
    load_urls,
    show_progress,
)
from crawler.config import LOG_LEVELS
from crawler.distributed import (
    JobMode,
    create_job,
    export_job_stats,
    format_job_progress,
    job_progress,
    job_stats,
    run_worker,
    watch_job,
)
from crawler.rendering import browser_problem
from crawler.urls import hide_password

# The commands of crawl jobs; without one, the arguments are those of a crawl of its own.
COMMANDS = ("job", "worker", "report", "status")
# Seconds between the updates of `status --watch`.
WATCH_INTERVAL = 2.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl websites: follow links from start URLs, save the pages, report statistics.",
        epilog="An option left out keeps the value of the configuration file, or the default without a file. "
        "A crawl that workers share is run by the commands job create, worker, report and status: "
        "see job create --help, worker --help, report --help and status --help.",
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
        "--proxy",
        action="append",
        type=proxy_url,
        metavar="URL",
        help="send the requests through a proxy, http://[user:password@]host:port; "
        "repeat for several, in place of those of the configuration",
    )
    parser.add_argument(
        "--render",
        action="store_true",
        help="render every HTML page in a headless Chromium, so that links JavaScript makes are found; "
        "in place of rendering.mode and rendering.include of the configuration. Needs playwright install chromium",
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


def parse_command_args(argv: list[str]) -> argparse.Namespace:
    """The arguments of a command of crawl jobs, such as `job create`; `argv` starts with the command."""
    parser = argparse.ArgumentParser(
        description="Run a crawl that workers share through PostgreSQL.",
        epilog="The database is distributed.database_url of the configuration, or the CRAWLER_DATABASE_URL variable.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    job = commands.add_parser("job", help="crawl jobs")
    actions = job.add_subparsers(dest="action", required=True)
    create = actions.add_parser(
        "create",
        help="create a crawl job: check the configuration, read the sitemaps, queue the start URLs",
        epilog="The job keeps what and how to crawl: urls, sitemaps, crawler, retry, circuit_breaker, filters "
        "and rendering of the configuration. The database is distributed.database_url of the configuration, "
        "or the CRAWLER_DATABASE_URL variable.",
    )
    create.add_argument("--config", required=True, metavar="PATH", help="configuration file, YAML or JSON")
    create.add_argument("--name", required=True, metavar="NAME", help="name of the job, which the workers are given")
    mode = create.add_mutually_exclusive_group()
    mode.add_argument(
        "--resume",
        dest="mode",
        action="store_const",
        const=JobMode.RESUME,
        default=JobMode.NEW,
        help="go on with the job of that name: queue the start URLs it never queued; the configuration must not differ",
    )
    mode.add_argument(
        "--restart",
        dest="mode",
        action="store_const",
        const=JobMode.RESTART,
        help="delete the job of that name with its pages and create it anew",
    )
    worker = commands.add_parser(
        "worker",
        help="crawl the pages of a crawl job until none is left; start as many as wanted",
        epilog="The configuration of the job says what and how to crawl; that of the worker gives the rest: "
        "distributed, session, proxy, storage, logging, report and crawler.max_concurrent. Every file of the "
        "storage must have {worker} in its name, such as pages-{worker}.jsonl. The database is "
        "distributed.database_url of the configuration, or the CRAWLER_DATABASE_URL variable.",
    )
    worker.add_argument("--job", required=True, metavar="NAME", help="name of the crawl job")
    worker.add_argument("--config", metavar="PATH", help="configuration file of the worker, YAML or JSON")
    worker.add_argument(
        "--concurrency",
        type=positive(int),
        metavar="N",
        help="pages crawled at a time, in place of crawler.max_concurrent",
    )
    worker.add_argument(
        "--name",
        type=worker_name,
        metavar="WORKER",
        help="name of the worker in the database and for {worker} in file names; "
        "by default the host name, the process id and a random part",
    )
    report = commands.add_parser(
        "report",
        help="write the statistics of a crawl job, of all its workers, to a JSON file and an HTML report",
        epilog="Without --stats-json and --report, the files are report.stats_json and report.html of the "
        "configuration. The job may still be running: the reports show it as it is. The database is "
        "distributed.database_url of the configuration, or the CRAWLER_DATABASE_URL variable.",
    )
    report.add_argument("--job", required=True, metavar="NAME", help="name of the crawl job")
    report.add_argument(
        "--config",
        metavar="PATH",
        help="configuration file, YAML or JSON: the database and the report section are read from it",
    )
    report.add_argument("--stats-json", metavar="PATH", help="write the statistics of the job to a JSON file")
    report.add_argument("--report", metavar="PATH", help="write an HTML report with charts and the workers")
    status = commands.add_parser(
        "status",
        help="show the progress of a crawl job: percent, speed, time left, workers",
        epilog="The speed is that of the last 30 seconds, the time left what the pages left to max_pages take at "
        "that speed. With --watch the line is updated until the job is finished; Ctrl-C stops watching. The "
        "database is distributed.database_url of the configuration, or the CRAWLER_DATABASE_URL variable.",
    )
    status.add_argument("--job", required=True, metavar="NAME", help="name of the crawl job")
    status.add_argument(
        "--config", metavar="PATH", help="configuration file, YAML or JSON: the database is read from it"
    )
    status.add_argument("--watch", action="store_true", help="update the line until the job is finished")
    status.add_argument(
        "--interval",
        type=positive(float),
        metavar="SECONDS",
        help=f"seconds between the updates of --watch (default: {WATCH_INTERVAL:g})",
    )
    args = parser.parse_args(argv)
    if args.command == "status" and args.interval is not None and not args.watch:
        status.error("argument --interval: only with --watch")
    return args


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
    if args.proxy is not None:
        overrides["proxy"] = {"urls": args.proxy, "from_env": False}
    if args.render:
        # Every page: the patterns of the file would name only some of them.
        overrides["rendering"] = {"mode": "always", "include": []}
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
    print_stats(stats)
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


def print_stats(stats: dict[str, Any]) -> None:
    """The lines of the summary that `get_stats()` gives: pages, status codes, domains, errors, proxies, rendering."""
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
    if "proxies" in stats:
        proxies = [
            f"{label} ({proxy['requests']} sent, {proxy['failures']} failed"
            f"{', out of rotation' if proxy['state'] == 'out' else ''})"
            for label, proxy in stats["proxies"].items()
        ]
        print(f"Proxies: {', '.join(proxies)}")
    if "rendering" in stats:
        rendering = stats["rendering"]
        print(
            f"Rendering: {rendering['rendered']} pages rendered, {rendering['failed']} failed, "
            f"average {rendering['avg_render_time']:.2f}s"
        )
    if stats.get("workers"):
        workers = [
            f"{name} ({worker['state']}, {worker['pages']} pages, {worker['pages_per_second']:.1f} pages/s)"
            for name, worker in stats["workers"].items()
        ]
        print(f"Workers: {', '.join(workers)}")


async def run(config: CrawlerConfig, *, progress: bool = True) -> int:
    """Crawl by the configuration, print the summary; return the exit code.

    Cancelled (Ctrl-C), it stops the crawl, writes the reports of the pages
    fetched so far, the cookies and those pages before the cancellation goes on.
    SIGTERM (`docker stop`, systemd) cancels it the same way; a second
    SIGTERM kills the process. So it does when the progress cannot be
    shown, e.g. stderr is a pipe that was closed.

    Raises:
        ConfigError: the file of `session.cookies_file` cannot be read, with
            `proxy.from_env` a variable is not the URL of a proxy, or pages are
            to be rendered and Chromium is not installed; nothing is requested.
        OSError: a directory cannot be created, or the log file cannot be opened.
        StorageError: an output file or the database cannot be opened; nothing is requested.
    """
    if config.rendering.mode != "off":
        # Before anything is created: without the check, every page would fail.
        problem = await browser_problem()
        if problem is not None:
            raise ConfigError([f"rendering.mode: {problem}"])
    with _stop_on_sigterm():
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


async def run_job_create(config: CrawlerConfig, name: str, mode: JobMode, *, dsn: str) -> int:
    """Create the crawl job `name` and seed it, print what the workers are to be given; return the exit code.

    Cancelled (Ctrl-C, SIGTERM), the job stays seeding: `--resume` seeds it again.

    Raises:
        ConfigError: as `create_job`.
        JobError: as `create_job`.
        FrontierError: the database cannot be reached or failed.
        OSError: the log file cannot be opened.
    """
    with _stop_on_sigterm():
        failed = await create_job(config, name, dsn=dsn, mode=mode, configure_logging=True)
    if failed:
        print(f"Sitemaps not read: {', '.join(f'{url} ({reason})' for url, reason in failed.items())}")
    print(f"Crawl job {name} is ready: start its workers with worker --job {name}")
    return 0


async def run_worker_command(config: CrawlerConfig, job: str, worker: str | None) -> int:
    """Run a worker of the crawl job `job` until no page is left, print its summary; return the exit code.

    Cancelled (Ctrl-C, SIGTERM), the worker writes what its storage
    buffers and its reports, and queues the pages it had in progress
    again; nothing is printed then, the log tells.

    Raises:
        ConfigError, JobError, FrontierError, StorageError, OSError: as `run_worker`.
    """
    with _stop_on_sigterm():
        stats = await run_worker(config, job, worker=worker)
    print(f"\n=== Worker {stats['worker']} finished on crawl job {job} ({stats['elapsed_seconds']:.2f}s) ===")
    print_stats(stats)
    if config.storage.outputs:
        not_saved = f", {stats['save_failed']} not saved" if stats["save_failed"] else ""
        print(f"Saved: {stats['saved']} pages{not_saved}")
    # The pages the other workers crawled are theirs to count: running out of pages is the end of a worker.
    return 1 if stats["save_failed"] else 0


async def run_report_command(config: CrawlerConfig, job: str, *, dsn: str) -> int:
    """Write the reports of the crawl job `job`, of all its workers, by the `report` section; print its summary.

    Returns the exit code.

    Raises:
        JobError: there is no crawl job of that name.
        FrontierError: the database cannot be reached or failed.
        OSError: a report cannot be written.
    """
    options = config.report
    stats = await job_stats(dsn, job, top_domains=options.top_domains)
    written = export_job_stats(stats, stats_json=options.stats_json, html=options.html, title=f"{options.title}: {job}")
    print(f"=== Crawl job {job}: {stats['state']} ({stats['elapsed_seconds']:.2f}s) ===")
    print_stats(stats)
    if stats["state"] != "finished":
        print(f"Left: {stats['queued']} pages queued, {stats['in_progress']} in progress")
    print(f"Reports: {', '.join(map(str, written))}")
    return 0


async def run_status_command(config: CrawlerConfig, job: str, *, dsn: str, interval: float | None) -> int:
    """Print the progress line of the crawl job `job`, every `interval` seconds until it is finished if given.

    Returns the exit code.

    Raises:
        JobError: there is no crawl job of that name.
        FrontierError: the database cannot be reached or failed.
    """
    if interval is None:
        print(format_job_progress(await job_progress(dsn, job)))
    else:
        with _stop_on_sigterm():
            await watch_job(dsn, job, interval=interval)
    return 0


@contextlib.contextmanager
def _stop_on_sigterm() -> Iterator[None]:
    """Cancel the task running on SIGTERM, as Ctrl-C does; a second SIGTERM kills the process, as a second Ctrl-C does."""
    loop, task = asyncio.get_running_loop(), asyncio.current_task()
    assert task is not None

    def stop() -> None:
        loop.remove_signal_handler(signal.SIGTERM)
        task.cancel()

    loop.add_signal_handler(signal.SIGTERM, stop)
    try:
        yield
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    # The start URLs are given by options, so the first word can only be a command.
    if argv and argv[0] in COMMANDS:
        return run_command(parse_command_args(argv))
    args = parse_args(argv)
    try:
        config = build_config(args)
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2  # as argparse exits for a wrong flag
    except KeyboardInterrupt:  # while the URLs are typed into stdin
        return 130
    return _exit_code(run(config, progress=not args.no_progress))


def run_command(args: argparse.Namespace) -> int:
    """Run the command of crawl jobs that `parse_command_args` gave; return the exit code."""
    try:
        if args.command == "job":
            config = load_config(args.config)
            command = run_job_create(config, args.name, args.mode, dsn=config.distributed.dsn())
        elif args.command == "report":
            config = _report_config(args)
            command = run_report_command(config, args.job, dsn=config.distributed.dsn())
        elif args.command == "status":
            config = CrawlerConfig() if args.config is None else load_config(args.config)
            interval = (WATCH_INTERVAL if args.interval is None else args.interval) if args.watch else None
            command = run_status_command(config, args.job, dsn=config.distributed.dsn(), interval=interval)
        else:
            overrides = {} if args.concurrency is None else {"crawler": {"max_concurrent": args.concurrency}}
            config = CrawlerConfig.from_dict(overrides) if args.config is None else load_config(args.config, overrides)
            command = run_worker_command(config, args.job, args.name)
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return _exit_code(command)


def _report_config(args: argparse.Namespace) -> CrawlerConfig:
    """The configuration of the `report` command: the file, if one is given, with the files of the options over it.

    Raises:
        ConfigError: the file is invalid, or no report is to be written.
    """
    files = {key: value for key, value in (("stats_json", args.stats_json), ("html", args.report)) if value}
    config = (
        CrawlerConfig.from_dict({"report": files})
        if args.config is None
        else load_config(args.config, {"report": files})
    )
    if config.report.stats_json is None and config.report.html is None:
        raise ConfigError(
            ["report: nothing to write, give --stats-json or --report, or report.stats_json or report.html"]
        )
    return config


def _exit_code(command: Coroutine[Any, Any, int]) -> int:
    """Run `command`; return its exit code, or that of the error it raised, printed to stderr."""
    try:
        return asyncio.run(command)
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except (OSError, StorageError, JobError, FrontierError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130  # 128 + SIGINT, as a shell reports a program stopped by Ctrl-C
    except asyncio.CancelledError:
        return 143  # 128 + SIGTERM: only the signal cancels the run from outside


if __name__ == "__main__":
    sys.exit(main())
