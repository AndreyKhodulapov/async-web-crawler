"""AdvancedCrawler: a crawler assembled from a configuration, with storage, statistics, reports and logging."""

import dataclasses
import logging
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from crawler import logging_setup
from crawler.circuit_breaker import CircuitBreaker
from crawler.client import AsyncCrawler
from crawler.config import CrawlerConfig, load_config
from crawler.exceptions import ConfigError
from crawler.frontier import Frontier
from crawler.models import ParsedPage
from crawler.report import render_html, render_json
from crawler.retry import RetryStrategy
from crawler.session import save_cookies_file
from crawler.stats import CrawlerStats
from crawler.storage import CompositeStorage, DataStorage

logger = logging.getLogger(__name__)


class AdvancedCrawler:
    """Everything a crawl needs, put together from a `CrawlerConfig`.

    Usage::

        crawler = AdvancedCrawler.from_config("config.yaml")
        await crawler.crawl()
        stats = crawler.get_stats()
        crawler.export_to_html_report("report.html")
        await crawler.close()

    or `async with AdvancedCrawler(config) as crawler:`. The configuration
    says what to crawl (start URLs, sitemaps, filters), how (limits,
    retries, the circuit breaker), where to save the pages, where to write
    the log and the reports. The parts are there to be used directly:
    `crawler` is the `AsyncCrawler` that does the work, `storage` its
    storage (None without outputs), `stats` its `CrawlerStats`,
    `crawler.proxies` its proxies (None without any) and
    `crawler.rendering` the settings of its browser (None if pages are
    not rendered). `reports`
    are the report files the latest crawl wrote, `cookie_file` the file the
    cookies were saved to.

    "{worker}" in the paths of the files written, such as
    "pages-{worker}.jsonl", is `worker`: "local" for a crawl of its own,
    the name of the worker for a worker of a crawl job (see
    `CrawlerConfig.for_worker`); `config` keeps the paths with it replaced.
    Directories of the log, the reports and the files of the storage are
    created if they are missing. Logging is set up when the crawler is
    made and reset by `close()`; it belongs to the whole process, so with
    two crawlers at once the log is written as the later one says. With
    `configure_logging=False` the crawler leaves logging alone and the
    `logging` section is ignored: for a program that sets up logging itself.
    """

    def __init__(
        self, config: CrawlerConfig | None = None, *, configure_logging: bool = True, worker: str = "local"
    ) -> None:
        """
        Raises:
            ConfigError: `session.cookies_file` cannot be read, or is not a cookies.txt file;
                with `proxy.from_env`, a variable is not the URL of a proxy.
            OSError: a directory cannot be created, or the log file cannot be opened.
        """
        self.config = config = (CrawlerConfig() if config is None else config).for_worker(worker)
        options = config.crawler
        cookies = config.session.initial_cookies()
        proxies = config.proxy.build()
        self.storage = config.storage.build()
        self.crawler = AsyncCrawler(
            max_concurrent=options.max_concurrent,
            max_depth=options.max_depth,
            max_per_domain=options.max_per_domain,
            requests_per_second=options.rate_limit,
            per_domain_rate=options.per_domain_rate,
            min_delay=options.min_delay,
            jitter=options.jitter,
            respect_robots=options.respect_robots,
            retry_strategy=RetryStrategy(
                max_retries=config.retry.max_retries,
                backoff_factor=config.retry.backoff_factor,
                base_delay=config.retry.base_delay,
                max_delay=config.retry.max_delay,
            ),
            circuit_breaker=CircuitBreaker(
                config.circuit_breaker.failure_threshold,
                min_requests=config.circuit_breaker.min_requests,
                window=config.circuit_breaker.window,
                cooldown=config.circuit_breaker.cooldown,
            ),
            total_timeout=options.total_timeout,
            connect_timeout=options.connect_timeout,
            read_timeout=options.read_timeout,
            timeout_growth=options.timeout_growth,
            max_page_size=options.max_page_size,
            max_parsing=options.max_parsing,
            max_retry_after=options.max_retry_after,
            user_agent=options.user_agent,
            user_agents=options.user_agents,
            headers=config.session.headers,
            cookies=cookies,
            keep_cookies=config.session.keep_cookies,
            proxies=proxies,
            rendering=config.rendering.build(),
            storage=self.storage,
            keep_pages=options.keep_pages,
        )
        self.crawler.sitemaps.max_urls = config.sitemaps.max_urls
        self.crawler.stats.top_domains = config.report.top_domains
        self.reports: list[Path] = []
        self.cookie_file: Path | None = None
        self._closed = False
        self._configures_logging = configure_logging

        for path in _storage_files(self.storage):
            make_directory(path)
        if configure_logging:
            if config.logging.file is not None:
                make_directory(config.logging.file)
            # The last step that can fail: nothing after it can leave the log file open.
            logging_setup.configure_logging(
                config.logging.level,
                config.logging.file,
                max_bytes=config.logging.max_bytes,
                backup_count=config.logging.backup_count,
                console_format=config.logging.console_format,
            )
        if config.proxy.from_env and proxies is None:
            logger.warning("proxy.from_env: neither HTTP_PROXY nor HTTPS_PROXY is set, requests go directly")

    @classmethod
    def from_config(
        cls, path: str | Path, overrides: Mapping[str, Any] | None = None, *, configure_logging: bool = True
    ) -> Self:
        """The crawler for a YAML or a JSON configuration file; `overrides` win over the file, see `load_config`.

        `configure_logging` is that of the constructor.

        Raises:
            ConfigError: the file cannot be read, holds an unknown key or an invalid value, or as the constructor.
            OSError: as the constructor.
        """
        return cls(load_config(path, overrides), configure_logging=configure_logging)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def stats(self) -> CrawlerStats:
        """The statistics of the latest crawl, see `get_stats`."""
        return self.crawler.stats

    @property
    def closed(self) -> bool:
        return self._closed

    async def crawl(self) -> dict[str, ParsedPage]:
        """Crawl what the configuration says; return the pages by normalized URL.

        With `crawler.keep_pages: false` the pages are not kept in memory and
        the result is empty.

        The pages are saved to the storage as they are crawled. Afterwards
        the statistics are written to the files of the `report` section,
        if it names any, and the cookies to `session.save_cookies`; a file
        that cannot be written is logged and does not fail the crawl. See `AsyncCrawler.crawl` for the rules of
        the crawl itself. A closed crawler fetches nothing: every page
        fails with `CrawlerClosedError`.

        Raises:
            ConfigError: the configuration has neither start URLs nor sitemaps.
            StorageError: the storage cannot be opened (the output file is
                of another layout, the database cannot be reached); nothing
                is requested.
        """
        self.check_start()
        config = self.config
        pages = await self.crawler.crawl(
            config.urls,
            max_pages=config.crawler.max_pages,
            max_pages_per_host=config.crawler.max_pages_per_host,
            same_domain_only=config.filters.same_domain_only,
            include_patterns=config.filters.include,
            exclude_patterns=config.filters.exclude,
            exclude_extensions=config.filters.exclude_extensions,
            sitemap_urls=config.sitemaps.urls,
            robots_sitemaps=config.sitemaps.from_robots,
        )
        self.write_reports()
        self.save_cookies()
        return pages

    async def crawl_frontier(self, frontier: Frontier) -> dict[str, ParsedPage]:
        """Crawl the pages of `frontier`, which seed() filled, as crawl() crawls those of the configuration.

        The limits are those of the frontier, and the sitemaps are not read
        again; see `AsyncCrawler.crawl_frontier`. The reports and the
        cookies are written afterwards, as crawl() writes them. The
        frontier is left open.

        Raises:
            ConfigError: as crawl().
            StorageError: as crawl().
        """
        self.check_start()
        config = self.config
        pages = await self.crawler.crawl_frontier(
            frontier,
            config.urls,
            same_domain_only=config.filters.same_domain_only,
            include_patterns=config.filters.include,
            exclude_patterns=config.filters.exclude,
            exclude_extensions=config.filters.exclude_extensions,
            sitemap_urls=config.sitemaps.urls,
        )
        self.write_reports()
        self.save_cookies()
        return pages

    async def seed(self, frontier: Frontier, *, sitemaps: bool = True) -> dict[str, str]:
        """Queue the start URLs and the pages of the sitemaps of the configuration in `frontier`; crawl nothing.

        Without `sitemaps`, the sitemaps are not read. Returns the sitemaps
        that could not be read, with the reasons; see `AsyncCrawler.seed`.

        Raises:
            ConfigError: as crawl().
        """
        self.check_start()
        config = self.config
        return await self.crawler.seed(
            frontier,
            config.urls,
            same_domain_only=config.filters.same_domain_only,
            include_patterns=config.filters.include,
            exclude_patterns=config.filters.exclude,
            exclude_extensions=config.filters.exclude_extensions,
            sitemap_urls=config.sitemaps.urls if sitemaps else (),
            robots_sitemaps=config.sitemaps.from_robots and sitemaps,
        )

    def check_start(self) -> None:
        """Raises ConfigError if the configuration has neither start URLs nor sitemaps."""
        if not self.config.urls and not self.config.sitemaps.urls:
            raise ConfigError(["urls: nothing to crawl, give start URLs here or sitemaps in sitemaps.urls"])

    def write_reports(self) -> list[Path]:
        """Write the statistics to the files of the `report` section; return those written.

        `crawl()` does it when the crawl ends; call it yourself after a
        crawl that was cancelled. A report that cannot be written is logged
        and left out. The files written are kept in `reports` as well.
        """
        written = []
        for path, export in (
            (self.config.report.stats_json, self.export_to_json),
            (self.config.report.html, self.export_to_html_report),
        ):
            if path is None:
                continue
            try:
                export(path)
            except OSError as error:
                logger.error("Failed to write the report %s: %s", path, error)
            else:
                logger.info("Report written to %s", path)
                written.append(Path(path))
        self.reports = written
        return written

    def save_cookies(self) -> Path | None:
        """Write the cookies to the cookies.txt file of `session.save_cookies`; return it, None if not written.

        `crawl()` does it when the crawl ends; call it yourself after a
        crawl that was cancelled. The file can be read by its owner only.
        One that cannot be written is logged. The file is kept in
        `cookie_file` as well.
        """
        path = self.config.session.save_cookies
        self.cookie_file = None
        if path is None:
            return None
        cookies = self.crawler.export_cookies()
        try:
            save_cookies_file(cookies, make_directory(path))
        except OSError as error:
            logger.error("Failed to save the cookies to %s: %s", path, error)
            return None
        logger.info("Saved %d cookies to %s", len(cookies), path)
        self.cookie_file = Path(path)
        return self.cookie_file

    def get_stats(self) -> dict[str, Any]:
        """The statistics of the latest crawl: `total_pages`, `successful`, `failed` and the rest of `CrawlerStats.get_stats`.

        With proxies, `proxies` too: label (the URL with the password
        hidden) -> `state`, `requests`, `failures` and `times_removed`, see
        `ProxyStats`. With rendering, `rendering` too: `rendered`, `failed`
        and `avg_render_time`, see `RenderStats`.
        """
        stats = self.crawler.stats.get_stats()
        if self.crawler.proxies is not None:
            stats["proxies"] = {label: dataclasses.asdict(proxy) for label, proxy in self.crawler.proxy_stats().items()}
        render_stats = self.crawler.render_stats()
        if render_stats is not None:
            stats["rendering"] = dataclasses.asdict(render_stats)
        return stats

    def export_to_json(self, filename: str | Path) -> None:
        """Write `get_stats()` to a JSON file (UTF-8), replacing the file if it exists.

        Raises:
            OSError: the file cannot be written.
        """
        make_directory(filename).write_text(render_json(self.get_stats()), encoding="utf-8")

    def export_to_html_report(self, filename: str | Path, *, title: str | None = None) -> None:
        """Write the HTML report of `get_stats()`, see `CrawlerStats.export_to_html_report`.

        The title is that of the configuration unless given.

        Raises:
            OSError: the file cannot be written.
        """
        report = render_html(self.get_stats(), title=self.config.report.title if title is None else title)
        make_directory(filename).write_text(report, encoding="utf-8")

    async def close(self) -> None:
        """Close the crawler, write what the storage still holds and stop logging to the file. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        try:
            await self.crawler.close()
        finally:
            if self._configures_logging:
                logging_setup.reset_logging()


def _storage_files(storage: DataStorage | None) -> list[Path]:
    """The files a storage writes to; a database on a server has none."""
    if storage is None:
        return []
    if isinstance(storage, CompositeStorage):
        return [path for part in storage.storages for path in _storage_files(part)]
    path = getattr(storage, "path", None)
    return [] if path is None else [path]


def make_directory(file: str | Path) -> Path:
    """Create the directory of a file if it is missing; return the path of the file with `~` expanded."""
    path = Path(file).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
