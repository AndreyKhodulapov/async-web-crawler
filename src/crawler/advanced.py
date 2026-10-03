"""AdvancedCrawler: a crawler assembled from a configuration, with storage, statistics, reports and logging."""

import logging
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from crawler.circuit_breaker import CircuitBreaker
from crawler.client import AsyncCrawler
from crawler.config import CrawlerConfig, load_config
from crawler.exceptions import ConfigError
from crawler.logging_setup import configure_logging, reset_logging
from crawler.models import ParsedPage
from crawler.retry import RetryStrategy
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
    storage (None without outputs), `stats` its `CrawlerStats`. `reports`
    are the report files the latest crawl wrote.

    Directories of the log, the reports and the files of the storage are
    created if they are missing. Logging is set up when the crawler is
    made and reset by `close()`; it belongs to the whole process, so with
    two crawlers at once the log is written as the later one says.
    """

    def __init__(self, config: CrawlerConfig | None = None) -> None:
        """
        Raises:
            OSError: a directory cannot be created, or the log file cannot be opened.
        """
        self.config = config = CrawlerConfig() if config is None else config
        options = config.crawler
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
            user_agent=options.user_agent,
            user_agents=options.user_agents,
            storage=self.storage,
            keep_pages=options.keep_pages,
        )
        self.crawler.sitemaps.max_urls = config.sitemaps.max_urls
        self.crawler.stats.top_domains = config.report.top_domains
        self.reports: list[Path] = []
        self._closed = False

        for path in _storage_files(self.storage):
            _make_directory(path)
        if config.logging.file is not None:
            _make_directory(config.logging.file)
        # The last step: nothing after it can fail and leave the log file open.
        configure_logging(
            config.logging.level,
            config.logging.file,
            max_bytes=config.logging.max_bytes,
            backup_count=config.logging.backup_count,
        )

    @classmethod
    def from_config(cls, path: str | Path, overrides: Mapping[str, Any] | None = None) -> Self:
        """The crawler for a YAML or a JSON configuration file; `overrides` win over the file, see `load_config`.

        Raises:
            ConfigError: the file cannot be read, or holds an unknown key or an invalid value.
            OSError: as the constructor.
        """
        return cls(load_config(path, overrides))

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
        if it names any; a report that cannot be written is logged and
        does not fail the crawl. See `AsyncCrawler.crawl` for the rules of
        the crawl itself. A closed crawler fetches nothing: every page
        fails with `CrawlerClosedError`.

        Raises:
            ConfigError: the configuration has neither start URLs nor sitemaps.
        """
        config = self.config
        if not config.urls and not config.sitemaps.urls:
            raise ConfigError(["urls: nothing to crawl, give start URLs here or sitemaps in sitemaps.urls"])
        pages = await self.crawler.crawl(
            config.urls,
            max_pages=config.crawler.max_pages,
            same_domain_only=config.filters.same_domain_only,
            include_patterns=config.filters.include,
            exclude_patterns=config.filters.exclude,
            sitemap_urls=config.sitemaps.urls,
            robots_sitemaps=config.sitemaps.from_robots,
        )
        self.write_reports()
        return pages

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

    def get_stats(self) -> dict[str, Any]:
        """The statistics of the latest crawl: `total_pages`, `successful`, `failed` and the rest of `CrawlerStats.get_stats`."""
        return self.crawler.stats.get_stats()

    def export_to_json(self, filename: str | Path) -> None:
        """Write `get_stats()` to a JSON file, see `CrawlerStats.export_to_json`.

        Raises:
            OSError: the file cannot be written.
        """
        self.crawler.stats.export_to_json(_make_directory(filename))

    def export_to_html_report(self, filename: str | Path, *, title: str | None = None) -> None:
        """Write the HTML report, see `CrawlerStats.export_to_html_report`; the title is that of the configuration.

        Raises:
            OSError: the file cannot be written.
        """
        self.crawler.stats.export_to_html_report(
            _make_directory(filename), title=self.config.report.title if title is None else title
        )

    async def close(self) -> None:
        """Close the crawler, write what the storage still holds and stop logging to the file. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        try:
            await self.crawler.close()
        finally:
            reset_logging()


def _storage_files(storage: DataStorage | None) -> list[Path]:
    """The files a storage writes to; a database on a server has none."""
    if storage is None:
        return []
    if isinstance(storage, CompositeStorage):
        return [path for part in storage.storages for path in _storage_files(part)]
    path = getattr(storage, "path", None)
    return [] if path is None else [path]


def _make_directory(file: str | Path) -> Path:
    """Create the directory of a file if it is missing; return the path of the file with `~` expanded."""
    path = Path(file).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
