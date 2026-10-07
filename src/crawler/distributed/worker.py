"""A worker of a crawl job: crawls the pages of the job's frontier side by side with the other workers."""

import contextlib
import dataclasses
import json
import logging
import re
from collections.abc import Iterator
from typing import Any

import asyncpg

from crawler.advanced import AdvancedCrawler
from crawler.config import CrawlerConfig, CrawlOptions
from crawler.distributed.frontier import PostgresFrontier, worker_name
from crawler.distributed.job import JOB_SECTIONS, config_differences, job_config
from crawler.distributed.schema import create_schema
from crawler.exceptions import ConfigError, FrontierError, JobError
from crawler.storage import storage_from_output

logger = logging.getLogger(__name__)

# A name that can be a part of a file name, such as "pages-{worker}.jsonl".
_WORKER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


async def run_worker(
    config: CrawlerConfig, job: str, *, worker: str | None = None, configure_logging: bool = True
) -> dict[str, Any]:
    """Crawl the pages of the crawl job `job` until none is left; return the statistics of this worker.

    What and how to crawl is the job's: the sections of `JOB_SECTIONS`,
    which `create_job` kept. `config` gives the rest: the database and the
    leases in `distributed`, the session, the proxies, the storage, the
    log, the reports and `crawler.max_concurrent`. The keys of the job that
    `config` sets otherwise are ignored, which is logged as a warning. The
    pages are not kept in memory, whatever `crawler.keep_pages` says: they
    go to the storage, if the worker has one.

    `worker` names the worker in the database and stands for "{worker}" in
    the paths of the files it writes; by default it is made of the host
    name, the process id and a random part. Every file of the storage must
    have "{worker}" in its name: workers write side by side, and a page of
    a worker that stopped is crawled again by another one, so a page may
    be in the files of two workers. A database keeps a row per URL.

    The workers ask a host together at the rate of the job, as one process
    asks it (see `host_interval`); with `crawler.per_domain_rate: false`,
    the rate for all hosts together is that of each worker. The statistics
    and the reports are those of the pages this worker crawled. Once the
    crawl is over, the storage has been written, the frontier is closed,
    which finishes the job if no page is left, then the crawler.

    The worker takes no page while its storage cannot write, and tries it
    again until it can: the pages it buffers stay leased, and the worker
    does not stop before they are written. The database failing stops the
    worker at once (`FrontierError`): what its storage buffers is written,
    and the pages it had in progress come back to the other workers once
    their leases expire. Running it again is the business of whatever
    started it, such as a restart policy of the container. Cancelled,
    the worker writes what its storage buffers, queues the pages it had in
    flight again, uncounted, closes the frontier and raises `CancelledError`.

    Raises:
        ConfigError: there is no database, a file of the storage is
            without "{worker}", or the part of the job cannot be used here,
            such as rendering without Playwright.
        JobError: there is no crawl job of that name.
        FrontierError: the database cannot be reached or failed an operation.
        ValueError: `worker` cannot be a part of a file name.
        StorageError: the storage cannot be opened; nothing is requested.
    """
    if worker is not None and not _WORKER_NAME.fullmatch(worker):
        raise ValueError(f"a worker name may hold letters, digits, '.', '_' and '-' only, got {worker!r}")
    dsn = config.distributed.dsn()
    _check_files(config)
    with _database_errors(job):
        settings = await _job_settings(dsn, job)
    config = _worker_config(config, job, settings)
    name = worker or worker_name()
    options = config.distributed
    async with AdvancedCrawler(config, configure_logging=configure_logging, worker=name) as crawler:
        if crawler.storage is None:
            logger.warning("Worker %s has no storage: the pages it crawls are not saved", name)
        with _database_errors(job):
            frontier = await PostgresFrontier.open(
                dsn,
                job=job,
                worker=name,
                lease_seconds=options.lease_seconds,
                heartbeat_seconds=options.heartbeat_seconds,
                max_attempts=options.max_attempts,
                host_interval=host_interval(config.crawler),
                poll_interval=options.poll_interval,
            )
        logger.info("Worker %s started on crawl job %s", name, job)
        try:
            await crawler.crawl_frontier(frontier)
        finally:
            await frontier.close()
        return crawler.get_stats()


def host_interval(options: CrawlOptions) -> float:
    """Seconds between two pages of a host that the workers of a job take, as `RateLimiter` spaces the requests of one process."""
    if not options.per_domain_rate:
        return 0.0
    return max(0.0 if options.rate_limit is None else 1 / options.rate_limit, options.min_delay)


@contextlib.contextmanager
def _database_errors(job: str) -> Iterator[None]:
    """Raise the errors of the database of the job as FrontierError, as a crawl of its frontier does."""
    try:
        yield
    except PostgresFrontier.ERRORS as error:
        raise FrontierError(f"the database of crawl job {job} failed: {type(error).__name__}: {error}") from error


def _check_files(config: CrawlerConfig) -> None:
    """Raises ConfigError if a file of the storage is not one of the worker's own."""
    problems = [
        f"storage.outputs[{index}]: workers of a crawl job write files of their own, "
        "put {worker} in the name, such as pages-{worker}.jsonl"
        for index, output in enumerate(config.storage.outputs)
        if "{worker}" not in output and getattr(storage_from_output(output), "path", None) is not None
    ]
    if problems:
        raise ConfigError(problems)


async def _job_settings(dsn: str, job: str) -> dict[str, Any]:
    """The part of the configuration that the job keeps."""
    connection = await asyncpg.connect(dsn)
    try:
        await create_schema(connection)
        row = await connection.fetchrow("SELECT config FROM crawl_jobs WHERE name = $1", job)
    finally:
        await connection.close()
    if row is None:
        raise JobError(f'There is no crawl job named "{job}"')
    if row["config"] is None:
        raise JobError(f'Crawl job "{job}" has no configuration: create it with create_job')
    return json.loads(row["config"])


def _worker_config(config: CrawlerConfig, job: str, settings: dict[str, Any]) -> CrawlerConfig:
    """`config` with the sections of the job taken from `settings`, but `crawler.max_concurrent`; pages not kept."""
    given = job_config(config)
    # Those the worker sets, as opposed to defaults it was left with.
    set_here = set(config_differences(job_config(CrawlerConfig()), given))
    ignored = [key for key in config_differences(settings, given) if key in set_here]
    if ignored:
        logger.warning(
            "The configuration of the worker sets %s otherwise than crawl job %s: those of the job are used",
            ", ".join(ignored),
            job,
        )
    part = CrawlerConfig.from_dict(settings, source=f'crawl job "{job}"')
    crawler = dataclasses.replace(part.crawler, max_concurrent=config.crawler.max_concurrent, keep_pages=False)
    sections = {name: getattr(part, name) for name in JOB_SECTIONS}
    return dataclasses.replace(config, **{**sections, "crawler": crawler})
