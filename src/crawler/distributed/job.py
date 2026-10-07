"""Crawl jobs in PostgreSQL: one crawl that workers share, created, seeded, resumed or restarted here."""

import contextlib
import dataclasses
import enum
import json
import logging
from collections.abc import Iterator
from typing import Any

import asyncpg

from crawler.advanced import AdvancedCrawler
from crawler.config import CrawlerConfig, StorageOptions
from crawler.distributed.frontier import PostgresFrontier
from crawler.distributed.schema import Connection, create_schema
from crawler.exceptions import FrontierError, JobError

logger = logging.getLogger(__name__)

# What and how to crawl, the same for every worker of a job. The other
# sections, session, proxy, storage, logging and report, are each worker's
# own, and so is crawler.max_concurrent; the secrets of a configuration,
# cookies, headers and proxies, are all in those.
JOB_SECTIONS = ("urls", "sitemaps", "crawler", "retry", "circuit_breaker", "filters", "rendering")


class JobMode(enum.Enum):
    """What `create_job` does with a job of the name given, if there is one."""

    NEW = "new"  # there must be none
    RESUME = "resume"  # go on with it; there must be one
    RESTART = "restart"  # delete it with its pages and create it anew


def job_config(config: CrawlerConfig) -> dict[str, Any]:
    """The part of a configuration that is the job's, as JSON holds it: `JOB_SECTIONS` without `crawler.max_concurrent`."""
    settings = config.to_dict()
    job = {name: settings[name] for name in JOB_SECTIONS}
    del job["crawler"]["max_concurrent"]
    return json.loads(json.dumps(job))


async def create_job(
    config: CrawlerConfig, name: str, *, dsn: str, mode: JobMode = JobMode.NEW, configure_logging: bool = False
) -> dict[str, str]:
    """Create the crawl job `name` in the database of `dsn` and seed it; return the sitemaps that could not be read.

    The job keeps `job_config(config)` and the limits of `config.crawler`.
    It is seeding until its start URLs and the pages of its sitemaps are
    queued, see `AsyncCrawler.seed`: workers may be started meanwhile, and
    wait. The sitemaps are read with the session and the proxies of
    `config`; its storage is not opened.

    `JobMode.RESUME` goes on with the job: its start URLs are seeded again,
    which queues only those never queued, and a finished job runs again.
    Its sitemaps are not read again, unless its seeding did not finish.
    `JobMode.RESTART` deletes the job and its pages, then creates it anew.
    A job whose seeding failed, say as the database went away, stays
    seeding: resumed, it is seeded again, sitemaps included.

    With `configure_logging`, logging is set up by the `logging` section
    of `config` for the time of the call, as `AdvancedCrawler` sets it up.

    Raises:
        JobError: with `JobMode.NEW`, the name is taken; with
            `JobMode.RESUME`, there is no such job, or `config` differs from
            that of the job in its part.
        ConfigError: as `AdvancedCrawler.seed`; nothing is created.
        FrontierError: the database cannot be reached or failed an operation.
    """
    settings = job_config(config)
    # The storage is that of a worker, not of the job.
    async with AdvancedCrawler(
        dataclasses.replace(config, storage=StorageOptions()), configure_logging=configure_logging
    ) as crawler:
        crawler.check_start()
        with database_errors(name):
            connection = await asyncpg.connect(dsn)
            try:
                await create_schema(connection)
                seeding = await _prepare_job(
                    connection, name, mode, settings, config, frontier_factor=crawler.crawler.FRONTIER_FACTOR
                )
                frontier = await PostgresFrontier.open(dsn, job=name)
                try:
                    failed = await crawler.seed(frontier, sitemaps=seeding)
                    await frontier.refresh_stats()
                finally:
                    await frontier.close()
                if seeding:
                    await connection.execute("UPDATE crawl_jobs SET state = 'running' WHERE name = $1", name)
            finally:
                await connection.close()
        # Before the crawler is closed, which resets the logging it set up.
        logger.info("Crawl job %s is seeded: %d pages queued", name, frontier.stats().queued)
    return failed


@contextlib.contextmanager
def database_errors(job: str) -> Iterator[None]:
    """Raise the errors of the database of the job as FrontierError, as a crawl of its frontier does."""
    try:
        yield
    except PostgresFrontier.ERRORS as error:
        raise FrontierError(f"the database of crawl job {job} failed: {type(error).__name__}: {error}") from error


async def _prepare_job(
    connection: Connection,
    name: str,
    mode: JobMode,
    settings: dict[str, Any],
    config: CrawlerConfig,
    *,
    frontier_factor: int,
) -> bool:
    """Make the row of the job, or find the one to resume; whether the job is to be seeded with its sitemaps."""
    async with connection.transaction():
        row = await connection.fetchrow("SELECT id, state, config FROM crawl_jobs WHERE name = $1 FOR UPDATE", name)
        if row is not None and mode is JobMode.NEW:
            raise JobError(f'A crawl job named "{name}" exists already: resume or restart it')
        if row is not None and mode is JobMode.RESTART:
            await connection.execute("DELETE FROM crawl_jobs WHERE id = $1", row["id"])
            logger.info("Crawl job %s is deleted, to be created anew", name)
            row = None
        if row is None:
            if mode is JobMode.RESUME:
                raise JobError(f'There is no crawl job named "{name}" to resume')
            try:
                await connection.execute(
                    "INSERT INTO crawl_jobs (name, config, max_pages, max_pages_per_host, frontier_factor)"
                    " VALUES ($1, $2::jsonb, $3, $4, $5)",
                    name,
                    json.dumps(settings),
                    config.crawler.max_pages,
                    config.crawler.max_pages_per_host,
                    frontier_factor,
                )
            except asyncpg.UniqueViolationError:
                # Created by another process since it was looked for.
                raise JobError(f'A crawl job named "{name}" exists already: resume or restart it') from None
            logger.info("Crawl job %s is created", name)
            return True
        differences = config_differences(json.loads(row["config"]), settings)
        if differences:
            raise JobError(
                f'The configuration differs from that of crawl job "{name}" in: {", ".join(differences)}; '
                "restart the job to change it"
            )
        if row["state"] == "seeding":
            logger.info("Crawl job %s is resumed: its seeding did not finish, it goes on", name)
            return True
        await connection.execute("UPDATE crawl_jobs SET state = 'running', finished_at = NULL WHERE id = $1", row["id"])
        logger.info("Crawl job %s is resumed", name)
        return False


def config_differences(stored: Any, given: Any, path: str = "") -> list[str]:
    """The keys, such as "crawler.max_pages", whose values differ between two configurations."""
    if not isinstance(stored, dict) or not isinstance(given, dict):
        return [] if stored == given else [path]
    keys = list(dict.fromkeys([*stored, *given]))
    return [
        difference
        for key in keys
        for difference in config_differences(stored.get(key), given.get(key), f"{path}.{key}" if path else key)
    ]
