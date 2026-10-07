"""The progress of a crawl job, read from the tables its workers share: percent, speed, time left, workers."""

import asyncio
import sys
from dataclasses import dataclass
from typing import TextIO

import asyncpg

from crawler.distributed.job import database_errors
from crawler.distributed.schema import Connection, create_schema
from crawler.distributed.stats import FINISHED
from crawler.exceptions import JobError
from crawler.progress import format_duration, print_progress_line, progress_bar

# The time of a job ends with the job, so the speed of a job finished is that of its last seconds.
_JOB = """
WITH job AS (
    SELECT id, state, max_pages, CASE WHEN state = 'finished' THEN finished_at ELSE now() END AS moment
    FROM crawl_jobs
    WHERE name = $1
)
SELECT
    job.id,
    job.state,
    job.max_pages,
    job.moment,
    coalesce(extract(epoch FROM job.moment - min(w.started_at)), 0) AS elapsed,
    count(w.worker) FILTER (WHERE w.stopped_at IS NULL AND w.lease_until > now()) AS running,
    count(w.worker) FILTER (WHERE w.stopped_at IS NULL AND w.lease_until <= now()) AS lost
FROM job
LEFT JOIN workers AS w ON w.job = job.id
GROUP BY job.id, job.state, job.max_pages, job.moment
"""

# Pages done are those requested: the pages failed without a request, by
# an open circuit breaker, and those over max_pages_per_host are not counted.
_PAGES = """
SELECT
    count(*) FILTER (WHERE counted AND state = ANY($2::text[])) AS done,
    count(*) FILTER (
        WHERE counted AND state = ANY($2::text[]) AND finished_at > $3::timestamptz - make_interval(secs => $4)
    ) AS recent,
    count(*) FILTER (WHERE state = 'failed') AS failed,
    count(*) FILTER (WHERE state = 'leased') AS in_progress,
    count(*) FILTER (WHERE state = 'queued') AS queued
FROM frontier
WHERE job = $1
"""


@dataclass(frozen=True, slots=True)
class JobProgress:
    """How far a crawl job has got at one moment, all its workers together.

    `state` is that of the job: seeding, running or finished. `done` counts
    the pages requested and finished, as `Progress.done` does; `total` is
    max_pages of the job and `percent` `done` of it, both `None` for a job
    without the limit. `failed` counts every page failed, those failed
    without a request too. `pages_per_second` is the speed over the last
    seconds of the job, `eta` the seconds left at that speed until `total`
    pages are done: `None` while the speed is 0 or without the limit, and
    0 once the job is finished. `workers` counts the workers running, `lost` those that
    stopped renewing their leases without a word, killed say. `in_progress`
    counts the pages workers are crawling, `queued` the pages waiting, but
    no more than the limit leaves to request. `elapsed` runs from the start
    of the first worker to the end of the job, or to now.
    """

    state: str
    done: int
    total: int | None
    failed: int
    percent: float | None
    pages_per_second: float
    eta: float | None
    workers: int
    lost: int
    in_progress: int
    queued: int
    elapsed: float


async def job_progress(dsn: str, job: str, *, window: float = 30.0) -> JobProgress:
    """The progress of the crawl job `job`, from the database of `dsn`; the speed is that of the last `window` seconds.

    Raises:
        JobError: there is no crawl job of that name.
        FrontierError: the database cannot be reached or failed.
    """
    with database_errors(job):
        connection = await asyncpg.connect(dsn)
        try:
            await create_schema(connection)
            return await _read_progress(connection, job, window)
        finally:
            await connection.close()


async def watch_job(
    dsn: str,
    job: str,
    *,
    interval: float = 2.0,
    window: float = 30.0,
    stream: TextIO | None = None,
) -> None:
    """Print the progress line of the crawl job `job` every `interval` seconds until the job is finished.

    The line goes to `stream`, stdout by default. In a terminal it is
    redrawn in place; in a file or a pipe every update goes on a line of
    its own. The job may have no worker yet: the line waits for them.

    Raises:
        JobError: there is no crawl job of that name.
        FrontierError: the database cannot be reached or failed.
    """
    stream = sys.stdout if stream is None else stream
    live = stream.isatty()
    with database_errors(job):
        connection = await asyncpg.connect(dsn)
        try:
            await create_schema(connection)
            while True:
                progress = await _read_progress(connection, job, window)
                print_progress_line(format_job_progress(progress), stream, live=live)
                if progress.state == "finished":
                    break
                await asyncio.sleep(interval)
        finally:
            if live:
                print(file=stream)
            await connection.close()


def format_job_progress(progress: JobProgress) -> str:
    """One line: a bar, the percent, pages done, speed, time left, workers, pages in progress and queued, the time."""
    if progress.state == "seeding":
        left = "seeding"
    elif progress.state == "finished":
        left = "done"
    elif progress.eta is None:
        left = "ETA --"
    else:
        left = f"ETA {format_duration(progress.eta)}"
    if progress.percent is None:
        pages = f"{progress.done} pages"
    else:
        pages = f"{progress_bar(progress.percent)} | {progress.done}/{progress.total} pages"
    lost = f" ({progress.lost} lost)" if progress.lost else ""
    return (
        f"{pages}, {progress.failed} failed | {progress.pages_per_second:.1f} pages/s | {left} | "
        f"workers {progress.workers}{lost} | in progress {progress.in_progress} | queued {progress.queued} | "
        f"{format_duration(progress.elapsed)}"
    )


async def _read_progress(connection: Connection, job: str, window: float) -> JobProgress:
    # One snapshot: the workers go on meanwhile.
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        row = await connection.fetchrow(_JOB, job)
        if row is None:
            raise JobError(f'There is no crawl job named "{job}"')
        pages = await connection.fetchrow(_PAGES, row["id"], FINISHED, row["moment"], window)
    total, done, elapsed = row["max_pages"], pages["done"], float(row["elapsed"])
    # A job younger than the window has run for less time than that.
    speed = pages["recent"] / min(window, elapsed) if elapsed > 0 else 0.0
    queued = pages["queued"]
    if row["state"] == "finished":
        eta = 0.0
    elif total is None or speed == 0:
        eta = None
    else:
        eta = (total - done) / speed
    if total is not None:
        # The rest of the queue will not be fetched.
        queued = min(queued, max(total - done - pages["in_progress"], 0))
    return JobProgress(
        state=row["state"],
        done=done,
        total=total,
        failed=pages["failed"],
        percent=None if total is None else 100.0 * done / total,
        pages_per_second=speed,
        eta=eta,
        workers=row["running"],
        lost=row["lost"],
        in_progress=pages["in_progress"],
        queued=queued,
        elapsed=elapsed,
    )
