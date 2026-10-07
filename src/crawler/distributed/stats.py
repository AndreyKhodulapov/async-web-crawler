"""The statistics of a crawl job, read from the tables its workers share, and its reports."""

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from crawler.advanced import make_directory
from crawler.distributed.job import database_errors
from crawler.distributed.schema import Connection, create_schema
from crawler.exceptions import JobError
from crawler.report import render_html, render_json

# The states of the pages the statistics count, as `CrawlerStats` counts
# them: a page pending its save is processed. Pages never requested, such as
# those robots.txt disallows, are left out.
_COUNTED = ["processed", "saving", "failed", "skipped"]

_JOB = """
SELECT
    j.id,
    j.state,
    CASE WHEN j.state = 'finished' THEN j.finished_at END AS finished_at,
    min(w.started_at) AS started_at,
    extract(epoch FROM CASE WHEN j.state = 'finished' THEN j.finished_at ELSE now() END - min(w.started_at)) AS elapsed
FROM crawl_jobs AS j
LEFT JOIN workers AS w ON w.job = j.id
WHERE j.name = $1
GROUP BY j.id
"""

_PAGES = """
SELECT
    count(*) FILTER (WHERE state IN ('processed', 'saving')) AS successful,
    count(*) FILTER (WHERE state = 'failed') AS failed,
    count(*) FILTER (WHERE state = 'skipped') AS skipped,
    coalesce(avg(elapsed) FILTER (WHERE state = ANY($2::text[])), 0) AS avg_response_time,
    count(*) FILTER (WHERE state = 'queued') AS queued,
    count(*) FILTER (WHERE state = 'leased') AS in_progress
FROM frontier
WHERE job = $1
"""

_STATUS_CODES = """
SELECT status, count(*) FROM frontier
WHERE job = $1 AND state = ANY($2::text[]) AND status IS NOT NULL
GROUP BY status ORDER BY status
"""
# Equal counts go by name, in the order of the code points, as `CrawlerStats` sorts them.
_ERRORS = """
SELECT error, count(*) FROM frontier
WHERE job = $1 AND state = 'failed'
GROUP BY error ORDER BY count(*) DESC, error COLLATE "C"
"""
_DOMAINS = """
SELECT host, count(*) FROM frontier
WHERE job = $1 AND state = ANY($2::text[])
GROUP BY host ORDER BY count(*) DESC, host COLLATE "C"
LIMIT $3
"""

# A worker runs while it renews its lease; one whose lease ran out without
# a stop was killed, or lost the database. The time since it was last
# seen counts as active while it runs.
_WORKERS = """
SELECT
    w.worker,
    CASE WHEN w.stopped_at IS NOT NULL THEN 'stopped' WHEN w.lease_until > now() THEN 'running' ELSE 'lost' END
        AS state,
    w.started_at,
    w.active_seconds + CASE
        WHEN w.stopped_at IS NULL AND w.lease_until > now() THEN extract(epoch FROM now() - w.seen_at) ELSE 0
    END AS active_seconds,
    count(f.url) FILTER (WHERE f.state IN ('processed', 'saving')) AS successful,
    count(f.url) FILTER (WHERE f.state = 'failed') AS failed,
    count(f.url) FILTER (WHERE f.state = 'skipped') AS skipped
FROM workers AS w
LEFT JOIN frontier AS f ON f.job = w.job AND f.worker = w.worker AND f.state = ANY($2::text[])
WHERE w.job = $1
GROUP BY w.job, w.worker
ORDER BY w.started_at, w.worker COLLATE "C"
"""


async def job_stats(dsn: str, job: str, *, top_domains: int = 10) -> dict[str, Any]:
    """The statistics of the crawl job `job`, from the database of `dsn`: those of all its workers together.

    The keys are those of `CrawlerStats.get_stats`, counted over the pages
    of the job: `total_pages`, `successful`, `failed`, `skipped`,
    `status_codes`, `errors`, `top_domains` and the rest. A page that
    failed as its lease expired `max_attempts` times has the error
    `LeaseExpired`. The time of the job runs from the start of its first
    worker to the end of the job, or to now while it runs: the pauses of
    a job stopped and resumed are a part of it.

    Besides, `job` is the name, `state` the state of the job (seeding,
    running or finished), `queued` the pages left to crawl, deferred ones
    included, `in_progress` those workers hold, and `workers` the workers
    in the order they started: name -> `state` (running, stopped, or lost:
    it stopped renewing its lease without a word, killed say), `started_at`,
    `active_seconds`, the time it ran, `pages`, `successful`, `failed`,
    `skipped` and `pages_per_second`. A page is counted for the worker that
    finished it. The pages of a host given up, finished in the database
    at once, and those failed as their leases expired are no worker's.

    Raises:
        JobError: there is no crawl job of that name.
        FrontierError: the database cannot be reached or failed.
    """
    with database_errors(job):
        connection = await asyncpg.connect(dsn)
        try:
            await create_schema(connection)
            # One snapshot: the workers go on meanwhile.
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                return await _read_stats(connection, job, top_domains)
        finally:
            await connection.close()


async def _read_stats(connection: Connection, job: str, top_domains: int) -> dict[str, Any]:
    row = await connection.fetchrow(_JOB, job)
    if row is None:
        raise JobError(f'There is no crawl job named "{job}"')
    job_id = row["id"]
    pages = await connection.fetchrow(_PAGES, job_id, _COUNTED)
    total = pages["successful"] + pages["failed"] + pages["skipped"]
    elapsed = float(row["elapsed"] or 0)
    workers = {}
    for worker in await connection.fetch(_WORKERS, job_id, _COUNTED):
        worker_pages = worker["successful"] + worker["failed"] + worker["skipped"]
        active = float(worker["active_seconds"])
        workers[worker["worker"]] = {
            "state": worker["state"],
            "started_at": worker["started_at"].isoformat(),
            "active_seconds": active,
            "pages": worker_pages,
            "successful": worker["successful"],
            "failed": worker["failed"],
            "skipped": worker["skipped"],
            "pages_per_second": worker_pages / active if active > 0 else 0.0,
        }
    return {
        "job": job,
        "state": row["state"],
        "total_pages": total,
        "successful": pages["successful"],
        "failed": pages["failed"],
        "skipped": pages["skipped"],
        "elapsed_seconds": elapsed,
        "pages_per_second": total / elapsed if elapsed > 0 else 0.0,
        "avg_response_time": float(pages["avg_response_time"]),
        "status_codes": dict(await connection.fetch(_STATUS_CODES, job_id, _COUNTED)),
        "errors": dict(await connection.fetch(_ERRORS, job_id)),
        "top_domains": dict(await connection.fetch(_DOMAINS, job_id, _COUNTED, top_domains)),
        "started_at": _iso(row["started_at"]),
        "finished_at": _iso(row["finished_at"]),
        "queued": pages["queued"],
        "in_progress": pages["in_progress"],
        "workers": workers,
    }


def export_job_stats(
    stats: Mapping[str, Any],
    *,
    stats_json: str | Path | None = None,
    html: str | Path | None = None,
    title: str = "Crawl report",
) -> list[Path]:
    """Write the statistics of `job_stats` to a JSON file and an HTML report, those given; return the files written.

    The directories of the files are created if missing, and the files
    replaced if they exist. The HTML report has a table of the workers.

    Raises:
        OSError: a file cannot be written.
    """
    written = []
    for path, render in ((stats_json, render_json), (html, lambda stats: render_html(stats, title=title))):
        if path is not None:
            file = make_directory(path)
            file.write_text(render(stats), encoding="utf-8")
            written.append(file)
    return written


def _iso(moment: datetime | None) -> str | None:
    return None if moment is None else moment.isoformat()
