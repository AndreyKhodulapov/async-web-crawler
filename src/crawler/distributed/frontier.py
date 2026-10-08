"""A frontier kept in PostgreSQL and shared by the workers of one crawl job."""

import asyncio
import contextlib
import dataclasses
import functools
import logging
import os
import secrets
import socket
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from typing import ParamSpec, TypeVar

import asyncpg

from crawler.distributed.schema import Connection, create_schema
from crawler.exceptions import JobError
from crawler.frontier import Admission, Frontier, FrontierPage, FrontierStats, GivenUp, HostFailures, Outcome
from crawler.queue import queue_form
from crawler.urls import get_host

logger = logging.getLogger(__name__)

# What the database, or the way to it, fails an operation with.
DATABASE_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError)

_P = ParamSpec("_P")
_R = TypeVar("_R")

# What a worker that got no page waits for, if for anything.
_WAIT_STATE = """
SELECT
    j.state = 'seeding' AS seeding,
    j.max_pages IS NOT NULL AND j.requested >= j.max_pages AS closed,
    EXISTS (
        SELECT FROM hosts AS h
        WHERE h.job = $1 AND (h.next_allowed_at <= now() OR h.given_up_outcome IS NOT NULL) AND EXISTS (
            SELECT FROM frontier AS f
            WHERE f.job = $1 AND f.host = h.host AND f.state = 'queued' AND f.not_before <= now()
        )
    ) AS ready,
    EXISTS (SELECT FROM frontier WHERE job = $1 AND state = 'queued') AS queued,
    EXISTS (
        SELECT FROM frontier WHERE job = $1 AND state IN ('leased', 'saving') AND worker <> $2
    ) AS others_busy,
    EXISTS (
        SELECT FROM frontier WHERE job = $1 AND state IN ('leased', 'saving') AND worker <> $2 AND counted
    ) AS others_counted,
    extract(epoch FROM least(
        (SELECT min(not_before) FROM frontier WHERE job = $1 AND state = 'queued' AND not_before > now()),
        (
            SELECT min(h.next_allowed_at) FROM hosts AS h
            WHERE h.job = $1 AND h.next_allowed_at > now() AND h.given_up_outcome IS NULL AND EXISTS (
                SELECT FROM frontier AS f WHERE f.job = $1 AND f.host = h.host AND f.state = 'queued'
            )
        ),
        (SELECT min(lease_until) FROM frontier WHERE job = $1 AND state IN ('leased', 'saving') AND worker <> $2)
    ) - now()) AS due
FROM crawl_jobs AS j
WHERE j.id = $1
"""

# The job is over: no page is in progress or pending its save, and none is
# left to hand out, or max_pages is reached.
_FINISH_JOB = """
UPDATE crawl_jobs AS j SET state = 'finished', finished_at = now()
WHERE j.id = $1 AND j.state = 'running'
    AND NOT EXISTS (SELECT FROM frontier WHERE job = $1 AND state IN ('leased', 'saving'))
    AND (
        j.max_pages IS NOT NULL AND j.requested >= j.max_pages
        OR NOT EXISTS (SELECT FROM frontier WHERE job = $1 AND state = 'queued')
    )
RETURNING true
"""

_UNCOUNT_HOSTS = """
UPDATE hosts SET requested = hosts.requested - page.uncounted
FROM unnest($2::text[], $3::int[]) AS page (host, uncounted)
WHERE hosts.job = $1 AND hosts.host = page.host
"""

# A host held back until a moment no earlier than the one it has; the
# reason is that of the later one, or the one it has if none is given. The
# host may have no pages yet: the target of a redirect, say.
_HOLD_HOST = """
INSERT INTO hosts AS h (job, host, next_allowed_at, hold_reason)
VALUES ($1, $2, now() + make_interval(secs => $3), $4)
ON CONFLICT (job, host) DO UPDATE
SET next_allowed_at = greatest(h.next_allowed_at, excluded.next_allowed_at),
    hold_reason = CASE
        WHEN excluded.next_allowed_at >= h.next_allowed_at THEN coalesce(excluded.hold_reason, h.hold_reason)
        ELSE h.hold_reason
    END
"""

# The interval of a host, never shorter than it was; its next page waits
# that long from now, as a request is about to be sent to it.
_SET_INTERVAL = """
INSERT INTO hosts AS h (job, host, interval, next_allowed_at)
VALUES ($1, $2, $3, now() + make_interval(secs => $3))
ON CONFLICT (job, host) DO UPDATE
SET interval = greatest(h.interval, excluded.interval),
    next_allowed_at = greatest(h.next_allowed_at, excluded.next_allowed_at)
"""

# The failures of a host one worker saw, added to those of the job; with
# $5, robots.txt was read since the failures counted, which start from
# zero. Failures told less than $6 (circuit) or $7 (robots.txt) seconds
# after the last ones counted are the same failure, seen by several
# workers at once: they are not counted, and the moment stays. The host
# may have no pages yet: the target of a redirect, say.
_COUNT_FAILURES = """
INSERT INTO hosts AS h (job, host, circuit_openings, robots_failures, circuit_counted_at, robots_counted_at)
VALUES (
    $1, $2, $3, $4,
    CASE WHEN $3 > 0 THEN now() ELSE '-infinity' END,
    CASE WHEN $4 > 0 THEN now() ELSE '-infinity' END
)
ON CONFLICT (job, host) DO UPDATE
SET circuit_openings = CASE
        WHEN h.circuit_counted_at <= now() - make_interval(secs => $6) THEN h.circuit_openings + excluded.circuit_openings
        ELSE h.circuit_openings
    END,
    circuit_counted_at = CASE
        WHEN excluded.circuit_openings > 0 AND h.circuit_counted_at <= now() - make_interval(secs => $6) THEN now()
        ELSE h.circuit_counted_at
    END,
    robots_failures = CASE
        WHEN $5::boolean THEN excluded.robots_failures
        WHEN h.robots_counted_at <= now() - make_interval(secs => $7) THEN h.robots_failures + excluded.robots_failures
        ELSE h.robots_failures
    END,
    robots_counted_at = CASE
        WHEN excluded.robots_failures > 0
            AND ($5::boolean OR h.robots_counted_at <= now() - make_interval(secs => $7)) THEN now()
        WHEN $5::boolean THEN '-infinity'
        ELSE h.robots_counted_at
    END
RETURNING circuit_openings, robots_failures
"""

# A host given up, unless it was already: the first outcome and reason stay.
_GIVE_UP_HOST = """
INSERT INTO hosts AS h (job, host, given_up_outcome, given_up_reason, given_up_error)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (job, host) DO UPDATE
SET given_up_outcome = excluded.given_up_outcome,
    given_up_reason = excluded.given_up_reason,
    given_up_error = excluded.given_up_error
WHERE h.given_up_outcome IS NULL
RETURNING true
"""

# The pages of a host given up; the targets of their redirects, if one
# was followed before the page went back, may be queued again.
_FINISH_HOST_PAGES = """
WITH finished AS (
    UPDATE frontier SET state = $3, reason = $4, error = $5, finished_at = now()
    WHERE job = $1 AND host = $2 AND state = 'queued'
    RETURNING url
), unseen AS (
    DELETE FROM frontier WHERE job = $1 AND state = 'seen' AND seen_from IN (SELECT url FROM finished)
)
SELECT count(*) FROM finished
"""

# A worker that stops puts its pages in progress back; those pending their
# save stay leased until their lease expires.
_PUT_BACK_ALL = """
WITH mine AS (
    SELECT url, host, counted FROM frontier WHERE job = $1 AND worker = $2 AND state = 'leased' FOR UPDATE
)
UPDATE frontier AS f
SET state = 'queued', worker = NULL, lease_until = NULL, counted = false, seq = nextval('frontier_seq')
FROM mine
WHERE f.job = $1 AND f.url = mine.url
RETURNING mine.host, mine.counted
"""

# A worker that looks for its first page joins the job, or comes back to
# it under the same name: the time it was away is not counted as active.
_JOIN = """
INSERT INTO workers (job, worker, lease_until) VALUES ($1, $2, now() + make_interval(secs => $3))
ON CONFLICT (job, worker) DO UPDATE SET seen_at = now(), lease_until = excluded.lease_until, stopped_at = NULL
"""

# A worker that runs renews its lease and counts the time since it was
# last seen as active; with $4, it stops.
_SEEN = """
UPDATE workers
SET active_seconds = active_seconds + extract(epoch FROM now() - seen_at),
    seen_at = now(),
    lease_until = now() + make_interval(secs => $3),
    stopped_at = CASE WHEN $4::boolean THEN now() END
WHERE job = $1 AND worker = $2
"""

# How long a worker that found a page ready, but locked by another worker, waits before it tries again.
_RETRY_DELAY = 0.005


def _waits_for_others(state: asyncpg.Record) -> bool:
    """Whether a worker that got no page waits for the pages of other workers alone, see `_WAIT_STATE`."""
    if state["seeding"]:
        return False
    if state["closed"]:
        return state["others_counted"]
    return state["others_busy"] and not state["queued"]


class FrontierDatabaseError(Exception):
    """An operation of a `PostgresFrontier` failed in the database, or could not reach it; the cause is what it failed with."""


def _database_operation(operation: Callable[_P, Awaitable[_R]]) -> Callable[_P, Awaitable[_R]]:
    """Raise what the database fails `operation` with as `FrontierDatabaseError`.

    Only the operations of the frontier raise it: an OSError of the crawl
    itself, in parsing a page say, is not taken for the database gone.
    """

    @functools.wraps(operation)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return await operation(*args, **kwargs)
        except DATABASE_ERRORS as error:
            raise FrontierDatabaseError(f"{type(error).__name__}: {error}") from error

    return wrapper


def worker_name() -> str:
    """A name for a worker, unlike that of any other: the host name, the process id and a random part."""
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(2)}"


class PostgresFrontier(Frontier):
    """A `Frontier` in a PostgreSQL database, shared by the workers of one job.

    Each worker opens its own with `open`, under the name of a job made by
    `create_job`, whose limits it takes. No page is handed out while the
    job is seeding. A page goes from `queued` to `leased`, taken by one
    worker until `lease_until`; the worker renews the leases of its pages
    every `heartbeat_seconds`. A page whose lease expires, because its
    worker stopped, is queued again and uncounted, and fails once it has
    expired `max_attempts` times: at least once, not exactly once. A page
    processed with `pending_save` is `saving`, leased all the same, until
    `saved`, or `dropped`: failed then, with the error `RecordDropped`.
    A host has one page taken every `host_interval` seconds, or its own
    interval from `set_host_interval` if that is longer, whichever worker
    takes it, and none while it is held back by `hold_host`. The failures
    of a host count over the whole job; a host
    given up has its pages finished, those queued at once and the others
    by the worker that takes them. Once nothing is left to hand out and no page
    is in progress, the job is finished; so it is once max_pages pages are
    requested and done. A page finished keeps the status and time of its
    response, the class of its error and the moment it was finished, and
    a worker joins the table of workers with its first `take` and counts
    its time there, for the report and the progress of the job (see
    `job_stats` and `job_progress`).

    Times are those of the database clock. `take` waits by polling, at
    least every `poll_interval` seconds, and at once after an operation of
    this frontier; while no page is ready, one task of the worker polls,
    and the others wait behind it in the process. `stats` is a snapshot
    of the job, refreshed every `stats_seconds` apart from the heartbeat,
    so that counting the pages of a large job never holds up the leases,
    once more as `take` finds nothing left for this worker, and by
    `refresh_stats`; `requested` is also brought up to date by `admit`. Taking a page, admitting, putting
    back and finishing it, and adding links are one call each, of a function in the database
    (see `procedures.py`): the rows of the job and the host are locked for
    as long as the function runs, not across round trips. Workers never
    deadlock: an operation on a page locks
    its row, then the job, then the host; adding links locks the job and
    inserts new rows only; giving a host up locks the job, the host, then
    its pages queued; `take` and the taking back of expired leases skip
    the rows other workers hold. The job is locked FOR NO KEY UPDATE, so
    that the inserts of other workers check their foreign keys on it
    without waiting (see docs/architecture.md).

    The frontier commits without waiting for the disk, so the rows of the
    job and its hosts are let go sooner. A crash of the database itself
    loses its last changes, of up to three times `wal_writer_delay`
    (0.6 s by default), and nothing else: those pages are handed out or
    finished once more, as after a lease expires. A storage commits in
    full before its pages are `saved`; a `saved` lost leaves them
    `saving` until their leases expire.

    An operation fails with `FrontierDatabaseError`, its one `ERRORS`,
    when the database cannot be reached or refuses it, the error of the
    database as its cause; it is not tried again: the worker stops, and
    its pages come back to the others once their leases expire. So it
    fails, with a `JobError` as its cause, once the job is deleted or
    restarted under the worker: its rows, those of its pages and hosts
    with them, are gone.

    A worker that takes back a page of its own whose lease expired, while
    another of its tasks still crawls it, leaves the page to that task:
    `take` looks for another one.

    A URL is a key of the database, and a key holds about 2.7 KB: a URL
    longer than `MAX_URL_LENGTH` is never kept. `seed`, `add` and
    `hold_out_of_scope` leave it out, and `mark_seen` takes it for new
    without remembering it, so a start URL may redirect to a sign-in page
    with a long token in its query, and that target alone is not deduplicated.
    """

    shared = True
    ERRORS = (FrontierDatabaseError,)
    # As long as a link the crawler follows (AsyncCrawler.MAX_URL_LENGTH);
    # a key of an index holds 2704 bytes, the job and the header included.
    MAX_URL_LENGTH = 2048

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        job: str,
        job_id: int,
        worker: str,
        max_pages: int | None,
        max_pages_per_host: int | None,
        frontier_factor: int,
        lease_seconds: float,
        heartbeat_seconds: float,
        max_attempts: int,
        host_interval: float,
        poll_interval: float,
        stats_seconds: float = 60.0,
    ) -> None:
        super().__init__(max_pages=max_pages, max_pages_per_host=max_pages_per_host, frontier_factor=frontier_factor)
        self.job = job
        self.job_id = job_id
        self.worker = worker
        self.lease_seconds = float(lease_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.max_attempts = max_attempts
        self.host_interval = float(host_interval)
        self.poll_interval = float(poll_interval)
        self.stats_seconds = float(stats_seconds)
        self._pool = pool
        self._max_queued = None if max_pages is None else frontier_factor * max_pages
        self._max_host_queued = None if max_pages_per_host is None else frontier_factor * max_pages_per_host
        self._held: dict[str, int] = {}  # pages taken and not finished, with their waits
        self._given_up: dict[str, GivenUp] = {}  # pages held whose host is given up
        self._counted: set[str] = set()  # pages held and counted toward the limits
        self._putting_back = 0  # pages let go whose put_back the database has not answered yet
        # Set by an operation of this frontier, then replaced: each `take`
        # waits for the one it saw before it looked, which no other clears.
        self._wakeup = asyncio.Event()
        # Held by the one task of this worker that polls for a page, see `take`.
        self._polling = asyncio.Lock()
        # The heartbeat, saved and close each change many rows of this
        # worker: one at a time, or two of them may deadlock.
        self._rows_lock = asyncio.Lock()
        self._stats = FrontierStats()
        self._scope_version = 0
        self._scope_hosts: list[str] = []
        self._drop_logged = False
        self._heartbeat: asyncio.Task[None] | None = None
        self._refreshing: asyncio.Task[None] | None = None
        self._joined = False  # whether this worker is in the table of workers, see `take`
        self._closed = False

    @classmethod
    @_database_operation
    async def open(
        cls,
        dsn: str,
        *,
        job: str,
        worker: str | None = None,
        lease_seconds: float = 60.0,
        heartbeat_seconds: float = 20.0,
        max_attempts: int = 3,
        host_interval: float = 0.0,
        poll_interval: float = 1.0,
        pool_size: int = 4,
        stats_seconds: float = 60.0,
    ) -> "PostgresFrontier":
        """Connect a worker to the job named `job`; the limits are those of the job.

        `worker` names this worker in the database; by default it is made
        of the host name, the process id and a random part. The worker
        holds at most `pool_size` connections: its other tasks wait for one
        in the process, not for the row of the job in the database. They
        commit without waiting for the disk (`synchronous_commit` off).
        `stats` is refreshed every `stats_seconds`, see the class.

        Raises:
            JobError: there is no job of that name.
        """
        pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=pool_size,
            reset=_keep_session,
            server_settings={"synchronous_commit": "off"},
        )
        try:
            async with pool.acquire() as connection:
                await create_schema(connection)
                row = await connection.fetchrow(
                    "SELECT id, max_pages, max_pages_per_host, frontier_factor FROM crawl_jobs WHERE name = $1", job
                )
            if row is None:
                raise JobError(f'There is no crawl job named "{job}"')
            frontier = cls(
                pool,
                job=job,
                job_id=row["id"],
                worker=worker or worker_name(),
                max_pages=row["max_pages"],
                max_pages_per_host=row["max_pages_per_host"],
                frontier_factor=row["frontier_factor"],
                lease_seconds=lease_seconds,
                heartbeat_seconds=heartbeat_seconds,
                max_attempts=max_attempts,
                host_interval=host_interval,
                poll_interval=poll_interval,
                stats_seconds=stats_seconds,
            )
            await frontier.refresh_stats()
            async with pool.acquire() as connection:
                await frontier._read_scope(connection)
        except BaseException:
            await pool.close()
            raise
        frontier._heartbeat = asyncio.create_task(frontier._beat())
        frontier._refreshing = asyncio.create_task(frontier._refresh())
        return frontier

    @_database_operation
    async def seed(self, urls: Iterable[str]) -> list[str]:
        pages = self._pages_of(urls)
        await self._add(self._pool, pages, depth=0, bounded=False)
        self._wake()
        return list(pages)

    @_database_operation
    async def add(self, urls: Iterable[str], *, depth: int) -> int:
        pages = self._pages_of(urls)
        if not pages:
            return 0
        accepted, dropped, accepted_by_host = await self._add(self._pool, pages, depth=depth)
        self._added(accepted, dropped, accepted_by_host)
        return accepted

    async def _add(
        self, executor: Connection | asyncpg.Pool, pages: dict[str, str], *, depth: int, bounded: bool = True
    ) -> tuple[int, int, dict[str, int]]:
        """Accept the pages found at `depth`, as far as the bounds allow unless not `bounded`, see `frontier_add`.

        Returns the number accepted, the number dropped as the frontier is
        full, and the pages accepted by host since the job began, for `_added`.
        """
        row = await executor.fetchrow(
            "SELECT * FROM frontier_add($1, $2, $3, $4, $5)",
            self.job_id,
            list(pages),
            list(pages.values()),
            depth,
            bounded,
        )
        return row["accepted"], row["dropped"], dict(zip(row["accepted_hosts"], row["host_accepted"], strict=True))

    @_database_operation
    async def take(self) -> FrontierPage | None:
        async with self._pool.acquire() as connection:
            if not self._joined:
                # Not on open: the process that seeds the job opens a frontier, but takes no page.
                await connection.execute(_JOIN, self.job_id, self.worker, self.lease_seconds)
                self._joined = True
            page = await self._take_ready(connection)
        if page is not None:
            return page
        # None is ready: one task of this worker polls for pages, the others
        # wait behind it and look themselves once it has taken one.
        async with self._polling:
            page = await self._poll()
        if page is None:
            # The counts the summary of the crawl reports; on a connection of
            # its own, and with the polling let go, so the other tasks end meanwhile.
            await self.refresh_stats()
        return page

    async def _take_ready(self, connection: Connection) -> FrontierPage | None:
        """Take a page whose turn has come; None if there is none."""
        while True:
            row = await connection.fetchrow(
                "SELECT * FROM frontier_take($1, $2, $3, $4, $5)",
                self.job_id,
                self.worker,
                self.lease_seconds,
                self.host_interval,
                self.max_attempts,
            )
            self._log_reclaimed(row)
            if row["url"] in self._held:
                # Its lease expired while another task of this worker
                # crawls it, and was given back to this worker: that task
                # finishes it. Taken back, it was uncounted.
                logger.info("Lease of %s expired and came back to this worker: the page is crawled once", row["url"])
                self._counted.discard(row["url"])
                continue
            if row["url"] is None:
                return None
            self._held[row["url"]] = row["waits"]
            if row["given_up_outcome"] is not None:
                self._given_up[row["url"]] = GivenUp(
                    Outcome(row["given_up_outcome"]), row["given_up_reason"], row["given_up_error"]
                )
            if row["scope_version"] != self._scope_version:
                await self._read_scope(connection)
            return FrontierPage(row["url"], row["depth"])

    async def _poll(self) -> FrontierPage | None:
        """Take a page once one is ready; None once nothing is left for this worker to wait for."""
        while True:
            # Seen before looking: an operation of this frontier meanwhile wakes the wait below.
            wakeup = self._wakeup
            async with self._pool.acquire() as connection:
                if (page := await self._take_ready(connection)) is not None:
                    return page
                state = self._job_row(await connection.fetchrow(_WAIT_STATE, self.job_id, self.worker))
                delay = self._delay(state)
                if delay is None:
                    await self._finish_job(connection)
                    return None
            if self.on_waiting is not None and _waits_for_others(state):
                # Two workers, each with pages pending their save, would
                # otherwise wait for each other for good: the heartbeat
                # renews the leases of those pages.
                await self.on_waiting()
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(delay):
                    await wakeup.wait()

    @_database_operation
    async def admit(self, page: FrontierPage) -> Admission:
        row = await self._pool.fetchrow("SELECT * FROM frontier_admit($1, $2, $3)", self.job_id, page.url, self.worker)
        match row["admission"]:
            case "over_max_pages":
                return Admission.OVER_MAX_PAGES
            case "lease_lost":
                # Not requested: the caller puts it back, which finds the lease lost and tells so.
                return Admission.OVER_MAX_PAGES
            case "over_host_limit":
                return Admission.OVER_HOST_LIMIT
        self._counted.add(page.url)
        self._stats = dataclasses.replace(self._stats, requested=row["requested"])
        return Admission.ADMITTED

    @_database_operation
    async def put_back(self, page: FrontierPage, delay: float = 0.0, *, uncount: bool, waited: bool = False) -> None:
        self._check_held(page)
        uncount = uncount and page.url in self._counted
        # Let go before it is queued: another task of this worker may take
        # it once the database queues it, before the answer comes here.
        # Still held then, it would be left to this task, which is done with it.
        self._release(page)
        self._putting_back += 1
        try:
            row = await self._pool.fetchrow(
                "SELECT * FROM frontier_put_back($1, $2, $3, $4, $5, $6)",
                self.job_id,
                page.url,
                self.worker,
                float(delay),
                uncount,
                waited,
            )
        finally:
            self._putting_back -= 1
        self._left(page, row, uncount)

    @_database_operation
    async def finish(
        self,
        page: FrontierPage,
        outcome: Outcome,
        reason: str | None = None,
        *,
        uncount: bool = False,
        pending_save: bool = False,
        status: int | None = None,
        elapsed: float | None = None,
        error: str | None = None,
    ) -> None:
        if outcome is not Outcome.PROCESSED and reason is None:
            raise ValueError(f"a page {outcome.value} needs a reason")
        self._check_held(page)
        uncount = uncount and page.url in self._counted
        state = "saving" if pending_save and outcome is Outcome.PROCESSED else outcome.value
        try:
            row = await self._pool.fetchrow(
                "SELECT * FROM frontier_finish($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                self.job_id,
                page.url,
                self.worker,
                state,
                reason,
                uncount,
                status,
                elapsed,
                error,
            )
        finally:
            self._release(page)
        self._left(page, row, uncount)

    @_database_operation
    async def saved(self, urls: Iterable[str]) -> None:
        urls = list(urls)
        if not urls:
            return
        # Only pages of this worker: the storage may report the records of
        # an earlier crawl, and those of another worker are its own business.
        async with self._rows_lock:
            await self._pool.execute(
                "UPDATE frontier SET state = 'processed', lease_until = NULL"
                " WHERE job = $1 AND worker = $2 AND state = 'saving' AND url = ANY($3::text[])",
                self.job_id,
                self.worker,
                urls,
            )

    @_database_operation
    async def dropped(self, urls: Iterable[str]) -> None:
        urls = list(urls)
        if not urls:
            return
        async with self._rows_lock:
            await self._pool.execute(
                "UPDATE frontier SET state = 'failed', reason = 'its record could not be stored',"
                " error = 'RecordDropped', lease_until = NULL"
                " WHERE job = $1 AND worker = $2 AND state = 'saving' AND url = ANY($3::text[])",
                self.job_id,
                self.worker,
                urls,
            )

    def waits(self, page: FrontierPage) -> int:
        self._check_held(page)
        return self._held[page.url]

    @_database_operation
    async def mark_seen(self, url: str, source: str) -> bool:
        form = queue_form(url)
        if form is None:
            return False
        if len(form) > self.MAX_URL_LENGTH:
            # Not a key the database can hold: new every time, as seen nowhere.
            return True
        # A row seen from the same page is updated to itself, so that it is returned.
        new = await self._pool.fetchval(
            "INSERT INTO frontier AS f (job, url, host, state, seen_from) VALUES ($1, $2, $3, 'seen', $4)"
            " ON CONFLICT (job, url) DO UPDATE SET seen_from = excluded.seen_from"
            " WHERE f.state = 'seen' AND f.seen_from = excluded.seen_from"
            " RETURNING true",
            self.job_id,
            form,
            get_host(form),
            source,
        )
        return new is not None

    @_database_operation
    async def forget(self, url: str, source: str) -> None:
        form = queue_form(url)
        if form is not None:
            await self._pool.execute(
                "DELETE FROM frontier WHERE job = $1 AND url = $2 AND state = 'seen' AND seen_from = $3",
                self.job_id,
                form,
                source,
            )

    @_database_operation
    async def is_pending_or_processed(self, url: str) -> bool:
        form = queue_form(url)
        if form is None:
            return False
        state = await self._pool.fetchval("SELECT state FROM frontier WHERE job = $1 AND url = $2", self.job_id, form)
        return state in ("queued", "leased", "saving", "processed")

    @_database_operation
    async def full(self) -> bool:
        if self._max_queued is None:
            return False
        job = self._job_row(
            await self._pool.fetchrow("SELECT requested, unfinished FROM crawl_jobs WHERE id = $1", self.job_id)
        )
        return not self._closed_at(job["requested"]) and job["unfinished"] + job["requested"] >= self._max_queued

    @_database_operation
    async def hold_out_of_scope(self, urls: Iterable[str]) -> None:
        # A URL too long to keep is turned away by the filter whatever the scope.
        urls = [url for url in dict.fromkeys(urls) if len(url) <= self.MAX_URL_LENGTH]
        if urls:
            await self._pool.execute(
                "INSERT INTO out_of_scope (job, url) SELECT $1, page.url"
                " FROM unnest($2::text[]) WITH ORDINALITY AS page (url, position) ORDER BY page.position"
                " ON CONFLICT DO NOTHING",
                self.job_id,
                urls,
            )

    @_database_operation
    async def widen_scope(self, host: str, allows: Callable[[str], bool]) -> int:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                # The job first, as every addition locks it: the pages held are queued by one worker at a time.
                await self._lock_job(connection)
                added = await connection.fetchval(
                    "INSERT INTO job_scope (job, host) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING true",
                    self.job_id,
                    host,
                )
                if added:
                    await connection.execute(
                        "UPDATE crawl_jobs SET scope_version = scope_version + 1 WHERE id = $1", self.job_id
                    )
                held = await connection.fetch(
                    "SELECT url FROM out_of_scope WHERE job = $1 ORDER BY position", self.job_id
                )
                in_scope = [row["url"] for row in held if allows(row["url"])]
                await connection.execute(
                    "DELETE FROM out_of_scope WHERE job = $1 AND url = ANY($2::text[])", self.job_id, in_scope
                )
                accepted, dropped, accepted_by_host = await self._add(connection, self._pages_of(in_scope), depth=0)
            await self._read_scope(connection)
        self._added(accepted, dropped, accepted_by_host)
        return accepted

    def scope_hosts(self) -> list[str]:
        return list(self._scope_hosts)

    @_database_operation
    async def hold_host(self, host: str, seconds: float, reason: str | None) -> None:
        # A statement of its own: it locks the host alone, so it may wait
        # for a take() or admit() that holds it, but takes part in no deadlock.
        await self._pool.execute(_HOLD_HOST, self.job_id, host, float(seconds), reason)

    @_database_operation
    async def set_host_interval(self, host: str, seconds: float) -> None:
        # A statement of its own on the host alone, as in hold_host.
        await self._pool.execute(_SET_INTERVAL, self.job_id, host, float(seconds))

    @_database_operation
    async def count_host_failures(
        self,
        host: str,
        *,
        circuit_openings: int = 0,
        robots_failures: int = 0,
        robots_read: bool = False,
        circuit_apart: float = 0.0,
        robots_apart: float = 0.0,
    ) -> HostFailures:
        # A statement of its own on the host alone, as in hold_host.
        row = await self._pool.fetchrow(
            _COUNT_FAILURES,
            self.job_id,
            host,
            circuit_openings,
            robots_failures,
            robots_read,
            float(circuit_apart),
            float(robots_apart),
        )
        return HostFailures(row["circuit_openings"], row["robots_failures"])

    @_database_operation
    async def give_up_host(self, host: str, outcome: Outcome, reason: str, *, error: str | None = None) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            # The job before the host, as a page in progress is finished:
            # see the order of the locks in the docstring of the class.
            await self._lock_job(connection)
            if not await connection.fetchval(_GIVE_UP_HOST, self.job_id, host, outcome.value, reason, error):
                return
            finished = await connection.fetchval(_FINISH_HOST_PAGES, self.job_id, host, outcome.value, reason, error)
            await connection.execute(
                "UPDATE crawl_jobs SET unfinished = unfinished - $2 WHERE id = $1", self.job_id, finished
            )
        logger.warning(
            "Gave up on %s for the whole job: %s; %d pages queued are %s", host, reason, finished, outcome.value
        )
        self._wake()

    def given_up(self, page: FrontierPage) -> GivenUp | None:
        self._check_held(page)
        return self._given_up.get(page.url)

    def stats(self) -> FrontierStats:
        return self._stats

    @_database_operation
    async def refresh_stats(self) -> None:
        """Read the counts of the job's pages by state, for `stats`."""
        async with self._pool.acquire() as connection:
            counts = dict(
                await connection.fetch(
                    "SELECT state, count(*) FROM frontier WHERE job = $1 GROUP BY state", self.job_id
                )
            )
            job = self._job_row(
                await connection.fetchrow(
                    "SELECT requested, over_host_limit, links_dropped, links_dropped_by_host"
                    " FROM crawl_jobs WHERE id = $1",
                    self.job_id,
                )
            )
        self._stats = FrontierStats(
            queued=counts.get("queued", 0),
            in_progress=counts.get("leased", 0),
            # A page pending its save is processed, see Frontier.finish.
            processed=counts.get("processed", 0) + counts.get("saving", 0),
            failed=counts.get("failed", 0),
            skipped=counts.get("skipped", 0),
            blocked=counts.get("blocked", 0),
            unreachable=counts.get("unreachable", 0),
            requested=job["requested"],
            over_host_limit=job["over_host_limit"],
            links_dropped=job["links_dropped"],
            links_dropped_by_host=job["links_dropped_by_host"],
        )

    async def close(self) -> None:
        """Put the pages this worker has in progress back, uncounted, and disconnect.

        Pages pending their save stay leased: their records may still be
        written, and if not, the pages come back once the leases expire.
        The job is finished if this worker had its last pages. A database
        that cannot be reached is logged, not raised: the pages in progress
        come back once their leases expire too.
        """
        if self._closed:
            return
        self._closed = True
        for task in (self._heartbeat, self._refreshing):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        try:
            async with self._rows_lock, self._pool.acquire() as connection:
                async with connection.transaction():
                    rows = await connection.fetch(_PUT_BACK_ALL, self.job_id, self.worker)
                    await self._uncount_all(connection, [row["host"] for row in rows if row["counted"]])
                if rows:
                    logger.info("Worker %s stopped: %d pages in progress are queued again", self.worker, len(rows))
                # The last pages of the job may have been pending their save until now.
                await self._finish_job(connection)
                await connection.execute(_SEEN, self.job_id, self.worker, self.lease_seconds, True)
        except DATABASE_ERRORS as error:
            logger.warning(
                "Could not put back the pages worker %s has in progress: %s; they come back once their leases expire",
                self.worker,
                error,
            )
        finally:
            self._held.clear()
            self._given_up.clear()
            self._counted.clear()
            self._wake()
            await self._pool.close()

    async def _read_scope(self, connection: Connection) -> None:
        """Learn the hosts brought into the scope of the job, and the version of the scope they make."""
        row = await connection.fetchrow(
            "SELECT scope_version,"
            " array(SELECT host FROM job_scope WHERE job = $1 ORDER BY position) AS hosts"
            " FROM crawl_jobs WHERE id = $1",
            self.job_id,
        )
        self._scope_version, self._scope_hosts = row["scope_version"], list(row["hosts"])

    async def _finish_job(self, connection: Connection) -> None:
        """Mark the job finished if nothing is left to do in it."""
        if await connection.fetchval(_FINISH_JOB, self.job_id):
            logger.info("Crawl job %s is finished", self.job)

    async def _lock_job(self, connection: Connection) -> asyncpg.Record:
        """Lock the row of the job for the transaction of `connection`; returns its counts of pages."""
        # Not FOR UPDATE: a statement inserting a row of the job, such as
        # mark_seen, checks its foreign key by locking the job FOR KEY SHARE.
        # It would wait for this worker while holding its new row, which
        # this worker may insert next: each would wait for the other.
        return await connection.fetchrow(
            "SELECT requested, unfinished FROM crawl_jobs WHERE id = $1 FOR NO KEY UPDATE", self.job_id
        )

    async def _uncount_all(self, connection: Connection, hosts: list[str]) -> None:
        """Uncount pages, one per host listed, from the limits of the job and their hosts."""
        if not hosts:
            return
        await connection.execute(
            "UPDATE crawl_jobs SET requested = requested - $2 WHERE id = $1", self.job_id, len(hosts)
        )
        by_host = Counter(hosts)
        await connection.execute(_UNCOUNT_HOSTS, self.job_id, list(by_host), list(by_host.values()))

    async def _beat(self) -> None:
        """Renew the leases of this worker and of its pages every `heartbeat_seconds`."""
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            try:
                async with self._rows_lock:
                    await self._pool.execute(
                        "UPDATE frontier SET lease_until = now() + make_interval(secs => $3)"
                        " WHERE job = $1 AND worker = $2 AND state IN ('leased', 'saving')",
                        self.job_id,
                        self.worker,
                        self.lease_seconds,
                    )
                await self._pool.execute(_SEEN, self.job_id, self.worker, self.lease_seconds, False)
            except Exception:
                # The leases are renewed at the next beat; if the database
                # is gone for longer, they expire and others crawl the pages.
                logger.warning("Heartbeat of %s failed", self.worker, exc_info=True)

    async def _refresh(self) -> None:
        """Refresh the stats every `stats_seconds`: a task of its own, as counting the pages of a large job takes long."""
        while True:
            await asyncio.sleep(self.stats_seconds)
            try:
                await self.refresh_stats()
            except FrontierDatabaseError as error:
                # The stats stay as they were until the next refresh.
                logger.warning("Stats of crawl job %s could not be refreshed: %s", self.job, error)

    def _delay(self, state: asyncpg.Record) -> float | None:
        """How long `take` waits before it looks again; None if there is nothing left to wait for."""
        due = self.poll_interval if state["due"] is None else min(max(float(state["due"]), 0.0), self.poll_interval)
        if state["seeding"]:
            return self.poll_interval
        if state["closed"]:
            # Pages other workers counted may go back under the limit; this
            # worker's own pages come back only by its own hand, as in memory.
            return due if state["others_counted"] else None
        if state["ready"]:
            return _RETRY_DELAY
        if state["queued"] or self._held or self._putting_back or state["others_busy"]:
            return due
        return None

    def _job_row(self, row: asyncpg.Record | None) -> asyncpg.Record:
        """`row`, read from the row of the job; raises FrontierDatabaseError if the job was deleted, or restarted, under this worker."""
        if row is None:
            error = JobError(f'Crawl job "{self.job}" no longer exists: it was deleted or restarted')
            raise FrontierDatabaseError(f"{type(error).__name__}: {error}") from error
        return row

    def _closed_at(self, requested: int) -> bool:
        """Whether `requested` pages reach max_pages: no page is handed out or accepted until one is uncounted."""
        return self.max_pages is not None and requested >= self.max_pages

    def _pages_of(self, urls: Iterable[str]) -> dict[str, str]:
        """The URLs in the form the frontier keeps them, each with its host; invalid ones, repeats and those too long left out."""
        pages = {}
        for url in urls:
            form = queue_form(url)
            if form is None or form in pages:
                continue
            if len(form) > self.MAX_URL_LENGTH:
                logger.debug("Not queued, longer than %d characters: %s", self.MAX_URL_LENGTH, form)
                continue
            host = get_host(form)
            assert host is not None  # a valid URL has a host
            pages[form] = host
        return pages

    def in_progress(self, page: FrontierPage) -> bool:
        return page.url in self._held

    def _wake(self) -> None:
        """Wake every `take` that waits, and those that look meanwhile once they wait."""
        self._wakeup.set()
        self._wakeup = asyncio.Event()

    def _check_held(self, page: FrontierPage) -> None:
        if page.url not in self._held:
            raise ValueError(f"{page.url} is not in progress")

    def _release(self, page: FrontierPage) -> None:
        self._held.pop(page.url, None)
        self._given_up.pop(page.url, None)
        self._counted.discard(page.url)

    def _added(self, accepted: int, dropped: int, accepted_by_host: dict[str, int]) -> None:
        """Tell of the bounds an addition reached, and wake the waiting workers if pages were accepted; after it commits."""
        self._log_bounds(dropped, accepted_by_host)
        if accepted:
            self._wake()

    def _left(self, page: FrontierPage, row: asyncpg.Record, uncount: bool) -> None:
        """Tell of the lease of a page put back or finished that was lost, or of `requested` it uncounted."""
        if not row["held"]:
            _log_lease_lost(page)
        elif uncount:
            self._stats = dataclasses.replace(self._stats, requested=row["requested"])
        self._wake()

    def _log_reclaimed(self, row: asyncpg.Record) -> None:
        """Tell of the pages whose leases expired, which `take` queued again or failed."""
        reclaimed = zip(row["reclaimed_urls"], row["reclaimed_states"], row["reclaimed_workers"], strict=True)
        for url, state, worker in reclaimed:
            if state == "failed":
                logger.warning("Lease of %s by %s expired %d times: the page failed", url, worker, self.max_attempts)
            else:
                logger.warning("Lease of %s by %s expired: the page is queued again", url, worker)

    def _log_bounds(self, dropped: int, accepted_by_host: dict[str, int]) -> None:
        if dropped and not self._drop_logged:
            self._drop_logged = True
            logger.info(
                "Queue is full: pages queued, in progress and requested reached %d (%d x max_pages); "
                "new links are not queued until it has room",
                self._max_queued,
                self.frontier_factor,
            )
        for host, accepted in accepted_by_host.items():
            if accepted == self._max_host_queued:
                logger.info(
                    "Host %s has %d pages queued (%d x max_pages_per_host): its new links are not queued",
                    host,
                    self._max_host_queued,
                    self.frontier_factor,
                )


async def _keep_session(connection: asyncpg.Connection) -> None:
    """Leave a connection as it is when it goes back to the pool, saving a round trip an operation.

    asyncpg would reset the session: the frontier sets nothing in it that
    outlives a transaction (no settings but those it connects with, session
    locks, cursors or LISTEN).
    A transaction left open is still rolled back.
    """


def _log_lease_lost(page: FrontierPage) -> None:
    logger.warning("Lease of %s expired before the page was finished: it is crawled again", page.url)
