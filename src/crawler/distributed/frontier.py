"""A frontier kept in PostgreSQL and shared by the workers of one crawl job."""

import asyncio
import contextlib
import dataclasses
import logging
import os
import secrets
import socket
from collections import Counter
from collections.abc import Iterable

import asyncpg

from crawler.distributed.schema import Connection, create_schema
from crawler.frontier import Admission, Frontier, FrontierPage, FrontierStats, Outcome
from crawler.queue import queue_form
from crawler.urls import get_host

logger = logging.getLogger(__name__)

# The ready host with the shallowest page, locked so that no other worker
# takes a page of it at the same moment; the page locked and leased; the
# host's next turn moved on by its interval. The page is read from the
# snapshot of the statement: another worker may have taken it since, and
# may hold its row, waiting for the host this one holds. So its row is
# locked skipping, never waited for, and is leased only if still queued;
# otherwise nothing is taken and the worker looks again.
_TAKE = """
WITH page AS (
    SELECT h.host, p.url
    FROM hosts AS h
    CROSS JOIN LATERAL (
        SELECT f.url, f.depth, f.seq
        FROM frontier AS f
        WHERE f.job = h.job AND f.host = h.host AND f.state = 'queued' AND f.not_before <= now()
        ORDER BY f.depth, f.seq
        LIMIT 1
    ) AS p
    WHERE h.job = $1 AND h.next_allowed_at <= now()
        AND NOT EXISTS (SELECT FROM crawl_jobs AS j WHERE j.id = $1 AND j.requested >= j.max_pages)
    ORDER BY p.depth, p.seq
    LIMIT 1
    FOR UPDATE OF h SKIP LOCKED
), queued AS (
    SELECT f.url
    FROM frontier AS f
    JOIN page ON f.url = page.url
    WHERE f.job = $1 AND f.state = 'queued'
    FOR UPDATE OF f SKIP LOCKED
), leased AS (
    UPDATE frontier AS f
    SET state = 'leased', worker = $2, lease_until = now() + make_interval(secs => $3)
    FROM queued
    WHERE f.job = $1 AND f.url = queued.url
    RETURNING f.url, f.depth, f.host
), turn AS (
    UPDATE hosts AS h
    SET next_allowed_at = now() + make_interval(secs => $4)
    FROM leased
    WHERE h.job = $1 AND h.host = leased.host
)
SELECT url, depth FROM leased
"""

# What a worker that got no page waits for, if for anything.
_WAIT_STATE = """
SELECT
    j.max_pages IS NOT NULL AND j.requested >= j.max_pages AS closed,
    EXISTS (
        SELECT FROM hosts AS h
        WHERE h.job = $1 AND h.next_allowed_at <= now() AND EXISTS (
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
            WHERE h.job = $1 AND h.next_allowed_at > now() AND EXISTS (
                SELECT FROM frontier AS f WHERE f.job = $1 AND f.host = h.host AND f.state = 'queued'
            )
        ),
        (SELECT min(lease_until) FROM frontier WHERE job = $1 AND state IN ('leased', 'saving') AND worker <> $2)
    ) - now()) AS due
FROM crawl_jobs AS j
WHERE j.id = $1
"""

# Pages whose worker stopped renewing their leases: queued again, or
# failed after max_attempts. Rows locked by their worker are left for the next time.
_RECLAIM = """
WITH expired AS (
    SELECT url, host, state, worker, counted
    FROM frontier
    WHERE job = $1 AND state IN ('leased', 'saving') AND lease_until < now()
    FOR UPDATE SKIP LOCKED
)
UPDATE frontier AS f
SET state = CASE WHEN f.attempts + 1 >= $2 THEN 'failed' ELSE 'queued' END,
    reason = CASE WHEN f.attempts + 1 >= $2 THEN format('lease expired %s times', f.attempts + 1) END,
    attempts = f.attempts + 1,
    worker = NULL,
    lease_until = NULL,
    counted = false,
    not_before = '-infinity',
    seq = nextval('frontier_seq')
FROM expired
WHERE f.job = $1 AND f.url = expired.url
RETURNING f.url, f.state, expired.state AS was, expired.worker, expired.host, expired.counted
"""

_INSERT = """
INSERT INTO frontier (job, url, host, depth, state)
SELECT $1, page.url, page.host, $4, 'queued'
FROM unnest($2::text[], $3::text[]) WITH ORDINALITY AS page (url, host, position)
ORDER BY page.position
ON CONFLICT DO NOTHING
RETURNING url
"""

_COUNT_ACCEPTED = """
INSERT INTO hosts (job, host, accepted)
SELECT $1, page.host, page.accepted FROM unnest($2::text[], $3::int[]) AS page (host, accepted)
ON CONFLICT (job, host) DO UPDATE SET accepted = hosts.accepted + excluded.accepted
RETURNING host, accepted
"""

_UNCOUNT_HOSTS = """
UPDATE hosts SET requested = hosts.requested - page.uncounted
FROM unnest($2::text[], $3::int[]) AS page (host, uncounted)
WHERE hosts.job = $1 AND hosts.host = page.host
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

# How long a worker that found a page ready, but locked by another worker, waits before it tries again.
_RETRY_DELAY = 0.005


class _LeaseLost(Exception):
    """The page is no longer leased to this worker: the lease expired and the page went back to the queue."""


class _OverHostLimit(Exception):
    """The host of the page has max_pages_per_host pages counted."""


class PostgresFrontier(Frontier):
    """A `Frontier` in a PostgreSQL database, shared by the workers of one job.

    Each worker opens its own with `open`, under the same job name; the
    first one makes the tables and the job, and the limits of an existing
    job are kept. A page goes from `queued` to `leased`, taken by one
    worker until `lease_until`; the worker renews the leases of its pages
    every `heartbeat_seconds`. A page whose lease expires, because its
    worker stopped, is queued again and uncounted, and fails once it has
    expired `max_attempts` times: at least once, not exactly once. A page
    processed with `pending_save` is `saving`, leased all the same, until
    `saved`. A host has one page taken every `host_interval` seconds,
    whichever worker takes it.

    Times are those of the database clock. `take` waits by polling, at
    least every `poll_interval` seconds, and at once after an operation of
    this frontier. `stats` is a snapshot of the job, refreshed by the
    heartbeat and by `refresh_stats`; `requested` is also brought up to
    date by `admit`. Workers never deadlock: an operation on a page locks
    its row, then the job, then the host; adding links locks the job and
    inserts new rows only; `take` and the taking back of expired leases
    skip the rows other workers hold (see docs/architecture.md).
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
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
    ) -> None:
        super().__init__(max_pages=max_pages, max_pages_per_host=max_pages_per_host, frontier_factor=frontier_factor)
        self.job_id = job_id
        self.worker = worker
        self.lease_seconds = float(lease_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.max_attempts = max_attempts
        self.host_interval = float(host_interval)
        self.poll_interval = float(poll_interval)
        self._pool = pool
        self._max_queued = None if max_pages is None else frontier_factor * max_pages
        self._max_host_queued = None if max_pages_per_host is None else frontier_factor * max_pages_per_host
        self._held: set[str] = set()  # pages taken and not finished
        self._counted: set[str] = set()  # pages held and counted toward the limits
        self._wakeup = asyncio.Event()
        # The heartbeat, saved and close each change many rows of this
        # worker: one at a time, or two of them may deadlock.
        self._rows_lock = asyncio.Lock()
        self._stats = FrontierStats()
        self._drop_logged = False
        self._heartbeat: asyncio.Task[None] | None = None
        self._closed = False

    @classmethod
    async def open(
        cls,
        dsn: str,
        *,
        job: str,
        worker: str | None = None,
        max_pages: int | None = None,
        max_pages_per_host: int | None = None,
        frontier_factor: int = Frontier.FRONTIER_FACTOR,
        lease_seconds: float = 60.0,
        heartbeat_seconds: float = 20.0,
        max_attempts: int = 3,
        host_interval: float = 0.0,
        poll_interval: float = 1.0,
        pool_size: int = 10,
    ) -> "PostgresFrontier":
        """Connect a worker to the job named `job`, making the tables and the job if they are missing.

        The limits are those of a new job; an existing one keeps its own.
        `worker` names this worker in the database; by default it is made
        of the host name, the process id and a random part.
        """
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=pool_size)
        try:
            async with pool.acquire() as connection:
                await create_schema(connection)
                await connection.execute(
                    "INSERT INTO crawl_jobs (name, max_pages, max_pages_per_host, frontier_factor)"
                    " VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO NOTHING",
                    job,
                    max_pages,
                    max_pages_per_host,
                    frontier_factor,
                )
                row = await connection.fetchrow(
                    "SELECT id, max_pages, max_pages_per_host, frontier_factor FROM crawl_jobs WHERE name = $1", job
                )
        except BaseException:
            await pool.close()
            raise
        frontier = cls(
            pool,
            job_id=row["id"],
            worker=worker or f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(2)}",
            max_pages=row["max_pages"],
            max_pages_per_host=row["max_pages_per_host"],
            frontier_factor=row["frontier_factor"],
            lease_seconds=lease_seconds,
            heartbeat_seconds=heartbeat_seconds,
            max_attempts=max_attempts,
            host_interval=host_interval,
            poll_interval=poll_interval,
        )
        await frontier.refresh_stats()
        frontier._heartbeat = asyncio.create_task(frontier._beat())
        return frontier

    async def seed(self, urls: Iterable[str]) -> list[str]:
        pages = _pages_of(urls)
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT FROM crawl_jobs WHERE id = $1 FOR UPDATE", self.job_id)
            inserted = await self._insert(connection, pages, depth=0)
            await self._count_accepted(connection, inserted)
        self._wakeup.set()
        return list(inserted)

    async def add(self, urls: Iterable[str], *, depth: int) -> int:
        pages = _pages_of(urls)
        if not pages:
            return 0
        async with self._pool.acquire() as connection, connection.transaction():
            # One worker adds at a time: the bounds are checked against counts no one else changes meanwhile.
            job = await connection.fetchrow(
                "SELECT requested, unfinished FROM crawl_jobs WHERE id = $1 FOR UPDATE", self.job_id
            )
            if self._closed_at(job["requested"]):
                return 0
            seen = {
                row["url"]
                for row in await connection.fetch(
                    "SELECT url FROM frontier WHERE job = $1 AND url = ANY($2::text[])", self.job_id, list(pages)
                )
            }
            host_accepted = dict(
                await connection.fetch(
                    "SELECT host, accepted FROM hosts WHERE job = $1 AND host = ANY($2::text[])",
                    self.job_id,
                    list(set(pages.values())),
                )
            )
            room = None if self._max_queued is None else self._max_queued - job["unfinished"] - job["requested"]
            chosen: dict[str, str] = {}
            dropped = dropped_by_host = 0
            for url, host in pages.items():
                accepted = host_accepted.get(host, 0)
                if self._max_host_queued is not None and accepted >= self._max_host_queued:
                    if url not in seen:
                        dropped_by_host += 1
                    continue
                if room is not None and room <= 0:
                    if url not in seen:
                        dropped += 1
                    continue
                if url in seen:
                    continue
                chosen[url] = host
                host_accepted[host] = accepted + 1
                if room is not None:
                    room -= 1
            inserted = await self._insert(connection, chosen, depth=depth)
            accepted_by_host = await self._count_accepted(connection, inserted, dropped, dropped_by_host)
        self._log_bounds(dropped, accepted_by_host)
        if inserted:
            self._wakeup.set()
        return len(inserted)

    async def take(self) -> FrontierPage | None:
        while True:
            # Cleared before looking: an operation of this frontier meanwhile wakes the wait below.
            self._wakeup.clear()
            async with self._pool.acquire() as connection:
                await self._reclaim(connection)
                row = await connection.fetchrow(_TAKE, self.job_id, self.worker, self.lease_seconds, self.host_interval)
                if row is not None:
                    self._held.add(row["url"])
                    return FrontierPage(row["url"], row["depth"])
                state = await connection.fetchrow(_WAIT_STATE, self.job_id, self.worker)
            delay = self._delay(state)
            if delay is None:
                return None
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(delay):
                    await self._wakeup.wait()

    async def admit(self, page: FrontierPage) -> Admission:
        try:
            async with self._pool.acquire() as connection, connection.transaction():
                await self._lock_leased(connection, page)
                requested = await connection.fetchval(
                    "UPDATE crawl_jobs SET requested = requested + 1"
                    " WHERE id = $1 AND (max_pages IS NULL OR requested < max_pages) RETURNING requested",
                    self.job_id,
                )
                if requested is None:
                    # Taken while another worker was still checking the page that reached the limit.
                    return Admission.OVER_MAX_PAGES
                admitted = await connection.fetchval(
                    "UPDATE hosts SET requested = requested + 1"
                    " WHERE job = $1 AND host = $2 AND ($3::int IS NULL OR requested < $3) RETURNING true",
                    self.job_id,
                    _host_of(page),
                    self.max_pages_per_host,
                )
                if admitted is None:
                    raise _OverHostLimit
                await connection.execute(
                    "UPDATE frontier SET counted = true WHERE job = $1 AND url = $2", self.job_id, page.url
                )
        except _OverHostLimit:
            await self._pool.execute(
                "UPDATE crawl_jobs SET over_host_limit = over_host_limit + 1 WHERE id = $1", self.job_id
            )
            return Admission.OVER_HOST_LIMIT
        except _LeaseLost:
            # Not requested: the caller puts it back, which finds the lease lost and tells so.
            return Admission.OVER_MAX_PAGES
        self._counted.add(page.url)
        self._stats = dataclasses.replace(self._stats, requested=requested)
        return Admission.ADMITTED

    async def put_back(self, page: FrontierPage, delay: float = 0.0, *, uncount: bool) -> None:
        self._check_held(page)
        uncount = uncount and page.url in self._counted
        try:
            async with self._pool.acquire() as connection, connection.transaction():
                await self._lock_leased(connection, page)
                if uncount:
                    await self._uncount(connection, page)
                await connection.execute(
                    "UPDATE frontier SET state = 'queued', worker = NULL, lease_until = NULL, counted = false,"
                    " not_before = now() + make_interval(secs => $3), seq = nextval('frontier_seq')"
                    " WHERE job = $1 AND url = $2",
                    self.job_id,
                    page.url,
                    float(delay),
                )
        except _LeaseLost:
            _log_lease_lost(page)
        finally:
            self._release(page)
        self._wakeup.set()

    async def finish(
        self,
        page: FrontierPage,
        outcome: Outcome,
        reason: str | None = None,
        *,
        uncount: bool = False,
        pending_save: bool = False,
    ) -> None:
        if outcome is not Outcome.PROCESSED and reason is None:
            raise ValueError(f"a page {outcome.value} needs a reason")
        self._check_held(page)
        uncount = uncount and page.url in self._counted
        state = "saving" if pending_save and outcome is Outcome.PROCESSED else outcome.value
        try:
            async with self._pool.acquire() as connection, connection.transaction():
                await self._lock_leased(connection, page)
                await connection.execute("UPDATE crawl_jobs SET unfinished = unfinished - 1 WHERE id = $1", self.job_id)
                if uncount:
                    await self._uncount(connection, page)
                # A page pending its save keeps its worker and its lease; a
                # page finished keeps its worker for the report of the job.
                await connection.execute(
                    "UPDATE frontier SET state = $3, reason = $4, counted = counted AND NOT $5,"
                    " lease_until = CASE WHEN $3 = 'saving' THEN lease_until END"
                    " WHERE job = $1 AND url = $2",
                    self.job_id,
                    page.url,
                    state,
                    reason,
                    uncount,
                )
        except _LeaseLost:
            _log_lease_lost(page)
        finally:
            self._release(page)
        self._wakeup.set()

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

    async def mark_seen(self, url: str) -> bool:
        form = queue_form(url)
        if form is None:
            return False
        new = await self._pool.fetchval(
            "INSERT INTO frontier (job, url, host, state) VALUES ($1, $2, $3, 'seen')"
            " ON CONFLICT DO NOTHING RETURNING true",
            self.job_id,
            form,
            get_host(form),
        )
        return new is not None

    async def forget(self, url: str) -> None:
        form = queue_form(url)
        if form is not None:
            await self._pool.execute(
                "DELETE FROM frontier WHERE job = $1 AND url = $2 AND state = 'seen'", self.job_id, form
            )

    async def is_pending_or_processed(self, url: str) -> bool:
        form = queue_form(url)
        if form is None:
            return False
        state = await self._pool.fetchval("SELECT state FROM frontier WHERE job = $1 AND url = $2", self.job_id, form)
        return state in ("queued", "leased", "saving", "processed")

    async def full(self) -> bool:
        if self._max_queued is None:
            return False
        job = await self._pool.fetchrow("SELECT requested, unfinished FROM crawl_jobs WHERE id = $1", self.job_id)
        return not self._closed_at(job["requested"]) and job["unfinished"] + job["requested"] >= self._max_queued

    def stats(self) -> FrontierStats:
        return self._stats

    async def refresh_stats(self) -> None:
        """Read the counts of the job's pages by state, for `stats`."""
        async with self._pool.acquire() as connection:
            counts = dict(
                await connection.fetch(
                    "SELECT state, count(*) FROM frontier WHERE job = $1 GROUP BY state", self.job_id
                )
            )
            job = await connection.fetchrow(
                "SELECT requested, over_host_limit, links_dropped, links_dropped_by_host FROM crawl_jobs WHERE id = $1",
                self.job_id,
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
        """
        if self._closed:
            return
        self._closed = True
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat
        try:
            async with self._rows_lock, self._pool.acquire() as connection, connection.transaction():
                rows = await connection.fetch(_PUT_BACK_ALL, self.job_id, self.worker)
                await self._uncount_all(connection, [row["host"] for row in rows if row["counted"]])
        finally:
            self._held.clear()
            self._counted.clear()
            self._wakeup.set()
            await self._pool.close()

    async def _insert(self, connection: Connection, pages: dict[str, str], *, depth: int) -> dict[str, str]:
        """Queue the pages not in the frontier yet, in order; those queued, with their hosts."""
        if not pages:
            return {}
        rows = await connection.fetch(_INSERT, self.job_id, list(pages), list(pages.values()), depth)
        inserted = {row["url"] for row in rows}
        # A page seen by another worker since it was looked for is not queued.
        return {url: host for url, host in pages.items() if url in inserted}

    async def _count_accepted(
        self, connection: Connection, inserted: dict[str, str], dropped: int = 0, dropped_by_host: int = 0
    ) -> dict[str, int]:
        """Count the pages queued in their job and hosts; the pages accepted by host since the job began."""
        await connection.execute(
            "UPDATE crawl_jobs SET unfinished = unfinished + $2, links_dropped = links_dropped + $3,"
            " links_dropped_by_host = links_dropped_by_host + $4 WHERE id = $1",
            self.job_id,
            len(inserted),
            dropped,
            dropped_by_host,
        )
        if not inserted:
            return {}
        by_host = Counter(inserted.values())
        rows = await connection.fetch(_COUNT_ACCEPTED, self.job_id, list(by_host), list(by_host.values()))
        return dict(rows)

    async def _reclaim(self, connection: Connection) -> None:
        """Queue again, uncounted, the pages whose leases expired; fail those that expired max_attempts times."""
        expired = await connection.fetchval(
            "SELECT EXISTS (SELECT FROM frontier"
            " WHERE job = $1 AND state IN ('leased', 'saving') AND lease_until < now())",
            self.job_id,
        )
        if not expired:
            return
        async with connection.transaction():
            await connection.execute("SELECT FROM crawl_jobs WHERE id = $1 FOR UPDATE", self.job_id)
            rows = await connection.fetch(_RECLAIM, self.job_id, self.max_attempts)
            # A page in progress was unfinished and stays so if queued; a page pending its save was not.
            unfinished = sum((row["state"] == "queued") - (row["was"] == "leased") for row in rows)
            await connection.execute(
                "UPDATE crawl_jobs SET unfinished = unfinished + $2 WHERE id = $1", self.job_id, unfinished
            )
            await self._uncount_all(connection, [row["host"] for row in rows if row["counted"]])
        for row in rows:
            if row["state"] == "failed":
                logger.warning(
                    "Lease of %s by %s expired %d times: the page failed", row["url"], row["worker"], self.max_attempts
                )
            else:
                logger.warning("Lease of %s by %s expired: the page is queued again", row["url"], row["worker"])

    async def _lock_leased(self, connection: Connection, page: FrontierPage) -> None:
        """Lock the row of a page leased to this worker; raises _LeaseLost if it is not leased to it any more."""
        leased = await connection.fetchval(
            "SELECT true FROM frontier WHERE job = $1 AND url = $2 AND worker = $3 AND state = 'leased' FOR UPDATE",
            self.job_id,
            page.url,
            self.worker,
        )
        if leased is None:
            raise _LeaseLost(page.url)

    async def _uncount(self, connection: Connection, page: FrontierPage) -> None:
        requested = await connection.fetchval(
            "UPDATE crawl_jobs SET requested = requested - 1 WHERE id = $1 RETURNING requested", self.job_id
        )
        await connection.execute(
            "UPDATE hosts SET requested = requested - 1 WHERE job = $1 AND host = $2", self.job_id, _host_of(page)
        )
        self._stats = dataclasses.replace(self._stats, requested=requested)

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
        """Renew the leases of this worker's pages and refresh the stats, every `heartbeat_seconds`."""
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
                await self.refresh_stats()
            except Exception:
                # The leases are renewed at the next beat; if the database
                # is gone for longer, they expire and others crawl the pages.
                logger.warning("Heartbeat of %s failed", self.worker, exc_info=True)

    def _delay(self, state: asyncpg.Record) -> float | None:
        """How long `take` waits before it looks again; None if there is nothing left to wait for."""
        due = self.poll_interval if state["due"] is None else min(max(float(state["due"]), 0.0), self.poll_interval)
        if state["closed"]:
            # Pages other workers counted may go back under the limit; this
            # worker's own pages come back only by its own hand, as in memory.
            return due if state["others_counted"] else None
        if state["ready"]:
            return _RETRY_DELAY
        if state["queued"] or self._held or state["others_busy"]:
            return due
        return None

    def _closed_at(self, requested: int) -> bool:
        """Whether `requested` pages reach max_pages: no page is handed out or accepted until one is uncounted."""
        return self.max_pages is not None and requested >= self.max_pages

    def _check_held(self, page: FrontierPage) -> None:
        if page.url not in self._held:
            raise ValueError(f"{page.url} is not in progress")

    def _release(self, page: FrontierPage) -> None:
        self._held.discard(page.url)
        self._counted.discard(page.url)

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


def _pages_of(urls: Iterable[str]) -> dict[str, str]:
    """The URLs in the form the frontier keeps them, each with its host; invalid ones and repeats left out."""
    pages = {}
    for url in urls:
        form = queue_form(url)
        if form is not None and form not in pages:
            host = get_host(form)
            assert host is not None  # a valid URL has a host
            pages[form] = host
    return pages


def _host_of(page: FrontierPage) -> str:
    host = get_host(page.url)
    assert host is not None  # the frontier holds valid URLs only
    return host


def _log_lease_lost(page: FrontierPage) -> None:
    logger.warning("Lease of %s expired before the page was finished: it is crawled again", page.url)
