"""The tables of a distributed crawl in PostgreSQL, made by the first worker that connects."""

import asyncpg
from asyncpg.pool import PoolConnectionProxy

# A connection of its own or one of a pool.
Connection = asyncpg.Connection | PoolConnectionProxy

# Any number of its own: workers that connect at once make the tables one after another.
_SCHEMA_LOCK = 0x63726177

# A job is one crawl: its limits and the counts the limits are checked against.
_JOBS = """
CREATE TABLE IF NOT EXISTS crawl_jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    max_pages INTEGER,
    max_pages_per_host INTEGER,
    frontier_factor INTEGER NOT NULL,
    requested INTEGER NOT NULL DEFAULT 0,
    unfinished INTEGER NOT NULL DEFAULT 0,
    over_host_limit INTEGER NOT NULL DEFAULT 0,
    links_dropped INTEGER NOT NULL DEFAULT 0,
    links_dropped_by_host INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
)
"""

# Every URL of a job, once: the primary key is the deduplication. The
# sequence keeps the order pages were queued in among those of one depth.
_FRONTIER = """
CREATE TABLE IF NOT EXISTS frontier (
    job BIGINT NOT NULL REFERENCES crawl_jobs (id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    host TEXT NOT NULL,
    depth INTEGER,
    seq BIGINT NOT NULL DEFAULT nextval('frontier_seq'),
    state TEXT NOT NULL CHECK (
        state IN ('seen', 'queued', 'leased', 'saving', 'processed', 'failed', 'skipped', 'blocked', 'unreachable')
    ),
    not_before TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    lease_until TIMESTAMPTZ,
    worker TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    counted BOOLEAN NOT NULL DEFAULT false,
    reason TEXT,
    PRIMARY KEY (job, url)
)
"""

# Rate limit of a host for all the workers together.
_HOSTS = """
CREATE TABLE IF NOT EXISTS hosts (
    job BIGINT NOT NULL REFERENCES crawl_jobs (id) ON DELETE CASCADE,
    host TEXT NOT NULL,
    next_allowed_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    accepted INTEGER NOT NULL DEFAULT 0,
    requested INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job, host)
)
"""

_STATEMENTS = (
    _JOBS,
    "CREATE SEQUENCE IF NOT EXISTS frontier_seq",
    _FRONTIER,
    # The next page of a host; whether a host has pages queued.
    "CREATE INDEX IF NOT EXISTS frontier_queued ON frontier (job, host, depth, seq) WHERE state = 'queued'",
    # When the first page put off comes back.
    "CREATE INDEX IF NOT EXISTS frontier_put_off ON frontier (job, not_before) WHERE state = 'queued'",
    # Leases that expire.
    "CREATE INDEX IF NOT EXISTS frontier_leased ON frontier (job, lease_until) WHERE state IN ('leased', 'saving')",
    _HOSTS,
)


async def create_schema(connection: Connection) -> None:
    """Make the tables of distributed crawls that are missing; workers may do it at once."""
    async with connection.transaction():
        # CREATE ... IF NOT EXISTS of two sessions at once may still fail on a duplicate.
        await connection.execute("SELECT pg_advisory_xact_lock($1)", _SCHEMA_LOCK)
        for statement in _STATEMENTS:
            await connection.execute(statement)
