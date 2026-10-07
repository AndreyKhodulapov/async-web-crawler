"""The operations of `PostgresFrontier` a worker makes for every page, as PL/pgSQL functions.

Each is one call, so one round trip: the row of the job and that of the
host are locked for as long as the function runs, a fraction of a
millisecond, not across the round trips of a transaction made from the
client. Every worker needs the row of the job for each page it admits,
finishes or finds links on, so the time it is held is what bounds the
pages per second of the whole job. The locks are taken in the order the
docstring of `PostgresFrontier` gives; a function changes nothing and
returns a flag when the page is no longer leased to its worker.
"""

# The pages whose leases expired, queued again uncounted or failed after
# max_attempts, as `_RECLAIM` did; then the page handed out: the ready host
# with the shallowest page, locked so that no other worker takes a page of
# it at the same moment; the page locked and leased; the host's next turn
# moved on by the interval of the job, or by its own if that is longer. The
# page is read from the snapshot of the statement: another worker may have
# taken it since, and may hold its row, waiting for the host this one holds.
# So its row is locked skipping, never waited for, and is leased only if
# still queued; otherwise nothing is taken and the worker looks again.
# Nothing is handed out while the job is seeding. The version of the scope
# comes along, so that the worker learns of a host that joined it before it
# crawls the page. A host given up is ready however long it is held back,
# and keeps its turn: its pages come with the outcome to finish them with,
# unrequested. The pages taken back come along for the log of the worker.
_TAKE = """
CREATE OR REPLACE FUNCTION frontier_take(
    _job BIGINT,
    _worker TEXT,
    _lease DOUBLE PRECISION,
    _interval DOUBLE PRECISION,
    _max_attempts INTEGER,
    OUT url TEXT,
    OUT depth INTEGER,
    OUT waits INTEGER,
    OUT given_up_outcome TEXT,
    OUT given_up_reason TEXT,
    OUT given_up_error TEXT,
    OUT scope_version INTEGER,
    OUT reclaimed_urls TEXT[],
    OUT reclaimed_states TEXT[],
    OUT reclaimed_workers TEXT[]
) LANGUAGE plpgsql AS $$
#variable_conflict use_column
BEGIN
    reclaimed_urls := '{}';
    reclaimed_states := '{}';
    reclaimed_workers := '{}';
    IF EXISTS (
        SELECT FROM frontier AS f WHERE f.job = _job AND f.state IN ('leased', 'saving') AND f.lease_until < now()
    ) THEN
        PERFORM FROM crawl_jobs AS j WHERE j.id = _job FOR NO KEY UPDATE;
        -- Rows locked by their worker are left for the next time. A page
        -- failed was not crawled: the targets of its redirects may be queued
        -- again, as those of a page failed by a worker. A page in progress
        -- was unfinished and stays so if queued; one pending its save was not.
        WITH expired AS (
            SELECT f.url, f.host, f.state, f.worker, f.counted
            FROM frontier AS f
            WHERE f.job = _job AND f.state IN ('leased', 'saving') AND f.lease_until < now()
            FOR UPDATE SKIP LOCKED
        ), reclaimed AS (
            UPDATE frontier AS f
            SET state = CASE WHEN f.attempts + 1 >= _max_attempts THEN 'failed' ELSE 'queued' END,
                reason = CASE WHEN f.attempts + 1 >= _max_attempts THEN format('lease expired %s times', f.attempts + 1) END,
                error = CASE WHEN f.attempts + 1 >= _max_attempts THEN 'LeaseExpired' END,
                finished_at = CASE WHEN f.attempts + 1 >= _max_attempts THEN now() END,
                status = NULL,
                elapsed = NULL,
                attempts = f.attempts + 1,
                worker = NULL,
                lease_until = NULL,
                counted = false,
                not_before = '-infinity',
                seq = nextval('frontier_seq')
            FROM expired
            WHERE f.job = _job AND f.url = expired.url
            RETURNING f.url, f.state, expired.state AS was, expired.worker, expired.host, expired.counted
        ), unseen AS (
            DELETE FROM frontier AS s USING reclaimed AS r
            WHERE s.job = _job AND s.state = 'seen' AND s.seen_from = r.url AND r.state = 'failed'
        ), uncounted_hosts AS (
            UPDATE hosts AS h SET requested = h.requested - c.pages
            FROM (SELECT r.host, count(*) AS pages FROM reclaimed AS r WHERE r.counted GROUP BY r.host) AS c
            WHERE h.job = _job AND h.host = c.host
        ), counts AS (
            UPDATE crawl_jobs AS j
            SET unfinished = j.unfinished + c.unfinished, requested = j.requested - c.uncounted
            FROM (
                SELECT
                    coalesce(sum((r.state = 'queued')::int - (r.was = 'leased')::int), 0) AS unfinished,
                    count(*) FILTER (WHERE r.counted) AS uncounted
                FROM reclaimed AS r
            ) AS c
            WHERE j.id = _job
        )
        SELECT coalesce(array_agg(r.url), '{}'), coalesce(array_agg(r.state), '{}'), coalesce(array_agg(r.worker), '{}')
        INTO reclaimed_urls, reclaimed_states, reclaimed_workers
        FROM reclaimed AS r;
    END IF;

    WITH page AS (
        SELECT h.host, h.given_up_outcome, h.given_up_reason, h.given_up_error, p.url
        FROM hosts AS h
        CROSS JOIN LATERAL (
            SELECT f.url, f.depth, f.seq
            FROM frontier AS f
            WHERE f.job = h.job AND f.host = h.host AND f.state = 'queued' AND f.not_before <= now()
            ORDER BY f.depth, f.seq
            LIMIT 1
        ) AS p
        WHERE h.job = _job AND (h.next_allowed_at <= now() OR h.given_up_outcome IS NOT NULL)
            AND NOT EXISTS (
                SELECT FROM crawl_jobs AS j
                WHERE j.id = _job AND (j.requested >= j.max_pages OR j.state = 'seeding')
            )
        ORDER BY p.depth, p.seq
        LIMIT 1
        FOR UPDATE OF h SKIP LOCKED
    ), queued AS (
        SELECT f.url
        FROM frontier AS f
        JOIN page ON f.url = page.url
        WHERE f.job = _job AND f.state = 'queued'
        FOR UPDATE OF f SKIP LOCKED
    ), leased AS (
        UPDATE frontier AS f
        SET state = 'leased', worker = _worker, lease_until = now() + make_interval(secs => _lease)
        FROM queued
        WHERE f.job = _job AND f.url = queued.url
        RETURNING f.url, f.depth, f.host, f.waits
    ), turn AS (
        UPDATE hosts AS h
        SET next_allowed_at = now() + make_interval(secs => greatest(_interval, h.interval))
        FROM leased
        WHERE h.job = _job AND h.host = leased.host AND h.given_up_outcome IS NULL
    )
    SELECT
        leased.url,
        leased.depth,
        leased.waits,
        page.given_up_outcome,
        page.given_up_reason,
        page.given_up_error,
        (SELECT j.scope_version FROM crawl_jobs AS j WHERE j.id = _job)
    INTO url, depth, waits, given_up_outcome, given_up_reason, given_up_error, scope_version
    FROM leased
    JOIN page ON page.url = leased.url;
END
$$
"""

# A page leased to the worker counted toward max_pages and
# max_pages_per_host, or the limit it reached; `requested` is that of the job.
# A page over the limit of its host is not requested and is uncounted from
# the job at once.
_ADMIT = """
CREATE OR REPLACE FUNCTION frontier_admit(
    _job BIGINT, _url TEXT, _worker TEXT, OUT admission TEXT, OUT requested INTEGER
) LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    page_host TEXT;
    host_limit INTEGER;
BEGIN
    SELECT f.host INTO page_host
    FROM frontier AS f
    WHERE f.job = _job AND f.url = _url AND f.worker = _worker AND f.state = 'leased'
    FOR UPDATE;
    IF NOT FOUND THEN
        admission := 'lease_lost';
        RETURN;
    END IF;
    UPDATE crawl_jobs AS j SET requested = j.requested + 1
    WHERE j.id = _job AND (j.max_pages IS NULL OR j.requested < j.max_pages)
    RETURNING j.requested, j.max_pages_per_host INTO requested, host_limit;
    IF NOT FOUND THEN
        -- Taken while another worker was still checking the page that reached the limit.
        admission := 'over_max_pages';
        RETURN;
    END IF;
    UPDATE hosts AS h SET requested = h.requested + 1
    WHERE h.job = _job AND h.host = page_host AND (host_limit IS NULL OR h.requested < host_limit);
    IF NOT FOUND THEN
        UPDATE crawl_jobs AS j SET requested = j.requested - 1, over_host_limit = j.over_host_limit + 1
        WHERE j.id = _job
        RETURNING j.requested INTO requested;
        admission := 'over_host_limit';
        RETURN;
    END IF;
    UPDATE frontier AS f SET counted = true WHERE f.job = _job AND f.url = _url;
    admission := 'admitted';
END
$$
"""

# A page leased to the worker finished in `_state`, uncounted with
# `_uncount`; `held` is false if the page was not leased to it any more,
# and nothing is changed then. A page pending its save keeps its worker and
# its lease; a page finished keeps its worker for the report of the job.
_FINISH = """
CREATE OR REPLACE FUNCTION frontier_finish(
    _job BIGINT,
    _url TEXT,
    _worker TEXT,
    _state TEXT,
    _reason TEXT,
    _uncount BOOLEAN,
    _status INTEGER,
    _elapsed DOUBLE PRECISION,
    _error TEXT,
    OUT held BOOLEAN,
    OUT requested INTEGER
) LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    page_host TEXT;
BEGIN
    SELECT f.host INTO page_host
    FROM frontier AS f
    WHERE f.job = _job AND f.url = _url AND f.worker = _worker AND f.state = 'leased'
    FOR UPDATE;
    held := FOUND;
    IF NOT held THEN
        RETURN;
    END IF;
    UPDATE crawl_jobs AS j SET unfinished = j.unfinished - 1, requested = j.requested - _uncount::int
    WHERE j.id = _job
    RETURNING j.requested INTO requested;
    IF _uncount THEN
        UPDATE hosts AS h SET requested = h.requested - 1 WHERE h.job = _job AND h.host = page_host;
    END IF;
    UPDATE frontier AS f
    SET state = _state,
        reason = _reason,
        counted = f.counted AND NOT _uncount,
        lease_until = CASE WHEN _state = 'saving' THEN f.lease_until END,
        status = _status,
        elapsed = _elapsed,
        error = _error,
        finished_at = now()
    WHERE f.job = _job AND f.url = _url;
END
$$
"""

# A page leased to the worker queued again `_delay` seconds from now,
# uncounted with `_uncount`; `held` as in frontier_finish.
_PUT_BACK = """
CREATE OR REPLACE FUNCTION frontier_put_back(
    _job BIGINT,
    _url TEXT,
    _worker TEXT,
    _delay DOUBLE PRECISION,
    _uncount BOOLEAN,
    _waited BOOLEAN,
    OUT held BOOLEAN,
    OUT requested INTEGER
) LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    page_host TEXT;
BEGIN
    SELECT f.host INTO page_host
    FROM frontier AS f
    WHERE f.job = _job AND f.url = _url AND f.worker = _worker AND f.state = 'leased'
    FOR UPDATE;
    held := FOUND;
    IF NOT held THEN
        RETURN;
    END IF;
    IF _uncount THEN
        UPDATE crawl_jobs AS j SET requested = j.requested - 1 WHERE j.id = _job RETURNING j.requested INTO requested;
        UPDATE hosts AS h SET requested = h.requested - 1 WHERE h.job = _job AND h.host = page_host;
    END IF;
    UPDATE frontier AS f
    SET state = 'queued',
        worker = NULL,
        lease_until = NULL,
        counted = false,
        not_before = now() + make_interval(secs => _delay),
        seq = nextval('frontier_seq'),
        waits = f.waits + _waited::int
    WHERE f.job = _job AND f.url = _url;
END
$$
"""

# The pages found at `_depth` queued in order, as far as the bounds allow:
# the frontier holds at most frontier_factor x max_pages pages queued, in
# progress or requested, and accepts at most frontier_factor x
# max_pages_per_host pages of a host over the job; nothing once max_pages
# pages are requested. Pages already in the frontier are left out and not
# counted as dropped. Without `_bounded`, as the job is seeded, every new
# page is queued. One worker adds at a time, as the job is locked first:
# the bounds are checked against counts no one else changes meanwhile.
# Returns the pages accepted, those dropped as the frontier is full, and
# the pages accepted by host over the job for the hosts that got some.
#
# The pages are chosen in one statement, as a loop over them would decide:
# a page fits its host if fewer than the bound of pages of the host are
# accepted before it, which for the first pages of a host is its rank among
# them, as each of them fitted; a page that fits is accepted while the
# frontier has room. A page left out is dropped by its host if the pages of
# the host accepted before it reach the bound, and as the frontier is full
# otherwise.
_ADD = """
CREATE OR REPLACE FUNCTION frontier_add(
    _job BIGINT,
    _urls TEXT[],
    _hosts TEXT[],
    _depth INTEGER,
    _bounded BOOLEAN,
    OUT accepted INTEGER,
    OUT dropped INTEGER,
    OUT accepted_hosts TEXT[],
    OUT host_accepted INTEGER[]
) LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    job_requested INTEGER;
    job_unfinished INTEGER;
    max_pages INTEGER;
    max_pages_per_host INTEGER;
    factor INTEGER;
    room INTEGER;
    host_room INTEGER;
    dropped_by_host INTEGER;
    inserted_hosts TEXT[];
BEGIN
    accepted := 0;
    dropped := 0;
    accepted_hosts := '{}';
    host_accepted := '{}';
    SELECT j.requested, j.unfinished, j.max_pages, j.max_pages_per_host, j.frontier_factor
    INTO job_requested, job_unfinished, max_pages, max_pages_per_host, factor
    FROM crawl_jobs AS j
    WHERE j.id = _job
    FOR NO KEY UPDATE;
    IF _bounded THEN
        IF job_requested >= max_pages THEN
            RETURN;
        END IF;
        room := factor * max_pages - job_unfinished - job_requested;
        host_room := factor * max_pages_per_host;
    END IF;

    WITH page AS (
        SELECT p.url, p.host, p.position, coalesce(h.accepted, 0) AS host_before
        FROM unnest(_urls, _hosts) WITH ORDINALITY AS p (url, host, position)
        LEFT JOIN hosts AS h ON h.job = _job AND h.host = p.host
        WHERE NOT EXISTS (SELECT FROM frontier AS f WHERE f.job = _job AND f.url = p.url)
    ), fitting AS (
        SELECT
            page.*,
            host_room IS NULL
                OR page.host_before + row_number() OVER (PARTITION BY page.host ORDER BY page.position) <= host_room
                AS fits
        FROM page
    ), chosen AS (
        SELECT
            fitting.*,
            fitting.fits AND (
                room IS NULL OR count(*) FILTER (WHERE fitting.fits) OVER (ORDER BY fitting.position) <= room
            ) AS chosen
        FROM fitting
    ), left_out AS (
        SELECT
            c.chosen,
            NOT c.chosen AND host_room IS NOT NULL AND c.host_before + count(*) FILTER (WHERE c.chosen) OVER (
                PARTITION BY c.host ORDER BY c.position ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) >= host_room AS by_host
        FROM chosen AS c
    ), inserted AS (
        -- A page seen by another worker since it was looked for is not queued.
        INSERT INTO frontier (job, url, host, depth, state)
        SELECT _job, c.url, c.host, _depth, 'queued' FROM chosen AS c WHERE c.chosen ORDER BY c.position
        ON CONFLICT DO NOTHING
        RETURNING frontier.host
    )
    SELECT
        (SELECT count(*) FROM inserted),
        (SELECT coalesce(array_agg(i.host), '{}') FROM inserted AS i),
        count(*) FILTER (WHERE NOT l.chosen AND NOT l.by_host),
        count(*) FILTER (WHERE l.by_host)
    INTO accepted, inserted_hosts, dropped, dropped_by_host
    FROM left_out AS l;

    UPDATE crawl_jobs AS j
    SET unfinished = j.unfinished + accepted,
        links_dropped = j.links_dropped + dropped,
        links_dropped_by_host = j.links_dropped_by_host + dropped_by_host
    WHERE j.id = _job;
    WITH counted AS (
        INSERT INTO hosts AS h (job, host, accepted)
        SELECT _job, page.host, count(*) FROM unnest(inserted_hosts) AS page (host) GROUP BY page.host
        ON CONFLICT (job, host) DO UPDATE SET accepted = h.accepted + excluded.accepted
        RETURNING h.host, h.accepted
    )
    SELECT coalesce(array_agg(c.host), '{}'), coalesce(array_agg(c.accepted), '{}')
    INTO accepted_hosts, host_accepted
    FROM counted AS c;
END
$$
"""

FUNCTIONS = (_TAKE, _ADMIT, _FINISH, _PUT_BACK, _ADD)
