"""Integration tests for PostgresFrontier: several workers of one job on one database.

The contract both frontiers keep is checked in test_frontier.py; these
tests check what only a shared frontier has: leases, the heartbeat, pages
pending their save, the host interval and the limits held by all workers together.
"""

import asyncio
import functools
import itertools
import logging
import time
from collections.abc import AsyncGenerator, Awaitable, Callable

import asyncpg
import pytest
from helpers import POSTGRES_DSN, DatabaseLink, drop_frontier_tables, make_job

from crawler import Admission, FrontierPage, GivenUp, HostFailures, JobError, Outcome, PostgresFrontier

pytestmark = pytest.mark.postgres

FrontierOpener = Callable[..., Awaitable[PostgresFrontier]]


@pytest.fixture
async def open_frontier() -> AsyncGenerator[FrontierOpener, None]:
    """Opens workers of the job "test" on empty tables; closes them after the test.

    The workers poll often, so that a test does not wait for the pages of another one long.
    """
    await drop_frontier_tables()
    opened = []

    async def open_frontier(worker: str, *, job: str = "test", **options) -> PostgresFrontier:
        """A worker of `job`, made with the limits among `options` unless it exists."""
        limits = {
            key: options.pop(key) for key in ("max_pages", "max_pages_per_host", "frontier_factor") if key in options
        }
        await make_job(job, **limits)
        frontier = await PostgresFrontier.open(
            POSTGRES_DSN, job=job, worker=worker, **{"poll_interval": 0.02, **options}
        )
        opened.append(frontier)
        return frontier

    yield open_frontier
    for frontier in opened:
        await frontier.close()


async def fetch(query: str, *parameters: object) -> list[asyncpg.Record]:
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        return await connection.fetch(query, *parameters)
    finally:
        await connection.close()


async def row_of(url: str) -> asyncpg.Record:
    (row,) = await fetch("SELECT state, worker, attempts, reason, error FROM frontier WHERE url = $1", url)
    return row


async def job_state(name: str = "test") -> str:
    (row,) = await fetch("SELECT state FROM crawl_jobs WHERE name = $1", name)
    return row["state"]


async def take(frontier: PostgresFrontier) -> FrontierPage:
    page = await frontier.take()
    assert page is not None
    return page


async def still_waiting(operation: Awaitable[object], seconds: float = 0.3) -> bool:
    """Whether `operation` is still waiting after `seconds`; it is cancelled then."""
    try:
        await asyncio.wait_for(operation, seconds)
    except TimeoutError:
        return True
    return False


class TestJob:
    async def test_worker_of_a_job_that_does_not_exist_fails(self, open_frontier):
        await make_job("other")

        with pytest.raises(JobError, match='no crawl job named "test"'):
            await PostgresFrontier.open(POSTGRES_DSN, job="test")

    async def test_workers_opening_at_once_make_the_tables_once(self, open_frontier):
        results = await asyncio.gather(
            *(PostgresFrontier.open(POSTGRES_DSN, job="test") for _ in range(4)), return_exceptions=True
        )

        assert all(isinstance(result, JobError) for result in results)
        assert await fetch("SELECT id FROM crawl_jobs") == []

    async def test_worker_takes_the_limits_of_its_job(self, open_frontier):
        await make_job(max_pages=5, max_pages_per_host=2, frontier_factor=4)

        frontier = await open_frontier("worker")

        assert (frontier.max_pages, frontier.max_pages_per_host, frontier.frontier_factor) == (5, 2, 4)

    async def test_jobs_have_frontiers_of_their_own(self, open_frontier):
        first = await open_frontier("worker", job="first")
        second = await open_frontier("worker", job="second")

        assert await first.seed(["http://site/"]) == ["http://site/"]
        assert await second.seed(["http://site/"]) == ["http://site/"]
        await first.finish(await take(first), Outcome.PROCESSED)
        assert await take(second) == FrontierPage("http://site/", 0)


class TestDeduplication:
    async def test_link_found_by_two_workers_at_once_is_accepted_once(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        links = [f"http://site/{i}" for i in range(50)]

        accepted = await asyncio.gather(first.add(links, depth=1), second.add(links, depth=1))

        assert sum(accepted) == 50
        assert len(await fetch("SELECT url FROM frontier")) == 50

    async def test_one_worker_takes_a_url_for_new(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")

        new = await asyncio.gather(
            first.mark_seen("http://site/target", "http://site/a"),
            second.mark_seen("http://site/target", "http://site/b"),
        )

        assert sorted(new) == [False, True]

    async def test_page_is_handed_out_to_one_worker(self, open_frontier):
        workers = [await open_frontier(f"worker-{i}") for i in range(3)]
        await workers[0].seed([f"http://site/{i}" for i in range(30)])
        taken = []

        async def crawl(frontier: PostgresFrontier) -> None:
            while (page := await frontier.take()) is not None:
                taken.append(page.url)
                await frontier.finish(page, Outcome.PROCESSED)

        await asyncio.gather(*(crawl(frontier) for frontier in workers for _ in range(3)))

        assert sorted(taken) == sorted(f"http://site/{i}" for i in range(30))


class TestJobLock:
    """A worker that holds the job, to add links say, keeps no other from inserting a row of the job.

    A statement inserting a row checks its foreign key by locking the job
    FOR KEY SHARE; were the job locked FOR UPDATE, the statement would wait
    for the holder while holding its new row, which the holder may insert next.
    """

    @staticmethod
    def pause_holding_the_job(frontier: PostgresFrontier, monkeypatch) -> tuple[asyncio.Event, asyncio.Event]:
        """Make `frontier` stop once it has locked the job, until the second event is set; the first tells it did."""
        locked, go_on = asyncio.Event(), asyncio.Event()
        lock_job = frontier._lock_job

        async def lock_and_wait(connection):
            row = await lock_job(connection)
            locked.set()
            await go_on.wait()
            return row

        monkeypatch.setattr(frontier, "_lock_job", lock_and_wait)
        return locked, go_on

    async def insert_while_held(
        self, holder: PostgresFrontier, hold: Awaitable[object], insert: Awaitable[object], monkeypatch
    ) -> None:
        locked, go_on = self.pause_holding_the_job(holder, monkeypatch)
        holding = asyncio.create_task(hold)
        await asyncio.wait_for(locked.wait(), 5)
        try:
            assert not await still_waiting(insert, 1)
        finally:
            go_on.set()
            await asyncio.wait_for(holding, 5)

    @pytest.mark.parametrize(
        "insert",
        [
            # The very row the holder inserts next: the two used to deadlock.
            pytest.param(lambda other: other.mark_seen("http://site/new", "http://site/"), id="mark_seen"),
            pytest.param(lambda other: other.hold_host("new", 10, "Retry-After"), id="hold_host"),
            pytest.param(lambda other: other.set_host_interval("new", 1), id="set_host_interval"),
            pytest.param(lambda other: other.count_host_failures("new", circuit_openings=1), id="count_host_failures"),
            pytest.param(lambda other: other.hold_out_of_scope(["http://elsewhere/"]), id="hold_out_of_scope"),
            pytest.param(lambda other: other.take(), id="join"),
        ],
    )
    async def test_worker_inserts_while_another_adds_links(self, open_frontier, monkeypatch, insert):
        holder, other = await open_frontier("holder"), await open_frontier("other")
        # A page to take: with none, a worker would mark the job finished, and wait for the holder to do so.
        await holder.seed(["http://site/"])

        await self.insert_while_held(holder, holder.add(["http://site/new"], depth=1), insert(other), monkeypatch)

        # Whichever came first, the page is in the frontier once.
        assert len(await fetch("SELECT url FROM frontier WHERE url = 'http://site/new'")) <= 1

    @pytest.mark.parametrize(
        "hold",
        [
            pytest.param(lambda holder: holder.seed(["http://site/new"]), id="seed"),
            pytest.param(lambda holder: holder.add(["http://site/new"], depth=1), id="add"),
            pytest.param(lambda holder: holder.widen_scope("elsewhere", lambda url: True), id="widen_scope"),
            pytest.param(lambda holder: holder.give_up_host("site", Outcome.FAILED, "down"), id="give_up_host"),
            pytest.param(lambda holder: holder.take(), id="reclaim"),
        ],
    )
    async def test_worker_marks_a_redirect_target_while_another_holds_the_job(self, open_frontier, monkeypatch, hold):
        stopped = await open_frontier("stopped", lease_seconds=0.2, heartbeat_seconds=60)
        holder, other = await open_frontier("holder"), await open_frontier("other")
        await stopped.seed(["http://site/"])
        await take(stopped)
        await holder.hold_out_of_scope(["http://elsewhere/"])
        await asyncio.sleep(0.3)  # the lease of the page expires, for the holder to take it back

        await self.insert_while_held(
            holder, hold(holder), other.mark_seen("http://site/new", "http://site/moved"), monkeypatch
        )

        assert len(await fetch("SELECT url FROM frontier WHERE url = 'http://site/new'")) == 1


class TestLease:
    async def test_page_of_a_stopped_worker_is_handed_out_again_once_its_lease_expires(self, open_frontier):
        stopped = await open_frontier("stopped", lease_seconds=0.3, heartbeat_seconds=60)
        alive = await open_frontier("alive")
        await stopped.seed(["http://site/"])
        page = await take(stopped)
        assert await stopped.admit(page) is Admission.ADMITTED
        started = time.monotonic()

        assert await asyncio.wait_for(alive.take(), 5) == page

        assert time.monotonic() - started >= 0.2
        # The page went back to the queue: it no longer counts toward max_pages.
        await alive.refresh_stats()
        assert alive.stats().requested == 0
        assert (await row_of(page.url))["attempts"] == 1

    async def test_page_whose_lease_expired_max_attempts_times_fails(self, open_frontier):
        options = {"lease_seconds": 0.2, "heartbeat_seconds": 60, "max_attempts": 2}
        first, second = await open_frontier("first", **options), await open_frontier("second", **options)
        last = await open_frontier("last", max_attempts=2)
        await first.seed(["http://site/"])
        await take(first)
        await asyncio.wait_for(second.take(), 5)

        assert await asyncio.wait_for(last.take(), 5) is None

        row = await row_of("http://site/")
        assert (row["state"], row["reason"]) == ("failed", "lease expired 2 times")
        await last.refresh_stats()
        assert last.stats().failed == 1

    async def test_page_whose_lease_expired_follows_its_redirect_again_with_the_next_worker(self, open_frontier):
        stopped = await open_frontier("stopped", lease_seconds=0.3, heartbeat_seconds=60)
        alive = await open_frontier("alive")
        await stopped.seed(["http://site/moved"])
        page = await take(stopped)
        assert await stopped.mark_seen("http://site/target", page.url)

        assert await asyncio.wait_for(alive.take(), 5) == page

        assert await alive.mark_seen("http://site/target", page.url)
        assert not await alive.mark_seen("http://site/target", "http://site/other")

    async def test_redirect_targets_of_a_page_whose_lease_expired_max_attempts_times_may_be_queued(self, open_frontier):
        options = {"lease_seconds": 0.2, "heartbeat_seconds": 60, "max_attempts": 2}
        first, second = await open_frontier("first", **options), await open_frontier("second", **options)
        last = await open_frontier("last", max_attempts=2)
        await first.seed(["http://site/moved"])
        await first.mark_seen("http://site/target", (await take(first)).url)
        await first.mark_seen("http://site/kept", "http://site/other")
        await asyncio.wait_for(second.take(), 5)

        assert await asyncio.wait_for(last.take(), 5) is None

        assert (await row_of("http://site/moved"))["state"] == "failed"
        assert await last.add(["http://site/target", "http://site/kept"], depth=1) == 1
        assert (await row_of("http://site/target"))["state"] == "queued"

    async def test_heartbeat_keeps_the_lease_of_a_page_in_progress(self, open_frontier):
        busy = await open_frontier("busy", lease_seconds=0.3, heartbeat_seconds=0.05)
        other = await open_frontier("other")
        await busy.seed(["http://site/"])
        page = await take(busy)

        assert await still_waiting(other.take(), 0.8)

        await busy.finish(page, Outcome.PROCESSED)
        assert await other.take() is None
        assert (await row_of(page.url))["state"] == "processed"

    async def test_close_puts_the_pages_in_progress_back_uncounted(self, open_frontier):
        closing, other = await open_frontier("closing"), await open_frontier("other")
        await closing.seed(["http://site/a", "http://site/b"])
        a, b = await take(closing), await take(closing)
        assert await closing.admit(a) is Admission.ADMITTED

        await closing.close()

        await other.refresh_stats()
        assert (other.stats().queued, other.stats().requested) == (2, 0)
        assert {(await take(other)).url, (await take(other)).url} == {a.url, b.url}

    async def test_finishing_a_page_whose_lease_was_lost_changes_nothing(self, open_frontier):
        slow = await open_frontier("slow", lease_seconds=0.2, heartbeat_seconds=60)
        other = await open_frontier("other")
        await slow.seed(["http://site/"])
        page = await take(slow)
        assert await asyncio.wait_for(other.take(), 5) == page

        await slow.finish(page, Outcome.PROCESSED)

        assert (await row_of(page.url))[:2] == ("leased", "other")


class TestWaits:
    async def test_waits_of_a_page_are_known_to_the_next_worker_and_outlast_its_lease(self, open_frontier):
        first = await open_frontier("first", lease_seconds=0.2, heartbeat_seconds=60)
        second = await open_frontier("second")
        await first.seed(["http://site/"])
        await first.put_back(await take(first), waited=True, uncount=False)

        page = await take(second)
        assert second.waits(page) == 1
        await second.put_back(page, waited=True, uncount=False)
        await take(first)  # its lease expires: not a wait

        page = await asyncio.wait_for(second.take(), 5)
        assert second.waits(page) == 2
        assert (await row_of(page.url))["attempts"] == 1


class TestPendingSave:
    async def test_page_pending_its_save_holds_the_other_workers_until_saved(self, open_frontier):
        saving, other = await open_frontier("saving"), await open_frontier("other")
        await saving.seed(["http://site/"])
        page = await take(saving)
        await saving.finish(page, Outcome.PROCESSED, pending_save=True)

        # Its worker may stop before the save: then the page is crawled again.
        assert await still_waiting(other.take())

        await saving.saved([page.url])
        assert await other.take() is None
        assert (await row_of(page.url))["state"] == "processed"

    async def test_page_pending_its_save_is_crawled_again_if_its_worker_stops(self, open_frontier):
        stopped = await open_frontier("stopped", lease_seconds=0.3, heartbeat_seconds=60)
        other = await open_frontier("other")
        await stopped.seed(["http://site/"])
        page = await take(stopped)
        await stopped.finish(page, Outcome.PROCESSED, pending_save=True)

        assert await asyncio.wait_for(other.take(), 5) == page

    async def test_close_leaves_pages_pending_their_save(self, open_frontier):
        closing = await open_frontier("closing")
        await closing.seed(["http://site/"])
        page = await take(closing)
        await closing.finish(page, Outcome.PROCESSED, pending_save=True)

        await closing.close()

        assert (await row_of(page.url))["state"] == "saving"

    async def test_workers_that_wait_for_the_saves_of_each_other_write_their_own_first(self, open_frontier):
        workers = [await open_frontier("first"), await open_frontier("second")]
        await workers[0].seed(["http://site/a", "http://site/b"])
        for worker in workers:
            page = await take(worker)
            await worker.finish(page, Outcome.PROCESSED, pending_save=True)
            # As the run of a crawl does: its storage writes the buffer and reports the pages saved.
            worker.on_waiting = functools.partial(worker.saved, [page.url])

        assert await asyncio.wait_for(asyncio.gather(*(worker.take() for worker in workers)), 5) == [None, None]
        assert await job_state() == "finished"

    async def test_worker_waiting_for_a_host_is_not_asked_to_write(self, open_frontier):
        busy, other = await open_frontier("busy", host_interval=60), await open_frontier("other", host_interval=60)
        await busy.seed(["http://site/a", "http://site/b"])
        await take(busy)
        calls = []

        async def on_waiting() -> None:
            calls.append(True)

        other.on_waiting = on_waiting

        assert await still_waiting(other.take())
        assert calls == []

    async def test_saved_settles_only_the_pages_of_its_worker(self, open_frontier):
        saving, other = await open_frontier("saving"), await open_frontier("other")
        await saving.seed(["http://site/"])
        page = await take(saving)
        await saving.finish(page, Outcome.PROCESSED, pending_save=True)

        await other.saved([page.url])

        assert (await row_of(page.url))[:2] == ("saving", "saving")


class TestHosts:
    async def test_host_interval_holds_for_all_workers_together(self, open_frontier):
        workers = [await open_frontier(f"worker-{i}", host_interval=0.1) for i in range(2)]
        await workers[0].seed([f"http://site/{i}" for i in range(6)])
        taken_at = []

        async def crawl(frontier: PostgresFrontier) -> None:
            while (page := await frontier.take()) is not None:
                # By the clock of the database: a reply may reach the worker late.
                (row,) = await fetch(
                    "SELECT extract(epoch FROM lease_until) AS until FROM frontier WHERE url = $1", page.url
                )
                taken_at.append(float(row["until"]) - frontier.lease_seconds)
                await frontier.finish(page, Outcome.PROCESSED)

        await asyncio.gather(*(crawl(frontier) for frontier in workers for _ in range(2)))

        taken_at.sort()
        assert len(taken_at) == 6
        # Epoch seconds as floats lose a little below the microsecond.
        assert min(later - earlier for earlier, later in itertools.pairwise(taken_at)) >= 0.1 - 1e-6

    async def test_page_of_a_ready_host_comes_before_a_shallower_one_of_a_waiting_host(self, open_frontier):
        frontier = await open_frontier("worker", host_interval=60)
        await frontier.seed(["http://a/1", "http://a/2"])
        await frontier.add(["http://b/1"], depth=1)

        assert await take(frontier) == FrontierPage("http://a/1", 0)
        assert await take(frontier) == FrontierPage("http://b/1", 1)
        assert await still_waiting(frontier.take())


async def host_of(host: str) -> asyncpg.Record:
    """The hold of a host: seconds left until it may be asked again, and why."""
    (row,) = await fetch(
        "SELECT extract(epoch FROM next_allowed_at - now()) AS left, hold_reason FROM hosts WHERE host = $1", host
    )
    return row


class TestHoldHost:
    async def test_held_host_hands_out_no_page_to_any_worker_until_its_time(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.seed(["http://a/1", "http://a/2", "http://b/1"])
        page = await take(first)

        await first.hold_host("a", 0.5, "HTTP 429 Too Many Requests, Retry-After 1s")
        await first.put_back(page, uncount=False)

        assert first.shared
        assert await take(second) == FrontierPage("http://b/1", 0)
        assert await still_waiting(second.take(), 0.3)
        # The page put back is queued after the other page of its host.
        assert await take(second) == FrontierPage("http://a/2", 0)
        assert (await host_of("a"))["hold_reason"] == "HTTP 429 Too Many Requests, Retry-After 1s"

    async def test_hold_is_never_shortened_and_keeps_the_reason_of_the_one_that_ends_last(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.seed(["http://a/1"])

        await frontier.hold_host("a", 60, "long")
        await frontier.hold_host("a", 0.1, "short")
        held = await host_of("a")
        assert 59 < held["left"] <= 60
        assert held["hold_reason"] == "long"

        # A hold that does not say why keeps the reason it extends.
        await frontier.hold_host("a", 120, None)
        held = await host_of("a")
        assert 119 < held["left"] <= 120
        assert held["hold_reason"] == "long"

        await frontier.hold_host("a", 180, "longer")
        assert (await host_of("a"))["hold_reason"] == "longer"

    async def test_host_held_before_any_of_its_pages_is_queued_holds_them(self, open_frontier):
        # The target of a redirect may ask to wait before a link to it is found.
        frontier = await open_frontier("worker")

        await frontier.hold_host("a", 60, "HTTP 429 Too Many Requests, Retry-After 60s")
        await frontier.add(["http://a/1"], depth=1)

        assert await still_waiting(frontier.take())
        (row,) = await fetch("SELECT accepted FROM hosts WHERE host = 'a'")
        assert row["accepted"] == 1


class TestHostInterval:
    async def test_interval_of_a_host_spaces_its_pages_for_all_workers_together(self, open_frontier):
        workers = [await open_frontier(f"worker-{i}", host_interval=0.05) for i in range(2)]
        await workers[0].seed([f"http://slow/{i}" for i in range(4)])
        await workers[0].set_host_interval("slow", 0.2)
        taken_at = []

        async def crawl(frontier: PostgresFrontier) -> None:
            while (page := await frontier.take()) is not None:
                (row,) = await fetch(
                    "SELECT extract(epoch FROM lease_until) AS until FROM frontier WHERE url = $1", page.url
                )
                taken_at.append(float(row["until"]) - frontier.lease_seconds)
                await frontier.finish(page, Outcome.PROCESSED)

        await asyncio.gather(*(crawl(frontier) for frontier in workers for _ in range(2)))

        taken_at.sort()
        assert len(taken_at) == 4
        assert min(later - earlier for earlier, later in itertools.pairwise(taken_at)) >= 0.2 - 1e-6

    async def test_interval_of_the_job_holds_for_a_host_that_asks_for_less(self, open_frontier):
        frontier = await open_frontier("worker", host_interval=60)
        await frontier.seed(["http://a/1", "http://a/2"])

        assert await take(frontier) == FrontierPage("http://a/1", 0)
        await frontier.set_host_interval("a", 0.01)

        assert await still_waiting(frontier.take())

    async def test_interval_never_goes_down_nor_shortens_a_hold(self, open_frontier):
        # Told before any page of the host is queued, as at seeding: the next page waits the interval from now.
        frontier = await open_frontier("worker")
        await frontier.set_host_interval("a", 60)
        held = await host_of("a")
        assert 59 < held["left"] <= 60

        await frontier.set_host_interval("a", 1)
        await frontier.hold_host("a", 120, "HTTP 429 Too Many Requests, Retry-After 120s")
        await frontier.set_host_interval("a", 2)

        (row,) = await fetch("SELECT interval FROM hosts WHERE host = 'a'")
        assert row["interval"] == 60
        held = await host_of("a")
        assert 119 < held["left"] <= 120
        assert held["hold_reason"] == "HTTP 429 Too Many Requests, Retry-After 120s"


class TestHostFailures:
    async def test_failures_of_a_host_told_by_workers_add_up_over_the_job(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.seed(["http://a/1"])

        assert await first.count_host_failures("a", circuit_openings=1) == HostFailures(1, 0)
        assert await second.count_host_failures("a", circuit_openings=1, robots_failures=2) == HostFailures(2, 2)
        # The target of a redirect may fail before any page of it is queued.
        assert await second.count_host_failures("b", robots_failures=1) == HostFailures(0, 1)

    async def test_robots_txt_read_counts_its_failures_from_zero(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.count_host_failures("a", circuit_openings=1, robots_failures=3)

        failures = await second.count_host_failures("a", robots_failures=1, robots_read=True)

        # The circuit openings stay.
        assert failures == HostFailures(1, 1)

    async def test_host_given_up_has_its_pages_queued_finished_and_the_job_may_end(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.seed(["http://a/1", "http://a/2", "http://a/3", "http://b/1"])
        taken = await take(first)
        assert taken.url == "http://a/1"

        await second.give_up_host("a", Outcome.UNREACHABLE, "robots.txt is unreachable (HTTP 503)")

        assert (await row_of("http://a/2"))["state"] == "unreachable"
        assert (await row_of("http://a/3"))["reason"] == "robots.txt is unreachable (HTTP 503)"
        # A page in progress is left to its worker.
        assert (await row_of("http://a/1"))["state"] == "leased"
        await first.finish(taken, Outcome.PROCESSED)
        await second.finish(await take(second), Outcome.PROCESSED)
        assert await second.take() is None
        assert await job_state() == "finished"
        (job,) = await fetch("SELECT unfinished FROM crawl_jobs")
        assert job["unfinished"] == 0

    async def test_redirect_targets_of_the_pages_of_a_host_given_up_may_be_queued(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.seed(["http://a/1", "http://b/1"])
        page = await take(frontier)
        # Put back after its redirect was followed, e.g. to wait for the host of the target.
        await frontier.mark_seen("http://c/target", page.url)
        await frontier.mark_seen("http://c/kept", "http://b/1")
        await frontier.put_back(page, uncount=False)

        await frontier.give_up_host("a", Outcome.FAILED, "circuit breaker of a opened 3 times")

        assert (await row_of("http://a/1"))["state"] == "failed"
        assert await frontier.add(["http://c/target", "http://c/kept"], depth=1) == 1
        assert (await row_of("http://c/target"))["state"] == "queued"

    async def test_host_given_up_keeps_the_outcome_it_was_first_given_up_with(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.seed(["http://a/1"])

        await frontier.give_up_host(
            "a", Outcome.FAILED, "circuit breaker of a opened 3 times", error="CircuitOpenError"
        )
        await frontier.give_up_host("a", Outcome.UNREACHABLE, "robots.txt is unreachable (HTTP 503)")

        row = await row_of("http://a/1")
        assert (row["state"], row["reason"], row["error"]) == (
            "failed",
            "circuit breaker of a opened 3 times",
            "CircuitOpenError",
        )

    async def test_page_of_a_host_given_up_is_handed_out_with_its_outcome_however_long_the_host_is_held(
        self, open_frontier
    ):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.seed(["http://a/1", "http://a/2"])
        taken = await take(first)
        await first.hold_host("a", 60, "circuit breaker of a is open")
        await second.give_up_host("a", Outcome.FAILED, "circuit breaker of a opened 3 times", error="CircuitOpenError")
        # Put back by its worker, which did not know the host was given up; or found later.
        await first.put_back(taken, uncount=False)
        await second.add(["http://a/3"], depth=1)

        pages = [await take(second), await take(second)]

        assert [page.url for page in pages] == ["http://a/1", "http://a/3"]
        assert [second.given_up(page) for page in pages] == [
            GivenUp(Outcome.FAILED, "circuit breaker of a opened 3 times", "CircuitOpenError")
        ] * 2
        # Its turn did not move: it is still held as long as it was.
        assert 59 < (await host_of("a"))["left"] <= 60

    async def test_page_of_a_host_not_given_up_is_handed_out_without_an_outcome(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.seed(["http://a/1"])
        await frontier.count_host_failures("a", circuit_openings=2)

        assert frontier.given_up(await take(frontier)) is None


class TestMaxPages:
    async def test_max_pages_holds_for_all_workers_together(self, open_frontier):
        workers = [await open_frontier(f"worker-{i}", max_pages=5) for i in range(2)]
        await workers[0].seed([f"http://site/{i}" for i in range(20)])
        admitted = []

        async def crawl(frontier: PostgresFrontier) -> None:
            while (page := await frontier.take()) is not None:
                if await frontier.admit(page) is Admission.ADMITTED:
                    admitted.append(page.url)
                    await frontier.finish(page, Outcome.PROCESSED)
                else:
                    await frontier.put_back(page, uncount=False)

        await asyncio.gather(*(crawl(frontier) for frontier in workers for _ in range(4)))

        assert len(admitted) == 5
        await workers[0].refresh_stats()
        assert (workers[0].stats().requested, workers[0].stats().processed) == (5, 5)

    async def test_take_waits_while_another_worker_may_give_its_counted_page_back(self, open_frontier):
        holding = await open_frontier("holding", max_pages=1)
        other = await open_frontier("other", max_pages=1)
        await holding.seed(["http://site/a", "http://site/b"])
        a = await take(holding)
        assert await holding.admit(a) is Admission.ADMITTED

        assert await still_waiting(other.take())

        await holding.put_back(a, uncount=True)
        b = await asyncio.wait_for(other.take(), 5)
        assert await other.admit(b) is Admission.ADMITTED

    async def test_host_limit_holds_for_all_workers_together(self, open_frontier):
        first = await open_frontier("first", max_pages_per_host=1)
        second = await open_frontier("second", max_pages_per_host=1)
        await first.seed(["http://site/a", "http://site/b"])
        a, b = await take(first), await take(second)

        assert await first.admit(a) is Admission.ADMITTED
        assert await second.admit(b) is Admission.OVER_HOST_LIMIT


class TestWaiting:
    async def test_take_waits_for_the_pages_in_progress_of_another_worker(self, open_frontier):
        busy, other = await open_frontier("busy"), await open_frontier("other")
        await busy.seed(["http://site/"])
        page = await take(busy)
        waiter = asyncio.create_task(other.take())
        await asyncio.sleep(0.2)
        assert not waiter.done()

        await busy.add(["http://site/link"], depth=1)
        await busy.finish(page, Outcome.PROCESSED)

        assert await asyncio.wait_for(waiter, 5) == FrontierPage("http://site/link", 1)

    async def test_last_page_of_another_worker_ends_the_waiting(self, open_frontier):
        busy, other = await open_frontier("busy"), await open_frontier("other")
        await busy.seed(["http://site/"])
        page = await take(busy)
        waiter = asyncio.create_task(other.take())
        await asyncio.sleep(0.2)

        await busy.finish(page, Outcome.FAILED, "NetworkError: boom")

        assert await asyncio.wait_for(waiter, 5) is None


class TestSeeding:
    async def test_no_page_is_handed_out_while_the_job_is_seeding(self, open_frontier):
        await make_job(state="seeding")
        frontier = await open_frontier("worker")
        await frontier.seed(["http://site/"])

        waiter = asyncio.create_task(frontier.take())
        await asyncio.sleep(0.2)
        assert not waiter.done()
        await fetch("UPDATE crawl_jobs SET state = 'running'")

        assert await asyncio.wait_for(waiter, 5) == FrontierPage("http://site/", 0)

    async def test_seeding_job_is_waited_for_with_nothing_queued(self, open_frontier):
        await make_job(state="seeding")
        frontier = await open_frontier("worker")

        assert await still_waiting(frontier.take())
        assert await job_state() == "seeding"


class TestScopeOfTheJob:
    async def test_worker_learns_of_a_host_another_brought_into_the_scope_with_its_next_page(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.seed(["http://site/a", "http://site/b"])

        await first.widen_scope("other", lambda url: True)

        assert second.scope_hosts() == []
        await take(second)
        assert second.scope_hosts() == ["other"]

    async def test_new_worker_knows_the_scope_of_the_job(self, open_frontier):
        first = await open_frontier("first")
        await first.widen_scope("other", lambda url: True)

        second = await open_frontier("second")

        assert second.scope_hosts() == ["other"]

    async def test_pages_held_are_queued_once_when_two_workers_widen_the_scope_at_once(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.hold_out_of_scope([f"http://other/{i}" for i in range(20)])

        queued = await asyncio.gather(
            first.widen_scope("other", lambda url: True), second.widen_scope("other", lambda url: True)
        )

        assert sum(queued) == 20
        assert len(await fetch("SELECT url FROM frontier WHERE state = 'queued'")) == 20
        assert await fetch("SELECT url FROM out_of_scope") == []


class TestEnd:
    async def test_job_is_finished_once_every_page_is_done(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.seed(["http://site/a", "http://site/b"])
        await frontier.finish(await take(frontier), Outcome.PROCESSED)
        await frontier.finish(await take(frontier), Outcome.FAILED, "NetworkError: boom")

        assert await frontier.take() is None

        (job,) = await fetch("SELECT state, finished_at FROM crawl_jobs")
        assert job["state"] == "finished"
        assert job["finished_at"] is not None

    async def test_job_is_finished_once_max_pages_are_requested_with_pages_left_in_the_queue(self, open_frontier):
        frontier = await open_frontier("worker", max_pages=1)
        await frontier.seed([f"http://site/{i}" for i in range(3)])
        page = await take(frontier)
        assert await frontier.admit(page) is Admission.ADMITTED
        await frontier.finish(page, Outcome.PROCESSED)

        assert await frontier.take() is None

        assert await job_state() == "finished"
        assert len(await fetch("SELECT url FROM frontier WHERE state = 'queued'")) == 2

    async def test_pages_held_out_of_scope_do_not_hold_the_end(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.hold_out_of_scope(["http://other/a"])

        assert await frontier.take() is None
        assert await job_state() == "finished"

    async def test_job_is_not_finished_while_another_worker_has_a_page(self, open_frontier):
        busy, other = await open_frontier("busy"), await open_frontier("other")
        await busy.seed(["http://site/"])
        page = await take(busy)

        assert await still_waiting(other.take())
        assert await job_state() == "running"
        await busy.finish(page, Outcome.PROCESSED)
        assert await other.take() is None
        assert await job_state() == "finished"

    async def test_job_is_finished_at_close_once_its_last_pages_are_saved(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.seed(["http://site/"])
        await frontier.finish(await take(frontier), Outcome.PROCESSED, pending_save=True)
        assert await frontier.take() is None
        assert await job_state() == "running"

        await frontier.saved(["http://site/"])
        await frontier.close()

        assert await job_state() == "finished"

    async def test_job_is_not_finished_at_close_while_pages_wait_for_their_save(self, open_frontier):
        frontier = await open_frontier("worker")
        await frontier.seed(["http://site/"])
        await frontier.finish(await take(frontier), Outcome.PROCESSED, pending_save=True)

        await frontier.close()

        assert await job_state() == "running"


class TestResume:
    async def test_new_worker_goes_on_with_the_job(self, open_frontier):
        first = await open_frontier("first")
        await first.seed([f"http://site/{i}" for i in range(3)])
        await first.finish(await take(first), Outcome.PROCESSED)
        await first.close()

        second = await open_frontier("second")

        assert [await take(second), await take(second)] == [
            FrontierPage("http://site/1", 0),
            FrontierPage("http://site/2", 0),
        ]
        assert await second.add(["http://site/0"], depth=1) == 0


class TestStats:
    async def test_stats_are_those_of_the_job(self, open_frontier):
        first, second = await open_frontier("first"), await open_frontier("second")
        await first.seed([f"http://site/{i}" for i in range(3)])
        await second.finish(await take(second), Outcome.PROCESSED)
        await take(second)

        await first.refresh_stats()

        stats = first.stats()
        assert (stats.queued, stats.in_progress, stats.processed) == (1, 1, 1)

    async def test_heartbeat_refreshes_the_stats(self, open_frontier):
        watching = await open_frontier("watching", heartbeat_seconds=0.05)
        other = await open_frontier("other")

        await other.seed(["http://site/a", "http://site/b"])
        await asyncio.sleep(0.3)

        assert watching.stats().queued == 2


class TestDatabaseGone:
    async def test_operations_fail_with_the_errors_of_the_frontier(self, open_frontier):
        await open_frontier("seeder")
        async with DatabaseLink() as link:
            frontier = await PostgresFrontier.open(link.dsn, job="test", worker="cut-off")
            try:
                await frontier.seed(["http://site/1", "http://site/2"])
                page = await take(frontier)
                await link.cut()

                with pytest.raises(PostgresFrontier.ERRORS):
                    await frontier.finish(page, Outcome.PROCESSED)
                with pytest.raises(PostgresFrontier.ERRORS):
                    await frontier.take()
            finally:
                await frontier.close()

    async def test_close_without_the_database_leaves_the_pages_to_their_leases(self, open_frontier, caplog):
        caplog.set_level(logging.WARNING, logger="crawler")
        other = await open_frontier("other")
        async with DatabaseLink() as link:
            frontier = await PostgresFrontier.open(link.dsn, job="test", worker="cut-off", lease_seconds=0.5)
            await frontier.seed(["http://site/1"])
            await take(frontier)
            await link.cut()

            async with asyncio.timeout(5):
                await frontier.close()

        assert "Could not put back the pages worker cut-off has in progress" in caplog.text
        assert (await row_of("http://site/1"))["state"] == "leased"
        # Its lease expires: another worker takes the page.
        async with asyncio.timeout(5):
            assert await take(other) == FrontierPage("http://site/1", 0)
