"""Integration tests for PostgresFrontier: several workers of one job on one database.

The contract both frontiers keep is checked in test_frontier.py; these
tests check what only a shared frontier has: leases, the heartbeat, pages
pending their save, the host interval and the limits held by all workers together.
"""

import asyncio
import itertools
import time
from collections.abc import AsyncGenerator, Awaitable, Callable

import asyncpg
import pytest
from helpers import POSTGRES_DSN, drop_frontier_tables, make_job

from crawler import Admission, FrontierPage, JobError, Outcome, PostgresFrontier

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
    (row,) = await fetch("SELECT state, worker, attempts, reason FROM frontier WHERE url = $1", url)
    return row


async def job_state(name: str = "test") -> str:
    (row,) = await fetch("SELECT state FROM crawl_jobs WHERE name = $1", name)
    return row["state"]


async def take(frontier: PostgresFrontier) -> FrontierPage:
    page = await frontier.take()
    assert page is not None
    return page


async def still_waiting(take: Awaitable[FrontierPage | None], seconds: float = 0.3) -> bool:
    """Whether `take` is still waiting after `seconds`; it is cancelled then."""
    try:
        await asyncio.wait_for(take, seconds)
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

        new = await asyncio.gather(first.mark_seen("http://site/target"), second.mark_seen("http://site/target"))

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
