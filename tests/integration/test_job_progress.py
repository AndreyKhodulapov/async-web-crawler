"""Integration tests for the progress of a crawl job, read from the tables its workers share."""

import asyncio
import io

import asyncpg
import pytest
from helpers import POSTGRES_DSN, READ_ONLY_DSN, drop_frontier_tables, frontier_tables_exist, make_job

from crawler import Admission, FrontierError, JobError, Outcome, PostgresFrontier
from crawler.distributed import job_progress, watch_job
from demo_site import free_port

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
async def empty_tables() -> None:
    await drop_frontier_tables()


async def open_worker(worker: str, **options) -> PostgresFrontier:
    return await PostgresFrontier.open(POSTGRES_DSN, job="test", worker=worker, **{"poll_interval": 0.02, **options})


async def execute(statement: str, *arguments: object) -> None:
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        await connection.execute(statement, *arguments)
    finally:
        await connection.close()


async def finished_at() -> dict[str, bool]:
    """Whether each page of the frontier has the moment it was finished."""
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        return {row["url"]: row["finished_at"] is not None for row in await connection.fetch("SELECT * FROM frontier")}
    finally:
        await connection.close()


async def crawl(frontier: PostgresFrontier, pages: int) -> None:
    """Take, admit and process `pages` pages."""
    for _ in range(pages):
        page = await frontier.take()
        assert await frontier.admit(page) is Admission.ADMITTED
        await frontier.finish(page, Outcome.PROCESSED, status=200, elapsed=0.1)


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


class TestFinishedAt:
    async def test_page_has_the_moment_it_was_finished_and_keeps_it_once_saved(self):
        await make_job("test")
        frontier = await open_worker("w")
        try:
            await frontier.seed(["http://a/1", "http://a/2", "http://a/3"])
            first, second = await frontier.take(), await frontier.take()
            await frontier.finish(first, Outcome.PROCESSED, pending_save=True)
            await frontier.finish(second, Outcome.FAILED, "HTTP 404", status=404, error="HTTPError")
            assert await finished_at() == {"http://a/1": True, "http://a/2": True, "http://a/3": False}

            await execute("UPDATE frontier SET finished_at = '2026-01-01' WHERE url = 'http://a/1'")
            await frontier.saved(["http://a/1"])
        finally:
            await frontier.close()

        connection = await asyncpg.connect(POSTGRES_DSN)
        try:
            row = await connection.fetchrow("SELECT state, finished_at FROM frontier WHERE url = 'http://a/1'")
        finally:
            await connection.close()
        assert (row["state"], row["finished_at"].year) == ("processed", 2026)

    async def test_pages_of_a_host_given_up_are_finished_at_once(self):
        await make_job("test")
        frontier = await open_worker("w")
        try:
            await frontier.seed(["http://a/1", "http://a/2", "http://b/1"])
            await frontier.give_up_host("a", Outcome.FAILED, "circuit breaker of a opened 3 times")
        finally:
            await frontier.close()

        assert await finished_at() == {"http://a/1": True, "http://a/2": True, "http://b/1": False}

    async def test_page_failed_by_expired_leases_is_finished_one_queued_again_is_not(self):
        await make_job("test")
        stopped = await open_worker("stopped", lease_seconds=0.2, heartbeat_seconds=60, max_attempts=2)
        last = await open_worker("last", max_attempts=2)
        try:
            await stopped.seed(["http://a/1"])
            await stopped.finish(await stopped.take(), Outcome.PROCESSED, pending_save=True)
            await asyncio.sleep(0.3)
            # The page pending its save comes back unfinished.
            again = await asyncio.wait_for(last.take(), 5)
            assert again.url == "http://a/1"
            assert await finished_at() == {"http://a/1": False}
            await last.put_back(again, uncount=True)

            await stopped.take()
            await asyncio.sleep(0.3)
            assert await asyncio.wait_for(last.take(), 5) is None
        finally:
            await stopped.close()
            await last.close()

        assert await finished_at() == {"http://a/1": True}


class TestJobProgress:
    async def test_speed_is_that_of_the_pages_finished_in_the_last_seconds(self):
        await make_job("test", max_pages=10)
        frontier = await open_worker("w")
        try:
            await frontier.seed([f"http://a/{n}" for n in range(8)])
            await crawl(frontier, 4)
            await execute("UPDATE workers SET started_at = now() - interval '100 seconds'")
            await execute("UPDATE frontier SET finished_at = now() - interval '60 seconds' WHERE url IN ($1, $2)",
                          "http://a/0", "http://a/1")  # fmt: skip

            progress = await job_progress(POSTGRES_DSN, "test", window=30)
        finally:
            await frontier.close()

        assert (progress.state, progress.done, progress.total, progress.percent) == ("running", 4, 10, 40.0)
        assert progress.pages_per_second == pytest.approx(2 / 30)
        assert progress.eta == pytest.approx(6 / (2 / 30))
        assert progress.elapsed == pytest.approx(100, abs=1)
        # Four pages left to crawl; the limit leaves six to request.
        assert (progress.queued, progress.in_progress, progress.workers, progress.lost) == (4, 0, 1, 0)

    async def test_job_younger_than_the_window_has_the_speed_of_its_time(self):
        await make_job("test", max_pages=10)
        frontier = await open_worker("w")
        try:
            await frontier.seed([f"http://a/{n}" for n in range(4)])
            await crawl(frontier, 4)
            await execute("UPDATE workers SET started_at = now() - interval '10 seconds'")

            progress = await job_progress(POSTGRES_DSN, "test", window=30)
        finally:
            await frontier.close()

        assert progress.pages_per_second == pytest.approx(0.4, rel=0.05)

    async def test_pages_not_requested_are_not_done(self):
        await make_job("test", max_pages=10, max_pages_per_host=1)
        frontier = await open_worker("w")
        try:
            await frontier.seed(["http://a/1", "http://a/2", "http://b/1"])
            pages = {page.url: page for page in [await frontier.take() for _ in range(3)]}
            assert await frontier.admit(pages["http://a/1"]) is Admission.ADMITTED
            await frontier.finish(pages["http://a/1"], Outcome.PROCESSED)
            assert await frontier.admit(pages["http://a/2"]) is Admission.OVER_HOST_LIMIT
            await frontier.finish(pages["http://a/2"], Outcome.SKIPPED, "max_pages_per_host reached")
            # Refused by an open circuit breaker: not held against the limit.
            assert await frontier.admit(pages["http://b/1"]) is Admission.ADMITTED
            await frontier.finish(pages["http://b/1"], Outcome.FAILED, "circuit breaker open", uncount=True)

            progress = await job_progress(POSTGRES_DSN, "test")
        finally:
            await frontier.close()

        assert (progress.done, progress.failed, progress.percent) == (1, 1, 10.0)

    async def test_queue_shows_no_more_than_the_limit_leaves_to_request(self):
        await make_job("test", max_pages=3)
        seeding = await open_worker("seeding")
        try:
            await seeding.seed([f"http://a/{n}" for n in range(10)])
        finally:
            await seeding.close()

        progress = await job_progress(POSTGRES_DSN, "test")

        assert (progress.queued, progress.done, progress.workers, progress.elapsed) == (3, 0, 0, 0.0)
        assert (progress.pages_per_second, progress.eta) == (0.0, None)

    async def test_job_without_a_page_limit_has_no_percent(self):
        await make_job("test")
        frontier = await open_worker("w")
        try:
            await frontier.seed(["http://a/1", "http://a/2"])
            await crawl(frontier, 1)

            progress = await job_progress(POSTGRES_DSN, "test")
        finally:
            await frontier.close()

        assert (progress.total, progress.percent, progress.eta, progress.queued) == (None, None, None, 1)

    async def test_workers_running_and_lost_are_counted(self):
        await make_job("test")
        running = await open_worker("running")
        lost = await open_worker("lost", lease_seconds=0.2, heartbeat_seconds=60)
        stopped = await open_worker("stopped")
        try:
            await running.seed(["http://a/1", "http://b/1", "http://c/1"])
            for frontier in (running, lost, stopped):
                await frontier.take()
            await stopped.close()
            await asyncio.sleep(0.3)

            progress = await job_progress(POSTGRES_DSN, "test")
        finally:
            await running.close()
            await lost.close()

        # The page of the stopped worker went back to the queue.
        assert (progress.workers, progress.lost, progress.in_progress, progress.queued) == (1, 1, 2, 1)

    async def test_finished_job_has_no_time_left(self):
        await make_job("test", max_pages=10)
        frontier = await open_worker("w")
        try:
            await frontier.seed(["http://a/1", "http://a/2"])
            await crawl(frontier, 2)
            assert await frontier.take() is None
        finally:
            await frontier.close()
        await asyncio.sleep(0.1)

        progress = await job_progress(POSTGRES_DSN, "test")

        assert (progress.state, progress.done, progress.eta) == ("finished", 2, 0.0)
        # The speed of the last seconds of the job, not of the time since.
        assert progress.pages_per_second == pytest.approx(2 / progress.elapsed)

    async def test_progress_of_a_job_that_does_not_exist_fails(self):
        await make_job("other")

        with pytest.raises(JobError, match='no crawl job named "test"'):
            await job_progress(POSTGRES_DSN, "test")

    async def test_progress_of_a_database_without_jobs_fails_and_makes_no_tables(self):
        with pytest.raises(JobError, match='no crawl job named "test"'):
            await job_progress(POSTGRES_DSN, "test")

        assert not await frontier_tables_exist()

    async def test_progress_is_read_by_a_session_that_may_not_write(self):
        await make_job("test", max_pages=10)

        progress = await job_progress(READ_ONLY_DSN, "test")

        assert (progress.state, progress.done, progress.total) == ("running", 0, 10)

    async def test_progress_without_the_database_fails_as_the_frontier_does(self):
        dsn = f"postgresql://crawler:crawler@127.0.0.1:{free_port()}/crawler"

        with pytest.raises(FrontierError, match="the database of crawl job test failed") as raised:
            await job_progress(dsn, "test")

        assert isinstance(raised.value.__cause__, OSError)


class TestWatchJob:
    async def test_prints_a_line_per_update_until_the_job_is_finished(self):
        await make_job("test", max_pages=10)
        frontier = await open_worker("w")
        stream = io.StringIO()
        try:
            await frontier.seed(["http://a/1", "http://a/2"])
            watching = asyncio.create_task(watch_job(POSTGRES_DSN, "test", interval=0.02, stream=stream))
            while not stream.getvalue():
                await asyncio.sleep(0.01)
            await crawl(frontier, 2)
            assert await frontier.take() is None
            await asyncio.wait_for(watching, 5)
        finally:
            await frontier.close()

        lines = stream.getvalue().splitlines()
        assert len(lines) >= 2
        assert lines[0].startswith("[--------------------]   0% | 0/10 pages")
        assert lines[-1].startswith("[####----------------]  20% | 2/10 pages")
        assert "| done |" in lines[-1]
        assert all("| done |" not in line for line in lines[:-1])

    async def test_redraws_the_line_in_a_terminal(self):
        await make_job("test", max_pages=10, state="finished")
        stream = Terminal()

        await watch_job(POSTGRES_DSN, "test", interval=0.02, stream=stream)

        assert stream.getvalue().startswith("\r\033[K[--------------------]   0% | 0/10 pages")
        assert stream.getvalue().endswith("| done | workers 0 | in progress 0 | queued 0 | 0s\n")

    async def test_job_that_does_not_exist_fails(self):
        await make_job("other")

        with pytest.raises(JobError):
            await watch_job(POSTGRES_DSN, "test", stream=io.StringIO())

    async def test_watched_by_a_session_that_may_not_write(self):
        await make_job("test", max_pages=10, state="finished")
        stream = io.StringIO()

        await watch_job(READ_ONLY_DSN, "test", interval=0.02, stream=stream)

        assert "| done |" in stream.getvalue()
