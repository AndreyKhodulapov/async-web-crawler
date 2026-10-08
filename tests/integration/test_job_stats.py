"""Integration tests for the statistics of a crawl job, read from the tables its workers share."""

import asyncio
import json

import pytest
from helpers import POSTGRES_DSN, READ_ONLY_DSN, drop_frontier_tables, frontier_tables_exist, make_config, make_job

from crawler import AdvancedCrawler, FrontierError, JobError, Outcome, PostgresFrontier
from crawler.distributed import create_job, export_job_stats, job_stats, run_worker
from demo_site import free_port

pytestmark = pytest.mark.postgres

# The statistics of a job that are those of a crawl of one process; the times differ.
SHARED_KEYS = ("total_pages", "successful", "failed", "skipped", "status_codes", "errors", "top_domains")


@pytest.fixture(autouse=True)
async def empty_tables() -> None:
    await drop_frontier_tables()


async def open_worker(worker: str, **options) -> PostgresFrontier:
    return await PostgresFrontier.open(POSTGRES_DSN, job="test", worker=worker, **{"poll_interval": 0.02, **options})


async def test_statistics_of_a_job_are_those_of_a_crawl_of_one_process(url):
    # Broken links fail, a page that asks TestBot not to keep it is skipped.
    # Each page is reached one way only, so the order the workers take the
    # pages in does not change them: a page that links to b.html, or a depth
    # of 3 (c.html by a link as well as by the redirect of moved), would.
    job = make_config(
        urls=[url("/site/"), url("/site/for-testbot.html")], crawler={"max_depth": 2, "respect_robots": True}
    )
    async with AdvancedCrawler(job, configure_logging=False) as crawler:
        await crawler.crawl()
        local = crawler.get_stats()
    await create_job(job, "test", dsn=POSTGRES_DSN)
    config = make_config(distributed={"database_url": POSTGRES_DSN, "poll_interval": 0.1})

    await asyncio.gather(*(run_worker(config, "test", worker=f"w{n}", configure_logging=False) for n in range(2)))
    stats = await job_stats(POSTGRES_DSN, "test")

    assert local["failed"] and local["skipped"] and len(local["status_codes"]) > 1
    assert {key: stats[key] for key in SHARED_KEYS} == {key: local[key] for key in SHARED_KEYS}
    assert (stats["job"], stats["state"], stats["queued"], stats["in_progress"]) == ("test", "finished", 0, 0)
    assert stats["avg_response_time"] > 0
    assert stats["started_at"] < stats["finished_at"]
    assert stats["pages_per_second"] == pytest.approx(stats["total_pages"] / stats["elapsed_seconds"])
    # The process that seeded the job is no worker of it.
    workers = stats["workers"]
    assert sorted(workers) == ["w0", "w1"]
    assert sum(worker["pages"] for worker in workers.values()) == stats["total_pages"]
    assert sum(worker["failed"] for worker in workers.values()) == stats["failed"]
    assert {worker["state"] for worker in workers.values()} == {"stopped"}
    assert all(worker["active_seconds"] > 0 for worker in workers.values())


async def test_job_no_worker_has_started_has_its_pages_queued_and_no_time():
    await make_job("test")
    seeding = await open_worker("seeding")
    try:
        await seeding.seed(["http://a/1", "http://a/2"])
    finally:
        await seeding.close()

    stats = await job_stats(POSTGRES_DSN, "test")

    assert (stats["state"], stats["queued"], stats["total_pages"], stats["workers"]) == ("running", 2, 0, {})
    assert (stats["started_at"], stats["finished_at"], stats["elapsed_seconds"]) == (None, None, 0.0)
    assert (stats["pages_per_second"], stats["avg_response_time"]) == (0.0, 0.0)


async def test_worker_run_again_under_its_name_counts_the_time_it_ran_without_the_pause():
    await make_job("test")
    first = await open_worker("w")
    await first.seed(["http://a/1", "http://a/2"])
    await first.finish(await first.take(), Outcome.PROCESSED, status=200, elapsed=0.1)
    await first.close()
    started = (await job_stats(POSTGRES_DSN, "test"))["workers"]["w"]["started_at"]
    await asyncio.sleep(0.6)

    again = await open_worker("w")
    await again.finish(await again.take(), Outcome.FAILED, "HTTP 404", status=404, elapsed=0.3, error="HTTPError")
    assert await again.take() is None
    await again.close()
    stats = await job_stats(POSTGRES_DSN, "test")

    worker = stats["workers"]["w"]
    assert (worker["state"], worker["pages"], worker["successful"], worker["failed"]) == ("stopped", 2, 1, 1)
    assert worker["started_at"] == started
    assert worker["active_seconds"] < 0.5
    # The job ran from the first start of a worker to its end, the pause included.
    assert stats["elapsed_seconds"] > 0.6
    assert stats["state"] == "finished"
    assert (stats["status_codes"], stats["errors"]) == ({200: 1, 404: 1}, {"HTTPError": 1})
    assert stats["avg_response_time"] == pytest.approx(0.2)


async def test_worker_that_stopped_renewing_its_lease_is_lost():
    await make_job("test")
    running = await open_worker("running")
    lost = await open_worker("lost", lease_seconds=0.2, heartbeat_seconds=60)
    try:
        await running.seed(["http://a/1", "http://b/1"])
        await running.take()
        await lost.take()
        await asyncio.sleep(0.3)

        stats = await job_stats(POSTGRES_DSN, "test")
    finally:
        await running.close()
        await lost.close()

    assert {name: worker["state"] for name, worker in stats["workers"].items()} == {
        "running": "running",
        "lost": "lost",
    }
    # The time since the lost worker was last seen is not counted.
    assert stats["workers"]["lost"]["active_seconds"] < stats["workers"]["running"]["active_seconds"]
    assert stats["in_progress"] == 2


async def test_page_whose_lease_expired_max_attempts_times_fails_with_lease_expired():
    await make_job("test")
    stopped = await open_worker("stopped", lease_seconds=0.2, heartbeat_seconds=60, max_attempts=1)
    last = await open_worker("last", max_attempts=1)
    try:
        await stopped.seed(["http://a/1"])
        await stopped.take()
        assert await asyncio.wait_for(last.take(), 5) is None

        stats = await job_stats(POSTGRES_DSN, "test")
    finally:
        await stopped.close()
        await last.close()

    assert (stats["failed"], stats["errors"]) == (1, {"LeaseExpired": 1})
    # The page failed in the database, by no worker.
    assert [worker["pages"] for worker in stats["workers"].values()] == [0, 0]


async def test_statistics_of_a_job_that_does_not_exist_fail():
    await make_job("other")

    with pytest.raises(JobError, match='There is no crawl job named "test"'):
        await job_stats(POSTGRES_DSN, "test")


async def test_statistics_of_a_database_without_jobs_fail_and_make_no_tables():
    with pytest.raises(JobError, match='There is no crawl job named "test"'):
        await job_stats(POSTGRES_DSN, "test")

    assert not await frontier_tables_exist()


async def test_statistics_are_read_by_a_session_that_may_not_write():
    await make_job("test")

    stats = await job_stats(READ_ONLY_DSN, "test")

    assert (stats["job"], stats["state"], stats["workers"]) == ("test", "running", {})


async def test_statistics_without_the_database_fail_as_the_frontier_does():
    dsn = f"postgresql://crawler:crawler@127.0.0.1:{free_port()}/crawler"

    with pytest.raises(FrontierError, match="the database of crawl job test failed") as raised:
        await job_stats(dsn, "test")

    assert isinstance(raised.value.__cause__, OSError)


async def test_reports_of_a_job_are_written_to_missing_directories(tmp_path):
    await make_job("test")
    stats = await job_stats(POSTGRES_DSN, "test")

    written = export_job_stats(
        stats, stats_json=tmp_path / "out" / "stats.json", html=tmp_path / "out" / "report.html", title="Books"
    )

    assert written == [tmp_path / "out" / "stats.json", tmp_path / "out" / "report.html"]
    assert json.loads(written[0].read_text(encoding="utf-8"))["job"] == "test"
    html = written[1].read_text(encoding="utf-8")
    assert "<title>Books</title>" in html
    assert "No worker has started." in html
