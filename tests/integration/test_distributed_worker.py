"""Integration tests for the workers of a crawl job in PostgreSQL: several of them crawl one local site together."""

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Awaitable, Callable

import asyncpg
import pytest
from helpers import POSTGRES_DSN, UNTHROTTLED, drop_frontier_tables, make_config, urlset

from crawler import (
    AdvancedCrawler,
    AsyncCrawler,
    ConfigError,
    CrawlerConfig,
    JobError,
    PostgresFrontier,
    RobotsParser,
)
from crawler.distributed import create_job, run_worker

pytestmark = pytest.mark.postgres

SITEMAP = "/sitemaps/sitemap.xml"
# The page /wide/0 and the 50 pages it links to.
WIDE_PAGES = 51
# Requests are logged by the site as they arrive, holds are kept by the clock of the database.
EPSILON = 0.05


@pytest.fixture(autouse=True)
async def empty_tables() -> None:
    await drop_frontier_tables()


async def fetch(query: str, *parameters: object) -> list[asyncpg.Record]:
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        return await connection.fetch(query, *parameters)
    finally:
        await connection.close()


async def job_state() -> str:
    (job,) = await fetch("SELECT state FROM crawl_jobs")
    return job["state"]


async def state_of(url: str) -> str:
    (row,) = await fetch("SELECT state FROM frontier WHERE url = $1", url)
    return row["state"]


async def urls_in(state: str) -> set[str]:
    return {row["url"] for row in await fetch("SELECT url FROM frontier WHERE state = $1", state)}


def worker_config(**sections) -> CrawlerConfig:
    """A configuration of a worker: the database of the tests, polled often so that the workers end soon."""
    distributed = {"database_url": POSTGRES_DSN, "poll_interval": 0.1, **sections.pop("distributed", {})}
    return make_config(distributed=distributed, **sections)


async def local_crawl(config: CrawlerConfig) -> set[str]:
    """The pages a local crawl of the configuration processes."""
    async with AdvancedCrawler(config, configure_logging=False) as crawler:
        return set(await crawler.crawl())


async def run_workers(config: CrawlerConfig, count: int, **options) -> list[dict]:
    return await asyncio.gather(
        *(
            run_worker(config, "test", worker=f"w{number}", configure_logging=False, **options)
            for number in range(count)
        )
    )


async def run_worker_until(config: CrawlerConfig, done: Callable[[], Awaitable[bool]], *, then: float = 0.0) -> None:
    """Run a worker until `done` says so and `then` seconds more, then stop it: the next one knows of what it did from the database alone."""
    worker = asyncio.create_task(run_worker(config, "test", worker="first", configure_logging=False))
    try:
        async with asyncio.timeout(5):
            while not await done():
                await asyncio.sleep(0.02)
        await asyncio.sleep(then)
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


def saved_urls(directory) -> list[str]:
    """The URLs of the pages saved to the JSON Lines files of the workers in `directory`."""
    return [
        json.loads(line)["url"]
        for path in sorted(directory.glob("pages-*.jsonl"))
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


async def test_workers_crawl_every_page_once_as_a_local_crawl_does(url, site, tmp_path):
    job = make_config(urls=[url("/wide/0")])
    expected = await local_crawl(job)
    site.hits.clear()
    await create_job(job, "test", dsn=POSTGRES_DSN)

    stats = await run_workers(
        worker_config(crawler={"max_concurrent": 2}, storage={"outputs": [str(tmp_path / "pages-{worker}.jsonl")]}), 3
    )

    assert len(expected) == WIDE_PAGES
    assert sorted(saved_urls(tmp_path)) == sorted(expected)
    assert [path for path in site.hits if site.hits[path] != 1] == []
    assert sum(worker["total_pages"] for worker in stats) == WIDE_PAGES
    assert await urls_in("processed") == expected
    assert await job_state() == "finished"


async def test_start_url_that_redirects_to_another_host_brings_it_into_scope_for_every_worker(url, site):
    # A start URL that ends on localhost, and a sitemap page there, held out of scope until then.
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"), url("/site/c.html", "localhost"))}
    job = make_config(urls=[url("/site/to-other-host")], sitemaps={"urls": [url(SITEMAP)]})
    expected = await local_crawl(job)
    await create_job(job, "test", dsn=POSTGRES_DSN)

    await run_workers(worker_config(), 2)

    assert {url("/site/a.html", "localhost"), url("/site/c.html", "localhost")} <= expected
    assert await urls_in("processed") == expected
    assert await fetch("SELECT url FROM out_of_scope") == []


async def test_workers_stop_at_max_pages_of_the_job_and_it_is_finished(url, site):
    await create_job(make_config(urls=[url("/wide/0")], crawler={"max_pages": 5}), "test", dsn=POSTGRES_DSN)

    stats = await run_workers(worker_config(), 2)

    assert site.hits.total() == 5
    assert sum(worker["total_pages"] for worker in stats) == 5
    assert await job_state() == "finished"


async def test_job_is_crawled_on_after_its_workers_stop(url, site):
    site.latency = 0.05
    await create_job(make_config(urls=[url("/wide/0")]), "test", dsn=POSTGRES_DSN)
    config = worker_config(crawler={"max_concurrent": 2})
    stopped = asyncio.create_task(run_worker(config, "test", worker="stopped", configure_logging=False))
    while len(await urls_in("processed")) < 5:
        await asyncio.sleep(0.02)
    stopped.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopped
    assert await job_state() == "running"
    assert await fetch("SELECT url FROM frontier WHERE state = 'leased'") == []

    await run_worker(config, "test", worker="next", configure_logging=False)

    assert len(await urls_in("processed")) == WIDE_PAGES
    assert await job_state() == "finished"


async def test_requests_to_a_host_are_spaced_out_by_all_workers_together(url, site):
    await create_job(
        make_config(urls=[url("/wide/0")], crawler={"max_pages": 6, "rate_limit": 5.0}), "test", dsn=POSTGRES_DSN
    )

    await run_workers(worker_config(), 2)

    times = [moment for path, moment in site.log if path.startswith("/wide/")]
    assert len(times) == 6
    # The workers take a page of the host every 0.2 s, five intervals in
    # all; about 0.45 s if each kept its own requests apart only. A request
    # starts a little after its page is taken, the first of a worker later,
    # as it opens connections: up to an interval is allowed for it.
    assert times[-1] - times[0] >= 4 * 0.2


def held_job(url, first: str, *, crawler: dict | None = None, **sections) -> tuple[list[str], list[str], CrawlerConfig]:
    """A job whose first page, `first`, asks its host to wait; the URLs of that host, those of another one and the job.

    A page of a host is taken every 0.5 s, and the first request of a
    worker comes up to 0.2 s after its page: the hold reaches the database
    before any worker may take the next page of the host. The two hosts
    are one server: the paths tell their requests apart.
    """
    held = [url(first), url("/site/a.html"), url("/site/b.html")]
    other = [url(f"/wide/{n}", "localhost") for n in range(1, 5)]
    crawler = {"max_depth": 0, "rate_limit": 2.0, **(crawler or {})}
    return held, other, make_config(urls=[*held, *other], crawler=crawler, **sections)


@pytest.mark.parametrize(
    ("first", "retry", "fails"),
    [
        # Not retried: the page fails, and its host is held back all the same.
        ("/busy/2", {}, True),
        # Too long to retry: the page goes back and comes back with its host.
        ("/overloaded/1/2", {"max_retries": 1, "max_delay": 0.5}, False),
    ],
)
async def test_host_that_asked_to_wait_is_left_alone_by_the_next_worker(url, site, first, retry, fails):
    # The worker that was asked to wait stops once it has put the page off
    # or failed it: the next one knows of the hold from the database alone.
    held, other, job = held_job(url, first, retry=retry)
    await create_job(job, "test", dsn=POSTGRES_DSN)
    config = worker_config()

    async def asked() -> bool:
        return site.hits[first] > 0 and await state_of(held[0]) != "leased"

    await run_worker_until(config, asked)
    await run_worker(config, "test", worker="next", configure_logging=False)

    asked = next(moment for path, moment in site.log if path == first)
    later = [moment for path, moment in site.log if path in (first, "/site/a.html", "/site/b.html") and moment > asked]
    assert len(later) == (2 if fails else 3)
    assert all(moment - asked >= 2 - EPSILON for moment in later)
    # The other host is crawled meanwhile.
    assert any(path.startswith("/wide/") and moment - asked < 2 for path, moment in site.log)
    assert await urls_in("failed") == ({held[0]} if fails else set())
    assert await urls_in("processed") == {*held[fails:], *other}


async def test_pause_before_a_retry_holds_the_host_back_for_every_worker(url, site):
    # HTTP 429 without a wait asked for: the retry waits 1..2 s, and the
    # workers that would ask the host wait as long. One page at a time: the
    # worker that waits to retry takes no other page meanwhile.
    first = "/overloaded/1/0"
    held, other, job = held_job(url, first, retry={"max_retries": 1, "base_delay": 2.0, "max_delay": 2.0})
    await create_job(job, "test", dsn=POSTGRES_DSN)

    await run_workers(worker_config(crawler={"max_concurrent": 1}), 2)

    asked, retried = [moment for path, moment in site.log if path == first]
    assert retried - asked >= 1 - EPSILON
    later = [moment for path, moment in site.log if path.startswith("/site/")]
    assert len(later) == 2
    assert all(moment >= retried - EPSILON for moment in later)
    assert await urls_in("processed") == {*held, *other}


async def test_page_whose_host_asked_to_wait_goes_back_without_a_time_of_its_own(url, site):
    # The host is held back instead, with the reason it asked to wait for.
    job = make_config(urls=[url("/busy/60")], retry={"max_retries": 1, "max_delay": 0.5})
    await create_job(job, "test", dsn=POSTGRES_DSN)
    worker = asyncio.create_task(run_worker(worker_config(), "test", worker="w", configure_logging=False))
    query = (
        "SELECT f.state, f.not_before <= now() AS at_once, h.hold_reason, extract(epoch FROM h.next_allowed_at - now()) AS left"
        " FROM frontier AS f JOIN hosts AS h USING (job, host)"
    )
    try:
        async with asyncio.timeout(5):
            while True:
                (row,) = await fetch(query)
                if site.hits["/busy/60"] and row["state"] == "queued" and row["hold_reason"] is not None:
                    break
                await asyncio.sleep(0.02)
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

    assert row["at_once"]
    assert 55 < row["left"] <= 60
    assert row["hold_reason"] == "HTTP 429 Too Many Requests, Retry-After 60s"
    assert site.hits["/busy/60"] == 1


def failing_job(url, fails: int, cooldown: float) -> tuple[list[str], list[str], CrawlerConfig]:
    """A job whose host 127.0.0.1 answers 503 to its first `fails` requests; the URLs of that host, those of another one and the job.

    Two failures open the circuit of a worker for `cooldown` seconds. A
    page of a host is taken every 0.5 s, as in `held_job`.
    """
    failing = [url(f"/flaky/{fails}?page={n}") for n in range(5)]
    other = [url(f"/wide/{n}", "localhost") for n in range(1, 5)]
    job = make_config(
        urls=[*failing, *other],
        crawler={"max_depth": 0, "rate_limit": 2.0},
        circuit_breaker={"failure_threshold": 0.5, "min_requests": 2, "cooldown": cooldown},
    )
    return failing, other, job


async def test_open_circuit_holds_its_host_back_for_the_next_worker(url, site):
    # The second page opens the circuit of the first worker, which stops
    # then: the circuit of the next one is closed, it knows of the hold
    # from the database alone.
    failing, other, job = failing_job(url, fails=2, cooldown=2.0)
    await create_job(job, "test", dsn=POSTGRES_DSN)
    config = worker_config()

    async def opened() -> bool:
        return site.hits["/flaky/2"] == 2 and not set(failing) & await urls_in("leased")

    await run_worker_until(config, opened)
    await run_worker(config, "test", worker="next", configure_logging=False)

    opened_at = [moment for path, moment in site.log if path == "/flaky/2"][1]
    later = [moment for path, moment in site.log if path == "/flaky/2" and moment > opened_at]
    # The page that opened the circuit, put off, and the three others.
    assert len(later) == 4
    assert all(moment - opened_at >= 2 - EPSILON for moment in later)
    assert any(path.startswith("/wide/") and moment - opened_at < 2 for path, moment in site.log)
    assert await urls_in("failed") == {failing[0]}
    assert await urls_in("processed") == {*failing[1:], *other}


async def test_failed_probe_holds_the_host_back_again(url, site):
    # The probe, the third request, fails too: its page fails with its
    # error, and the circuit opens again for another second.
    failing, other, job = failing_job(url, fails=3, cooldown=1.0)
    await create_job(job, "test", dsn=POSTGRES_DSN)
    config = worker_config()

    async def probed() -> bool:
        return len(set(failing) & await urls_in("failed")) == 2 and not set(failing) & await urls_in("leased")

    await run_worker_until(config, probed)
    await run_worker(config, "test", worker="next", configure_logging=False)

    probe = [moment for path, moment in site.log if path == "/flaky/3"][2]
    later = [moment for path, moment in site.log if path == "/flaky/3" and moment > probe]
    assert len(later) == 3
    assert all(moment - probe >= 1 - EPSILON for moment in later)
    failed = await urls_in("failed")
    assert len(failed) == 2
    assert await urls_in("processed") == {*failing, *other} - failed


async def test_unreachable_robots_txt_holds_its_host_back_for_the_next_worker(url, site, monkeypatch):
    # robots.txt of 127.0.0.1 answers 503 once and is downloaded again 2 s
    # later: the next worker, which has not downloaded it yet, waits as long.
    monkeypatch.setattr(RobotsParser, "UNREACHABLE_TTL", 2.0)
    site.robots, site.robots_failures_by_host = "", {"127.0.0.1": 1}
    held, other, job = held_job(url, "/site/", crawler={"respect_robots": True})
    await create_job(job, "test", dsn=POSTGRES_DSN)
    config = worker_config()

    async def refused() -> bool:
        return site.robots_hits["127.0.0.1"] == 1 and not set(held) & await urls_in("leased")

    await run_worker_until(config, refused)
    await run_worker(config, "test", worker="next", configure_logging=False)

    first = next(moment for path, moment in site.log if path == "/robots.txt")
    later = [moment for path, moment in site.log if path.startswith("/site/")]
    assert len(later) == 3
    assert all(moment - first >= 2 - EPSILON for moment in later)
    assert any(path.startswith("/wide/") and moment - first < 2 for path, moment in site.log)
    assert site.robots_hits["127.0.0.1"] == 2
    assert await urls_in("processed") == {*held, *other}


def count_calls(monkeypatch, cls: type, name: str, calls: Counter[str]) -> None:
    """Count the calls of the method `name` of `cls` in `calls`."""
    method = getattr(cls, name)

    async def counted(self, *args, **kwargs):
        calls[name] += 1
        return await method(self, *args, **kwargs)

    monkeypatch.setattr(cls, name, counted)


@pytest.mark.parametrize("pages", [10, 200])
async def test_pages_of_a_host_with_an_open_circuit_cost_no_more_the_more_there_are(
    url, site, monkeypatch, caplog, pages
):
    # The first failure opens the circuit for a minute; the two tasks of
    # the worker send the first two pages together. The pages taken before
    # the hold reached the database are put off, at most two per task,
    # then the host is handed out no more.
    caplog.set_level(logging.INFO, logger="crawler")
    calls: Counter[str] = Counter()
    count_calls(monkeypatch, PostgresFrontier, "put_back", calls)
    count_calls(monkeypatch, PostgresFrontier, "hold_host", calls)
    job = make_config(
        urls=[url(f"/flaky/1000?page={n}") for n in range(pages)],
        crawler={"max_depth": 0},
        circuit_breaker={"failure_threshold": 0.5, "min_requests": 1, "cooldown": 60},
    )
    await create_job(job, "test", dsn=POSTGRES_DSN)

    async def opened() -> bool:
        return site.hits["/flaky/1000"] > 0

    await run_worker_until(worker_config(crawler={"max_concurrent": 2}), opened, then=0.5)

    deferred = [record for record in caplog.records if record.getMessage().startswith("Deferred ")]
    assert site.hits["/flaky/1000"] <= 2
    assert 0 < len(deferred) <= 4
    assert calls["put_back"] <= 4
    assert 0 < calls["hold_host"] <= 4


@pytest.mark.parametrize(
    ("start", "sections", "reason"),
    [
        (
            "/site/to-busy",
            {"retry": {"max_retries": 1, "max_delay": 0.5}},
            "HTTP 429 Too Many Requests, Retry-After 60s",
        ),
        (
            "/site/to-flaky",
            {"circuit_breaker": {"failure_threshold": 0.5, "min_requests": 1, "cooldown": 60}},
            "circuit breaker of localhost is open",
        ),
        ("/site/to-other-host", {"crawler": {"respect_robots": True}}, "robots.txt is unreachable (HTTP 503)"),
    ],
)
async def test_page_that_redirects_to_a_held_host_waits_as_long_on_its_own(
    url, site, monkeypatch, start, sections, reason
):
    # Its own host is not held back: without a time of its own, the page
    # would be handed out at once and redirect to the held host again.
    monkeypatch.setattr(RobotsParser, "UNREACHABLE_TTL", 60.0)
    site.robots, site.robots_failures_by_host = "", {"localhost": 100}
    await create_job(make_config(urls=[url(start)], **sections), "test", dsn=POSTGRES_DSN)

    async def put_off() -> bool:
        return site.hits[start] == 1 and await state_of(url(start)) == "queued"

    await run_worker_until(worker_config(), put_off)

    (page,) = await fetch(
        "SELECT extract(epoch FROM not_before - now()) AS left FROM frontier WHERE url = $1", url(start)
    )
    holds = {
        row["host"]: row
        for row in await fetch(
            "SELECT host, hold_reason, extract(epoch FROM next_allowed_at - now()) AS left FROM hosts"
        )
    }
    assert 55 < page["left"] <= 60
    assert holds["localhost"]["hold_reason"].startswith(reason)
    assert 55 < holds["localhost"]["left"] <= 60
    assert holds["127.0.0.1"]["hold_reason"] is None


async def test_job_sections_of_the_configuration_of_a_worker_give_way_to_those_of_the_job(url, site, caplog):
    site.latency = 0.05
    await create_job(
        make_config(urls=[url("/site/")], crawler={"max_pages": 3, "max_concurrent": 10}), "test", dsn=POSTGRES_DSN
    )
    config = worker_config(urls=[url("/site/b.html")], crawler={"max_pages": 1, "max_concurrent": 1})

    with caplog.at_level(logging.WARNING, logger="crawler.distributed"):
        await run_worker(config, "test", configure_logging=False)

    assert site.hits.total() == 3
    assert site.peak_in_flight == 1
    (warning,) = [record.getMessage() for record in caplog.records if "otherwise than" in record.getMessage()]
    assert "urls, crawler.max_pages" in warning
    assert "max_concurrent" not in warning


async def test_files_of_the_storage_are_named_after_the_worker(url, tmp_path):
    await create_job(make_config(urls=[url("/site/b.html")], crawler={"max_depth": 0}), "test", dsn=POSTGRES_DSN)

    await run_worker(
        worker_config(storage={"outputs": [str(tmp_path / "pages-{worker}.jsonl")]}),
        "test",
        worker="first",
        configure_logging=False,
    )

    assert [path.name for path in tmp_path.iterdir()] == ["pages-first.jsonl"]
    assert saved_urls(tmp_path) == [url("/site/b.html")]


@pytest.mark.parametrize("output", ["{}/pages.jsonl", "{}/pages.db", "sqlite:///{}/pages.db"])
async def test_file_of_the_storage_without_the_name_of_the_worker_is_refused(url, site, tmp_path, output):
    await create_job(make_config(urls=[url("/site/b.html")]), "test", dsn=POSTGRES_DSN)
    config = worker_config(storage={"outputs": [str(tmp_path / "ok-{worker}.csv"), output.format(tmp_path)]})

    with pytest.raises(ConfigError, match=r"storage.outputs\[1\]: .*\{worker\}"):
        await run_worker(config, "test", configure_logging=False)

    assert site.hits.total() == 0
    assert list(tmp_path.iterdir()) == []


async def test_worker_of_a_job_that_does_not_exist_is_refused():
    with pytest.raises(JobError, match='no crawl job named "missing"'):
        await run_worker(worker_config(), "missing", configure_logging=False)


async def test_name_of_a_worker_must_fit_a_file_name(url):
    await create_job(make_config(urls=[url("/site/b.html")]), "test", dsn=POSTGRES_DSN)

    with pytest.raises(ValueError, match="worker name"):
        await run_worker(worker_config(), "test", worker="../w", configure_logging=False)


async def test_crawl_of_a_frontier_in_the_database_leaves_the_outcomes_there(url):
    await create_job(make_config(urls=[url("/site/b.html")], crawler={"max_depth": 0}), "test", dsn=POSTGRES_DSN)
    frontier = await PostgresFrontier.open(POSTGRES_DSN, job="test")
    try:
        async with AsyncCrawler(max_depth=0, **UNTHROTTLED) as crawler:
            pages = await crawler.crawl_frontier(frontier, [url("/site/b.html")])
    finally:
        await frontier.close()

    assert list(pages) == [url("/site/b.html")]
    assert (crawler.visited_urls, crawler.failed_urls, crawler.url_depths) == (set(), {}, {})
    assert await urls_in("processed") == {url("/site/b.html")}
