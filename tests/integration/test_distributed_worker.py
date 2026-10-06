"""Integration tests for the workers of a crawl job in PostgreSQL: several of them crawl one local site together."""

import asyncio
import json
import logging

import asyncpg
import pytest
from helpers import POSTGRES_DSN, UNTHROTTLED, drop_frontier_tables, make_config, urlset

from crawler import AdvancedCrawler, AsyncCrawler, ConfigError, CrawlerConfig, JobError, PostgresFrontier
from crawler.distributed import create_job, run_worker

pytestmark = pytest.mark.postgres

SITEMAP = "/sitemaps/sitemap.xml"
# The page /wide/0 and the 50 pages it links to.
WIDE_PAGES = 51


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
