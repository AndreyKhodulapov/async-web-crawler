"""Integration tests for crawl jobs in PostgreSQL: creating, seeding, resuming and restarting them, on a local site."""

import asyncio
import dataclasses
import json
import typing

import asyncpg
import pytest
from helpers import POSTGRES_DSN, DatabaseLink, drop_frontier_tables, index, make_config, urlset

from crawler import AsyncCrawler, ConfigError, CrawlerConfig, FrontierError, JobError, SitemapParser
from crawler.distributed import JOB_SECTIONS, JobMode, create_job, job_config

pytestmark = pytest.mark.postgres

SITEMAP = "/sitemaps/sitemap.xml"


@pytest.fixture(autouse=True)
async def empty_tables() -> None:
    await drop_frontier_tables()


async def fetch(query: str, *parameters: object) -> list[asyncpg.Record]:
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        return await connection.fetch(query, *parameters)
    finally:
        await connection.close()


async def queued_urls() -> list[str]:
    """The pages queued in the job, in the order they were queued."""
    return [row["url"] for row in await fetch("SELECT url FROM frontier WHERE state = 'queued' ORDER BY seq")]


async def the_job() -> asyncpg.Record:
    (job,) = await fetch("SELECT * FROM crawl_jobs")
    return job


def sitemap_config(url, **sections) -> CrawlerConfig:
    return make_config(urls=[url("/site/b.html")], sitemaps={"urls": [url(SITEMAP)]}, **sections)


async def test_new_job_is_seeded_with_its_start_urls_then_its_sitemap_pages(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"), url("/site/c.html"))}
    config = sitemap_config(url, crawler={"max_pages": 7, "max_pages_per_host": 3})

    assert await create_job(config, "test", dsn=POSTGRES_DSN) == {}

    assert await queued_urls() == [url("/site/b.html"), url("/site/a.html"), url("/site/c.html")]
    job = await the_job()
    assert (job["name"], job["state"], job["max_pages"], job["max_pages_per_host"]) == ("test", "running", 7, 3)
    assert job["frontier_factor"] == AsyncCrawler.FRONTIER_FACTOR
    assert json.loads(job["config"]) == job_config(config)


async def test_sitemaps_that_cannot_be_read_are_returned(url):
    reasons = await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN)

    assert list(reasons) == [url(SITEMAP)]
    assert await queued_urls() == [url("/site/b.html")]


async def test_seeding_stops_once_the_queue_is_full(url, site):
    names = [f"{number}.xml" for number in range(300)]
    site.sitemaps = {"sitemap.xml": index(*(url(f"/sitemaps/{name}") for name in names))} | {
        name: urlset(*(url(f"/wide/{number * 100 + page}") for page in range(100))) for number, name in enumerate(names)
    }

    await create_job(
        make_config(sitemaps={"urls": [url(SITEMAP)]}, crawler={"max_pages": 10}), "test", dsn=POSTGRES_DSN
    )

    assert len(await queued_urls()) == AsyncCrawler.FRONTIER_FACTOR * 10
    assert sum(site.hits[f"/sitemaps/{name}"] for name in names) == SitemapParser.CONCURRENCY


async def test_sitemap_pages_out_of_scope_are_held_for_a_redirect(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"), url("/site/c.html", "localhost"))}

    await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN)

    assert await queued_urls() == [url("/site/b.html"), url("/site/a.html")]
    assert [row["url"] for row in await fetch("SELECT url FROM out_of_scope")] == [url("/site/c.html", "localhost")]


async def test_job_keeps_none_of_the_secrets_of_the_configuration(url):
    config = make_config(
        urls=[url("/site/b.html")],
        session={
            "headers": {"Authorization": "Bearer header-secret"},
            "cookies": [{"name": "id", "value": "cookie-secret", "domain": "localhost"}],
        },
        proxy={"urls": ["http://user:proxy-secret@127.0.0.1:9/"]},
    )

    await create_job(config, "test", dsn=POSTGRES_DSN)

    stored = (await the_job())["config"]
    assert not any(secret in stored for secret in ("header-secret", "cookie-secret", "proxy-secret"))
    # Nor could any: no key of the part of the job is a secret.
    hints = typing.get_type_hints(CrawlerConfig)
    for name in JOB_SECTIONS:
        if dataclasses.is_dataclass(hints[name]):
            assert not [field.name for field in dataclasses.fields(hints[name]) if "secret" in field.metadata]
    assert "secret" not in CrawlerConfig.__dataclass_fields__["urls"].metadata


async def test_job_whose_database_cannot_be_reached_is_refused(url):
    async with DatabaseLink() as link:
        await link.cut()

        with pytest.raises(
            FrontierError, match="the database of crawl job test failed: .*Connect call failed"
        ) as raised:
            await create_job(make_config(urls=[url("/site/b.html")]), "test", dsn=link.dsn)

    assert isinstance(raised.value.__cause__, OSError)


@pytest.mark.usefixtures("restore_logging")
async def test_job_created_with_logging_logs_by_its_configuration(url, tmp_path):
    log = tmp_path / "crawler.log"
    config = make_config(urls=[url("/site/b.html")], logging={"level": "INFO", "file": str(log)})

    await create_job(config, "test", dsn=POSTGRES_DSN, configure_logging=True)

    messages = [json.loads(line)["message"] for line in log.read_text(encoding="utf-8").splitlines()]
    assert "Crawl job test is created" in messages
    assert messages[-1] == "Crawl job test is seeded: 1 pages queued"


async def test_job_without_start_urls_or_sitemaps_is_not_created():
    with pytest.raises(ConfigError, match="nothing to crawl"):
        await create_job(make_config(), "test", dsn=POSTGRES_DSN)

    assert (await fetch("SELECT to_regclass('crawl_jobs') AS table"))[0]["table"] is None


class TestModes:
    async def test_name_of_a_job_is_taken(self, url):
        await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN)

        with pytest.raises(JobError, match='"test" exists already'):
            await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN)

    async def test_jobs_of_one_name_created_at_once_make_one_job(self, url):
        results = await asyncio.gather(
            *(create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN) for _ in range(2)), return_exceptions=True
        )

        assert sorted(type(result).__name__ for result in results) == ["JobError", "dict"]
        assert len(await fetch("SELECT id FROM crawl_jobs")) == 1

    async def test_resumed_job_runs_again_without_reading_its_sitemaps(self, url, site):
        site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"))}
        await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN)
        await fetch("UPDATE crawl_jobs SET state = 'finished', finished_at = now()")
        await fetch("UPDATE frontier SET state = 'processed' WHERE url = $1", url("/site/b.html"))
        site.hits.clear()

        await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN, mode=JobMode.RESUME)

        job = await the_job()
        assert (job["state"], job["finished_at"]) == ("running", None)
        assert site.hits[SITEMAP] == 0
        # The start URL processed is not queued again.
        assert await queued_urls() == [url("/site/a.html")]

    async def test_resumed_job_whose_seeding_did_not_finish_is_seeded_again(self, url, site):
        site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"))}
        await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN)
        await fetch("UPDATE crawl_jobs SET state = 'seeding'")
        await fetch("DELETE FROM frontier WHERE url = $1", url("/site/a.html"))

        await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN, mode=JobMode.RESUME)

        assert (await the_job())["state"] == "running"
        assert site.hits[SITEMAP] == 2
        assert await queued_urls() == [url("/site/b.html"), url("/site/a.html")]

    async def test_job_is_resumed_with_the_configuration_of_its_part_only(self, url):
        await create_job(sitemap_config(url, crawler={"max_pages": 10}), "test", dsn=POSTGRES_DSN)
        # Each worker has its own concurrency, storage and log.
        own = sitemap_config(
            url,
            crawler={"max_pages": 10, "max_concurrent": 3},
            storage={"outputs": ["pages.jsonl"]},
            logging={"level": "DEBUG"},
        )
        await create_job(own, "test", dsn=POSTGRES_DSN, mode=JobMode.RESUME)

        with pytest.raises(JobError, match=r"differs .* in: crawler\.max_pages, filters\.exclude;"):
            await create_job(
                sitemap_config(url, crawler={"max_pages": 20}, filters={"exclude": ["/private/"]}),
                "test",
                dsn=POSTGRES_DSN,
                mode=JobMode.RESUME,
            )

    async def test_job_that_does_not_exist_is_not_resumed(self, url):
        with pytest.raises(JobError, match='no crawl job named "test" to resume'):
            await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN, mode=JobMode.RESUME)

        assert await fetch("SELECT id FROM crawl_jobs") == []

    async def test_restarted_job_starts_anew(self, url, site):
        site.sitemaps = {"sitemap.xml": urlset(url("/site/a.html"))}
        await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN)
        await fetch("UPDATE frontier SET state = 'processed'")
        before = (await the_job())["id"]

        await create_job(sitemap_config(url, crawler={"max_pages": 20}), "test", dsn=POSTGRES_DSN, mode=JobMode.RESTART)

        job = await the_job()
        assert job["id"] != before
        assert job["max_pages"] == 20
        assert await queued_urls() == [url("/site/b.html"), url("/site/a.html")]

    async def test_restart_creates_a_job_that_does_not_exist(self, url):
        await create_job(sitemap_config(url), "test", dsn=POSTGRES_DSN, mode=JobMode.RESTART)

        assert (await the_job())["state"] == "running"
