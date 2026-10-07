"""Integration tests: the commands `job create`, `worker`, `report` and `status` crawl a local site in processes of their own."""

import asyncio
import json
import os
import signal
import sys
from collections import Counter
from pathlib import Path

import asyncpg
import pytest
import yaml
from helpers import FAST_CONFIG, POSTGRES_DSN, drop_frontier_tables, make_config

import main
from crawler.distributed import create_job

pytestmark = pytest.mark.postgres

# The page /wide/0 and the 50 pages it links to.
WIDE_PAGES = 51


@pytest.fixture(autouse=True)
async def empty_tables() -> None:
    await drop_frontier_tables()


async def urls_in(state: str) -> set[str]:
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        return {row["url"] for row in await connection.fetch("SELECT url FROM frontier WHERE state = $1", state)}
    finally:
        await connection.close()


async def job_state() -> str:
    connection = await asyncpg.connect(POSTGRES_DSN)
    try:
        return await connection.fetchval("SELECT state FROM crawl_jobs")
    finally:
        await connection.close()


def write_yaml(path: Path, data: dict) -> str:
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def worker_file(tmp_path: Path, **sections) -> str:
    """A configuration of a worker: JSON Lines files of its own, the database polled often so that it ends soon."""
    data = {
        "storage": {"outputs": [str(tmp_path / "pages-{worker}.jsonl")]},
        "distributed": {"poll_interval": 0.1},
        "logging": {"level": "WARNING"},
        **sections,
    }
    return write_yaml(tmp_path / "worker.yaml", data)


async def start(tmp_path: Path, *argv: str, env: dict[str, str] | None = None) -> asyncio.subprocess.Process:
    """The command line in a process of its own, with the database of the tests in CRAWLER_DATABASE_URL and `env`."""
    return await asyncio.create_subprocess_exec(
        sys.executable, str(Path(main.__file__)), *argv,
        cwd=tmp_path, env={**os.environ, "CRAWLER_DATABASE_URL": POSTGRES_DSN, **(env or {})},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip


async def finish(process: asyncio.subprocess.Process) -> tuple[int, str, str]:
    output, errors = await asyncio.wait_for(process.communicate(), timeout=60)
    return process.returncode, output.decode(), errors.decode()


def saved_urls(directory: Path) -> list[str]:
    return [
        json.loads(line)["url"]
        for path in sorted(directory.glob("pages-*.jsonl"))
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


async def test_job_created_by_a_command_is_crawled_by_two_worker_processes_page_by_page_once(url, site, tmp_path):
    job = write_yaml(tmp_path / "job.yaml", {**FAST_CONFIG, "logging": {"level": "WARNING"}, "urls": [url("/wide/0")]})
    code, output, errors = await finish(await start(tmp_path, "job", "create", "--config", job, "--name", "test"))
    assert code == 0, errors
    assert output == "Crawl job test is ready: start its workers with worker --job test\n"
    assert await urls_in("queued") == {url("/wide/0")}

    config = worker_file(tmp_path)
    workers = [
        await start(tmp_path, "worker", "--job", "test", "--config", config, "--name", name) for name in ("w1", "w2")
    ]
    results = await asyncio.gather(*map(finish, workers))

    for name, (code, output, errors) in zip(("w1", "w2"), results, strict=True):
        assert code == 0, errors
        assert f"=== Worker {name} finished on crawl job test (" in output
    assert len(await urls_in("processed")) == WIDE_PAGES
    assert Counter(saved_urls(tmp_path)) == Counter(await urls_in("processed"))  # each page once
    assert await job_state() == "finished"

    code, output, errors = await finish(
        await start(
            tmp_path, "report", "--job", "test", "--stats-json", "out/stats.json", "--report", "out/report.html"
        )
    )
    assert code == 0, errors
    assert output.startswith("=== Crawl job test: finished (")
    stats = json.loads((tmp_path / "out" / "stats.json").read_text(encoding="utf-8"))
    assert (stats["total_pages"], stats["successful"], sorted(stats["workers"])) == (
        WIDE_PAGES,
        WIDE_PAGES,
        ["w1", "w2"],
    )
    assert sum(worker["pages"] for worker in stats["workers"].values()) == WIDE_PAGES
    assert "<h2>Workers</h2>" in (tmp_path / "out" / "report.html").read_text(encoding="utf-8")

    # The job is finished: the line is printed once, with --watch too.
    for watch in ([], ["--watch", "--interval", "0.1"]):
        code, output, errors = await finish(await start(tmp_path, "status", "--job", "test", *watch))
        assert code == 0, errors
        assert output.startswith(f"[##########----------]  {WIDE_PAGES}% | {WIDE_PAGES}/100 pages, 0 failed |")
        assert "| done | workers 0 | in progress 0 | queued 0 |" in output
        assert output.count("\n") == 1


async def test_job_create_with_a_name_taken_is_an_error_unless_the_job_is_resumed(url, tmp_path):
    job = write_yaml(tmp_path / "job.yaml", {**FAST_CONFIG, "logging": {"level": "WARNING"}, "urls": [url("/ok")]})
    create = ("job", "create", "--config", job, "--name", "test")
    assert (await finish(await start(tmp_path, *create)))[0] == 0

    code, output, errors = await finish(await start(tmp_path, *create))
    assert (code, output) == (1, "")
    assert errors == 'error: A crawl job named "test" exists already: resume or restart it\n'

    code, _, errors = await finish(await start(tmp_path, *create, "--resume"))
    assert code == 0, errors


async def test_worker_without_a_job_exits_with_1(tmp_path):
    code, _, errors = await finish(
        await start(tmp_path, "worker", "--job", "missing", "--config", worker_file(tmp_path))
    )

    assert (code, errors) == (1, 'error: There is no crawl job named "missing"\n')


async def test_worker_of_a_job_that_renders_its_pages_without_chromium_exits_with_2(url, site, tmp_path):
    await create_job(make_config(urls=[url("/site/")], rendering={"mode": "always"}), "books", dsn=POSTGRES_DSN)

    # Playwright looks for its browsers in an empty directory, as in the image without Chromium.
    code, output, errors = await finish(
        await start(
            tmp_path, "worker", "--job", "books", "--config", worker_file(tmp_path),
            env={"PLAYWRIGHT_BROWSERS_PATH": str(tmp_path / "browsers")},
        )
    )  # fmt: skip

    assert (code, output) == (2, "")
    assert errors.startswith(
        'error: Invalid configuration: crawl job "books": rendering.mode: Chromium is not installed'
    )
    assert site.hits.total() == 0
    assert await urls_in("queued") == {url("/site/")}


async def test_worker_stopped_with_sigterm_queues_its_pages_again_and_exits_with_143(url, site, tmp_path):
    site.latency = 0.05
    await create_job(make_config(urls=[url("/wide/0")]), "test", dsn=POSTGRES_DSN)
    config = worker_file(
        tmp_path,
        crawler={"max_concurrent": 2},
        storage={"outputs": [str(tmp_path / "pages-{worker}.jsonl")], "batch_size": 100},
        report={"stats_json": str(tmp_path / "stats-{worker}.json")},
    )
    worker = await start(tmp_path, "worker", "--job", "test", "--config", config, "--name", "stopped")
    async with asyncio.timeout(30):
        while len(await urls_in("saving")) < 5:
            await asyncio.sleep(0.02)
    worker.send_signal(signal.SIGTERM)
    code, output, errors = await finish(worker)

    assert code == 143, errors
    assert output == ""
    # What its storage buffered is written and saved; the pages in flight are queued again.
    processed = await urls_in("processed")
    assert len(processed) >= 5
    assert sorted(saved_urls(tmp_path)) == sorted(processed)
    assert await urls_in("saving") == set()
    assert await urls_in("leased") == set()
    assert await job_state() == "running"
    stats = json.loads((tmp_path / "stats-stopped.json").read_text(encoding="utf-8"))
    assert stats["total_pages"] == len(processed)
