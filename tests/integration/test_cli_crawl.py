"""Integration tests: the command line of the crawler crawls a local site, saves, reports and stops when interrupted."""

import asyncio
import json
import logging
import sys
from pathlib import Path

import pytest
import yaml
from helpers import BOT

import main
from crawler import AdvancedCrawler
from crawler.logging_setup import reset_logging
from main import build_config, parse_args, run

# No rate limit, robots.txt, retries or circuit breaker, as in the tests of AdvancedCrawler.
FAST = {
    "crawler": {"rate_limit": None, "respect_robots": False, "user_agent": BOT, "max_depth": 1},
    "retry": {"max_retries": 0},
    "circuit_breaker": {"failure_threshold": None},
    "logging": {"level": "WARNING"},
}


@pytest.fixture(autouse=True)
def restore_logging():
    level = logging.getLogger().level
    yield
    reset_logging()
    logging.getLogger().setLevel(level)


@pytest.fixture
def config_file(tmp_path, url):
    def write(**sections) -> str:
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(FAST | {"urls": [url("/site/")]} | sections), encoding="utf-8")
        return str(path)

    return write


def saved_urls(path: Path) -> set[str]:
    return {json.loads(line)["url"] for line in path.read_text(encoding="utf-8").splitlines()}


async def test_crawl_by_a_file_and_options(url, config_file, tmp_path, capsys):
    out = tmp_path / "out"  # does not exist yet
    argv = [
        "--config", config_file(filters={"same_domain_only": True}),
        "--output", str(out / "pages.jsonl"),
        "--stats-json", str(out / "stats.json"),
        "--report", str(out / "report.html"),
        "--log-file", str(out / "crawler.log"),
    ]  # fmt: skip

    code = await run(build_config(parse_args(argv)))

    assert code == 0
    # The start page and its links on the same host; two more of them are a 404.
    crawled = {url("/site/"), url("/site/a.html"), url("/site/b.html")}
    assert saved_urls(out / "pages.jsonl") == crawled
    stats = json.loads((out / "stats.json").read_text(encoding="utf-8"))
    assert (stats["total_pages"], stats["successful"], stats["failed"]) == (5, 3, 2)
    assert "data:image/png" in (out / "report.html").read_text(encoding="utf-8")  # a chart
    assert (out / "crawler.log").exists()

    captured = capsys.readouterr()
    summary = captured.out
    assert "=== Crawl finished (" in summary
    assert "Pages: 5 (3 successful, 2 failed, 0 skipped)" in summary
    assert "Status codes: 200: 3, 404: 2" in summary
    assert "Top domains: 127.0.0.1: 5" in summary
    assert "Errors: PermanentHTTPError: 2" in summary
    assert f"Saved: 3 pages to {out / 'pages.jsonl'}\n" in summary
    assert f"Reports: {out / 'stats.json'}, {out / 'report.html'}\n" in summary
    assert f"Log: {out / 'crawler.log'}\n" in summary
    # The progress line goes to stderr and ends with the crawl done.
    assert "| 5/100 pages, 2 failed |" in captured.err.splitlines()[-1]


async def test_options_limit_the_crawl_of_the_file(url, site, config_file, capsys):
    argv = ["--config", config_file(), "--max-depth", "0", "--no-progress"]

    code = await run(build_config(parse_args(argv)), progress=False)

    assert code == 0
    assert set(site.hits) == {"/site/"}
    captured = capsys.readouterr()
    assert "Pages: 1 (1 successful" in captured.out
    assert "Saved:" not in captured.out and "Reports:" not in captured.out and "Log:" not in captured.out
    assert captured.err == ""


async def test_password_of_a_database_is_not_shown(url, config_file, capsys, monkeypatch):
    class NoStorage(AdvancedCrawler):
        def __init__(self, config):
            super().__init__(config)
            self.crawler.storage = self.storage = None  # no server to connect to

    monkeypatch.setattr(main, "AdvancedCrawler", NoStorage)
    argv = ["--config", config_file(), "--max-depth", "0", "--output", "postgresql://crawler:secret@db.example/pages"]

    await run(build_config(parse_args(argv)), progress=False)

    output = capsys.readouterr().out
    assert "postgresql://crawler:***@db.example/pages" in output
    assert "secret" not in output


async def test_no_page_fetched_is_exit_code_1(url, config_file, capsys):
    argv = ["--config", config_file(), "--urls", url("/status/500"), "--max-depth", "0"]

    assert await run(build_config(parse_args(argv)), progress=False) == 1
    assert "Pages: 1 (0 successful, 1 failed, 0 skipped)" in capsys.readouterr().out


async def test_log_file_that_cannot_be_opened_is_an_os_error(url, config_file, tmp_path):
    (tmp_path / "taken").write_text("a file, not a directory")
    argv = ["--config", config_file(), "--log-file", str(tmp_path / "taken" / "crawler.log")]

    with pytest.raises(OSError):
        await run(build_config(parse_args(argv)), progress=False)


async def test_interrupted_crawl_saves_and_reports_the_pages_it_fetched(url, site, config_file, tmp_path, capsys):
    pages, stats_file = tmp_path / "pages.jsonl", tmp_path / "stats.json"
    argv = [
        "--config", config_file(urls=[url("/ok"), url("/delay/30")], storage={"batch_size": 100}),
        "--max-depth", "0",
        "--output", str(pages),
        "--stats-json", str(stats_file),
    ]  # fmt: skip
    task = asyncio.create_task(run(build_config(parse_args(argv)), progress=True))
    async with asyncio.timeout(10):
        while "1/100 pages" not in capsys.readouterr().err:  # /ok is fetched, /delay/30 is in flight
            await asyncio.sleep(0.05)

    task.cancel()  # what Ctrl-C does to the main task
    with pytest.raises(asyncio.CancelledError):
        await task

    assert saved_urls(pages) == {url("/ok")}
    stats = json.loads(stats_file.read_text(encoding="utf-8"))
    assert (stats["total_pages"], stats["successful"]) == (1, 1)
    assert stats["finished_at"] is not None
    summary = capsys.readouterr().out
    assert "=== Crawl interrupted (" in summary
    assert f"Saved: 1 pages to {pages}\n" in summary
    assert f"Reports: {stats_file}\n" in summary


async def test_summary_lists_only_the_reports_that_were_written(url, config_file, tmp_path, capsys):
    (tmp_path / "taken").write_text("a file, not a directory")
    stats_file = tmp_path / "stats.json"
    argv = [
        "--config", config_file(logging={"level": "CRITICAL"}),
        "--max-depth", "0",
        "--stats-json", str(stats_file),
        "--report", str(tmp_path / "taken" / "report.html"),
    ]  # fmt: skip

    await run(build_config(parse_args(argv)), progress=False)

    assert f"Reports: {stats_file}\n" in capsys.readouterr().out


async def test_crawl_is_stopped_when_the_progress_cannot_be_shown(url, site, config_file, tmp_path, monkeypatch):
    async def broken_pipe(crawler, crawl_task, max_pages):
        while crawler.crawl_stats().processed < 1:  # /ok is fetched, /delay/30 is in flight
            await asyncio.sleep(0.05)
        raise BrokenPipeError("Broken pipe")

    monkeypatch.setattr(main, "show_progress", broken_pipe)
    pages, stats_file = tmp_path / "pages.jsonl", tmp_path / "stats.json"
    argv = [
        "--config", config_file(urls=[url("/ok"), url("/delay/30")], storage={"batch_size": 100}),
        "--max-depth", "0",
        "--output", str(pages),
        "--stats-json", str(stats_file),
    ]  # fmt: skip

    async with asyncio.timeout(10):
        with pytest.raises(BrokenPipeError):
            await run(build_config(parse_args(argv)))

    assert saved_urls(pages) == {url("/ok")}
    stats = json.loads(stats_file.read_text(encoding="utf-8"))
    # The page in flight was dropped with the crawl, not failed by a crawler closed under it.
    assert (stats["total_pages"], stats["successful"], stats["errors"]) == (1, 1, {})


async def test_command_runs_as_a_script(url, config_file, tmp_path):
    script = Path(main.__file__)
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(script), "--config", config_file(), "--max-depth", "0",
        "--output", str(tmp_path / "pages.jsonl"), "--no-progress",
        cwd=tmp_path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip
    output, errors = await asyncio.wait_for(process.communicate(), timeout=30)

    assert process.returncode == 0, errors.decode()
    assert "Pages: 1 (1 successful, 0 failed, 0 skipped)" in output.decode()
    assert saved_urls(tmp_path / "pages.jsonl") == {url("/site/")}
