"""Integration tests: the examples of examples/ crawl a local site by a configuration file and render its pages."""

import asyncio
import json
import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler

import pytest
import yaml
from helpers import BOT, EXAMPLES, load_example

from crawler import load_config
from crawler.distributed.worker import _check_files

SRC = EXAMPLES.parent / "src"


pytestmark = pytest.mark.usefixtures("restore_logging")


@pytest.fixture
def example():
    return load_example("advanced_usage")


def test_configuration_of_the_example_is_valid(example):
    config = load_config(example.CONFIG)

    assert config.urls
    assert config.storage.outputs
    assert config.crawler.respect_robots and config.crawler.rate_limit  # it crawls a real site


def test_configurations_of_the_containers_are_valid():
    """Those of docker-compose.yml: a crawl of its own, a crawl job and its workers."""
    crawl, job, worker = (load_config(EXAMPLES / "docker" / name) for name in ("crawl.yaml", "job.yaml", "worker.yaml"))

    assert crawl.urls and job.urls  # a worker takes them from the job
    assert crawl.crawler.respect_robots and job.crawler.rate_limit  # real sites
    _check_files(worker)  # every file of a worker is its own
    assert worker.logging.console_format == "json"


async def test_example_crawls_saves_and_reports(example, url, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)  # the paths of the configuration and of the report are relative
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "urls": [url("/site/")],
                "crawler": {"rate_limit": None, "respect_robots": False, "user_agent": BOT, "max_depth": 1},
                "retry": {"max_retries": 0},
                "filters": {"same_domain_only": True},
                "storage": {"outputs": ["out/pages.jsonl"]},
                "logging": {"level": "WARNING", "file": "out/crawler.log"},
            }
        ),
        encoding="utf-8",
    )

    await example.main(config_file)

    output = capsys.readouterr().out
    # The link to files/manual.pdf is not followed: the configuration leaves files alone.
    assert "Processed: 4 pages in " in output
    assert "Successful: 3\n" in output
    assert "Failed: 1\n" in output
    assert "Status codes: {200: 3, 404: 1}" in output
    assert f"links: {url('/site/')}\n" in output
    assert "Report: out/report.html\n" in output
    assert "Pages: out/pages.jsonl\n" in output
    saved = (tmp_path / "out" / "pages.jsonl").read_text(encoding="utf-8").splitlines()
    assert {json.loads(line)["url"] for line in saved} == {url("/site/"), url("/site/a.html"), url("/site/b.html")}
    assert "data:image/png" in (tmp_path / "out" / "report.html").read_text(encoding="utf-8")
    # The example closed the crawler, and with it the log file.
    assert not [handler for handler in logging.getLogger().handlers if isinstance(handler, RotatingFileHandler)]


async def test_interrupted_example_stops_the_crawl_before_closing(example, url, tmp_path, monkeypatch, capsys, caplog):
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "urls": [url("/ok"), url("/delay/30")],
                "crawler": {"rate_limit": None, "respect_robots": False, "user_agent": BOT, "max_depth": 0},
                "logging": {"level": "WARNING"},
            }
        ),
        encoding="utf-8",
    )
    task = asyncio.create_task(example.main(config_file))
    async with asyncio.timeout(10):
        while "1/100 pages" not in capsys.readouterr().err:  # /ok is fetched, /delay/30 is in flight
            await asyncio.sleep(0.05)

    task.cancel()  # what Ctrl-C does to the main task
    with pytest.raises(asyncio.CancelledError):
        await task

    # A crawl left running would have its request fail on the closed session and be retried.
    assert "Connector is closed" not in caplog.text


@pytest.mark.browser
@pytest.mark.usefixtures("chromium")
async def test_rendering_example_shows_what_javascript_adds(url, capsys):
    await load_example("render_js").main(url("/js/links"))

    output = capsys.readouterr().out
    assert re.search(r"without rendering: \d+ characters of text, 0 links\n", output)
    assert re.search(r"with rendering: +\d+ characters of text, 1 links, rendered in \d+\.\d\ds\n", output)
    assert f"links only JavaScript shows: {url('/js/target')}\n" in output
    assert "made by javascript" in output


async def test_rendering_example_fails_with_the_error_of_the_page(url):
    # Without a browser: a page that cannot be downloaded never gets to one.
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(EXAMPLES / "render_js.py"),
        url("/status/404"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=os.environ | {"PYTHONPATH": str(SRC)},
    )
    async with asyncio.timeout(60):
        _, stderr = await process.communicate()

    assert process.returncode == 1
    assert stderr.decode().endswith(f"error: {url('/status/404')}: HTTP 404 Not Found\n")
