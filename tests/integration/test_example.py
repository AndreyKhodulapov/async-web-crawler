"""Integration tests: the example of examples/ crawls a local site by a configuration file."""

import asyncio
import importlib.util
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest
import yaml
from helpers import BOT

from crawler import load_config
from crawler.logging_setup import reset_logging

EXAMPLES = Path(__file__).parents[2] / "examples"


@pytest.fixture(autouse=True)
def restore_logging():
    level = logging.getLogger().level
    yield
    reset_logging()
    logging.getLogger().setLevel(level)


@pytest.fixture
def example():
    spec = importlib.util.spec_from_file_location("advanced_usage", EXAMPLES / "advanced_usage.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_configuration_of_the_example_is_valid(example):
    config = load_config(example.CONFIG)

    assert config.urls
    assert config.storage.outputs
    assert config.crawler.respect_robots and config.crawler.rate_limit  # it crawls a real site


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
    assert "Processed: 5 pages in " in output
    assert "Successful: 3\n" in output
    assert "Failed: 2\n" in output
    assert "Status codes: {200: 3, 404: 2}" in output
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
