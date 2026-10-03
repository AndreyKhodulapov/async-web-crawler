"""Integration tests: AdvancedCrawler set up by a configuration crawls a local site, saves, logs and reports."""

import json
import logging
from logging.handlers import RotatingFileHandler

import pytest
import yaml
from helpers import BOT, FAST_CONFIG, urlset

from crawler import AdvancedCrawler, ConfigError, CrawlerConfig, CSVStorage, JSONStorage, configure_logging

pytestmark = pytest.mark.usefixtures("restore_logging")


def make_config(**sections) -> CrawlerConfig:
    data = {name: dict(section) for name, section in FAST_CONFIG.items()}
    for name, section in sections.items():
        data[name] = {**data[name], **section} if isinstance(section, dict) and name in data else section
    return CrawlerConfig.from_dict(data)


def file_handlers() -> list[logging.Handler]:
    """The handlers of log files the crawler opened; pytest keeps one of its own on the root logger."""
    return [handler for handler in logging.getLogger().handlers if isinstance(handler, RotatingFileHandler)]


async def test_crawl_by_a_configuration_file(url, site, tmp_path):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/c.html"))}
    out = tmp_path / "out"  # does not exist yet
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.safe_dump(
            FAST_CONFIG
            | {
                "urls": [url("/site/")],
                "sitemaps": {"urls": [url("/sitemaps/sitemap.xml")]},
                "filters": {"same_domain_only": True, "exclude": [r"\.pdf$"]},
                "storage": {"outputs": [str(out / "pages.jsonl"), str(out / "data" / "pages.csv")], "batch_size": 2},
                "logging": {"level": "DEBUG", "file": str(out / "logs" / "crawler.log")},
                "report": {
                    "stats_json": str(out / "stats.json"),
                    "html": str(out / "report.html"),
                    "title": "Local site",
                },
            }
        ),
        encoding="utf-8",
    )

    crawler = AdvancedCrawler.from_config(config_file)
    pages = await crawler.crawl()
    stats = crawler.get_stats()
    await crawler.close()

    # The start page, its links on the same host that are not PDF, and the page of the sitemap.
    crawled = {url("/site/"), url("/site/a.html"), url("/site/b.html"), url("/site/c.html")}
    assert set(pages) == crawled
    assert (stats["total_pages"], stats["successful"], stats["failed"]) == (5, 4, 1)
    assert stats["status_codes"] == {200: 4, 404: 1}
    assert stats["top_domains"] == {"127.0.0.1": 5}

    saved = [json.loads(line) for line in (out / "pages.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {record["url"] for record in saved} == crawled
    assert len((out / "data" / "pages.csv").read_text(encoding="utf-8").splitlines()) == 5  # with the header

    log = [json.loads(line) for line in (out / "logs" / "crawler.log").read_text(encoding="utf-8").splitlines()]
    assert {"crawler.client", "crawler.advanced"} <= {entry["logger"] for entry in log}
    assert any(entry["level"] == "DEBUG" for entry in log)
    assert any(url("/site/missing.html") in entry["message"] for entry in log if entry["level"] == "WARNING")

    assert json.loads((out / "stats.json").read_text(encoding="utf-8"))["total_pages"] == 5
    report = (out / "report.html").read_text(encoding="utf-8")
    assert "<title>Local site</title>" in report
    assert "127.0.0.1" in report


async def test_overrides_win_over_the_file(url, tmp_path):
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(FAST_CONFIG | {"urls": [url("/site/")]}), encoding="utf-8")

    async with AdvancedCrawler.from_config(config_file, {"crawler": {"max_pages": 2}}) as crawler:
        await crawler.crawl()

    assert crawler.config.crawler.user_agent == BOT  # the rest of the section is kept
    assert crawler.get_stats()["total_pages"] == 2


async def test_configuration_reaches_every_part(tmp_path):
    config = make_config(
        crawler={"max_concurrent": 3, "max_depth": 4, "user_agent": BOT, "keep_pages": False},
        sitemaps={"max_urls": 7},
        retry={"max_retries": 5},
        circuit_breaker={"failure_threshold": 0.9},
        storage={"outputs": [str(tmp_path / "pages.jsonl"), str(tmp_path / "pages.csv")]},
        logging={"level": "ERROR"},
        report={"top_domains": 3},
    )
    async with AdvancedCrawler(config) as advanced:
        crawler = advanced.crawler
        assert (crawler.max_concurrent, crawler.max_depth, crawler.keep_pages) == (3, 4, False)
        assert crawler.sitemaps.max_urls == 7
        assert crawler.retry_strategy.max_retries == 5
        assert crawler.circuit_breaker.enabled
        assert advanced.stats is crawler.stats
        assert advanced.stats.top_domains == 3
        assert advanced.storage is crawler.storage
        assert [type(storage) for storage in advanced.storage.storages] == [JSONStorage, CSVStorage]
        assert logging.getLogger().level == logging.ERROR


async def test_defaults_without_a_configuration():
    async with AdvancedCrawler() as crawler:
        assert crawler.config == CrawlerConfig()
        assert crawler.storage is None
        assert file_handlers() == []
        assert crawler.get_stats()["total_pages"] == 0


async def test_nothing_to_crawl_is_an_error_of_the_configuration():
    async with AdvancedCrawler(make_config()) as crawler:
        with pytest.raises(ConfigError, match="urls: nothing to crawl"):
            await crawler.crawl()


async def test_sitemaps_alone_are_enough(url, site):
    site.sitemaps = {"sitemap.xml": urlset(url("/site/c.html"))}
    async with AdvancedCrawler(make_config(sitemaps={"urls": [url("/sitemaps/sitemap.xml")]})) as crawler:
        assert set(await crawler.crawl()) == {url("/site/c.html")}


async def test_links_to_files_are_not_followed_by_default(url, site):
    async with AdvancedCrawler(make_config(urls=[url("/site/")])) as crawler:
        await crawler.crawl()

    assert site.hits["/site/files/manual.pdf"] == 0
    assert set(crawler.crawler.failed_urls) == {url("/site/missing.html")}


@pytest.mark.parametrize(("filters", "leaves"), [({}, False), ({"same_domain_only": False}, True)])
async def test_links_to_other_hosts_are_not_followed_by_default(url, server, filters, leaves):
    other_host = f"http://localhost:{server.port}/site/"  # the start page, linked by another name of the server
    async with AdvancedCrawler(make_config(urls=[url("/site/")], filters=filters)) as crawler:
        await crawler.crawl()

    assert (other_host in crawler.crawler.processed_urls) is leaves


async def test_max_pages_per_host_reaches_the_crawl(url, site):
    config = make_config(urls=[url("/site/")], crawler={"max_pages_per_host": 2})
    async with AdvancedCrawler(config) as crawler:
        await crawler.crawl()

    assert site.hits.total() == 2
    assert crawler.crawler.skipped_urls


async def test_close_stops_logging_to_the_file_and_closes_the_crawler(url, tmp_path):
    crawler = AdvancedCrawler(make_config(urls=[url("/site/c.html")], logging={"file": str(tmp_path / "crawler.log")}))
    assert len(file_handlers()) == 1
    assert not crawler.closed

    await crawler.close()
    await crawler.close()  # safe to repeat

    assert crawler.closed
    assert crawler.crawler.closed
    assert file_handlers() == []
    # As AsyncCrawler: a closed crawler fetches nothing.
    assert await crawler.crawl() == {}
    assert crawler.get_stats()["errors"] == {"CrawlerClosedError": 1}


async def test_logging_is_left_alone_when_asked(url, tmp_path):
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump(FAST_CONFIG | {"urls": [url("/site/c.html")]}), encoding="utf-8")
    log_file = tmp_path / "logs" / "crawler.log"
    config = make_config(urls=[url("/site/c.html")], logging={"level": "DEBUG", "file": str(log_file)})

    for crawler in (
        AdvancedCrawler(config, configure_logging=False),
        AdvancedCrawler.from_config(config_file, {"logging": {"level": "DEBUG"}}, configure_logging=False),
    ):
        assert (root.handlers, root.level) == (handlers, level)
        await crawler.crawl()
        await crawler.close()
        assert (root.handlers, root.level) == (handlers, level)

    assert not log_file.parent.exists()


async def test_close_leaves_logging_set_up_by_others_alone(url):
    configure_logging("WARNING")
    handlers = list(logging.getLogger().handlers)

    crawler = AdvancedCrawler(make_config(urls=[url("/site/c.html")]), configure_logging=False)
    await crawler.close()

    assert logging.getLogger().handlers == handlers
    assert logging.getLogger().level == logging.WARNING


async def test_report_that_cannot_be_written_does_not_fail_the_crawl(url, tmp_path, caplog):
    (tmp_path / "taken").write_text("a file, not a directory")
    config = make_config(urls=[url("/site/c.html")], report={"html": str(tmp_path / "taken" / "report.html")})

    async with AdvancedCrawler(config) as crawler:
        with caplog.at_level(logging.ERROR, logger="crawler.advanced"):
            pages = await crawler.crawl()

    assert set(pages) == {url("/site/c.html")}
    assert "Failed to write the report" in caplog.text


async def test_exports_create_the_directory_and_take_the_title_of_the_configuration(url, tmp_path):
    config = make_config(urls=[url("/site/c.html")], report={"title": "Nightly crawl"})
    async with AdvancedCrawler(config) as crawler:
        await crawler.crawl()
        crawler.export_to_json(tmp_path / "new" / "stats.json")
        crawler.export_to_html_report(tmp_path / "new" / "report.html")
        crawler.export_to_html_report(tmp_path / "new" / "other.html", title="Another")

    assert json.loads((tmp_path / "new" / "stats.json").read_text(encoding="utf-8"))["successful"] == 1
    assert "<title>Nightly crawl</title>" in (tmp_path / "new" / "report.html").read_text(encoding="utf-8")
    assert "<title>Another</title>" in (tmp_path / "new" / "other.html").read_text(encoding="utf-8")


async def test_log_file_that_cannot_be_opened_fails_the_constructor(tmp_path):
    (tmp_path / "taken").write_text("a file, not a directory")

    with pytest.raises(OSError):
        AdvancedCrawler(make_config(logging={"file": str(tmp_path / "taken" / "crawler.log")}))

    assert file_handlers() == []
