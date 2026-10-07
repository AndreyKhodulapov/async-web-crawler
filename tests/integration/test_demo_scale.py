"""Integration tests: the site of the `scale` demo, the synchronous crawler and the measurements that compare it."""

import asyncio
import json
import threading
import time
import urllib.error
import urllib.request

import pytest
from helpers import POSTGRES_DSN

from crawler import AsyncCrawler, CircuitBreaker, RetryStrategy
from crawler.distributed import job_stats
from demo_main import parse_args, run_scale
from demo_scale import FANOUT, ScaleSite, SyncCrawler, compare, crawl_async, crawl_job, crawl_sync, measure


def crawl_concurrently(site: ScaleSite) -> AsyncCrawler:
    async def crawl() -> AsyncCrawler:
        async with AsyncCrawler(
            max_concurrent=5,
            max_depth=site.pages,
            requests_per_second=None,
            respect_robots=False,
            retry_strategy=RetryStrategy(max_retries=0),
            circuit_breaker=CircuitBreaker(None),
        ) as crawler:
            await crawler.crawl([site.url], max_pages=site.pages, same_domain_only=True)
        return crawler

    return asyncio.run(crawl())


class TestScaleSite:
    def test_serves_its_pages_and_nothing_beyond_them(self):
        with ScaleSite(25) as site:
            with urllib.request.urlopen(site.page_url(24)) as response:
                assert response.headers.get_content_type() == "text/html"
                assert "<title>Page 24</title>" in response.read().decode()
            with pytest.raises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(site.page_url(25))
            assert error.value.code == 404
            assert site.requests == 2

    def test_stops_serving_on_exit(self):
        with ScaleSite(1) as site:
            url = site.url
        with pytest.raises(urllib.error.URLError):
            urllib.request.urlopen(url, timeout=2)

    @pytest.mark.parametrize("pages", [1, 2, FANOUT + 1, 57])
    def test_every_page_is_linked_from_the_start(self, pages):
        with ScaleSite(pages) as site:
            crawler = SyncCrawler(max_depth=pages)
            crawler.crawl([site.url], max_pages=pages * 2)

            assert set(crawler.processed_urls) == site.urls()
            assert len(site.urls()) == pages
            assert crawler.failed_urls == {}  # no link leads out of the site
            assert site.requests == pages

    @pytest.mark.parametrize(("pages", "delay"), [(0, 0.0), (10, -1.0)])
    def test_invalid_arguments(self, pages, delay):
        with pytest.raises(ValueError):
            ScaleSite(pages, delay)


class TestSyncCrawler:
    def test_fetches_the_same_pages_as_the_async_crawler(self):
        with ScaleSite(20) as site:
            sync = SyncCrawler(max_depth=20)
            sync.crawl([site.url], max_pages=20)
            concurrent = crawl_concurrently(site)

            assert set(sync.processed_urls) == set(concurrent.processed_urls) == site.urls()
            # The same parser: the pages are equal field by field.
            assert sync.processed_urls == concurrent.processed_urls
            assert sync.failed_urls == {} == concurrent.failed_urls

    def test_max_pages_and_max_depth_limit_the_crawl(self):
        with ScaleSite(50) as site:
            crawler = SyncCrawler(max_depth=50)
            assert len(crawler.crawl([site.url], max_pages=7)) == 7
            assert site.requests == 7
            # Breadth-first: the start page, then its links in order.
            assert list(crawler.processed_urls) == [site.page_url(number) for number in range(7)]

            pages = SyncCrawler(max_depth=1).crawl([site.url], max_pages=50)
            assert set(pages) == {site.page_url(number) for number in range(FANOUT + 1)}

    def test_failed_page_is_reported_and_counts_toward_max_pages(self):
        with ScaleSite(3) as site:
            crawler = SyncCrawler()
            missing = site.page_url(99)

            pages = crawler.crawl([missing, site.url], max_pages=2)

            assert list(pages) == [site.url]
            assert crawler.failed_urls == {missing: "HTTPError: HTTP Error 404: Not Found"}

    def test_links_to_other_hosts_are_not_followed(self, monkeypatch):
        with ScaleSite(2) as site:
            html = site._html
            monkeypatch.setattr(site, "_html", lambda number: html(number) + '<a href="http://other.invalid/">x</a>')
            crawler = SyncCrawler(max_depth=5)

            crawler.crawl([site.url], max_pages=10)

            assert set(crawler.processed_urls) == site.urls()
            assert crawler.failed_urls == {}


class TestMeasurements:
    def test_both_crawls_fetch_the_whole_site(self):
        with ScaleSite(12) as site:
            assert crawl_sync(site) == (12, 0)
            assert crawl_async(site, 4) == (12, 0)
            assert crawl_async(site, 4, keep_pages=False) == (12, 0)

    def test_measure_takes_time_and_memory(self):
        run = measure(crawl_sync, 5, 0.02)

        assert (run.pages, run.failed) == (5, 0)
        assert run.elapsed >= 5 * 0.02
        assert run.pages_per_second == pytest.approx(5 / run.elapsed)
        assert run.peak_memory > 0
        assert measure(crawl_sync, 5, 0.0, memory=False).peak_memory is None

    def test_concurrent_crawl_of_a_slow_site_is_faster(self):
        result = compare(30, 0.05, 10, memory=False)

        assert (result.sync.pages, result.concurrent.pages) == (30, 30)
        assert result.sync.elapsed >= 30 * 0.05
        # 10 requests at once on a site that takes 50 ms: several times faster, whatever the machine.
        assert result.speedup > 2
        assert result.lean_memory is None

    def test_scale_command_prints_a_row_per_size_and_saves_them(self, tmp_path, capsys):
        report = tmp_path / "scale.json"

        asyncio.run(
            run_scale(parse_args(["scale", "5", "12", "--delay", "0", "--concurrency", "3", "--json", str(report)]))
        )

        output = capsys.readouterr().out.splitlines()
        assert "=== Scale: one request at a time vs 3 at once (the site answers in 0 ms) ===" in output
        rows = [line.split() for line in output if line.split()[:1] in (["5"], ["12"])]
        assert [row[0] for row in rows] == ["5", "12"]
        assert all(row[5].endswith("x") for row in rows)  # the speedup
        saved = json.loads(report.read_text(encoding="utf-8"))
        assert (saved["delay"], saved["concurrency"]) == (0.0, 3)
        assert [
            (result["pages"], result["sync"]["pages"], result["concurrent"]["pages"]) for result in saved["results"]
        ] == [
            (5, 5, 5),
            (12, 12, 12),
        ]
        assert all(result["lean_memory"] > 0 and result["sync"]["peak_memory"] > 0 for result in saved["results"])


class TestStopping:
    def test_stopped_sync_crawl_ends_after_the_page_it_is_fetching(self):
        stop = threading.Event()
        with ScaleSite(30, 0.02) as site:
            threading.Timer(0.1, stop.set).start()
            fetched, failed = crawl_sync(site, stop)

        assert 0 < fetched < 30
        assert failed == 0

    def test_stopped_concurrent_crawl_is_cancelled(self):
        stop = threading.Event()
        stop.set()
        started = time.perf_counter()
        # Left alone, the crawl takes 30 * 0.5 / 2 seconds.
        with ScaleSite(30, 0.5) as site, pytest.raises(asyncio.CancelledError):
            crawl_async(site, 2, stop=stop)

        assert time.perf_counter() - started < 3

    async def test_interrupted_scale_command_stops_its_crawls(self):
        # The synchronous crawl alone takes 200 * 0.05 seconds.
        command = asyncio.create_task(run_scale(parse_args(["scale", "200", "--delay", "0.05", "--no-memory"])))
        await asyncio.sleep(0.3)
        command.cancel()
        with pytest.raises(asyncio.CancelledError):
            await command

        # The crawls run in a thread, which the program waits for at exit: it must not go on for long.
        deadline = time.monotonic() + 3
        while any(thread.name == "scale-site" for thread in threading.enumerate()) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert not [thread for thread in threading.enumerate() if thread.name == "scale-site"]


@pytest.mark.postgres
class TestCrawlJobs:
    def test_workers_crawl_every_page_of_the_site_once(self):
        with ScaleSite(30) as site:
            run = crawl_job(site, 2, 3, dsn=POSTGRES_DSN)

            assert (run.pages, run.failed, run.peak_memory) == (30, 0, None)
            assert site.requests == 30
        stats = asyncio.run(job_stats(POSTGRES_DSN, "scale"))
        assert stats["state"] == "finished"
        assert run.elapsed == stats["elapsed_seconds"] > 0
        assert {name: worker["state"] for name, worker in stats["workers"].items()} == {
            "scale-1": "stopped",
            "scale-2": "stopped",
        }

    def test_job_is_created_anew_for_every_crawl(self):
        with ScaleSite(5) as site:
            crawl_job(site, 1, 2, dsn=POSTGRES_DSN)
        with ScaleSite(8) as site:
            run = crawl_job(site, 1, 2, dsn=POSTGRES_DSN)

            assert (run.pages, site.requests) == (8, 8)

    def test_failed_workers_are_named_with_their_exit_codes(self):
        # A name a worker cannot have: its command line is refused.
        with ScaleSite(3) as site, pytest.raises(RuntimeError) as error:
            crawl_job(site, 2, 2, dsn=POSTGRES_DSN, job="scale job")

        assert str(error.value) == (
            "workers of crawl job scale job failed, with the exit codes: scale job-1 (2), scale job-2 (2)"
        )

    def test_stopped_crawl_job_puts_the_pages_of_its_workers_back(self):
        stop = threading.Event()

        def stop_once_crawling() -> None:
            # Not before: a worker that is still starting has no page to put back.
            while site.requests == 0:
                time.sleep(0.01)
            stop.set()

        # Left alone, the crawl takes 300 * 0.05 / 2 seconds.
        with ScaleSite(300, 0.05) as site, pytest.raises(asyncio.CancelledError):
            threading.Thread(target=stop_once_crawling, daemon=True).start()
            crawl_job(site, 1, 2, dsn=POSTGRES_DSN, stop=stop)

        stats = asyncio.run(job_stats(POSTGRES_DSN, "scale"))
        assert [worker["state"] for worker in stats["workers"].values()] == ["stopped"]
        assert stats["in_progress"] == 0
        assert stats["queued"] > 0
        assert stats["total_pages"] < 300

    def test_scale_command_with_workers_prints_a_row_per_crawl_and_saves_them(self, tmp_path, capsys):
        report = tmp_path / "scale.json"
        argv = ["scale", "20", "--delay", "0", "--concurrency", "3", "--workers", "2", "1", "2"]

        asyncio.run(run_scale(parse_args([*argv, "--database-url", POSTGRES_DSN, "--json", str(report)])))

        output = capsys.readouterr().out.splitlines()
        assert (
            "=== Scale: one process vs crawl jobs of 1, 2 worker processes, 3 requests at once in each "
            "(the site answers in 0 ms) ==="
        ) in output
        rows = [line.split() for line in output if line.split()[:1] == ["20"]]
        assert [row[1:-3] for row in rows] == [["local"], ["1", "worker"], ["2", "workers"]]
        assert [row[-1] for row in rows] == ["-", "1.0x", rows[2][-1]]
        (cost,) = [line for line in output if line.startswith("  a page of one worker takes ")]
        assert cost.endswith(" ms more than a local one")
        saved = json.loads(report.read_text(encoding="utf-8"))
        assert (saved["delay"], saved["concurrency"], saved["workers"]) == (0.0, 3, [1, 2])
        (result,) = saved["results"]
        assert result["local"]["pages"] == 20
        assert {workers: run["pages"] for workers, run in result["jobs"].items()} == {"1": 20, "2": 20}
        assert result["requests"] == {"1": 20, "2": 20}
        assert result["speedup"]["1"] == 1.0
        assert result["database_cost"] is not None

    def test_scale_command_without_the_database_exits_with_its_error(self, capsys):
        args = parse_args(
            [
                "scale",
                "5",
                "--delay",
                "0",
                "--workers",
                "1",
                "--database-url",
                "postgresql://crawler@127.0.0.1:1/crawler",
            ]
        )

        with pytest.raises(SystemExit) as exit_info:
            asyncio.run(run_scale(args))

        assert str(exit_info.value).startswith("error: the database of crawl job scale failed: ")
