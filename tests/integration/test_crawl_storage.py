"""Integration tests: a crawl saves its pages to a storage."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from helpers import UNTHROTTLED, MemoryStorage

from crawler import AsyncCrawler, CSVStorage, DataStorage, JSONStorage, PageRecord, SQLiteStorage, StorageError

DISK_FULL = OSError("disk full")
# More failures than any test has writes: the storage never recovers.
ALWAYS = 1000


async def crawl(storage: DataStorage | None, start_url: str, *, max_depth: int = 2, **options) -> AsyncCrawler:
    """Run a crawl and return the closed crawler with its state."""
    options.setdefault("same_domain_only", True)
    async with AsyncCrawler(max_concurrent=5, max_depth=max_depth, storage=storage, **UNTHROTTLED) as crawler:
        await crawler.crawl([start_url], **options)
    return crawler


def saved_records(storage: MemoryStorage) -> dict[str, PageRecord]:
    return {record["url"]: record for batch in storage.batches for record in batch}


STORAGES: dict[str, Callable[..., DataStorage]] = {
    "pages.jsonl": JSONStorage,
    "pages.csv": CSVStorage,
    "pages.db": SQLiteStorage,
}


class TestSavedPages:
    @pytest.mark.parametrize("file_name", STORAGES)
    async def test_every_processed_page_is_saved(self, url, tmp_path, file_name):
        open_storage = STORAGES[file_name]

        crawler = await crawl(open_storage(tmp_path / file_name), url("/site/"))

        async with open_storage(tmp_path / file_name) as storage:
            saved = {record["url"]: record async for record in storage.read()}
        assert set(saved) == set(crawler.processed_urls)
        assert len(saved) == 5
        for page_url, record in saved.items():
            page = crawler.processed_urls[page_url]
            assert (record["title"], record["text"], record["links"]) == (page["title"], page["text"], page["links"])
            assert (record["status_code"], record["content_type"]) == (200, "text/html")
        stats = crawler.crawl_stats()
        assert (stats.saved, stats.save_failed) == (5, 0)

    async def test_record_describes_the_page_and_its_response(self, url):
        storage = MemoryStorage()
        started = datetime.now(UTC)

        await crawl(storage, url("/catalog/tools/"), max_depth=0)

        (record,) = saved_records(storage).values()
        assert list(record) == [
            "url",
            "title",
            "text",
            "links",
            "metadata",
            "crawled_at",
            "status_code",
            "content_type",
        ]
        assert record["url"] == url("/catalog/tools/")
        assert record["title"]
        assert record["text"]
        assert record["links"]
        assert record["status_code"] == 200
        assert record["content_type"] == "text/html"
        assert timedelta(0) <= record["crawled_at"] - started < timedelta(seconds=10)
        assert record["crawled_at"].utcoffset() == timedelta(0)
        assert set(record["metadata"]) == {
            "description", "keywords", "language", "canonical", "robots", "final_url", "depth"
        }  # fmt: skip
        assert record["metadata"]["final_url"] == url("/catalog/tools/")
        assert record["metadata"]["depth"] == 0

    async def test_redirected_page_keeps_the_requested_url(self, url):
        storage = MemoryStorage()

        await crawl(storage, url("/site/"))

        record = saved_records(storage)[url("/site/moved")]
        assert record["title"] == "C"
        assert record["metadata"]["final_url"] == url("/site/c.html")
        assert record["metadata"]["depth"] == 2

    async def test_page_without_a_title_has_an_empty_one(self, url):
        storage = MemoryStorage()

        await crawl(storage, url("/ok"), max_depth=0)

        (record,) = saved_records(storage).values()
        assert record["title"] == ""
        assert record["text"] == "hello"

    async def test_failed_and_skipped_pages_are_not_saved(self, url):
        storage = MemoryStorage()

        crawler = await crawl(storage, url("/site/exits.html"))

        assert crawler.skipped_urls
        assert set(saved_records(storage)) == set(crawler.processed_urls)

        failing = MemoryStorage()
        crawler = await crawl(failing, url("/status/404"), max_depth=0)

        assert failing.batches == []
        assert crawler.crawl_stats().saved == crawler.crawl_stats().save_failed == 0


class TestBatches:
    async def test_pages_are_written_in_batches(self, url):
        storage = MemoryStorage(batch_size=2)

        await crawl(storage, url("/site/"))

        assert [len(batch) for batch in storage.batches] == [2, 2, 1]

    async def test_crawl_flushes_the_storage_before_it_returns(self, url):
        storage = MemoryStorage(batch_size=100)
        async with AsyncCrawler(max_depth=2, storage=storage, **UNTHROTTLED) as crawler:
            await crawler.crawl([url("/site/")], same_domain_only=True)

            assert (storage.pending, storage.written) == (0, 5)
            assert storage.released == 0
            assert crawler.crawl_stats().saved == 5

    async def test_pages_left_by_an_earlier_crawl_are_not_counted_as_saved(self, url):
        class FailsAfterOneWrite(MemoryStorage):
            async def _write_batch(self, records):
                if self.batches:
                    raise DISK_FULL
                await super()._write_batch(records)

        # The first crawl leaves its 5 pages in the buffer; the first page
        # of the second one completes the batch, the other 4 are not written.
        storage = FailsAfterOneWrite(batch_size=6, failures=[DISK_FULL] * 4)
        async with AsyncCrawler(max_depth=2, storage=storage, **UNTHROTTLED) as crawler:
            await crawler.crawl([url("/site/")], same_domain_only=True)
            assert (storage.pending, storage.written) == (5, 0)

            await crawler.crawl([url("/site/")], same_domain_only=True)

            assert (storage.pending, storage.written) == (4, 6)
            stats = crawler.crawl_stats()
            assert (stats.saved, stats.save_failed) == (1, 4)

    async def test_closing_the_crawler_closes_the_storage(self, url):
        storage = MemoryStorage()
        crawler = AsyncCrawler(storage=storage, **UNTHROTTLED)

        await crawler.close()
        await crawler.close()

        assert storage.released == 1

    async def test_stats_count_the_pages_of_the_latest_crawl(self, url):
        storage = MemoryStorage()
        async with AsyncCrawler(max_depth=2, storage=storage, **UNTHROTTLED) as crawler:
            await crawler.crawl([url("/site/")], same_domain_only=True)
            await crawler.crawl([url("/site/b.html")], max_pages=1)

            assert crawler.crawl_stats().saved == 1
        assert storage.written == 6

    async def test_crawler_without_a_storage_saves_nothing(self, url):
        crawler = await crawl(None, url("/site/"))

        stats = crawler.crawl_stats()
        assert stats.processed == 5
        assert (stats.saved, stats.save_failed) == (0, 0)


class TestSaveErrors:
    async def test_storage_that_always_fails_does_not_stop_the_crawl(self, url, caplog):
        storage = MemoryStorage(batch_size=2, failures=[DISK_FULL] * ALWAYS)

        with caplog.at_level(logging.ERROR, logger="crawler"):
            crawler = await crawl(storage, url("/site/"))

        assert len(crawler.processed_urls) == 5
        assert len(crawler.failed_urls) == 2  # as without a storage
        stats = crawler.crawl_stats()
        assert (stats.processed, stats.saved, stats.save_failed) == (5, 0, 5)
        assert "Failed to save " in caplog.text
        assert "failed to write 2 records: disk full" in caplog.text
        assert "Failed to save the pages the storage buffers" in caplog.text
        assert "Failed to close MemoryStorage" in caplog.text
        assert storage.released == 1

    async def test_pages_of_a_failed_write_are_saved_by_the_next_one(self, url):
        # The first batch fails with its retries; the storage works after that.
        storage = MemoryStorage(batch_size=2, failures=[DISK_FULL] * 4)

        crawler = await crawl(storage, url("/site/"))

        assert set(saved_records(storage)) == set(crawler.processed_urls)
        stats = crawler.crawl_stats()
        assert (stats.saved, stats.save_failed) == (5, 0)

    async def test_write_is_retried(self, url):
        storage = MemoryStorage(failures=[DISK_FULL, DISK_FULL])

        crawler = await crawl(storage, url("/site/"))

        assert storage.attempts == 3
        assert crawler.crawl_stats().saved == 5

    async def test_unexpected_error_of_the_storage_does_not_stop_the_crawl(self, url, caplog):
        storage = MemoryStorage(batch_size=1, failures=[TypeError("not serializable")] * ALWAYS)

        with caplog.at_level(logging.ERROR, logger="crawler"):
            crawler = await crawl(storage, url("/site/"))

        stats = crawler.crawl_stats()
        assert (stats.processed, stats.saved, stats.save_failed) == (5, 0, 5)
        assert "Unexpected error while saving " in caplog.text
        assert "TypeError: not serializable" in caplog.text

    async def test_page_that_cannot_be_saved_is_the_only_one_lost(self, url, caplog):
        storage = MemoryStorage(refused={url("/site/b.html")})

        with caplog.at_level(logging.ERROR, logger="crawler"):
            crawler = await crawl(storage, url("/site/"))

        assert set(saved_records(storage)) == set(crawler.processed_urls) - {url("/site/b.html")}
        stats = crawler.crawl_stats()
        assert (stats.processed, stats.saved, stats.save_failed) == (5, 4, 1)
        assert f"Dropped the record of {url('/site/b.html')}" in caplog.text

    async def test_closed_storage_fails_the_crawl_before_it_requests_anything(self, url, site):
        storage = MemoryStorage()
        await storage.close()

        with pytest.raises(StorageError, match="MemoryStorage is closed"):
            await crawl(storage, url("/site/"))

        assert site.hits == {}

    async def test_buffered_pages_are_not_failures_while_the_crawl_runs(self, url):
        seen = []

        class WatchingStorage(MemoryStorage):
            async def save(self, record: PageRecord) -> None:
                await super().save(record)
                stats = crawler.crawl_stats()
                seen.append((stats.saved, stats.save_failed))

        async with AsyncCrawler(max_concurrent=1, storage=WatchingStorage(batch_size=2), **UNTHROTTLED) as crawler:
            await crawler.crawl([url("/site/")], same_domain_only=True)

        assert seen == [(0, 0), (2, 0), (2, 0), (4, 0), (4, 0)]
        assert crawler.crawl_stats().saved == 5


class TestPagesNotKept:
    async def test_pages_go_to_the_storage_and_not_to_memory(self, url):
        storage = MemoryStorage()

        async with AsyncCrawler(max_concurrent=5, storage=storage, keep_pages=False, **UNTHROTTLED) as crawler:
            pages = await crawler.crawl([url("/site/")], same_domain_only=True)

        kept = await crawl(MemoryStorage(), url("/site/"))
        assert pages == {} and crawler.processed_urls == {}
        # The links of the pages were followed all the same, and every page was saved and counted.
        assert set(saved_records(storage)) == set(kept.processed_urls)
        assert crawler.visited_urls == kept.visited_urls
        assert crawler.failed_urls.keys() == kept.failed_urls.keys()
        stats = crawler.crawl_stats()
        assert (stats.processed, stats.saved, stats.save_failed) == (5, 5, 0)
        assert crawler.stats.get_stats()["successful"] == 5

    async def test_pages_are_kept_by_default(self, url):
        async with AsyncCrawler(**UNTHROTTLED) as crawler:
            pages = await crawler.crawl([url("/site/")], same_domain_only=True)

        assert crawler.keep_pages
        assert len(pages) == 5 and pages is crawler.processed_urls
