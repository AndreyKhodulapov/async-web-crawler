"""Unit tests for CrawlerQueue: ordering, deduplication, status and completion."""

import asyncio

import pytest

from crawler import CrawlerQueue


async def take(queue: CrawlerQueue) -> str:
    url = await queue.get_next()
    assert url is not None
    return url


async def drain(queue: CrawlerQueue) -> list[str]:
    """Take every URL, marking each processed right away."""
    urls = []
    while (url := await queue.get_next()) is not None:
        urls.append(url)
        queue.mark_processed(url)
    return urls


class TestOrder:
    async def test_lower_priority_value_comes_first(self):
        queue = CrawlerQueue()
        queue.add_url("http://site/low", priority=5)
        queue.add_url("http://site/high", priority=-1)
        queue.add_url("http://site/default")
        assert await drain(queue) == ["http://site/high", "http://site/default", "http://site/low"]

    async def test_equal_priorities_keep_insertion_order(self):
        queue = CrawlerQueue()
        urls = [f"http://site/{i}" for i in range(5)]
        for url in urls:
            queue.add_url(url, priority=1)
        assert await drain(queue) == urls


class TestAddUrl:
    def test_duplicates_are_rejected(self):
        queue = CrawlerQueue()
        assert queue.add_url("http://site/a") is True
        assert queue.add_url("HTTP://Site:80/a#top", priority=-10) is False
        assert queue.get_stats()["queued"] == 1

    async def test_url_is_not_queued_again_after_it_was_taken(self):
        queue = CrawlerQueue()
        queue.add_url("http://site/a")
        url = await take(queue)
        assert queue.add_url("http://site/a") is False
        queue.mark_processed(url)
        assert queue.add_url("http://site/a") is False

    def test_invalid_url_is_rejected(self):
        queue = CrawlerQueue()
        assert queue.add_url("mailto:someone@site") is False
        assert queue.get_stats()["seen"] == 0

    def test_url_is_stored_normalized_with_its_depth(self):
        queue = CrawlerQueue()
        queue.add_url("HTTP://Site", depth=3)
        assert dict(queue.depths) == {"http://site/": 3}
        assert queue.depth("http://site/") == 3

    def test_marked_seen_url_is_not_queued(self):
        queue = CrawlerQueue()
        queue.mark_seen("http://site/redirect-target")
        assert queue.add_url("http://site/redirect-target") is False


class TestGetNext:
    async def test_returns_none_when_empty_and_idle(self):
        assert await CrawlerQueue().get_next() is None

    async def test_waits_while_work_is_in_progress(self):
        queue = CrawlerQueue()
        queue.add_url("http://site/")
        page = await take(queue)
        waiter = asyncio.create_task(queue.get_next())
        await asyncio.sleep(0)
        assert not waiter.done()

        # The page in progress finds a link: the waiting worker gets it.
        queue.add_url("http://site/link")
        queue.mark_processed(page)
        assert await waiter == "http://site/link"

    async def test_waiters_finish_when_last_page_is_done(self):
        queue = CrawlerQueue()
        queue.add_url("http://site/")
        page = await take(queue)
        waiters = [asyncio.create_task(queue.get_next()) for _ in range(3)]
        await asyncio.sleep(0)

        queue.mark_failed(page, "NetworkError: boom")

        assert await asyncio.gather(*waiters) == [None, None, None]

    async def test_close_releases_waiters_and_keeps_queued_urls(self):
        queue = CrawlerQueue()
        queue.add_url("http://site/a")
        queue.add_url("http://site/b")
        page = await take(queue)
        queue.close()

        assert await queue.get_next() is None
        assert queue.add_url("http://site/c") is False
        queue.mark_processed(page)  # pages in progress can still finish
        assert queue.get_stats()["queued"] == 1


class TestStatus:
    async def test_stats_follow_the_lifecycle(self):
        queue = CrawlerQueue()
        for name in ("a", "b", "c", "d", "e", "f"):
            queue.add_url(f"http://site/{name}")
        a, b, c, d, e = [await take(queue) for _ in range(5)]
        assert queue.get_stats() == {
            "queued": 1,
            "in_progress": 5,
            "processed": 0,
            "failed": 0,
            "skipped": 0,
            "blocked": 0,
            "unreachable": 0,
            "seen": 6,
        }

        queue.mark_processed(a)
        queue.mark_failed(b, "HTTPStatusError: HTTP 404 Not Found")
        queue.mark_skipped(c, "redirected out of scope: http://other/")
        queue.mark_blocked(d, "disallowed by robots.txt")
        queue.mark_unreachable(e, "robots.txt is unreachable (HTTP 503)")

        assert queue.get_stats() == {
            "queued": 1,
            "in_progress": 0,
            "processed": 1,
            "failed": 1,
            "skipped": 1,
            "blocked": 1,
            "unreachable": 1,
            "seen": 6,
        }
        assert queue.visited == {a, b, c, d, e}
        assert queue.failed == {b: "HTTPStatusError: HTTP 404 Not Found"}
        assert queue.skipped == {c: "redirected out of scope: http://other/"}
        assert queue.blocked == {d: "disallowed by robots.txt"}
        assert queue.unreachable == {e: "robots.txt is unreachable (HTTP 503)"}

    async def test_requeue_puts_a_url_back_even_after_close(self):
        queue = CrawlerQueue()
        queue.add_url("http://site/a", priority=1, depth=1)
        page = await take(queue)
        queue.close()

        queue.requeue(page, priority=1)

        assert queue.visited == set()
        assert queue.depth(page) == 1
        assert queue.get_stats()["queued"] == 1
        assert queue.get_stats()["in_progress"] == 0
        assert await queue.get_next() is None  # still closed

    @pytest.mark.parametrize(
        ("mark", "args"),
        [
            ("mark_processed", ()),
            ("mark_failed", ("error",)),
            ("mark_skipped", ("reason",)),
            ("mark_blocked", ("reason",)),
            ("mark_unreachable", ("reason",)),
            ("requeue", ()),
        ],
    )
    async def test_marking_a_url_not_in_progress_is_an_error(self, mark, args):
        queue = CrawlerQueue()
        queue.add_url("http://site/a")
        with pytest.raises(ValueError, match="not in progress"):
            getattr(queue, mark)("http://site/a", *args)
