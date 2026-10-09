"""Integration tests for the Frontier contract, in memory and in PostgreSQL: order, deduplication, outcomes, completion and the limits on pages."""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest
from helpers import POSTGRES_DSN, drop_frontier_tables, make_job

from crawler import (
    Admission,
    Frontier,
    FrontierPage,
    FrontierStats,
    HostFailures,
    MemoryFrontier,
    Outcome,
    PostgresFrontier,
    UnsavedPage,
)

FrontierFactory = Callable[..., Awaitable[Frontier]]


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
async def make_frontier(request) -> AsyncGenerator[FrontierFactory, None]:
    """Makes frontiers of every implementation with the limits given as keywords; closes them after the test.

    In PostgreSQL every frontier of a test is a worker of one job on empty tables.
    """
    opened = []
    if request.param == "postgres":
        await drop_frontier_tables()

    async def make_frontier(**limits) -> Frontier:
        if request.param == "postgres":
            await make_job(**limits)
            frontier = await PostgresFrontier.open(POSTGRES_DSN, job="test")
        else:
            frontier = MemoryFrontier(**limits)
        opened.append(frontier)
        return frontier

    yield make_frontier
    for frontier in opened:
        await frontier.close()


@pytest.fixture
async def frontier(make_frontier) -> Frontier:
    return await make_frontier()


async def current_stats(frontier: Frontier) -> FrontierStats:
    """The stats of a frontier, refreshed first where they are a snapshot of a shared database."""
    if isinstance(frontier, PostgresFrontier):
        await frontier.refresh_stats()
    return frontier.stats()


async def take(frontier: Frontier) -> FrontierPage:
    page = await frontier.take()
    assert page is not None
    return page


async def drain(frontier: Frontier) -> list[str]:
    """Take every page, finishing each as processed right away."""
    urls = []
    while (page := await frontier.take()) is not None:
        urls.append(page.url)
        await frontier.finish(page, Outcome.PROCESSED)
    return urls


class TestOrder:
    async def test_lower_depth_comes_first(self, frontier):
        await frontier.add(["http://site/deep"], depth=2)
        await frontier.add(["http://site/shallow"], depth=1)
        await frontier.seed(["http://site/"])
        assert await drain(frontier) == ["http://site/", "http://site/shallow", "http://site/deep"]

    async def test_equal_depths_keep_the_order_they_were_added_in(self, frontier):
        urls = [f"http://site/{i}" for i in range(5)]
        await frontier.add(urls[:2], depth=1)
        await frontier.add(urls[2:], depth=1)
        assert await drain(frontier) == urls


class TestSeed:
    async def test_start_urls_are_returned_in_the_form_kept(self, frontier):
        seeded = await frontier.seed(["HTTP://Site:80/a#top", "http://site/a", "http://site/b?utm_source=x"])

        assert seeded == ["http://site/a", "http://site/b"]
        assert await take(frontier) == FrontierPage("http://site/a", 0)

    async def test_start_urls_are_accepted_whatever_the_bounds(self, make_frontier):
        frontier = await make_frontier(max_pages=1, max_pages_per_host=1, frontier_factor=1)

        seeded = await frontier.seed([f"http://site/{i}" for i in range(4)])

        assert len(seeded) == 4
        assert (await current_stats(frontier)).queued == 4
        assert await frontier.full()

    async def test_start_urls_seeded_again_are_returned_but_not_queued_again(self, frontier):
        await frontier.seed(["http://site/a"])
        await frontier.finish(await take(frontier), Outcome.PROCESSED)

        seeded = await frontier.seed(["http://site/a", "http://site/b"])

        assert seeded == ["http://site/a", "http://site/b"]
        assert await drain(frontier) == ["http://site/b"]


class TestAdd:
    async def test_duplicates_are_not_accepted(self, frontier):
        assert await frontier.add(["http://site/a", "HTTP://Site:80/a#top"], depth=1) == 1
        assert await frontier.add(["http://site/a"], depth=0) == 0
        assert (await current_stats(frontier)).queued == 1

    async def test_page_is_not_accepted_again_after_it_was_taken(self, frontier):
        await frontier.add(["http://site/a"], depth=0)
        page = await take(frontier)
        assert await frontier.add(["http://site/a"], depth=0) == 0
        await frontier.finish(page, Outcome.PROCESSED)
        assert await frontier.add(["http://site/a"], depth=0) == 0

    async def test_invalid_url_is_not_accepted(self, frontier):
        assert await frontier.add(["mailto:someone@site"], depth=0) == 0
        assert await frontier.take() is None

    async def test_tracking_parameters_are_dropped(self, frontier):
        assert await frontier.add(["http://site/a?id=1&utm_source=mail"], depth=0) == 1
        assert await frontier.add(["http://site/a?fbclid=x&id=1"], depth=0) == 0
        assert not await frontier.mark_seen("http://site/a?id=1&gclid=y", "http://site/from")
        assert await take(frontier) == FrontierPage("http://site/a?id=1", 0)

    async def test_page_keeps_its_depth(self, frontier):
        await frontier.add(["HTTP://Site/a"], depth=3)
        assert await take(frontier) == FrontierPage("http://site/a", 3)

    async def test_url_marked_seen_is_not_accepted(self, frontier):
        await frontier.mark_seen("http://site/redirect-target", "http://site/from")
        assert await frontier.add(["http://site/redirect-target"], depth=0) == 0

    async def test_mark_seen_tells_whether_the_url_is_new(self, frontier):
        await frontier.add(["http://site/page"], depth=0)

        assert await frontier.mark_seen("http://site/target", "http://site/from")
        assert not await frontier.mark_seen("http://site/target?utm_source=x", "http://site/other")
        assert not await frontier.mark_seen("http://site/page", "http://site/from")
        assert not await frontier.mark_seen("not a url", "http://site/from")

    async def test_url_marked_seen_is_new_again_to_the_page_that_marked_it(self, frontier):
        # The page went back after it followed its redirect: whoever takes it follows it again.
        await frontier.add(["http://site/page"], depth=0)
        assert await frontier.mark_seen("http://site/target", "http://site/from")

        assert await frontier.mark_seen("http://site/target?utm_source=x", "http://site/from")
        assert not await frontier.mark_seen("http://site/target", "http://site/other")
        assert not await frontier.mark_seen("http://site/page", "http://site/page")

    async def test_forgotten_url_is_accepted_again_unless_it_was_accepted(self, frontier):
        await frontier.mark_seen("http://site/redirect-target", "http://site/from")
        await frontier.add(["http://site/page"], depth=0)

        await frontier.forget("http://site/redirect-target?utm_source=x", "http://site/from")
        await frontier.forget("http://site/page", "http://site/from")
        await frontier.forget("not a url", "http://site/from")

        assert await frontier.add(["http://site/redirect-target"], depth=0) == 1
        assert await frontier.add(["http://site/page"], depth=0) == 0

    async def test_url_is_forgotten_only_by_the_page_that_marked_it(self, frontier):
        await frontier.mark_seen("http://site/target", "http://site/from")

        await frontier.forget("http://site/target", "http://site/other")

        assert not await frontier.mark_seen("http://site/target", "http://site/other")
        assert await frontier.add(["http://site/target"], depth=0) == 0


class TestTake:
    async def test_returns_none_when_empty_and_idle(self, frontier):
        assert await frontier.take() is None

    async def test_waits_while_a_page_is_in_progress(self, frontier):
        await frontier.seed(["http://site/"])
        page = await take(frontier)
        waiter = asyncio.create_task(frontier.take())
        await asyncio.sleep(0)
        assert not waiter.done()

        # The page in progress finds a link: the waiting worker gets it.
        await frontier.add(["http://site/link"], depth=1)
        await frontier.finish(page, Outcome.PROCESSED)
        assert await waiter == FrontierPage("http://site/link", 1)

    async def test_waiters_finish_when_the_last_page_is_done(self, frontier):
        await frontier.seed(["http://site/"])
        page = await take(frontier)
        waiters = [asyncio.create_task(frontier.take()) for _ in range(3)]
        await asyncio.sleep(0)

        await frontier.finish(page, Outcome.FAILED, "NetworkError: boom")

        assert await asyncio.gather(*waiters) == [None, None, None]

    async def test_page_is_in_progress_until_put_back_or_finished(self, frontier):
        await frontier.seed(["http://site/a", "http://site/b"])
        a, b = await take(frontier), await take(frontier)
        assert frontier.in_progress(a) and frontier.in_progress(b)

        await frontier.put_back(a, uncount=False)
        await frontier.finish(b, Outcome.PROCESSED)

        assert not frontier.in_progress(a) and not frontier.in_progress(b)


class TestPutBack:
    async def test_page_put_back_is_taken_again_with_its_depth(self, frontier):
        await frontier.add(["http://site/a"], depth=1)
        page = await take(frontier)

        await frontier.put_back(page, uncount=False)

        assert (await current_stats(frontier)).queued == 1
        assert await take(frontier) == page

    async def test_page_put_off_comes_back_after_the_delay(self, frontier):
        await frontier.add(["http://site/a"], depth=1)
        page = await take(frontier)

        await frontier.put_back(page, 0.05, uncount=False)

        stats = await current_stats(frontier)
        assert (stats.queued, stats.in_progress) == (1, 0)
        # Nothing is in progress, yet the crawl is not over.
        waiter = asyncio.create_task(frontier.take())
        await asyncio.sleep(0.01)
        assert not waiter.done()
        assert await waiter == page

    async def test_page_put_off_waits_its_turn_by_depth(self, frontier):
        await frontier.seed(["http://site/a"])
        page = await take(frontier)
        await frontier.put_back(page, 0.01, uncount=False)
        await asyncio.sleep(0.02)
        await frontier.add(["http://site/b"], depth=1)
        await frontier.seed(["http://site/c"])

        assert await drain(frontier) == ["http://site/a", "http://site/c", "http://site/b"]


class TestWaits:
    """How many times a page waited for its host, which bounds its waits across takes."""

    async def test_page_counts_the_waits_it_was_put_back_for(self, frontier):
        await frontier.seed(["http://site/a"])
        page = await take(frontier)
        assert frontier.waits(page) == 0

        await frontier.put_back(page, waited=True, uncount=False)
        page = await take(frontier)
        assert frontier.waits(page) == 1

        await frontier.put_back(page, waited=True, uncount=False)
        page = await take(frontier)
        assert frontier.waits(page) == 2

    async def test_page_put_back_without_a_wait_keeps_its_count(self, frontier):
        await frontier.seed(["http://site/a"])
        await frontier.put_back(await take(frontier), waited=True, uncount=False)
        await frontier.put_back(await take(frontier), uncount=False)

        assert frontier.waits(await take(frontier)) == 1


class TestOutcomes:
    async def test_stats_follow_the_lifecycle(self, frontier):
        await frontier.seed([f"http://site/{name}" for name in "abcdef"])
        a, b, c, d, e = [await take(frontier) for _ in range(5)]
        stats = await current_stats(frontier)
        assert (stats.queued, stats.in_progress, stats.processed) == (1, 5, 0)

        await frontier.finish(a, Outcome.PROCESSED)
        await frontier.finish(b, Outcome.FAILED, "HTTPStatusError: HTTP 404 Not Found")
        await frontier.finish(c, Outcome.SKIPPED, "redirected out of scope: http://other/")
        await frontier.finish(d, Outcome.BLOCKED, "disallowed by robots.txt")
        await frontier.finish(e, Outcome.UNREACHABLE, "robots.txt is unreachable (HTTP 503)")

        stats = await current_stats(frontier)
        assert (stats.queued, stats.in_progress) == (1, 0)
        assert (stats.processed, stats.failed, stats.skipped, stats.blocked, stats.unreachable) == (1, 1, 1, 1, 1)

    async def test_frontier_in_memory_lists_the_pages_without_a_record(self):
        frontier = MemoryFrontier()
        await frontier.seed([f"http://site/{name}" for name in "abcdef"])
        a, b, c, d, e, f = [await take(frontier) for _ in range(6)]

        await frontier.finish(a, Outcome.PROCESSED, pending_save=True)
        await frontier.finish(c, Outcome.SKIPPED, "not HTML: application/pdf", status=200, elapsed=0.1)
        await frontier.finish(
            b, Outcome.FAILED, "HTTPStatusError: HTTP 404", status=404, elapsed=0.1, error="HTTPStatusError"
        )
        await frontier.finish(d, Outcome.BLOCKED, "disallowed by robots.txt")
        await frontier.finish(e, Outcome.UNREACHABLE, "robots.txt is unreachable (HTTP 503)")
        await frontier.finish(f, Outcome.PROCESSED, pending_save=True)
        await frontier.saved([a.url])
        await frontier.dropped([f.url])

        assert list(frontier.unsaved.values()) == [
            UnsavedPage(c.url, "skipped", "not HTML: application/pdf", 200),
            UnsavedPage(b.url, "failed", "HTTPStatusError: HTTP 404", 404, "HTTPStatusError"),
            UnsavedPage(d.url, "blocked", "disallowed by robots.txt"),
            UnsavedPage(e.url, "unreachable", "robots.txt is unreachable (HTTP 503)"),
            UnsavedPage(f.url, "failed", "its record could not be stored", error="RecordDropped"),
        ]
        assert (await current_stats(frontier)).processed == 2

    async def test_page_processed_pending_its_save_is_processed(self, frontier):
        await frontier.seed(["http://site/a"])
        page = await take(frontier)

        await frontier.finish(page, Outcome.PROCESSED, pending_save=True)

        # The worker that saves it is not kept waiting for its own save.
        assert await frontier.take() is None
        assert (await current_stats(frontier)).processed == 1
        await frontier.saved([page.url])
        stats = await current_stats(frontier)
        assert (stats.processed, stats.in_progress) == (1, 0)

    async def test_pending_or_processed_urls(self, frontier):
        await frontier.seed([f"http://site/{name}" for name in "abcd"])
        await frontier.mark_seen("http://site/target", "http://site/from")
        a, b = await take(frontier), await take(frontier)
        await frontier.finish(a, Outcome.PROCESSED)
        await frontier.finish(b, Outcome.FAILED, "error")
        c = await take(frontier)

        assert await frontier.is_pending_or_processed("http://site/a?utm_source=x")
        assert await frontier.is_pending_or_processed(c.url)
        assert await frontier.is_pending_or_processed("http://site/d")
        assert not await frontier.is_pending_or_processed(b.url)
        assert not await frontier.is_pending_or_processed("http://site/target")
        assert not await frontier.is_pending_or_processed("http://site/never")
        assert not await frontier.is_pending_or_processed("not a url")

    @pytest.mark.parametrize("outcome", [Outcome.FAILED, Outcome.SKIPPED, Outcome.BLOCKED, Outcome.UNREACHABLE])
    async def test_outcome_other_than_processed_needs_a_reason(self, frontier, outcome):
        await frontier.seed(["http://site/a"])
        page = await take(frontier)
        with pytest.raises(ValueError, match="needs a reason"):
            await frontier.finish(page, outcome)

    @pytest.mark.parametrize(
        "finish",
        [
            lambda frontier, page: frontier.finish(page, Outcome.PROCESSED),
            lambda frontier, page: frontier.finish(page, Outcome.FAILED, "error"),
            lambda frontier, page: frontier.put_back(page, uncount=False),
            lambda frontier, page: frontier.put_back(page, 1.0, uncount=False),
        ],
        ids=["processed", "failed", "put back", "put off"],
    )
    async def test_page_not_taken_cannot_be_finished(self, frontier, finish):
        await frontier.seed(["http://site/a"])
        with pytest.raises(ValueError, match="not in progress"):
            await finish(frontier, FrontierPage("http://site/a", 0))
        if isinstance(frontier, MemoryFrontier):
            assert frontier.unsaved == {}


class TestMaxPages:
    async def test_pages_admitted_up_to_max_pages_and_then_none_is_handed_out(self, make_frontier):
        frontier = await make_frontier(max_pages=2)
        await frontier.seed([f"http://site/{name}" for name in "abcd"])
        a, b, c = [await take(frontier) for _ in range(3)]

        assert await frontier.admit(a) is Admission.ADMITTED
        assert await frontier.admit(b) is Admission.ADMITTED
        # Taken before the limit was reached: it goes back unrequested.
        assert await frontier.admit(c) is Admission.OVER_MAX_PAGES
        await frontier.put_back(c, uncount=False)

        assert await frontier.take() is None
        await frontier.finish(a, Outcome.PROCESSED)
        await frontier.finish(b, Outcome.PROCESSED)
        stats = await current_stats(frontier)
        assert (stats.requested, stats.processed, stats.queued) == (2, 2, 2)

    async def test_page_put_back_uncounted_frees_its_place_until_taken_again(self, make_frontier):
        frontier = await make_frontier(max_pages=1)
        await frontier.seed(["http://site/a", "http://site/b"])
        a = await take(frontier)
        assert await frontier.admit(a) is Admission.ADMITTED

        await frontier.put_back(a, 0.01, uncount=True)

        assert (await current_stats(frontier)).requested == 0
        # The put-off page comes back after the page that took its place.
        b = await take(frontier)
        assert await frontier.admit(b) is Admission.ADMITTED
        await frontier.finish(b, Outcome.PROCESSED)
        assert await frontier.take() is None
        assert (await current_stats(frontier)).queued == 1

    async def test_page_given_up_uncounted_frees_its_place(self, make_frontier):
        frontier = await make_frontier(max_pages=1)
        await frontier.seed(["http://site/a", "http://site/b"])
        a = await take(frontier)
        assert await frontier.admit(a) is Admission.ADMITTED

        await frontier.finish(a, Outcome.FAILED, "CircuitOpenError: refused", uncount=True)

        b = await take(frontier)
        assert await frontier.admit(b) is Admission.ADMITTED
        assert (await current_stats(frontier)).requested == 1

    async def test_page_given_up_after_its_request_stays_counted(self, make_frontier):
        frontier = await make_frontier(max_pages=1)
        await frontier.seed(["http://site/a", "http://site/b"])
        a = await take(frontier)
        assert await frontier.admit(a) is Admission.ADMITTED

        await frontier.finish(a, Outcome.FAILED, "HTTPStatusError: HTTP 503")

        assert await frontier.take() is None
        assert (await current_stats(frontier)).requested == 1

    async def test_without_limits_every_page_is_admitted(self, frontier):
        await frontier.add([f"http://site/{i}" for i in range(50)], depth=1)

        for _ in range(50):
            assert await frontier.admit(await take(frontier)) is Admission.ADMITTED
        assert not await frontier.full()


class TestMaxPagesPerHost:
    async def test_page_over_the_host_limit_is_not_admitted(self, make_frontier):
        frontier = await make_frontier(max_pages_per_host=1)
        await frontier.seed(["http://a/1", "http://a/2", "http://b/1"])
        a1, a2, b1 = [await take(frontier) for _ in range(3)]

        assert await frontier.admit(a1) is Admission.ADMITTED
        assert await frontier.admit(a2) is Admission.OVER_HOST_LIMIT
        assert await frontier.admit(b1) is Admission.ADMITTED
        stats = await current_stats(frontier)
        assert (stats.over_host_limit, stats.requested) == (1, 2)

    async def test_page_of_the_host_uncounted_frees_its_place(self, make_frontier):
        frontier = await make_frontier(max_pages_per_host=1)
        await frontier.seed(["http://a/1", "http://a/2"])
        a1, a2 = await take(frontier), await take(frontier)
        assert await frontier.admit(a1) is Admission.ADMITTED

        await frontier.put_back(a1, uncount=True)

        assert await frontier.admit(a2) is Admission.ADMITTED


class TestBounds:
    async def test_found_pages_are_accepted_until_the_frontier_is_full(self, make_frontier):
        frontier = await make_frontier(max_pages=1, frontier_factor=3)
        await frontier.seed(["http://site/"])

        assert await frontier.add([f"http://site/{i}" for i in range(5)], depth=1) == 2
        assert await frontier.full()
        assert (await current_stats(frontier)).links_dropped == 3
        # Not remembered: a page found again once there is room is accepted then.
        assert await frontier.mark_seen("http://site/4", "http://site/")

    async def test_pages_requested_take_room_in_the_frontier(self, make_frontier):
        frontier = await make_frontier(max_pages=2, frontier_factor=1)
        await frontier.seed(["http://site/a"])
        page = await take(frontier)
        await frontier.admit(page)
        await frontier.finish(page, Outcome.PROCESSED)

        assert await frontier.add(["http://site/b", "http://site/c"], depth=1) == 1
        assert await frontier.full()

    async def test_host_has_a_bounded_share_of_the_frontier(self, make_frontier):
        frontier = await make_frontier(max_pages_per_host=1, frontier_factor=2)
        await frontier.seed(["http://a/"])

        assert await frontier.add([f"http://a/{i}" for i in range(4)], depth=1) == 1
        assert await frontier.add(["http://b/1"], depth=1) == 1
        assert (await current_stats(frontier)).links_dropped_by_host == 3

    async def test_duplicates_are_not_counted_as_dropped(self, make_frontier):
        frontier = await make_frontier(max_pages=1, frontier_factor=1)
        await frontier.seed(["http://site/"])

        assert await frontier.add(["http://site/", "http://site/new"], depth=1) == 0
        assert (await current_stats(frontier)).links_dropped == 1


def on_host(host: str) -> Callable[[str], bool]:
    """A filter that lets through the URLs of `host` only."""
    return lambda url: f"//{host}/" in url


class TestScope:
    async def test_pages_held_out_of_scope_are_neither_queued_nor_seen(self, frontier):
        await frontier.hold_out_of_scope(["http://other/a"])

        assert await frontier.take() is None
        assert await frontier.add(["http://other/a"], depth=1) == 1

    async def test_widening_the_scope_queues_the_pages_held_that_the_filter_lets_through(self, frontier):
        await frontier.hold_out_of_scope(["http://other/a", "http://third/c", "http://other/b"])

        assert await frontier.widen_scope("other", on_host("other")) == 2

        assert await drain(frontier) == ["http://other/a", "http://other/b"]
        # The page of the third host is still held.
        assert await frontier.widen_scope("third", on_host("third")) == 1
        assert await take(frontier) == FrontierPage("http://third/c", 0)

    async def test_page_held_is_queued_once(self, frontier):
        await frontier.hold_out_of_scope(["http://other/a", "http://other/a"])

        assert await frontier.widen_scope("other", on_host("other")) == 1
        assert await frontier.widen_scope("other", on_host("other")) == 0

    async def test_pages_brought_into_the_scope_keep_to_the_bounds(self, make_frontier):
        frontier = await make_frontier(max_pages=1, frontier_factor=1)
        await frontier.add(["http://site/a"], depth=1)
        await frontier.hold_out_of_scope(["http://other/a"])

        assert await frontier.widen_scope("other", on_host("other")) == 0
        assert (await current_stats(frontier)).links_dropped == 1

    async def test_scope_hosts_are_listed_once_in_the_order_brought_in(self, frontier):
        assert frontier.scope_hosts() == []

        for host in ("b", "a", "b"):
            await frontier.widen_scope(host, on_host(host))

        assert frontier.scope_hosts() == ["b", "a"]


class TestHostFailures:
    async def test_frontier_of_one_process_counts_no_failure_of_a_host_and_gives_none_up(self):
        # The process refuses the pages of a host it gave up itself.
        frontier = MemoryFrontier()
        await frontier.seed(["http://a/1"])

        assert await frontier.count_host_failures("a", circuit_openings=3, robots_failures=4) == HostFailures()
        await frontier.give_up_host("a", Outcome.FAILED, "circuit breaker of a opened 3 times")

        page = await take(frontier)
        assert page == FrontierPage("http://a/1", 0)
        assert frontier.given_up(page) is None


class TestHoldHost:
    async def test_frontier_of_one_process_is_not_shared_and_holds_no_host(self):
        # Its host is held back by the rate limiter of the one process.
        frontier = MemoryFrontier()
        await frontier.seed(["http://a/1"])

        await frontier.hold_host("a", 60, "HTTP 429 Too Many Requests, Retry-After 60s")

        assert not frontier.shared
        assert await take(frontier) == FrontierPage("http://a/1", 0)

    async def test_frontier_of_one_process_keeps_no_interval_of_a_host(self):
        # The rate limiter of the one process keeps its requests apart.
        frontier = MemoryFrontier()
        await frontier.seed(["http://a/1", "http://a/2"])

        await frontier.set_host_interval("a", 60)

        assert await take(frontier) == FrontierPage("http://a/1", 0)
        assert await take(frontier) == FrontierPage("http://a/2", 0)
