"""Unit tests for the live progress: percent, current speed, time left, the progress line and its printing."""

import asyncio
import io

import pytest

from crawler import CrawlStats, Progress, ProgressTracker, format_progress, show_progress
from crawler.progress import format_duration


def snapshot(elapsed: float, processed: int = 0, **fields: int) -> CrawlStats:
    return CrawlStats(processed=processed, elapsed=elapsed, **fields)


class TestProgressTracker:
    def test_counts_every_requested_page_as_done(self):
        stats = CrawlStats(
            processed=5, failed=2, skipped=1, blocked=4, unreachable=3, queued=7, in_progress=6, active_requests=2
        )
        progress = ProgressTracker(max_pages=40).update(stats)

        assert (progress.done, progress.total, progress.failed, progress.percent) == (8, 40, 2, 20.0)
        assert (progress.active, progress.in_flight, progress.queued) == (6, 2, 7)
        assert not progress.finished

    def test_first_snapshot_uses_the_average_speed(self):
        progress = ProgressTracker(max_pages=100).update(snapshot(5.0, 10))

        assert progress.pages_per_second == 2.0
        assert progress.eta == 45.0

    def test_no_time_left_estimate_before_the_first_page(self):
        tracker = ProgressTracker(max_pages=100)

        assert tracker.update(snapshot(0.0)).eta is None
        progress = tracker.update(snapshot(1.0))
        assert (progress.pages_per_second, progress.eta, progress.percent) == (0.0, None, 0.0)

    def test_speed_is_that_of_the_last_seconds(self):
        tracker = ProgressTracker(max_pages=100, window=10.0)
        for second in range(11):  # a page per second
            tracker.update(snapshot(float(second), second))
        for second in range(11, 21):  # then four
            progress = tracker.update(snapshot(float(second), 10 + 4 * (second - 10)))

        assert progress.done == 50
        assert progress.pages_per_second == 4.0  # the average is 2.5
        assert progress.eta == 12.5

    def test_no_time_left_estimate_while_the_crawl_stands_still(self):
        tracker = ProgressTracker(max_pages=100, window=5.0)
        tracker.update(snapshot(1.0, 10))
        progress = tracker.update(snapshot(8.0, 10))  # waiting for a rate limit or a retry

        assert (progress.pages_per_second, progress.eta) == (0.0, None)

    def test_finished_crawl_has_no_time_left(self):
        tracker = ProgressTracker(max_pages=100)
        tracker.update(snapshot(1.0, 10))
        progress = tracker.update(snapshot(2.0, 100), finished=True)

        assert (progress.percent, progress.eta, progress.finished) == (100.0, 0.0, True)

    def test_site_smaller_than_the_limit_ends_below_100_percent(self):
        tracker = ProgressTracker(max_pages=100)
        tracker.update(snapshot(1.0, 10))
        progress = tracker.update(snapshot(2.0, 12), finished=True)

        assert (progress.done, progress.percent, progress.eta, progress.finished) == (12, 12.0, 0.0, True)

    def test_percent_never_exceeds_100(self):
        progress = ProgressTracker(max_pages=2).update(snapshot(1.0, 3))
        assert (progress.percent, progress.eta) == (100.0, 0.0)

    def test_next_crawl_starts_the_speed_anew(self):
        tracker = ProgressTracker(max_pages=100)
        tracker.update(snapshot(9.0, 90))
        progress = tracker.update(snapshot(1.0, 2))

        assert progress.pages_per_second == 2.0

    @pytest.mark.parametrize(("options", "message"), [({"max_pages": 0}, "max_pages"), ({"window": 0}, "window")])
    def test_rejects_invalid_options(self, options, message):
        with pytest.raises(ValueError, match=message):
            ProgressTracker(**{"max_pages": 10} | options)


@pytest.mark.parametrize(
    ("seconds", "shown"),
    [
        (0, "0s"),
        (59.4, "59s"),
        (59.6, "1m 00s"),
        (125, "2m 05s"),
        (3599, "59m 59s"),
        (3725, "1h 02m"),
        (90000, "25h 00m"),
    ],
)
def test_format_duration(seconds, shown):
    assert format_duration(seconds) == shown


def make_progress(**fields: object) -> Progress:
    defaults = {
        "done": 9,
        "total": 30,
        "failed": 1,
        "percent": 30.0,
        "pages_per_second": 1.55,
        "eta": 13.5,
        "active": 6,
        "in_flight": 2,
        "queued": 88,
        "elapsed": 7.0,
    }
    return Progress(**defaults | fields)


class TestFormatProgress:
    def test_running_crawl(self):
        assert format_progress(make_progress()) == (
            "[######--------------]  30% | 9/30 pages, 1 failed | 1.6 pages/s | ETA 14s | "
            "active 6 (2 in flight) | queued 88 | 7s"
        )

    def test_unknown_time_left(self):
        assert "| ETA -- |" in format_progress(make_progress(pages_per_second=0.0, eta=None))

    def test_finished_crawl(self):
        line = format_progress(make_progress(done=30, percent=100.0, eta=0.0, finished=True))
        assert line.startswith("[####################] 100% | 30/30 pages")
        assert "| done |" in line
        assert "ETA" not in line

    def test_bar_is_empty_at_the_start(self):
        assert format_progress(make_progress(done=0, percent=0.0)).startswith(
            "[--------------------]   0% | 0/30 pages"
        )

    def test_bar_is_full_only_at_100_percent(self):
        assert format_progress(make_progress(percent=99.9)).startswith("[###################-]  99%")


class FakeCrawler:
    """Gives the next snapshot on every call, then repeats the last one."""

    def __init__(self, *snapshots: CrawlStats) -> None:
        self._snapshots = list(snapshots)

    def crawl_stats(self) -> CrawlStats:
        return self._snapshots.pop(0) if len(self._snapshots) > 1 else self._snapshots[0]


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


class TestShowProgress:
    async def test_prints_a_line_per_update_and_the_final_state(self):
        crawler = FakeCrawler(snapshot(0.1, 1), snapshot(0.2, 2), snapshot(0.3, 4))
        release = asyncio.Event()
        task = asyncio.create_task(release.wait())
        stream = io.StringIO()

        shown = asyncio.create_task(show_progress(crawler, task, 4, interval=0.01, stream=stream))
        while stream.getvalue().count("\n") < 2:
            await asyncio.sleep(0.01)
        release.set()
        await shown

        lines = stream.getvalue().splitlines()
        assert "1/4 pages" in lines[0]
        assert "ETA" in lines[0]
        assert lines[-1].startswith("[####################] 100% | 4/4 pages")
        assert "| done |" in lines[-1]
        assert "\033" not in stream.getvalue()

    async def test_redraws_the_line_in_a_terminal(self):
        task = asyncio.create_task(asyncio.sleep(0))
        stream = Terminal()

        await show_progress(FakeCrawler(snapshot(1.0, 2)), task, 4, interval=0.01, stream=stream)

        assert stream.getvalue().startswith("\r\033[K[##########----------]  50% | 2/4 pages")
        assert stream.getvalue().endswith("\n")
        assert stream.getvalue().count("\n") == 1

    async def test_leaves_the_error_of_the_crawl_to_the_caller(self):
        async def fail() -> None:
            raise RuntimeError("boom")

        task = asyncio.create_task(fail())
        await show_progress(FakeCrawler(snapshot(1.0)), task, 4, interval=0.01, stream=io.StringIO())

        with pytest.raises(RuntimeError, match="boom"):
            await task

    async def test_writes_to_stderr_by_default(self, capsys):
        task = asyncio.create_task(asyncio.sleep(0))
        await show_progress(FakeCrawler(snapshot(1.0, 2)), task, 4, interval=0.01)

        captured = capsys.readouterr()
        assert "2/4 pages" in captured.err
        assert captured.out == ""
