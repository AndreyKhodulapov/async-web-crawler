"""Live progress of a crawl: share of the page limit, current speed, time left, active tasks."""

import asyncio
import sys
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, TextIO

from crawler.models import CrawlStats

if TYPE_CHECKING:
    from crawler.client import AsyncCrawler


@dataclass(frozen=True, slots=True)
class Progress:
    """How far a crawl has got at one moment.

    `done` counts the pages requested and finished: processed, failed
    (`failed` of them) and skipped, but not the pages skipped without a
    request over `max_pages_per_host`; `total` is the page limit of the crawl.
    Pages that failed without a request, refused by an open circuit breaker,
    are not held against the limit by the crawl, so `done` stops at `total`
    while `failed` and the speed count them all. `percent` is `done` of `total`. `pages_per_second` is the speed over the
    last seconds, `eta` the seconds left at that speed until `total` pages
    are done: `None` while the speed is 0, and 0 once the crawl has
    finished. `active` counts the pages taken by workers, `in_flight` the
    HTTP requests being made, `queued` the pages waiting, but no more than
    the limit leaves to request: the rest of the queue will not be fetched.
    """

    done: int
    total: int
    failed: int
    percent: float
    pages_per_second: float
    eta: float | None
    active: int
    in_flight: int
    queued: int
    elapsed: float
    finished: bool = False


class ProgressTracker:
    """Turns snapshots of a crawl into `Progress`: adds the percent, the current speed and the time left.

    Usage::

        tracker = ProgressTracker(max_pages=100)
        progress = tracker.update(crawler.crawl_stats())
        print(format_progress(progress))

    The speed is measured over the last `window` seconds of the crawl, so
    it follows a crawl that slows down or speeds up; the time left is what
    the remaining pages take at that speed. Both are estimates: a site with
    fewer pages than `max_pages` ends the crawl sooner, at less than 100%.
    """

    def __init__(self, max_pages: int, *, window: float = 10.0) -> None:
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1, got {max_pages}")
        if window <= 0:
            raise ValueError(f"window must be positive, got {window}")
        self.max_pages = max_pages
        self.window = window
        # (seconds since the crawl started, pages finished) of the latest updates.
        self._samples: deque[tuple[float, int]] = deque()

    def update(self, stats: CrawlStats, *, finished: bool = False) -> Progress:
        """Progress as of `stats`, the latest snapshot of the crawl; `finished` marks the last one."""
        # Pages over max_pages_per_host are not requested and leave max_pages to the others.
        finished_pages = stats.processed + stats.failed + stats.skipped - stats.over_host_limit
        done = min(finished_pages, self.max_pages)
        if self._samples and stats.elapsed < self._samples[-1][0]:
            self._samples.clear()  # another crawl has started
        self._samples.append((stats.elapsed, finished_pages))
        # The oldest sample kept is the latest one at least `window` old.
        while len(self._samples) > 1 and self._samples[1][0] <= stats.elapsed - self.window:
            self._samples.popleft()

        since, finished_before = self._samples[0]
        if stats.elapsed > since:
            speed = (finished_pages - finished_before) / (stats.elapsed - since)
        else:
            # The first snapshot: the average since the crawl started.
            speed = stats.pages_per_second
        if finished:
            eta = 0.0
        elif speed > 0:
            eta = (self.max_pages - done) / speed
        else:
            eta = None
        return Progress(
            done=done,
            total=self.max_pages,
            failed=stats.failed,
            percent=100.0 * done / self.max_pages,
            pages_per_second=speed,
            eta=eta,
            active=stats.in_progress,
            in_flight=stats.active_requests,
            queued=min(stats.queued, max(self.max_pages - done - stats.in_progress, 0)),
            elapsed=stats.elapsed,
            finished=finished,
        )


def format_duration(seconds: float) -> str:
    """Seconds as `45s`, `2m 05s` or `1h 02m`."""
    seconds = round(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60:02d}s"
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m"


def progress_bar(percent: float) -> str:
    """A bar of 20 cells and the percent, as `[####----]  20%`."""
    # Rounded down: the bar is full and the percent is 100 only when every page is done.
    width = 20
    filled = int(width * percent / 100)
    return f"[{'#' * filled}{'-' * (width - filled)}] {int(percent):3d}%"


def print_progress_line(line: str, stream: TextIO, *, live: bool) -> None:
    """Redraw the progress line in place in a terminal (`live`), or print it on a line of its own."""
    if live:
        print(f"\r\033[K{line}", end="", file=stream, flush=True)
    else:
        print(line, file=stream, flush=True)


def format_progress(progress: Progress) -> str:
    """One line: a bar, the percent, pages done, speed, time left, active tasks, the queue and the time passed."""
    if progress.finished:
        left = "done"
    elif progress.eta is None:
        left = "ETA --"
    else:
        left = f"ETA {format_duration(progress.eta)}"
    return (
        f"{progress_bar(progress.percent)} | {progress.done}/{progress.total} pages, {progress.failed} failed | "
        f"{progress.pages_per_second:.1f} pages/s | {left} | "
        f"active {progress.active} ({progress.in_flight} in flight) | queued {progress.queued} | "
        f"{format_duration(progress.elapsed)}"
    )


async def show_progress(
    crawler: "AsyncCrawler",
    crawl_task: asyncio.Task[object],
    max_pages: int,
    *,
    interval: float = 1.0,
    off_tty_interval: float = 30.0,
    stream: TextIO | None = None,
) -> None:
    """Print the progress of a crawl every `interval` seconds until `crawl_task` ends.

    `max_pages` is the limit given to `crawl()`. The line goes to `stream`,
    stderr by default. In a terminal it is redrawn in place; in a file or a
    pipe, such as `docker logs`, every line printed stays, so a line goes
    there every `off_tty_interval` seconds, while the speed is still
    measured every `interval`. The last line shows the crawl as it ended.
    The result or the error of the task is left to the caller: await the
    task afterwards.
    """
    stream = sys.stderr if stream is None else stream
    live = stream.isatty()
    tracker = ProgressTracker(max_pages)
    loop = asyncio.get_running_loop()
    printed_at = None
    while True:
        await asyncio.wait({crawl_task}, timeout=interval)
        finished = crawl_task.done()
        progress = tracker.update(crawler.crawl_stats(), finished=finished)
        if live or finished or printed_at is None or loop.time() - printed_at >= off_tty_interval:
            print_progress_line(format_progress(progress), stream, live=live)
            printed_at = loop.time()
        if finished:
            break
    if live:
        print(file=stream)
