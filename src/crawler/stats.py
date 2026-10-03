"""Collects the statistics of a crawl: pages, status codes, domains, speed and running time."""

import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from crawler.report import render_html, render_json
from crawler.urls import get_host


class CrawlerStats:
    """Counts the pages of a crawl by outcome, status code and domain; `get_stats` takes a snapshot.

    Usage::

        stats = CrawlerStats()
        stats.start()
        stats.record_page("https://example.com/", status=200, elapsed=0.3)
        stats.record_page("https://example.com/gone", status=404, error="PermanentHTTPError")
        stats.finish()
        stats.get_stats()["total_pages"]              # 2
        stats.export_to_html_report("report.html")    # or export_to_json("stats.json")

    A page is counted once, however many attempts it took. It is
    successful, failed (`error` names the class of its error) or skipped:
    fetched fine but left out of the results, or not requested over the
    page limit of its host. Other pages that were never requested, such as
    those robots.txt disallows, are not recorded here.

    Not to be confused with `CrawlStats`, the snapshot of the progress of a
    crawl (the queue, requests in flight, the request rate) that
    `AsyncCrawler.crawl_stats()` returns.
    """

    def __init__(self, *, top_domains: int = 10, clock: Callable[[], float] = time.monotonic) -> None:
        if top_domains < 1:
            raise ValueError(f"top_domains must be >= 1, got {top_domains}")
        self.top_domains = top_domains
        self._clock = clock
        self._reset()

    def _reset(self) -> None:
        self._successful = 0
        self._failed = 0
        self._skipped = 0
        self._status_codes: Counter[int] = Counter()
        self._errors: Counter[str] = Counter()
        self._domains: Counter[str] = Counter()
        self._timed = 0  # pages with a response time
        self._response_time = 0.0
        self._started: float | None = None
        self._finished: float | None = None
        self._started_at: datetime | None = None
        self._finished_at: datetime | None = None

    def start(self) -> None:
        """Forget everything recorded and start the clock."""
        self._reset()
        self._started, self._started_at = self._clock(), datetime.now(UTC)

    def finish(self) -> None:
        """Stop the clock: the running time and the speed no longer change. Does nothing unless started."""
        if self._started is not None and self._finished is None:
            self._finished, self._finished_at = self._clock(), datetime.now(UTC)

    def record_page(
        self,
        url: str,
        *,
        status: int | None = None,
        elapsed: float | None = None,
        error: str | None = None,
        skipped: bool = False,
    ) -> None:
        """Count a page that the crawl is done with.

        `status` is the HTTP status of the response, None if there was
        none; `elapsed` the time its request took, None if it was not sent.
        """
        if error is not None:
            self._failed += 1
            self._errors[error] += 1
        elif skipped:
            self._skipped += 1
        else:
            self._successful += 1
        if status is not None:
            self._status_codes[status] += 1
        if elapsed is not None:
            self._timed += 1
            self._response_time += elapsed
        self._domains[get_host(url) or "unknown"] += 1

    def get_stats(self) -> dict[str, Any]:
        """The statistics so far, or of the whole crawl once it has finished.

        `total_pages` is the sum of `successful`, `failed` and `skipped`.
        `pages_per_second` is the average over `elapsed_seconds`, the time
        since `start`. `avg_response_time` is the average time of a request,
        of its last attempt if it was retried. `status_codes` (code ->
        pages, by code) leaves out the pages that got no response; those are
        among `errors` (error class -> pages, most frequent first).
        `top_domains` (host -> pages, largest first) counts the pages of
        every outcome and lists at most `top_domains` hosts. `started_at`
        and `finished_at` are UTC times in ISO 8601, None until then.
        """
        total = self._successful + self._failed + self._skipped
        elapsed = 0.0 if self._started is None else (self._finished or self._clock()) - self._started
        return {
            "total_pages": total,
            "successful": self._successful,
            "failed": self._failed,
            "skipped": self._skipped,
            "elapsed_seconds": elapsed,
            "pages_per_second": total / elapsed if elapsed > 0 else 0.0,
            "avg_response_time": self._response_time / self._timed if self._timed else 0.0,
            "status_codes": dict(sorted(self._status_codes.items())),
            "errors": _largest_first(self._errors),
            "top_domains": _largest_first(self._domains, self.top_domains),
            "started_at": None if self._started_at is None else self._started_at.isoformat(),
            "finished_at": None if self._finished_at is None else self._finished_at.isoformat(),
        }

    def export_to_json(self, filename: str | Path) -> None:
        """Write `get_stats()` to a JSON file (UTF-8), replacing the file if it exists.

        JSON has only string keys, so the status codes are strings there.

        Raises:
            OSError: the file cannot be written, e.g. its directory does not exist.
        """
        Path(filename).write_text(render_json(self.get_stats()), encoding="utf-8")

    def export_to_html_report(self, filename: str | Path, *, title: str = "Crawl report") -> None:
        """Write a report to an HTML file: a summary, charts and tables of `get_stats()`.

        The file needs nothing else to be viewed: the styles and the charts
        are inside it. It replaces the file if it exists.

        Raises:
            OSError: the file cannot be written, e.g. its directory does not exist.
        """
        Path(filename).write_text(render_html(self.get_stats(), title=title), encoding="utf-8")


def _largest_first(counts: Counter[str], limit: int | None = None) -> dict[str, int]:
    # Equal counts go by name, so the order does not depend on which page came first.
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit])
