"""Helpers shared by unit and integration tests."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime

from crawler import CircuitBreaker, DataStorage, PageRecord, RetryStrategy

BOT = "TestBot/1.0 (+https://example.com/bot)"

# Crawler options for tests that check something other than politeness:
# without the rate limit, robots.txt, retries and the circuit breaker they
# run fast and see only the requests they make themselves.
UNTHROTTLED = {
    "requests_per_second": None,
    "respect_robots": False,
    "retry_strategy": RetryStrategy(max_retries=0),
    "circuit_breaker": CircuitBreaker(failure_threshold=None),
}


class FakeClock:
    """A clock for the `clock` option that moves only when a test sets `now`."""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def make_record(url: str = "https://site/page", **fields: object) -> PageRecord:
    """A page record for storage tests; `fields` replace the defaults."""
    record: PageRecord = {
        "url": url,
        "title": "Page",
        "text": "Some text",
        "links": ["https://site/a", "https://site/b"],
        "metadata": {"description": "A page", "keywords": ["one", "two"], "language": "en", "depth": 1},
        "crawled_at": datetime(2025, 3, 14, 15, 9, 26, 535897, tzinfo=UTC),
        "status_code": 200,
        "content_type": "text/html",
    }
    return record | fields


class MemoryStorage(DataStorage):
    """Keeps the batches in a list; a write fails with the next of `failures`, if any."""

    def __init__(self, batch_size: int = 100, *, failures: Sequence[Exception] = (), **options) -> None:
        options.setdefault("retry_strategy", RetryStrategy(retry_on=(OSError,), base_delay=0.001, max_delay=0.001))
        super().__init__(batch_size, **options)
        self.batches: list[list[PageRecord]] = []
        self.failures = list(failures)
        self.attempts = 0
        self.released = 0
        self._writing = False

    @property
    def urls(self) -> list[list[str]]:
        return [[record["url"] for record in batch] for batch in self.batches]

    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        assert not self._writing, "two writes at once"
        self._writing = True
        try:
            self.attempts += 1
            await asyncio.sleep(0)
            if self.failures:
                raise self.failures.pop(0)
            self.batches.append(list(records))
        finally:
            self._writing = False

    async def _read(self) -> AsyncIterator[PageRecord]:
        for batch in self.batches:
            for record in batch:
                yield record

    async def _close(self) -> None:
        self.released += 1
