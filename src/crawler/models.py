"""Data models shared across the crawler."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, TypedDict

from crawler.exceptions import FetchError, HTTPStatusError


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Outcome of fetching a single URL, successful or not.

    Exactly one of ``content`` and ``error`` is set. ``final_url`` is the
    address after redirects, and ``redirected`` tells whether there were
    any: comparing the two URLs is not enough, as the HTTP client spells
    ``final_url`` in its own way. ``content_type`` is the media type without
    parameters, or None if the server did not send a Content-Type header.

    ``body`` is set only for a download that asked for the bytes as they
    were sent, such as a sitemap, which may be gzipped; ``content`` is
    empty then.
    """

    url: str
    elapsed: float
    status: int | None = None
    content: str | None = None
    size: int = 0
    error: FetchError | None = None
    final_url: str | None = None
    content_type: str | None = None
    redirected: bool = False
    body: bytes | None = None

    @property
    def ok(self) -> bool:
        return self.error is None

    @classmethod
    def failure(cls, url: str, error: FetchError, elapsed: float) -> "FetchResult":
        # Keep the status code for HTTP errors: it is useful for reporting.
        status = error.status if isinstance(error, HTTPStatusError) else None
        return cls(url=url, elapsed=elapsed, status=status, error=error)


@dataclass(frozen=True, slots=True)
class CrawlStats:
    """Progress of a crawl at one moment.

    `skipped` counts pages fetched but left out because they redirected
    outside the crawl scope; `blocked` counts pages robots.txt did not allow
    to fetch, `unreachable` pages left unfetched because robots.txt of their
    site could not be read. `in_progress` counts pages taken by workers:
    waiting for a free slot, being fetched or parsed. `active_requests` counts only HTTP requests
    holding a slot, so it never exceeds the concurrency limits. `elapsed`
    runs from the start of the crawl to now, or to its end once it has finished.

    `requests` counts HTTP requests sent, robots.txt and retries included;
    `retries` counts the retries alone. `current_rps` is the request rate
    over the last few seconds, `avg_delay` the average gap between two
    requests to the same host, `avg_wait` the average time a request waited
    for the rate limit.

    `saved` counts the pages written to the storage of the crawler, and
    `save_failed` those that could not be written. A page still in the
    buffer of the storage is in neither while the crawl runs; once the
    crawl has finished, it counts as not saved until it is written. Both
    are 0 without a storage.
    """

    processed: int = 0
    failed: int = 0
    skipped: int = 0
    blocked: int = 0
    unreachable: int = 0
    queued: int = 0
    in_progress: int = 0
    active_requests: int = 0
    elapsed: float = 0.0
    requests: int = 0
    retries: int = 0
    current_rps: float = 0.0
    avg_delay: float = 0.0
    avg_wait: float = 0.0
    saved: int = 0
    save_failed: int = 0

    @property
    def pages_per_second(self) -> float:
        """Fetched pages per second: processed, failed and skipped."""
        finished = self.processed + self.failed + self.skipped
        return finished / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def requests_per_second(self) -> float:
        """Average request rate over the whole crawl."""
        return self.requests / self.elapsed if self.elapsed > 0 else 0.0


@dataclass(frozen=True, slots=True)
class ErrorStats:
    """Errors of page requests since the stats were last reset.

    `by_kind` and `by_class` count every failed attempt, including those a
    retry made good, by kind ("TransientError", "PermanentError",
    "NetworkError", "ParseError" or "other") and by exception class.
    `retries` counts retries made, `successful_retries` the pages they
    recovered, and `avg_retry_time` is the average time from a failed
    attempt to the end of the next one: the pause plus the request.
    `permanent_errors` maps the URLs that failed with a `PermanentError`
    to the error.
    """

    by_kind: Mapping[str, int] = field(default_factory=dict)
    by_class: Mapping[str, int] = field(default_factory=dict)
    retries: int = 0
    successful_retries: int = 0
    avg_retry_time: float = 0.0
    permanent_errors: Mapping[str, str] = field(default_factory=dict)

    @property
    def total(self) -> int:
        """All failed attempts."""
        return sum(self.by_kind.values())


@dataclass(frozen=True, slots=True)
class DomainRate:
    """Requests to one domain: how many, the interval enforced now, the average gap seen."""

    requests: int
    interval: float
    avg_gap: float | None


@dataclass(frozen=True, slots=True)
class RateStats:
    """Request rate since the limiter was created or its stats were reset.

    `current_rps` counts requests over the last few seconds. `avg_delay` is
    the average gap between two consecutive requests to the same domain,
    `avg_wait` the average time a request waited for its turn.
    """

    requests: int = 0
    current_rps: float = 0.0
    avg_delay: float = 0.0
    avg_wait: float = 0.0
    domains: Mapping[str, DomainRate] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CircuitStats:
    """The circuit breaker of one host.

    `state` is "closed", "open" or "half-open". `requests` and `failures`
    count the outcomes in the sliding window that decide when to open.
    `times_opened` and `rejected` (requests refused without being sent)
    count since the stats were last reset.
    """

    state: str
    requests: int = 0
    failures: int = 0
    times_opened: int = 0
    rejected: int = 0


class Metadata(TypedDict):
    title: str | None
    description: str | None
    keywords: list[str]
    language: str | None
    canonical: str | None


class Image(TypedDict):
    src: str
    alt: str


class Heading(TypedDict):
    level: int
    text: str


class Table(TypedDict):
    caption: str | None
    headers: list[str]
    rows: list[list[str]]


class ItemList(TypedDict):
    type: Literal["ul", "ol"]
    items: list[str]


class ParsedPage(TypedDict):
    """Structured data extracted from one page.

    `url` is the requested URL, `final_url` the one after redirects;
    relative links are resolved against the latter. `errors` lists problems
    met while parsing; the fields they affected keep their empty defaults.
    """

    url: str
    final_url: str
    title: str | None
    text: str
    links: list[str]
    metadata: Metadata
    headings: list[Heading]
    images: list[Image]
    tables: list[Table]
    lists: list[ItemList]
    errors: list[str]


class PageRecord(TypedDict):
    """A crawled page as the storages keep it.

    `url` is the requested URL. `title` and `content_type` are empty strings
    when the page has no title or the server sent no Content-Type header.
    `crawled_at` is an aware datetime. `metadata` must be serializable to
    JSON.
    """

    url: str
    title: str
    text: str
    links: list[str]
    metadata: dict[str, object]
    crawled_at: datetime
    status_code: int
    content_type: str
