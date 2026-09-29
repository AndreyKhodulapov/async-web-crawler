"""Data models shared across the crawler."""

from dataclasses import dataclass
from typing import Literal, TypedDict

from crawler.exceptions import FetchError, HTTPStatusError


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Outcome of fetching a single URL, successful or not.

    Exactly one of ``content`` and ``error`` is set. ``final_url`` is the
    address after redirects; ``content_type`` is the media type without
    parameters, or None if the server did not send a Content-Type header.
    """

    url: str
    elapsed: float
    status: int | None = None
    content: str | None = None
    size: int = 0
    error: FetchError | None = None
    final_url: str | None = None
    content_type: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None

    @classmethod
    def failure(cls, url: str, error: FetchError, elapsed: float) -> "FetchResult":
        # Keep the status code for HTTP errors: it is useful for reporting.
        status = error.status if isinstance(error, HTTPStatusError) else None
        return cls(url=url, elapsed=elapsed, status=status, error=error)


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
