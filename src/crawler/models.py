"""Data models shared across the crawler."""

from dataclasses import dataclass

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
