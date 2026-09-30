"""Collects the error statistics of a crawler."""

from collections import Counter

from crawler.exceptions import ERROR_KINDS, FetchError, PermanentError, error_kind
from crawler.models import ErrorStats


class ErrorTracker:
    """Counts failed attempts, retries and their outcomes; `get_stats` takes a snapshot."""

    def __init__(self) -> None:
        self._by_kind: Counter[str] = Counter({kind.__name__: 0 for kind in ERROR_KINDS} | {"other": 0})
        self._by_class: Counter[str] = Counter()
        self._retries = 0
        self._retry_time = 0.0
        self._successful_retries = 0
        self._permanent: dict[str, str] = {}

    def record_error(self, error: FetchError) -> None:
        """A failed attempt, whether it is retried or not."""
        self._by_kind[error_kind(error)] += 1
        self._by_class[type(error).__name__] += 1

    def record_retry(self, seconds: float) -> None:
        """A retry that took `seconds` from the failure before it to its own end."""
        self._retries += 1
        self._retry_time += seconds

    def record_outcome(self, url: str, error: FetchError | None, *, retried: bool) -> None:
        """How the attempts for `url` ended: `error` is None on success."""
        if error is None:
            self._permanent.pop(url, None)
            if retried:
                self._successful_retries += 1
        elif isinstance(error, PermanentError):
            self._permanent[url] = f"{type(error).__name__}: {error.message}"

    def get_stats(self) -> ErrorStats:
        return ErrorStats(
            by_kind=dict(self._by_kind),
            by_class=dict(self._by_class),
            retries=self._retries,
            successful_retries=self._successful_retries,
            avg_retry_time=self._retry_time / self._retries if self._retries else 0.0,
            permanent_errors=dict(self._permanent),
        )
