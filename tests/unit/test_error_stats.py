"""Unit tests for ErrorTracker: counting errors, retries and their outcomes."""

import pytest

from crawler import FetchTimeoutError, HTTPStatusError, NetworkError, ParseError, UnexpectedError
from crawler.error_stats import ErrorTracker

URL = "https://site/page"


def test_empty_stats_list_every_kind():
    stats = ErrorTracker().get_stats()
    assert stats.by_kind == {"TransientError": 0, "PermanentError": 0, "NetworkError": 0, "ParseError": 0, "other": 0}
    assert (stats.total, stats.retries, stats.successful_retries, stats.avg_retry_time) == (0, 0, 0, 0.0)
    assert stats.by_class == stats.permanent_errors == {}


def test_known_scenario():
    tracker = ErrorTracker()
    # /a: 503, timeout, then a page.
    tracker.record_error(HTTPStatusError(URL + "/a", 503, "Service Unavailable"))
    tracker.record_retry(1.0)
    tracker.record_error(FetchTimeoutError(URL + "/a", "read timeout (20.0s)"))
    tracker.record_retry(2.0)
    tracker.record_outcome(URL + "/a", None, retried=True)
    # /b: 404 at once.
    not_found = HTTPStatusError(URL + "/b", 404, "Not Found")
    tracker.record_error(not_found)
    tracker.record_outcome(URL + "/b", not_found, retried=False)
    # /c: a refused connection, retried in vain.
    for _ in range(2):
        tracker.record_error(NetworkError(URL + "/c", "connection refused"))
    tracker.record_retry(3.0)
    tracker.record_outcome(URL + "/c", NetworkError(URL + "/c", "connection refused"), retried=True)
    # /d: a page, then not HTML; and a bug.
    tracker.record_outcome(URL + "/d", None, retried=False)
    tracker.record_error(ParseError(URL + "/d", "unsupported content type: application/pdf"))
    tracker.record_error(UnexpectedError(URL + "/e", "KeyError: 'x'"))

    stats = tracker.get_stats()
    assert stats.by_kind == {"TransientError": 2, "PermanentError": 1, "NetworkError": 2, "ParseError": 1, "other": 1}
    assert stats.by_class == {
        "TransientHTTPError": 1,
        "FetchTimeoutError": 1,
        "PermanentHTTPError": 1,
        "NetworkError": 2,
        "ParseError": 1,
        "UnexpectedError": 1,
    }
    assert stats.total == 7
    assert (stats.retries, stats.successful_retries) == (3, 1)
    assert stats.avg_retry_time == pytest.approx(2.0)
    assert stats.permanent_errors == {URL + "/b": "PermanentHTTPError: HTTP 404 Not Found"}


def test_success_clears_an_earlier_permanent_error():
    tracker = ErrorTracker()
    tracker.record_outcome(URL, HTTPStatusError(URL, 403, "Forbidden"), retried=False)
    tracker.record_outcome(URL, None, retried=False)
    assert tracker.get_stats().permanent_errors == {}


def test_snapshot_does_not_change_later():
    tracker = ErrorTracker()
    stats = tracker.get_stats()
    tracker.record_error(HTTPStatusError(URL, 404, "Not Found"))
    tracker.record_outcome(URL, HTTPStatusError(URL, 404, "Not Found"), retried=False)
    assert stats.total == 0
    assert stats.permanent_errors == {}
