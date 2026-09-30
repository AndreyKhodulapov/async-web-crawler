"""Unit tests for the kinds of crawler errors: transient, permanent, network, parse."""

import pytest

from crawler import (
    CertificateError,
    CrawlerClosedError,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    ParseError,
    PermanentError,
    PermanentHTTPError,
    RobotsDisallowedError,
    RobotsUnreachableError,
    TooManyRedirectsError,
    TransientError,
    TransientHTTPError,
    UnexpectedError,
    error_kind,
)

URL = "https://site/page"
KINDS = (TransientError, PermanentError, NetworkError, ParseError)


def kind_of(error: Exception) -> type[Exception] | None:
    kinds = [kind for kind in KINDS if isinstance(error, kind)]
    assert len(kinds) <= 1, kinds
    return kinds[0] if kinds else None


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524])
def test_temporary_http_statuses_are_transient(status):
    error = HTTPStatusError(URL, status, "Error", retry_after=5.0)
    assert type(error) is TransientHTTPError
    assert kind_of(error) is TransientError
    assert (error.status, error.retry_after, error.message) == (status, 5.0, f"HTTP {status} Error")


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 501, 505, 507, 525])
def test_other_http_statuses_are_permanent(status):
    error = HTTPStatusError(URL, status, "Error")
    assert type(error) is PermanentHTTPError
    assert kind_of(error) is PermanentError


def test_http_error_subclass_keeps_its_own_kind():
    assert type(PermanentHTTPError(URL, 503, "Error")) is PermanentHTTPError


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (FetchTimeoutError(URL, "request timed out"), TransientError),
        (NetworkError(URL, "connection refused"), NetworkError),
        (TooManyRedirectsError(URL, "too many redirects (10)"), PermanentError),
        (CertificateError(URL, "certificate has expired"), PermanentError),
        (InvalidURLError(URL, "bad port"), PermanentError),
        (RobotsDisallowedError(URL, "disallowed by robots.txt"), PermanentError),
        (ParseError(URL, "empty document"), ParseError),
        (CrawlerClosedError(URL, "crawler is closed"), None),
        (RobotsUnreachableError(URL, "robots.txt is unreachable (HTTP 503)"), None),
        (UnexpectedError(URL, "KeyError: 'x'"), None),
    ],
)
def test_errors_have_one_kind_at_most(error, kind):
    assert kind_of(error) is kind


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (HTTPStatusError(URL, 503, "Service Unavailable"), "TransientError"),
        (FetchTimeoutError(URL, "read timeout (20.0s)"), "TransientError"),
        (HTTPStatusError(URL, 404, "Not Found"), "PermanentError"),
        (CertificateError(URL, "certificate has expired"), "PermanentError"),
        (NetworkError(URL, "connection refused"), "NetworkError"),
        (ParseError(URL, "empty document"), "ParseError"),
        (UnexpectedError(URL, "KeyError: 'x'"), "other"),
        (CrawlerClosedError(URL, "crawler is closed"), "other"),
        (KeyError("x"), "other"),
    ],
)
def test_error_kind(error, kind):
    assert error_kind(error) == kind
