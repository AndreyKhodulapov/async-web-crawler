"""Unit tests for URL validation, normalization and resolution."""

import pytest

from crawler.urls import is_same_host, is_valid_http_url, normalize_url, resolve_url

BASE = "https://example.com/docs/guide/intro.html?lang=en"


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("chapter2.html", "https://example.com/docs/guide/chapter2.html"),
        ("./chapter2.html", "https://example.com/docs/guide/chapter2.html"),
        ("../api/", "https://example.com/docs/api/"),
        ("../../../../top", "https://example.com/top"),
        ("/about", "https://example.com/about"),
        ("//cdn.example.org/lib.js", "https://cdn.example.org/lib.js"),
        ("?lang=de", "https://example.com/docs/guide/intro.html?lang=de"),
        ("http://other.org/page", "http://other.org/page"),
        ("  /padded  ", "https://example.com/padded"),
        ("/page#section", "https://example.com/page"),
    ],
)
def test_resolve_relative_links(href, expected):
    assert resolve_url(href, BASE) == expected


@pytest.mark.parametrize(
    "href",
    [
        "",
        "   ",
        "#top",
        "mailto:team@example.com",
        "tel:+1000",
        "javascript:void(0)",
        "data:image/png;base64,AAAA",
        "ftp://files.example.com/a.zip",
        "http://[::1/unclosed",
        "http://host:99999/",
        "http://",
    ],
)
def test_resolve_skips_non_crawlable_links(href):
    assert resolve_url(href, BASE) is None


def test_resolve_with_invalid_base_keeps_only_absolute_links():
    assert resolve_url("/about", "not a url") is None
    assert resolve_url("https://example.com/", "not a url") == "https://example.com/"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("HTTPS://Example.COM/Path", "https://example.com/Path"),
        ("https://example.com", "https://example.com/"),
        ("https://example.com:443/a", "https://example.com/a"),
        ("http://example.com:80/a", "http://example.com/a"),
        ("http://example.com:8080/a", "http://example.com:8080/a"),
        ("https://example.com/a?x=1#frag", "https://example.com/a?x=1"),
        ("http://User@Example.com/", "http://User@example.com/"),
        ("http://[::1]:80/", "http://[::1]/"),
        ("https://BÜCHER.de/", "https://xn--bcher-kva.de/"),
        ("https://xn--bcher-kva.de/", "https://xn--bcher-kva.de/"),
        ("http://example.com:/a", "http://example.com/a"),
        ("https://Example.com./a", "https://example.com/a"),
        ("http://./", None),
        ("http://user:pw@Example.com:8080/", "http://user:pw@example.com:8080/"),
        ("http://" + "a" * 70 + "é.com/", None),
    ],
)
def test_normalize(url, expected):
    assert normalize_url(url) == expected


@pytest.mark.parametrize(
    ("url", "valid"),
    [
        ("https://example.com", True),
        ("http://127.0.0.1:8080/x", True),
        ("http://bücher.de/", True),
        ("example.com", False),
        ("//example.com", False),
        ("ftp://example.com", False),
        ("http://[::1", False),
        ("http://host:99999/", False),
        ("http://host:abc/", False),
    ],
)
def test_is_valid_http_url(url, valid):
    assert is_valid_http_url(url) is valid


@pytest.mark.parametrize(
    ("url", "other", "same"),
    [
        ("https://example.com/a", "http://example.com:8080/b", True),
        ("https://EXAMPLE.com/", "https://example.com/", True),
        ("https://www.example.com/", "https://example.com/", False),
        ("https://example.com/", "not a url", False),
        ("http://[::1/", "http://[::1/", False),
        # yarl reports the final URL of a response in punycode.
        ("https://bücher.de/katalog", "https://xn--bcher-kva.de/", True),
        ("https://BÜCHER.de/", "https://bücher.de/", True),
        ("https://example.com./about", "https://example.com/", True),
    ],
)
def test_is_same_host(url, other, same):
    assert is_same_host(url, other) is same
