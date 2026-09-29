"""Unit tests for UrlFilter."""

import pytest

from crawler import UrlFilter


def test_no_rules_allow_everything():
    assert UrlFilter().allows("https://anything.example/page")


def test_allowed_hosts():
    url_filter = UrlFilter(allowed_hosts={"example.com"})
    assert url_filter.allows("https://example.com/page")
    assert url_filter.allows("http://EXAMPLE.com:8080/page")  # ports are ignored
    assert not url_filter.allows("https://www.example.com/page")
    assert not url_filter.allows("https://other.org/")
    assert not url_filter.allows("not a url")


def test_allow_host_of_extends_the_set():
    url_filter = UrlFilter(allowed_hosts={"example.com"})
    url_filter.allow_host_of("https://WWW.example.com:8443/landing")
    url_filter.allow_host_of("not a url")
    assert url_filter.allowed_hosts == {"example.com", "www.example.com"}


def test_allow_host_of_keeps_hosts_unrestricted():
    url_filter = UrlFilter()
    url_filter.allow_host_of("https://example.com/")
    assert url_filter.allowed_hosts is None
    assert url_filter.allows("https://other.org/")


def test_include_patterns():
    url_filter = UrlFilter(include_patterns=[r"/blog/", r"/news/"])
    assert url_filter.allows("https://site/blog/post")
    assert url_filter.allows("https://site/news/today")
    assert not url_filter.allows("https://site/shop/item")


def test_exclude_patterns():
    url_filter = UrlFilter(exclude_patterns=[r"\.pdf$", r"[?&]page="])
    assert not url_filter.allows("https://site/manual.pdf")
    assert not url_filter.allows("https://site/list?sort=asc&page=2")
    assert url_filter.allows("https://site/manual.pdf.html")


def test_patterns_match_both_encoded_and_decoded_url():
    # Links are normalized to the percent-encoded form.
    url = "https://site/caf%C3%A9/a%20b"
    assert UrlFilter(include_patterns=[r"/café/"]).allows(url)
    assert UrlFilter(include_patterns=[r"/caf%C3%A9/"]).allows(url)
    assert not UrlFilter(exclude_patterns=[r"a b$"]).allows(url)


def test_exclude_wins_over_include():
    url_filter = UrlFilter(include_patterns=[r"/blog/"], exclude_patterns=[r"/drafts/"])
    assert url_filter.allows("https://site/blog/post")
    assert not url_filter.allows("https://site/blog/drafts/post")


def test_all_rules_combined():
    url_filter = UrlFilter(allowed_hosts={"site"}, include_patterns=[r"/blog/"], exclude_patterns=[r"\?"])
    assert url_filter.allows("https://site/blog/post")
    assert not url_filter.allows("https://other/blog/post")
    assert not url_filter.allows("https://site/blog/post?share=1")


@pytest.mark.parametrize("field", ["include_patterns", "exclude_patterns"])
def test_single_string_instead_of_a_list_is_rejected(field):
    with pytest.raises(TypeError, match="got a string"):
        UrlFilter(**{field: "pdf"})


@pytest.mark.parametrize("field", ["include_patterns", "exclude_patterns"])
def test_invalid_pattern_is_rejected_early(field):
    with pytest.raises(ValueError, match=r"invalid pattern '\(unclosed'"):
        UrlFilter(**{field: ["(unclosed"]})
