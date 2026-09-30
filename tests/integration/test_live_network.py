"""Smoke tests against the real internet. Run with: pytest -m network"""

import pytest

from crawler import AsyncCrawler, NetworkError, RetryStrategy, RobotsUnreachableError

pytestmark = pytest.mark.network


async def test_fetch_real_https_page():
    async with AsyncCrawler() as crawler:
        html = await crawler.fetch_url("https://example.com")
    assert "Example Domain" in html


async def test_nonexistent_domain():
    # Its robots.txt cannot be fetched either, so the site is not touched.
    async with AsyncCrawler(retry_strategy=RetryStrategy(max_retries=0)) as crawler:
        with pytest.raises(RobotsUnreachableError, match="robots.txt is unreachable \\(NetworkError"):
            await crawler.fetch_url("https://nonexistent-domain.invalid")
    async with AsyncCrawler(retry_strategy=RetryStrategy(max_retries=0), respect_robots=False) as crawler:
        with pytest.raises(NetworkError):
            await crawler.fetch_url("https://nonexistent-domain.invalid")


async def test_parse_wikipedia():
    async with AsyncCrawler() as crawler:
        page = await crawler.fetch_and_parse("https://en.wikipedia.org/wiki/Main_Page")
    assert "Wikipedia" in page["title"]
    assert page["metadata"]["language"] == "en"
    assert len(page["links"]) > 100
    assert all(link.startswith(("http://", "https://")) for link in page["links"])
    assert page["images"]
    assert page["errors"] == []


async def test_parse_scraping_sandbox():
    async with AsyncCrawler() as crawler:
        page = await crawler.fetch_and_parse("https://apilearn.tukas.dev/")
    assert page["title"]
    assert any(link.startswith("https://apilearn.tukas.dev/") for link in page["links"])
    assert page["headings"]
