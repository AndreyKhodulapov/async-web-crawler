"""Smoke tests against the real internet. Run with: pytest -m network"""

import pytest

from crawler import AsyncCrawler, NetworkError

pytestmark = pytest.mark.network


async def test_fetch_real_https_page():
    async with AsyncCrawler() as crawler:
        html = await crawler.fetch_url("https://example.com")
    assert "Example Domain" in html


async def test_nonexistent_domain():
    async with AsyncCrawler() as crawler:
        with pytest.raises(NetworkError):
            await crawler.fetch_url("https://nonexistent-domain.invalid")
