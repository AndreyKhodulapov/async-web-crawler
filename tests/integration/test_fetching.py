"""Integration tests: real HTTP requests against a local aiohttp server."""

import time

import pytest

from crawler import AsyncCrawler, FetchTimeoutError, HTTPStatusError, NetworkError


@pytest.fixture
async def crawler():
    async with AsyncCrawler(max_concurrent=10) as crawler:
        yield crawler


async def test_fetch_valid_url(crawler, server):
    html = await crawler.fetch_url(str(server.make_url("/ok")))
    assert "hello" in html


@pytest.mark.parametrize("code", [404, 500])
async def test_http_error_status(crawler, server, code):
    with pytest.raises(HTTPStatusError) as exc_info:
        await crawler.fetch_url(str(server.make_url(f"/status/{code}")))
    assert exc_info.value.status == code


async def test_unreachable_host(crawler, closed_port_url):
    with pytest.raises(NetworkError):
        await crawler.fetch_url(closed_port_url)


async def test_invalid_url(crawler):
    with pytest.raises(NetworkError):
        await crawler.fetch_url("not a url")


async def test_redirect_loop(crawler, server):
    with pytest.raises(NetworkError, match="too many redirects"):
        await crawler.fetch_url(str(server.make_url("/redirect-loop")))


async def test_read_timeout(server):
    async with AsyncCrawler(read_timeout=0.2) as crawler:
        with pytest.raises(FetchTimeoutError):
            await crawler.fetch_url(str(server.make_url("/delay/2")))


async def test_total_timeout(server):
    async with AsyncCrawler(total_timeout=0.2) as crawler:
        with pytest.raises(FetchTimeoutError):
            await crawler.fetch_url(str(server.make_url("/delay/2")))


async def test_mixed_batch_does_not_crash(crawler, server, closed_port_url):
    ok_url = str(server.make_url("/ok"))
    urls = [ok_url, str(server.make_url("/status/404")), closed_port_url]
    assert list(await crawler.fetch_urls(urls)) == [ok_url]


async def test_concurrent_is_faster_than_sequential(server):
    delay, count = 0.3, 5
    urls = [str(server.make_url(f"/delay/{delay}?n={i}")) for i in range(count)]

    async with AsyncCrawler(max_concurrent=count) as crawler:
        started = time.perf_counter()
        for url in urls:
            await crawler.fetch_url(url)
        sequential = time.perf_counter() - started

        started = time.perf_counter()
        pages = await crawler.fetch_urls(urls)
        concurrent = time.perf_counter() - started

    assert len(pages) == count
    assert sequential >= delay * count
    # Requests overlap, so the batch takes about one delay instead of five.
    # A relative bound keeps the test stable on slow machines.
    assert concurrent < sequential / 2
