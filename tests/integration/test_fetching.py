"""Integration tests: real HTTP requests against a local aiohttp server."""

import time

import pytest
from helpers import UNTHROTTLED

from crawler import (
    AsyncCrawler,
    FetchTimeoutError,
    HTTPStatusError,
    InvalidURLError,
    NetworkError,
    RetryStrategy,
    TooManyRedirectsError,
)


@pytest.fixture
async def crawler():
    async with AsyncCrawler(max_concurrent=10, **UNTHROTTLED) as crawler:
        yield crawler


async def test_fetch_valid_url(crawler, server):
    html = await crawler.fetch_url(str(server.make_url("/ok")))
    assert "hello" in html


@pytest.mark.parametrize("code", [404, 500])
async def test_http_error_status(crawler, server, code):
    with pytest.raises(HTTPStatusError) as exc_info:
        await crawler.fetch_url(str(server.make_url(f"/status/{code}")))
    assert exc_info.value.status == code
    assert exc_info.value.url.endswith(f"/status/{code}")


async def test_unreachable_host(crawler, closed_port_url):
    with pytest.raises(NetworkError):
        await crawler.fetch_url(closed_port_url)


async def test_idna_error_is_an_invalid_url(crawler):
    # Passes URL validation, but aiohttp fails on IDNA-encoding the host.
    with pytest.raises(InvalidURLError):
        await crawler.fetch_url("http://" + "a" * 70 + ".com")


async def test_redirect_loop_is_not_retried(server):
    async with AsyncCrawler(**{**UNTHROTTLED, "retry_strategy": RetryStrategy(max_retries=2)}) as crawler:
        with pytest.raises(TooManyRedirectsError, match="too many redirects"):
            await crawler.fetch_url(str(server.make_url("/redirect-loop")))
        assert crawler.rate_limiter.get_stats().requests == 1  # one attempt, no retries


@pytest.mark.parametrize("timeout", ["read_timeout", "total_timeout"])
async def test_timeout(server, timeout):
    async with AsyncCrawler(**{timeout: 0.2}, **UNTHROTTLED) as crawler:
        with pytest.raises(FetchTimeoutError):
            await crawler.fetch_url(str(server.make_url("/delay/2")))


async def test_fetch_urls_skips_failures(crawler, server, closed_port_url):
    ok_url = str(server.make_url("/ok"))
    urls = [ok_url, str(server.make_url("/status/404")), closed_port_url]
    pages = await crawler.fetch_urls(urls)
    assert list(pages) == [ok_url]
    assert "hello" in pages[ok_url]


async def test_concurrent_is_faster_than_sequential(server):
    delay, count = 0.3, 5
    urls = [str(server.make_url(f"/delay/{delay}?n={i}")) for i in range(count)]

    async with AsyncCrawler(max_concurrent=count, **UNTHROTTLED) as crawler:
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


async def test_retry_strategy_wraps_fetch_url(crawler, url, site):
    retry_strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.01)
    html = await retry_strategy.execute_with_retry(crawler.fetch_url, url("/flaky/2"))
    assert "Recovered" in html
    assert site.hits["/flaky/2"] == 3


async def test_retry_strategy_does_not_repeat_not_found(crawler, url, site):
    retry_strategy = RetryStrategy(base_delay=0.01)
    with pytest.raises(HTTPStatusError):
        await retry_strategy.execute_with_retry(crawler.fetch_url, url("/site/missing.html"))
    assert site.hits["/site/missing.html"] == 1


@pytest.fixture
async def retrying_crawler():
    options = {**UNTHROTTLED, "retry_strategy": RetryStrategy(max_retries=3, base_delay=0.01)}
    async with AsyncCrawler(**options) as crawler:
        yield crawler


async def test_crawler_retries_service_unavailable(retrying_crawler, url, site):
    assert "Recovered" in await retrying_crawler.fetch_url(url("/flaky/2"))
    assert site.hits["/flaky/2"] == 3


@pytest.mark.parametrize(("path", "requests"), [("/status/404", 1), ("/status/403", 1), ("/status/500", 2)])
async def test_crawler_retries_by_status(retrying_crawler, server, path, requests):
    with pytest.raises(HTTPStatusError):
        await retrying_crawler.fetch_url(str(server.make_url(path)))
    assert retrying_crawler.rate_limiter.get_stats().requests == requests


async def test_crawler_retries_timeouts(server):
    options = {**UNTHROTTLED, "retry_strategy": RetryStrategy(max_retries=2, base_delay=0.01)}
    async with AsyncCrawler(read_timeout=0.1, **options) as crawler:
        with pytest.raises(FetchTimeoutError):
            await crawler.fetch_url(str(server.make_url("/delay/1")))
        assert crawler.rate_limiter.get_stats().requests == 3
