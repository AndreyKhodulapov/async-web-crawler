"""Integration tests: requests go through local proxies, rotate among them, and a dead proxy is taken out."""

import logging

import aiohttp
import pytest
from helpers import (
    BOT,
    PROXY_AUTHORIZATION,
    PROXY_PASSWORD,
    UNTHROTTLED,
    FakeClock,
    dead_proxy,
    with_password,
)

from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    CircuitState,
    FetchTimeoutError,
    NetworkError,
    NoProxyError,
    Proxy,
    ProxyError,
    ProxyNetworkError,
    ProxyPool,
    ProxyStats,
    RetryStrategy,
)
from crawler.transport import HttpTransport

pytestmark = pytest.mark.usefixtures("restore_logging")

# Counts every request, opens after two failures of the host in a row.
BREAKER = {"circuit_breaker": CircuitBreaker(0.5, min_requests=2)}


async def test_a_page_goes_through_the_proxy(url, site, make_proxy):
    site.robots = "User-agent: *\nAllow: /\n"
    proxy = await make_proxy()
    async with AsyncCrawler(**UNTHROTTLED | {"respect_robots": True}, proxies=ProxyPool([proxy.url])) as crawler:
        page = await crawler.fetch_url(url("/ok"))
        stats = crawler.proxy_stats()

    assert "hello" in page
    assert proxy.requests == [f"GET {url('/robots.txt')}", f"GET {url('/ok')}"]
    assert stats == {proxy.url: ProxyStats(state="active", requests=2)}


async def test_a_response_names_the_proxy_it_came_through(url, make_proxy):
    proxy = await make_proxy()
    timeout = aiohttp.ClientTimeout(total=5)
    responses = {}
    for proxies in (ProxyPool([proxy.url]), None):
        transport = HttpTransport(
            max_concurrent=1, timeout=timeout, user_agent=BOT, max_page_size=None, proxies=proxies
        )
        try:
            for path in ("/ok", "/moved", "/data.json"):
                response = await transport.get(
                    url(path), html_only=True, raw_limit=None, truncate_at=None, timeout=timeout
                )
                responses[path, proxies is not None] = response
        finally:
            await transport.close()

    assert responses["/moved", True].redirected
    assert responses["/data.json", True].content == ""  # not HTML
    for path in ("/ok", "/moved", "/data.json"):
        assert responses[path, True].proxy == Proxy.from_url(proxy.url)
        assert responses[path, False].proxy is None


async def test_an_https_page_goes_through_a_connect_tunnel(https_server, make_proxy):
    proxy = await make_proxy()
    page_url = f"https://localhost:{https_server.port}/ok"
    async with AsyncCrawler(**UNTHROTTLED, proxies=ProxyPool([proxy.url])) as crawler:
        page = await crawler.fetch_url(page_url)

    assert "hello" in page
    assert proxy.requests == [f"CONNECT localhost:{https_server.port}"]


@pytest.mark.parametrize("scheme", ["http", "https"])
async def test_the_password_goes_to_the_proxy_alone(url, site, https_server, https_site, make_proxy, scheme):
    proxy = await make_proxy(authorization=PROXY_AUTHORIZATION)
    server_site = site if scheme == "http" else https_site
    port = url("").rsplit(":", 1)[1] if scheme == "http" else https_server.port
    page_url = f"{scheme}://localhost:{port}/cookies/echo"
    async with AsyncCrawler(**UNTHROTTLED, proxies=ProxyPool([with_password(proxy.url)])) as crawler:
        await crawler.fetch_url(page_url)

    assert proxy.authorizations == [PROXY_AUTHORIZATION]
    assert "Proxy-Authorization" not in server_site.headers["/cookies/echo"]
    assert PROXY_PASSWORD not in str(server_site.headers["/cookies/echo"])


@pytest.mark.parametrize("scheme", ["http", "https"])
async def test_a_wrong_password_is_an_error_of_the_proxy(url, https_server, make_proxy, scheme):
    proxy = await make_proxy(authorization=PROXY_AUTHORIZATION)
    page_url = url("/ok") if scheme == "http" else f"https://localhost:{https_server.port}/ok"
    pool = ProxyPool([with_password(proxy.url, "wrong")], max_failures=5)
    async with AsyncCrawler(**UNTHROTTLED | BREAKER, proxies=pool) as crawler:
        results = await crawler.fetch_many([page_url] * 3)
        circuit = crawler.circuit_breaker.state("localhost" if scheme == "https" else "127.0.0.1")

    for result in results:
        assert isinstance(result.error, ProxyNetworkError)
        assert "HTTP 407" in result.error.message
        assert "wrong" not in str(result.error)
    assert circuit is CircuitState.CLOSED


async def test_a_dead_proxy_is_passed_over_within_the_retries(url, make_proxy):
    proxy = await make_proxy()
    dead = dead_proxy()
    retries = {"retry_strategy": RetryStrategy(max_retries=2, base_delay=0.01)}
    async with AsyncCrawler(**UNTHROTTLED | BREAKER | retries, proxies=ProxyPool([dead, proxy.url])) as crawler:
        pages = [await crawler.fetch_result(url(f"/site/{name}")) for name in ("a.html", "b.html", "c.html")]
        stats = crawler.proxy_stats()
        circuit = crawler.circuit_breaker.state("127.0.0.1")

    assert [page.error for page in pages] == [None, None, None]
    assert stats[dead].failures >= 1
    assert stats[proxy.url] == ProxyStats(state="active", requests=3)
    assert circuit is CircuitState.CLOSED


async def test_a_dead_proxy_is_passed_over_for_robots_txt_as_for_a_page(url, site, make_proxy):
    site.robots = "User-agent: *\nAllow: /\n"
    proxy = await make_proxy()
    options = {"respect_robots": True, "retry_strategy": RetryStrategy(max_retries=1, base_delay=0.01)}
    pool = ProxyPool([dead_proxy(), proxy.url], rotation="per_request", max_failures=5)
    async with AsyncCrawler(**UNTHROTTLED | options, proxies=pool) as crawler:
        page = await crawler.fetch_result(url("/ok"))

    # Each of the two requests met the dead proxy first.
    assert page.error is None
    assert proxy.requests == [f"GET {url('/robots.txt')}", f"GET {url('/ok')}"]


async def test_per_host_keeps_every_host_on_its_proxy(url, make_proxy):
    first, second = await make_proxy(), await make_proxy()
    pool = ProxyPool([first.url, second.url])
    async with AsyncCrawler(**UNTHROTTLED, proxies=pool) as crawler:
        for _ in range(3):
            await crawler.fetch_urls([url("/ok"), url("/ok", "localhost")])

    # By the hash of the host: 127.0.0.1 to the first proxy, localhost to the second.
    assert first.hosts() == {"127.0.0.1"}
    assert second.hosts() == {"localhost"}
    assert len(first.requests) == len(second.requests) == 3


async def test_per_request_takes_turns(url, make_proxy):
    first, second = await make_proxy(), await make_proxy()
    pool = ProxyPool([first.url, second.url], rotation="per_request")
    async with AsyncCrawler(**UNTHROTTLED, proxies=pool) as crawler:
        for _ in range(4):
            await crawler.fetch_url(url("/ok"))

    assert len(first.requests) == len(second.requests) == 2


async def test_a_proxy_out_of_rotation_waits_for_its_cooldown(url, make_proxy):
    clock = FakeClock()
    proxy = await make_proxy()
    dead = dead_proxy()
    pool = ProxyPool([dead, proxy.url], rotation="per_request", max_failures=1, cooldown=60, clock=clock)
    async with AsyncCrawler(**UNTHROTTLED, proxies=pool) as crawler:
        failed = await crawler.fetch_result(url("/ok"))
        for _ in range(3):
            assert (await crawler.fetch_result(url("/ok"))).error is None
        assert crawler.proxy_stats()[dead] == ProxyStats(state="out", requests=1, failures=1, times_removed=1)
        clock.now += 60
        again = await crawler.fetch_result(url("/ok"))

    assert isinstance(failed.error, ProxyNetworkError)
    assert isinstance(again.error, ProxyNetworkError)  # its turn again, once back
    assert len(proxy.requests) == 3


async def test_the_crawl_ends_when_the_proxies_never_come_back(url, site, caplog):
    # Each page waits for the proxies to come back as many times as for a host held back, then fails.
    # The cooldown is far longer than the retries of robots.txt: no proxy comes back while they go,
    # so each download ends with no proxy available, even on a slow machine.
    site.robots = "User-agent: *\nAllow: /\n"
    pool = ProxyPool([dead_proxy(), dead_proxy()], max_failures=1, cooldown=1.0)
    retries = {"retry_strategy": RetryStrategy(max_retries=3, base_delay=0.01)}
    caplog.set_level(logging.INFO, logger="crawler.crawl_run")
    async with AsyncCrawler(**UNTHROTTLED | BREAKER | retries | {"respect_robots": True}, proxies=pool) as crawler:
        await crawler.crawl([url("/site/"), url("/site/a.html")], max_pages=10)
        failed = dict(crawler.failed_urls)
        circuit = crawler.circuit_breaker.state("127.0.0.1")
        with pytest.raises(ProxyError):
            await crawler.robots.fetch_robots(url("/"))
        with pytest.raises(LookupError):  # no answer of the site: nothing is cached
            crawler.robots.unreachable_reason(url("/"))

    assert set(failed) == {url("/site/"), url("/site/a.html")}
    assert {reason.split(":")[0] for reason in failed.values()} <= {"ProxyNetworkError", "NoProxyError"}
    assert caplog.text.count(f"Deferred {url('/site/')} for ") == AsyncCrawler.MAX_WAITS_PER_PAGE
    assert circuit is CircuitState.CLOSED


async def test_sitemaps_of_robots_txt_are_given_up_when_every_proxy_is_out(url, site):
    site.robots = f"Sitemap: {url('/sitemaps/pages.xml')}\nUser-agent: *\nAllow: /\n"
    pool = ProxyPool([dead_proxy()], max_failures=1, cooldown=0.01)
    async with AsyncCrawler(**UNTHROTTLED | {"respect_robots": True}, proxies=pool) as crawler:
        pages = await crawler.crawl([url("/site/")], max_pages=5, robots_sitemaps=True)
        failed = dict(crawler.failed_urls)

    assert pages == {}
    assert list(failed) == [url("/site/")]


async def test_a_proxy_that_cannot_reach_the_site_stays_in_rotation(https_server, make_proxy):
    proxy = await make_proxy(connect_status=502)
    async with AsyncCrawler(**UNTHROTTLED, proxies=ProxyPool([proxy.url], max_failures=1)) as crawler:
        result = await crawler.fetch_result(f"https://localhost:{https_server.port}/ok")
        stats = crawler.proxy_stats()

    assert type(result.error) is NetworkError
    assert "answered CONNECT with HTTP 502" in result.error.message
    assert stats[proxy.url] == ProxyStats(state="active", requests=1)


@pytest.mark.parametrize(
    ("scheme", "error", "timeout", "failures"),
    [("https", ProxyNetworkError, "connect timeout", 1), ("http", FetchTimeoutError, "read timeout", 0)],
)
async def test_a_silent_proxy_is_blamed_for_the_connect_timeout_alone(
    url, https_server, make_proxy, scheme, error, timeout, failures
):
    # Over https the silence falls on CONNECT, over http on the request the proxy passes on to the site.
    proxy = await make_proxy(silent="localhost")
    page_url = url("/ok", "localhost") if scheme == "http" else f"https://localhost:{https_server.port}/ok"
    timeouts = {"connect_timeout": 0.2, "read_timeout": 0.2}
    async with AsyncCrawler(**UNTHROTTLED | timeouts, proxies=ProxyPool([proxy.url], max_failures=5)) as crawler:
        result = await crawler.fetch_result(page_url)
        stats = crawler.proxy_stats()

    assert type(result.error) is error
    assert result.error.message.endswith(f"{timeout} (0.2s)")
    assert stats[proxy.url] == ProxyStats(state="active", requests=1, failures=failures)


async def test_a_host_moves_off_a_proxy_silent_to_connect(https_server, make_proxy):
    proxy, silent = await make_proxy(), await make_proxy(silent="localhost")
    page_url = f"https://localhost:{https_server.port}/ok"
    retries = {"retry_strategy": RetryStrategy(max_retries=1, base_delay=0.01)}
    pool = ProxyPool([proxy.url, silent.url], max_failures=5)
    async with AsyncCrawler(**UNTHROTTLED | BREAKER | retries, connect_timeout=0.2, proxies=pool) as crawler:
        results = [await crawler.fetch_result(page_url) for _ in range(3)]
        stats = crawler.proxy_stats()
        circuit = crawler.circuit_breaker.state("localhost")

    # The hash of localhost gives the silent proxy; one timeout moves the host to the other for good.
    assert [result.error for result in results] == [None, None, None]
    assert silent.requests == proxy.requests == [f"CONNECT localhost:{https_server.port}"]  # one tunnel, kept
    assert stats == {
        proxy.url: ProxyStats(state="active", requests=3),
        silent.url: ProxyStats(state="active", requests=1, failures=1),
    }
    assert circuit is CircuitState.CLOSED


@pytest.mark.usefixtures("clean_proxy_environment")
async def test_proxies_of_the_environment(url, make_proxy, monkeypatch):
    proxy = await make_proxy()
    monkeypatch.setenv("HTTP_PROXY", proxy.url)
    monkeypatch.setenv("NO_PROXY", "localhost")
    async with AsyncCrawler(**UNTHROTTLED, proxies=ProxyPool.from_env()) as crawler:
        await crawler.fetch_url(url("/ok"))
        await crawler.fetch_url(url("/ok", "localhost"))

    assert proxy.requests == [f"GET {url('/ok')}"]


async def test_the_password_stays_out_of_the_log_and_the_errors(url, make_proxy, caplog):
    proxy = await make_proxy(authorization=PROXY_AUTHORIZATION)
    dead = with_password(dead_proxy())
    pool = ProxyPool([with_password(proxy.url, "wrong"), dead], rotation="per_request", max_failures=1)
    with caplog.at_level(logging.DEBUG):
        async with AsyncCrawler(**UNTHROTTLED, proxies=pool) as crawler:
            results = [await crawler.fetch_result(url("/ok")) for _ in range(3)]

    assert isinstance(results[2].error, NoProxyError)
    texts = [caplog.text, *(str(result.error) for result in results)]
    assert all(PROXY_PASSWORD not in text and "wrong" not in text for text in texts)
    assert "crawler:***@127.0.0.1" in caplog.text
