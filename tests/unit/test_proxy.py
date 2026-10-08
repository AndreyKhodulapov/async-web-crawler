"""Unit tests for ProxyPool: rotation, proxies out of rotation, the environment, the URLs of proxies."""

import logging
import zlib

import pytest
from helpers import FakeClock

from crawler import (
    FetchTimeoutError,
    HTTPStatusError,
    NoProxyError,
    Proxy,
    ProxyNetworkError,
    ProxyPool,
    ProxyStats,
)
from crawler.proxy import proxy_url_problem

URLS = ["http://proxy-0:3128", "http://proxy-1:3128", "http://proxy-2:3128"]
PAGE = "http://a.test/page"
OTHER_PAGE = "http://a.test/other"


def failed(url: str = PAGE) -> ProxyNetworkError:
    return ProxyNetworkError(url, "cannot connect to the proxy")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def pool(clock) -> ProxyPool:
    return ProxyPool(URLS, max_failures=2, cooldown=60.0, clock=clock)


def own_proxy(host: str) -> str:
    """The proxy that `per_host` gives `host` by its hash."""
    return URLS[zlib.crc32(host.encode()) % len(URLS)]


def fail(pool: ProxyPool, proxy: Proxy, times: int, url: str = PAGE) -> None:
    for _ in range(times):
        pool.record(proxy, url, failed(url))


class TestPerHost:
    def test_a_host_goes_through_the_proxy_of_its_hash(self, pool):
        assert pool.pick(PAGE).url == own_proxy("a.test")

    def test_every_request_of_a_host_goes_through_one_proxy(self, pool):
        picked = {pool.pick(url).url for url in [PAGE, OTHER_PAGE, "http://a.test/", "https://a.test/x"]}
        assert picked == {own_proxy("a.test")}

    def test_hosts_spread_over_the_proxies(self, pool):
        hosts = [f"site-{number}.test" for number in range(30)]
        assert {pool.pick(f"http://{host}/").url for host in hosts} == set(URLS)

    def test_a_failed_proxy_moves_its_host_to_the_next_one_at_once(self, pool):
        proxy = pool.pick(PAGE)
        pool.record(proxy, PAGE, failed())
        moved = pool.pick(PAGE)
        assert moved.url == URLS[(URLS.index(proxy.url) + 1) % len(URLS)]
        # The host stays there: no way back, even after the proxy that failed answers again.
        pool.record(proxy, OTHER_PAGE, None)
        assert pool.pick(PAGE) == moved

    def test_failures_of_requests_sent_at_once_move_the_host_once(self, pool):
        proxy = pool.pick(PAGE)
        pool.record(proxy, PAGE, failed())
        pool.record(proxy, OTHER_PAGE, failed(OTHER_PAGE))
        assert pool.pick(PAGE).url == URLS[(URLS.index(proxy.url) + 1) % len(URLS)]

    def test_a_late_failure_of_a_proxy_the_host_left_does_not_move_it_back(self, pool):
        first = pool.pick(PAGE)  # a request sent before the host moved
        pool.record(first, OTHER_PAGE, failed(OTHER_PAGE))
        second = pool.pick(PAGE)
        pool.record(second, OTHER_PAGE, failed(OTHER_PAGE))
        third = pool.pick(PAGE)
        assert len({first, second, third}) == 3

        pool.record(first, PAGE, failed())

        assert pool.pick(PAGE) == third

    def test_a_failure_of_the_proxy_standing_in_for_one_out_moves_the_host_on(self, pool):
        own = pool.pick(PAGE)
        fail(pool, own, 2, url="http://b.test/")  # another host takes it out
        stand_in = pool.pick(PAGE)

        pool.record(stand_in, PAGE, failed())

        assert pool.pick(PAGE) not in (own, stand_in)

    def test_other_errors_do_not_move_the_host(self, pool):
        proxy = pool.pick(PAGE)
        pool.record(proxy, PAGE, FetchTimeoutError(PAGE, "timed out"))
        pool.record(proxy, PAGE, HTTPStatusError(PAGE, 502, "Bad Gateway"))
        assert pool.pick(PAGE) == proxy

    def test_a_host_skips_a_proxy_out_of_rotation(self, pool):
        proxy = pool.pick(PAGE)
        fail(pool, proxy, 2, url="http://b.test/")  # another host takes it out
        assert pool.pick(PAGE) != proxy


class TestPerRequest:
    def test_proxies_take_turns(self, clock):
        pool = ProxyPool(URLS, rotation="per_request", clock=clock)
        assert [pool.pick(PAGE).url for _ in range(4)] == [*URLS, URLS[0]]

    def test_a_proxy_out_of_rotation_loses_its_turn(self, clock):
        pool = ProxyPool(URLS, rotation="per_request", max_failures=1, clock=clock)
        fail(pool, pool.proxies[1], 1)
        assert [pool.pick(PAGE).url for _ in range(3)] == [URLS[0], URLS[2], URLS[0]]


class TestOutOfRotation:
    def test_failures_in_a_row_take_a_proxy_out(self, pool, caplog):
        proxy = pool.proxies[0]
        fail(pool, proxy, 1)
        assert pool.get_stats()[proxy.label].state == "active"
        with caplog.at_level(logging.WARNING, logger="crawler.proxy"):
            fail(pool, proxy, 1)
        assert pool.get_stats()[proxy.label] == ProxyStats(state="out", requests=2, failures=2, times_removed=1)
        assert (
            "Proxy http://proxy-0:3128 is out of rotation for 60s, failures in a row: 2, the last: cannot connect"
            in caplog.text
        )

    def test_a_response_clears_the_count(self, pool):
        proxy = pool.proxies[0]
        fail(pool, proxy, 1)
        pool.record(proxy, PAGE, HTTPStatusError(PAGE, 404, "Not Found"))  # a response: through the proxy
        pool.record(proxy, PAGE, None)
        fail(pool, proxy, 1)
        assert pool.get_stats()[proxy.label].state == "active"

    def test_other_errors_count_neither_way(self, pool):
        proxy = pool.proxies[0]
        fail(pool, proxy, 1)
        pool.record(proxy, PAGE, FetchTimeoutError(PAGE, "timed out"))
        assert pool.get_stats()[proxy.label] == ProxyStats(state="active", requests=2, failures=1)
        fail(pool, proxy, 1)
        assert pool.get_stats()[proxy.label].state == "out"

    def test_the_proxy_is_not_picked_until_the_cooldown_ends(self, pool, clock, caplog):
        proxy = pool.proxies[0]
        fail(pool, proxy, 2)
        clock.now += 59.9
        assert proxy not in {pool.pick(f"http://site-{number}.test/") for number in range(30)}
        clock.now += 0.1
        with caplog.at_level(logging.INFO, logger="crawler.proxy"):
            assert proxy in {pool.pick(f"http://site-{number}.test/") for number in range(30)}
        assert "Proxy http://proxy-0:3128 is back in rotation" in caplog.text

    def test_one_failure_takes_a_proxy_back_out(self, pool, clock):
        proxy = pool.proxies[0]
        fail(pool, proxy, 2)
        clock.now += 60
        assert pool.get_stats()[proxy.label].state == "active"
        fail(pool, proxy, 1)
        assert pool.get_stats()[proxy.label] == ProxyStats(state="out", requests=3, failures=3, times_removed=2)

    def test_a_response_after_the_cooldown_keeps_it_in(self, pool, clock):
        proxy = pool.proxies[0]
        fail(pool, proxy, 2)
        clock.now += 60
        pool.record(proxy, PAGE, None)
        fail(pool, proxy, 1)
        assert pool.get_stats()[proxy.label].state == "active"

    def test_failures_while_out_do_not_extend_the_cooldown(self, pool, clock):
        proxy = pool.proxies[0]
        fail(pool, proxy, 2)
        clock.now += 30
        fail(pool, proxy, 1)  # a request sent before the proxy went out
        clock.now += 30
        assert pool.get_stats()[proxy.label] == ProxyStats(state="active", requests=3, failures=3, times_removed=1)

    def test_no_request_is_sent_when_every_proxy_is_out(self, pool, clock):
        for proxy in pool.proxies:
            fail(pool, proxy, 2)
            clock.now += 10
        with pytest.raises(NoProxyError) as raised:
            pool.pick(PAGE)
        assert (
            raised.value.message == "no proxy available: all 3 proxies are out of rotation, the first is back in 30.0s"
        )
        assert raised.value.url == PAGE
        assert raised.value.seconds == 30.0

    def test_a_pool_of_one_names_its_proxy(self, clock):
        pool = ProxyPool(["http://user:secret@proxy:3128"], max_failures=1, clock=clock)
        fail(pool, pool.proxies[0], 1)
        with pytest.raises(NoProxyError, match=r"http://user:\*\*\*@proxy:3128 is out of rotation, back in 60.0s"):
            pool.pick(PAGE)


class TestStats:
    def test_counts_requests_and_failures_by_proxy(self, pool):
        first, second, _ = pool.proxies
        pool.record(first, PAGE, None)
        pool.record(first, PAGE, failed())
        pool.record(second, PAGE, None)
        assert pool.get_stats() == {
            "http://proxy-0:3128": ProxyStats(state="active", requests=2, failures=1),
            "http://proxy-1:3128": ProxyStats(state="active", requests=1),
            "http://proxy-2:3128": ProxyStats(state="active"),
        }

    def test_reading_the_stats_brings_no_proxy_back(self, pool, clock, caplog):
        proxy = pool.proxies[0]
        fail(pool, proxy, 2)
        clock.now += 60
        with caplog.at_level(logging.INFO, logger="crawler.proxy"):
            assert pool.get_stats()[proxy.label].state == "active"  # the cooldown is over
            assert "back in rotation" not in caplog.text  # it comes back when a request picks it
            fail(pool, proxy, 1)

        assert pool.get_stats()[proxy.label].state == "out"
        assert pool.get_stats()[proxy.label].times_removed == 2

    def test_reset_keeps_the_proxies_out(self, pool):
        proxy = pool.proxies[0]
        fail(pool, proxy, 2)
        pool.reset_stats()
        assert pool.get_stats()[proxy.label] == ProxyStats(state="out")
        fail(pool, proxy, 1)  # the count in a row is kept too
        assert pool.get_stats()[proxy.label].failures == 1


@pytest.mark.usefixtures("clean_proxy_environment")
class TestFromEnv:
    def test_none_without_proxies(self, monkeypatch):
        monkeypatch.setenv("NO_PROXY", "localhost")
        assert ProxyPool.from_env() is None

    def test_a_url_goes_through_the_proxy_of_its_scheme(self, monkeypatch):
        monkeypatch.setenv("HTTP_PROXY", "http://plain:3128")
        monkeypatch.setenv("https_proxy", "http://user:secret@secure:3128")
        pool = ProxyPool.from_env()
        assert pool.pick("http://a.test/").url == "http://plain:3128"
        secure = pool.pick("https://a.test/")
        assert (secure.url, secure.label) == ("http://secure:3128", "http://user:***@secure:3128")
        assert secure.authorization == "Basic dXNlcjpzZWNyZXQ="

    def test_a_scheme_without_a_proxy_goes_directly(self, monkeypatch):
        monkeypatch.setenv("HTTPS_PROXY", "http://secure:3128")
        pool = ProxyPool.from_env()
        assert pool.pick("http://a.test/") is None
        assert pool.pick("https://a.test/").url == "http://secure:3128"

    @pytest.mark.parametrize(
        ("no_proxy", "url"),
        [
            ("a.test", "http://a.test/"),
            ("a.test", "http://www.a.test/"),
            (".a.test", "http://www.a.test/"),
            ("b.test, a.test", "http://a.test/"),
            ("a.test:8080", "http://a.test:8080/"),
            ("*", "http://a.test/"),
        ],
    )
    def test_no_proxy_sends_a_host_directly(self, monkeypatch, no_proxy, url):
        monkeypatch.setenv("HTTP_PROXY", "http://plain:3128")
        monkeypatch.setenv("NO_PROXY", no_proxy)
        assert ProxyPool.from_env().pick(url) is None

    def test_a_failure_for_a_host_of_no_proxy_is_counted(self, monkeypatch):
        # record() is public: a transport of its own may tell of a URL the pool sends directly.
        monkeypatch.setenv("HTTP_PROXY", "http://plain:3128")
        monkeypatch.setenv("NO_PROXY", "a.test")
        pool = ProxyPool.from_env()

        fail(pool, pool.proxies[0], 1, url="http://a.test/")

        assert pool.get_stats()["http://plain:3128"].failures == 1

    def test_the_pool_tells_its_no_proxy(self, monkeypatch):
        monkeypatch.setenv("HTTP_PROXY", "http://plain:3128")
        assert ProxyPool.from_env().no_proxy is None
        monkeypatch.setenv("no_proxy", "a.test, .b.test")
        assert ProxyPool.from_env().no_proxy == "a.test, .b.test"
        assert ProxyPool(["http://plain:3128"]).no_proxy is None

    def test_no_proxy_leaves_other_hosts_to_the_proxy(self, monkeypatch):
        monkeypatch.setenv("HTTP_PROXY", "http://plain:3128")
        monkeypatch.setenv("no_proxy", "a.test")
        assert ProxyPool.from_env().pick("http://b.test/").url == "http://plain:3128"

    def test_one_proxy_for_both_schemes_is_one_proxy(self, monkeypatch):
        monkeypatch.setenv("HTTP_PROXY", "http://shared:3128")
        monkeypatch.setenv("HTTPS_PROXY", "http://shared:3128")
        pool = ProxyPool.from_env(max_failures=1)
        fail(pool, pool.pick("http://a.test/"), 1)
        assert list(pool.get_stats()) == ["http://shared:3128"]
        with pytest.raises(NoProxyError):
            pool.pick("https://a.test/")

    def test_one_proxy_with_two_passwords_is_an_error(self, monkeypatch):
        # The label hides the password: one proxy for both would send one of the passwords for the other scheme.
        monkeypatch.setenv("HTTP_PROXY", "http://user:secret-1@shared:3128")
        monkeypatch.setenv("HTTPS_PROXY", "http://user:secret-2@shared:3128")
        with pytest.raises(ValueError, match="^HTTP_PROXY and HTTPS_PROXY name one proxy with different passwords$"):
            ProxyPool.from_env()

    def test_a_proxy_without_a_scheme_is_an_http_one(self, monkeypatch):
        monkeypatch.setenv("HTTP_PROXY", "plain:3128")
        assert ProxyPool.from_env().pick("http://a.test/").url == "http://plain:3128"

    def test_an_invalid_proxy_names_the_variable_not_the_value(self, monkeypatch):
        monkeypatch.setenv("HTTPS_PROXY", "socks5://user:secret@proxy:1080")
        with pytest.raises(ValueError, match="^HTTPS_PROXY: SOCKS proxies are not supported") as raised:
            ProxyPool.from_env()
        assert "secret" not in str(raised.value)


class TestProxyUrls:
    def test_the_password_goes_into_a_header(self):
        proxy = Proxy.from_url("http://user:p%40ss@proxy.example:3128")
        assert proxy.url == "http://proxy.example:3128"
        assert proxy.label == "http://user:***@proxy.example:3128"
        assert proxy.credentials == ("user", "p@ss")
        assert proxy.authorization == "Basic dXNlcjpwQHNz"  # user:p@ss
        assert all(secret not in repr(proxy) for secret in ("p%40ss", "p@ss", "dXNlcjpwQHNz"))

    def test_a_user_without_a_password(self):
        proxy = Proxy.from_url("HTTP://user@proxy.example:3128/")
        assert proxy.credentials == ("user", "")
        assert (proxy.url, proxy.label, proxy.authorization) == (
            "http://proxy.example:3128",
            "http://user@proxy.example:3128",
            "Basic dXNlcjo=",
        )

    def test_the_host_is_lowercased_but_not_the_user(self):
        proxy = Proxy.from_url("http://User:Secret@Proxy.Example:3128")
        assert (proxy.url, proxy.label) == ("http://proxy.example:3128", "http://User:***@proxy.example:3128")
        assert proxy.authorization == "Basic VXNlcjpTZWNyZXQ="  # User:Secret

    def test_a_proxy_without_a_user_has_no_header(self):
        proxy = Proxy.from_url("https://[::1]:8443")
        assert (proxy.credentials, proxy.authorization) == (None, None)

    @pytest.mark.parametrize(
        ("url", "problem"),
        [
            ("socks5://proxy:1080", "SOCKS proxies are not supported"),
            ("socks5h://user:secret@proxy:1080", "SOCKS proxies are not supported"),
            ("ftp://proxy:21", "expected an http:// or https:// proxy URL"),
            ("proxy:3128", "expected an http:// or https:// proxy URL"),
            ("http://:3128", "expected an http:// or https:// proxy URL"),
            ("http://proxy:99999", "not a URL of a proxy"),
            ("http://pro xy:3128", "cannot contain spaces or control characters"),
            ("http://proxy", "needs a port"),
            ("http://proxy:3128/path", "no path, query or fragment"),
            ("http://proxy:3128?x=1", "no path, query or fragment"),
            ("http://a%3Ab:secret@proxy:3128", "cannot contain a colon"),
        ],
    )
    def test_problems(self, url, problem):
        found = proxy_url_problem(url)
        assert found is not None and problem in found
        assert "secret" not in found

    @pytest.mark.parametrize(
        "url", ["http://proxy:3128", "https://user:secret@proxy.example:443/", "http://[::1]:3128"]
    )
    def test_valid_urls(self, url):
        assert proxy_url_problem(url) is None


class TestPoolArguments:
    @pytest.mark.parametrize(
        ("urls", "options", "message"),
        [
            ([], {}, "at least one proxy"),
            (URLS, {"rotation": "random"}, "rotation"),
            (URLS, {"max_failures": 0}, "max_failures"),
            (URLS, {"cooldown": 0}, "cooldown"),
            (URLS, {"cooldown": float("inf")}, "cooldown"),
            (["http://proxy:3128", "socks5://user:secret@proxy:1080"], {}, "^proxy 2: SOCKS proxies"),
            (["http://user:one@proxy:3128", "http://user:two@proxy:3128"], {}, r"proxy 2: .*\*\*\*.* is listed twice"),
            (["http://Proxy:3128", "http://proxy:3128"], {}, "proxy 2: http://proxy:3128 is listed twice"),
        ],
    )
    def test_rejects_invalid_arguments(self, urls, options, message):
        with pytest.raises(ValueError, match=message) as raised:
            ProxyPool(urls, **options)
        assert "secret" not in str(raised.value)

    def test_users_of_one_proxy_are_different_proxies(self):
        pool = ProxyPool(["http://one:secret@gateway:3128", "http://two:secret@gateway:3128"])
        assert list(pool.get_stats()) == ["http://one:***@gateway:3128", "http://two:***@gateway:3128"]
