"""Unit tests for the cookies of the session: cookies.txt files, the cookie jar and the checks of the crawler."""

import logging
import stat
import time
from http.cookies import SimpleCookie

import aiohttp
import pytest
from yarl import URL

from crawler import AsyncCrawler, load_cookies_file, make_cookie, save_cookies_file
from crawler.session import CookieJar
from crawler.transport import HttpTransport

HEADER = "# Netscape HTTP Cookie File\n"


def line(domain: str, name: str, value: str, *, expires: str = "0", path: str = "/", secure: bool = False) -> str:
    flag = "TRUE" if domain.startswith(".") else "FALSE"
    return "\t".join([domain, flag, path, "TRUE" if secure else "FALSE", expires, name, value]) + "\n"


def summary(cookies) -> list[tuple]:
    return sorted((cookie.domain, cookie.path, cookie.name, cookie.value, cookie.secure) for cookie in cookies)


class TestLoad:
    def test_cookies_of_a_file(self, tmp_path):
        later = str(int(time.time()) + 3600)
        path = tmp_path / "cookies.txt"
        path.write_text(
            HEADER
            + line("example.com", "host_only", "1", expires=later)
            + line(".example.com", "wide", "2", path="/app", secure=True, expires=later)
            + "#HttpOnly_example.org\tFALSE\t/\tFALSE\t0\thidden\t3\n"
            + "\n# a comment\n",
            encoding="utf-8",
        )

        cookies = load_cookies_file(path)

        assert summary(cookies) == [
            (".example.com", "/app", "wide", "2", True),
            ("example.com", "/", "host_only", "1", False),
            ("example.org", "/", "hidden", "3", False),
        ]
        hidden = next(cookie for cookie in cookies if cookie.name == "hidden")
        assert hidden.has_nonstandard_attr("HTTPOnly")

    def test_expired_cookies_are_left_out_session_ones_kept(self, tmp_path):
        path = tmp_path / "cookies.txt"
        path.write_text(
            HEADER
            + line("example.com", "expired", "1", expires=str(int(time.time()) - 60))
            + line("example.com", "curl_session", "2", expires="0")
            + line("example.com", "python_session", "3", expires=""),
            encoding="utf-8",
        )

        cookies = load_cookies_file(path)

        assert [cookie.name for cookie in cookies] == ["curl_session", "python_session"]
        assert all(cookie.expires is None and cookie.discard for cookie in cookies)

    def test_cookies_that_cannot_be_sent_are_left_out_and_logged(self, tmp_path, caplog):
        path = tmp_path / "cookies.txt"
        path.write_text(
            HEADER
            + line("127.0.0.1", "local", "secret-1")
            + line("example.com", "", "secret-2")
            + line("example.com", "Path", "secret-3")
            + line("example.com", "ok", "2"),
            encoding="utf-8",
        )

        with caplog.at_level(logging.WARNING, logger="crawler.session"):
            cookies = load_cookies_file(path)

        assert [cookie.name for cookie in cookies] == ["ok"]
        assert "'local' of 127.0.0.1" in caplog.text
        assert "a cookie without a name of example.com" in caplog.text
        assert "'Path' of example.com" in caplog.text
        assert "secret" not in caplog.text

    @pytest.mark.parametrize(
        "content",
        [
            b"not a cookies file\n",
            (HEADER + "example.com\tFALSE\t/\tsecret-value\n").encode(),
            (HEADER + "example.com\tTRUE\t/\tFALSE\t0\tname\tsecret-value\n").encode(),
            b"\xff\xfe\x00binary",
        ],
    )
    def test_malformed_file_is_not_quoted(self, tmp_path, content, recwarn):
        path = tmp_path / "cookies.txt"
        path.write_bytes(content)

        with pytest.raises(ValueError) as error:
            load_cookies_file(path)

        assert str(error.value) == "not a Netscape cookies.txt file, or a line of it is malformed"
        assert error.value.__cause__ is None and error.value.__suppress_context__
        assert not recwarn.list

    def test_missing_file(self, tmp_path):
        with pytest.raises(OSError):
            load_cookies_file(tmp_path / "missing.txt")


class TestSave:
    def test_saved_file_is_loaded_back(self, tmp_path):
        later = int(time.time()) + 3600
        cookies = [
            make_cookie("sid", "abc", "example.com", expires=later, http_only=True),
            make_cookie("wide", "x", ".example.com", path="/app", secure=True),
        ]
        path = tmp_path / "cookies.txt"

        save_cookies_file(cookies, path)

        loaded = load_cookies_file(path)
        assert summary(loaded) == summary(cookies)
        assert next(cookie for cookie in loaded if cookie.name == "sid").expires == later
        assert next(cookie for cookie in loaded if cookie.name == "sid").has_nonstandard_attr("HTTPOnly")

    def test_file_is_for_its_owner_only(self, tmp_path):
        path = tmp_path / "cookies.txt"
        path.write_text("old", encoding="utf-8")
        path.chmod(0o644)

        save_cookies_file([make_cookie("sid", "abc", "example.com")], path)

        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert list(tmp_path.iterdir()) == [path]

    def test_failed_save_leaves_nothing(self, tmp_path):
        with pytest.raises(OSError):
            save_cookies_file([make_cookie("sid", "abc", "example.com")], tmp_path / "missing" / "cookies.txt")

        assert list(tmp_path.iterdir()) == []


def set_cookie(jar: CookieJar, header: str, url: str) -> None:
    cookie = SimpleCookie()
    cookie.load(header)
    jar.update_cookies(cookie, URL(url))


def sent(jar: CookieJar, url: str) -> dict[str, str]:
    return {name: morsel.value for name, morsel in jar.filter_cookies(URL(url)).items()}


class TestCookieJar:
    async def test_cookies_of_sites_are_exported_with_their_scope(self):
        jar = CookieJar()
        set_cookie(jar, "host_only=1; Path=/", "http://www.example.com/page")
        set_cookie(jar, "wide=2; Domain=example.com; Secure; HttpOnly", "https://www.example.com/")

        cookies = jar.export()

        assert summary(cookies) == [
            (".example.com", "/", "wide", "2", True),
            ("www.example.com", "/", "host_only", "1", False),
        ]
        assert next(cookie for cookie in cookies if cookie.name == "wide").has_nonstandard_attr("HTTPOnly")

    async def test_max_age_becomes_an_expiry(self):
        jar = CookieJar()
        before = time.time()
        set_cookie(jar, "sid=1; Max-Age=3600", "http://example.com/")

        (cookie,) = jar.export()

        assert before + 3600 - 1 <= cookie.expires <= time.time() + 3600 + 1

    async def test_cookie_deleted_by_the_site_is_not_exported(self):
        jar = CookieJar()
        set_cookie(jar, "sid=1", "http://example.com/")
        set_cookie(jar, "sid=; Max-Age=0", "http://example.com/")

        assert jar.export() == []

    async def test_added_cookies_go_to_their_domain_only(self):
        jar = CookieJar()
        jar.add(
            [
                make_cookie("host_only", "1", "example.com"),
                make_cookie("wide", "2", ".example.org"),
                make_cookie("https_only", "3", "example.com", secure=True),
                make_cookie("app", "4", "example.com", path="/app"),
            ]
        )

        assert sent(jar, "http://example.com/") == {"host_only": "1"}
        assert sent(jar, "https://example.com/app/page") == {"host_only": "1", "https_only": "3", "app": "4"}
        assert sent(jar, "http://sub.example.com/") == {}
        assert sent(jar, "http://sub.example.org/") == {"wide": "2"}
        assert sent(jar, "http://other.com/") == {}

    async def test_added_cookies_are_exported_as_they_came(self):
        later = int(time.time()) + 3600
        cookies = [
            make_cookie("host_only", "1", "example.com", expires=later),
            make_cookie("wide", "2", ".example.org", path="/app", secure=True),
        ]
        jar = CookieJar()
        jar.add(cookies)

        exported = jar.export()

        assert summary(exported) == summary(cookies)
        assert [cookie.expires for cookie in sorted(exported, key=lambda cookie: cookie.name)] == [later, None]

    async def test_removed_cookies_go_by_domain_path_and_name(self):
        jar = CookieJar()
        jar.add(
            [
                make_cookie("sid", "1", "example.com"),
                make_cookie("sid", "2", "example.com", path="/app"),
                make_cookie("wide", "3", ".example.org"),
                make_cookie("other", "4", "example.com"),
            ]
        )

        jar.remove([make_cookie("sid", "any value", "example.com"), make_cookie("wide", "", ".example.org")])

        assert summary(jar.export()) == [
            ("example.com", "/", "other", "4", False),
            ("example.com", "/app", "sid", "2", False),
        ]


def make_transport(**options) -> HttpTransport:
    timeout = aiohttp.ClientTimeout(total=5)
    return HttpTransport(max_concurrent=1, timeout=timeout, user_agent="TestBot/1.0", max_page_size=None, **options)


class TestTransportCookies:
    async def test_cookies_are_updated_in_the_jar(self):
        transport = make_transport(cookies=[make_cookie("sid", "1", "example.com"), make_cookie("old", "2", "a.test")])
        transport._get_session()  # the jar is made with the session
        try:
            transport.update_cookies(
                [make_cookie("sid", "new", "example.com"), make_cookie("js", "3", ".example.com")],
                [make_cookie("old", "", "a.test")],
            )

            assert summary(transport.cookies()) == [
                (".example.com", "/", "js", "3", False),
                ("example.com", "/", "sid", "new", False),
            ]
        finally:
            await transport.close()

    def test_before_the_first_request_the_starting_cookies_are_updated(self):
        transport = make_transport(cookies=[make_cookie("sid", "1", "example.com"), make_cookie("old", "2", "a.test")])

        transport.update_cookies([make_cookie("sid", "new", "example.com")], [make_cookie("old", "", "a.test")])

        assert summary(transport.cookies()) == [("example.com", "/", "sid", "new", False)]

    def test_without_keep_cookies_none_are_kept(self):
        transport = make_transport(keep_cookies=False)

        transport.update_cookies([make_cookie("sid", "1", "example.com")], [])

        assert transport.cookies() == []


class TestCrawlerArguments:
    def test_reserved_header_is_refused(self):
        with pytest.raises(ValueError, match="'User-Agent': this header is set by crawler.user_agent"):
            AsyncCrawler(headers={"User-Agent": "Other/1.0"})

    def test_proxy_authorization_header_is_refused(self):
        # Sent with every request, it would reach the sites behind an https proxy too.
        with pytest.raises(ValueError, match="'proxy-authorization': this header is set by the user and password"):
            AsyncCrawler(headers={"proxy-authorization": "Basic secret"})

    def test_invalid_header_value_is_not_shown(self):
        with pytest.raises(ValueError) as error:
            AsyncCrawler(headers={"Authorization": "Bearer secret\r\nX-Injected: 1"})

        assert "secret" not in str(error.value)

    def test_cookies_need_keep_cookies(self):
        with pytest.raises(ValueError, match="keep_cookies"):
            AsyncCrawler(cookies=[make_cookie("sid", "1", "example.com")], keep_cookies=False)

    def test_cookie_named_as_an_attribute_is_refused(self):
        with pytest.raises(ValueError, match="cannot name a cookie"):
            AsyncCrawler(cookies=[make_cookie("Path", "1", "example.com")])

    def test_cookie_of_an_ip_address_is_refused(self):
        with pytest.raises(ValueError, match="cookies of an IP address are not kept"):
            AsyncCrawler(cookies=[make_cookie("sid", "1", "127.0.0.1")])

    async def test_cookies_before_the_first_request_are_the_starting_ones(self):
        cookie = make_cookie("sid", "1", "example.com")
        async with AsyncCrawler(cookies=[cookie]) as crawler:
            assert crawler.export_cookies() == [cookie]
        async with AsyncCrawler(keep_cookies=False) as crawler:
            assert crawler.export_cookies() == []
