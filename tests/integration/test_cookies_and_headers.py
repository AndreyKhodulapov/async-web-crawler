"""Integration tests: cookies and headers of the session reach a local site, are kept, saved and loaded back."""

import asyncio
import stat
import time

import pytest
import yaml
from helpers import FAST_CONFIG, UNTHROTTLED, cookies_file, make_config

import main
from crawler import AdvancedCrawler, AsyncCrawler, load_cookies_file, make_cookie
from main import build_config, parse_args, run

pytestmark = pytest.mark.usefixtures("restore_logging")

# aiohttp keeps no cookies of IP addresses, so the site is reached by its name.
HOST = "localhost"


async def test_cookie_of_the_site_comes_back_on_the_next_page(url):
    async with AsyncCrawler(**UNTHROTTLED) as crawler:
        await crawler.fetch_url(url("/cookies/set?sid=abc", HOST))
        page = await crawler.fetch_url(url("/cookies/echo", HOST))

    assert "cookie:sid=abc" in page


async def test_cookie_set_by_a_redirect_reaches_its_target(url):
    async with AsyncCrawler(**UNTHROTTLED) as crawler:
        page = await crawler.fetch_url(url("/cookies/set?sid=abc&redirect=1", HOST))

    assert "cookie:sid=abc" in page


async def test_without_keep_cookies_no_cookie_comes_back(url):
    async with AsyncCrawler(**UNTHROTTLED, keep_cookies=False) as crawler:
        await crawler.fetch_url(url("/cookies/set?sid=abc", HOST))
        page = await crawler.fetch_url(url("/cookies/echo", HOST))

        assert "cookie:" not in page
        assert crawler.export_cookies() == []


async def test_starting_cookie_goes_to_its_domain_only(url):
    cookies = [make_cookie("sid", "abc", HOST), make_cookie("https_only", "x", HOST, secure=True)]
    async with AsyncCrawler(**UNTHROTTLED, cookies=cookies) as crawler:
        own = await crawler.fetch_url(url("/cookies/echo", HOST))
        other = await crawler.fetch_url(url("/cookies/echo"))  # the same server by its IP address

    assert "cookie:sid=abc" in own
    assert "https_only" not in own  # not over http
    assert "cookie:" not in other


async def test_cookie_of_a_file_dated_in_milliseconds_reaches_the_site(url, tmp_path):
    later = (int(time.time()) + 3600) * 1000
    path = cookies_file(tmp_path / "cookies.txt", f"{HOST}\tFALSE\t/\tFALSE\t{later}\tsid\tabc")

    async with AsyncCrawler(**UNTHROTTLED, cookies=load_cookies_file(path)) as crawler:
        page = await crawler.fetch_url(url("/cookies/echo", HOST))

    assert "cookie:sid=abc" in page


async def test_headers_reach_the_pages_and_robots_txt(url, site):
    site.robots = "User-agent: *\nAllow: /\n"
    headers = {"Accept-Language": "de", "Authorization": "Bearer t0ken"}
    async with AsyncCrawler(**UNTHROTTLED | {"respect_robots": True}, headers=headers, user_agents=[]) as crawler:
        await crawler.fetch_url(url("/cookies/echo", HOST))

    for path in ("/robots.txt", "/cookies/echo"):
        assert site.headers[path]["Accept-Language"] == "de", path
        assert site.headers[path]["Authorization"] == "Bearer t0ken", path
        assert site.headers[path]["User-Agent"] == AsyncCrawler.DEFAULT_USER_AGENT, path


async def test_rotated_user_agents_keep_the_headers(url, site):
    agents = [f"{AsyncCrawler.DEFAULT_USER_AGENT} ({n})" for n in range(2)]
    async with AsyncCrawler(**UNTHROTTLED, user_agent=agents[0], user_agents=agents, headers={"X-Key": "1"}) as crawler:
        await crawler.fetch_url(url("/cookies/echo", HOST))

    assert site.headers["/cookies/echo"]["X-Key"] == "1"
    assert site.headers["/cookies/echo"]["User-Agent"] == agents[0]


async def test_cookies_are_saved_after_the_crawl_and_loaded_back(url, tmp_path):
    saved = tmp_path / "out" / "cookies.txt"  # the directory does not exist yet
    config = make_config(
        urls=[url("/cookies/set?sid=abc", HOST)],
        session={"save_cookies": str(saved), "cookies": [{"name": "given", "value": "1", "domain": HOST}]},
    )
    async with AdvancedCrawler(config) as crawler:
        await crawler.crawl()

    assert crawler.cookie_file == saved
    assert stat.S_IMODE(saved.stat().st_mode) == 0o600
    assert sorted((cookie.domain, cookie.name, cookie.value) for cookie in load_cookies_file(saved)) == [
        (HOST, "given", "1"),
        (HOST, "sid", "abc"),
    ]

    config = make_config(urls=[url("/cookies/echo", HOST)], session={"cookies_file": str(saved)})
    async with AdvancedCrawler(config) as crawler:
        pages = await crawler.crawl()

    assert "cookie:sid=abc" in pages[url("/cookies/echo", HOST)]["text"]


async def test_secrets_stay_out_of_the_log_and_the_reports(url, site, tmp_path):
    site.robots = "User-agent: *\nAllow: /\n"
    out = tmp_path / "out"
    config = make_config(
        urls=[url("/cookies/echo", HOST), url("/status/500", HOST)],
        crawler={"respect_robots": True},
        session={
            "cookies": [{"name": "sid", "value": "c00kie-secret", "domain": HOST}],
            "cookies_file": cookies_file(
                tmp_path / "cookies.txt",
                f"{HOST}\tFALSE\t/\tFALSE\t0\tfrom_file\tf1le-secret",
                "127.0.0.1\tFALSE\t/\tFALSE\t0\tlocal\tip-secret",
            ),
            "save_cookies": str(out / "saved.txt"),
            "headers": {"Authorization": "Bearer header-secret"},
        },
        logging={"level": "DEBUG", "file": str(out / "crawler.log")},
        report={"stats_json": str(out / "stats.json"), "html": str(out / "report.html")},
    )
    async with AdvancedCrawler(config) as crawler:
        pages = await crawler.crawl()

    page = pages[url("/cookies/echo", HOST)]["text"]
    assert "cookie:sid=c00kie-secret" in page
    assert "cookie:from_file=f1le-secret" in page
    log = (out / "crawler.log").read_text(encoding="utf-8")
    assert "Saved 2 cookies to" in log
    for text in (log, (out / "stats.json").read_text(encoding="utf-8"), (out / "report.html").read_text("utf-8")):
        for secret in ("c00kie-secret", "f1le-secret", "ip-secret", "header-secret"):
            assert secret not in text


async def test_command_line_loads_and_saves_the_cookies(url, tmp_path, capsys):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.safe_dump({**FAST_CONFIG, "logging": {"level": "WARNING"}, "urls": [url("/cookies/set?sid=abc", HOST)]}),
        encoding="utf-8",
    )
    loaded = cookies_file(tmp_path / "cookies.txt", f"{HOST}\tFALSE\t/\tFALSE\t0\tgiven\ts3cr3t")
    saved = tmp_path / "saved.txt"
    argv = ["--config", str(config_file), "--cookies-file", loaded, "--save-cookies", str(saved), "--no-progress"]

    code = await run(build_config(parse_args(argv)), progress=False)

    assert code == 0
    assert sorted(cookie.name for cookie in load_cookies_file(saved)) == ["given", "sid"]
    summary = capsys.readouterr().out
    assert f"Cookies: {saved}\n" in summary
    assert "s3cr3t" not in summary


async def test_interrupted_crawl_saves_the_cookies(url, tmp_path, capsys):
    saved = tmp_path / "saved.txt"
    config = make_config(
        urls=[url("/cookies/set?sid=abc", HOST), url("/delay/30", HOST)],
        crawler={"max_depth": 0},
        session={"save_cookies": str(saved)},
        logging={"level": "CRITICAL"},
    )
    task = asyncio.create_task(run(config, progress=True))
    async with asyncio.timeout(10):
        while "1/100 pages" not in capsys.readouterr().err:  # /cookies/set is fetched, /delay/30 is in flight
            await asyncio.sleep(0.05)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert [cookie.name for cookie in load_cookies_file(saved)] == ["sid"]
    assert f"Cookies: {saved}\n" in capsys.readouterr().out


def test_cookies_file_that_cannot_be_read_is_an_error_of_the_configuration(tmp_path, capsys):
    code = main.main(["--urls", "http://localhost/", "--cookies-file", str(tmp_path / "missing.txt"), "--no-progress"])

    assert code == 2
    error = capsys.readouterr().err
    assert error.startswith("error: Invalid configuration: session.cookies_file: cannot read the cookies: ")
