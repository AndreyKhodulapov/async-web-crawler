"""Integration tests: fetch and parse pages served by a local aiohttp server."""

import pytest
from conftest import ENCODING_PAGES

from crawler import AsyncCrawler, HTMLParser, HTTPStatusError


@pytest.fixture
async def crawler():
    async with AsyncCrawler() as crawler:
        yield crawler


async def test_fetch_and_parse(crawler, server):
    url = str(server.make_url("/catalog/tools/"))
    page = await crawler.fetch_and_parse(url)

    assert page["url"] == url
    assert page["title"] == "Garden Tools Catalog"
    assert page["metadata"]["description"] == "Tools for every garden."
    assert page["text"].startswith("Garden tools")
    assert str(server.make_url("/catalog/tools/shovels/steel.html")) in page["links"]
    assert len(page["images"]) == 2
    assert len(page["tables"]) == 4
    assert page["errors"] == []


async def test_links_are_resolved_after_redirect(crawler, server):
    # "about.html" on /moved would resolve to /about.html; after the redirect
    # to /catalog/tools/ it must resolve under that directory instead.
    page = await crawler.fetch_and_parse(str(server.make_url("/moved")))
    assert page["final_url"] == str(server.make_url("/catalog/tools/"))
    assert str(server.make_url("/catalog/tools/about.html")) in page["links"]
    assert page["metadata"]["canonical"] == str(server.make_url("/catalog/"))


async def test_same_host_only(server):
    async with AsyncCrawler(parser=HTMLParser(same_host_only=True)) as crawler:
        page = await crawler.fetch_and_parse(str(server.make_url("/catalog/tools/")))
    assert page["links"]
    assert all(link.startswith(str(server.make_url("/"))) for link in page["links"])


async def test_json_response_is_not_parsed(crawler, server):
    page = await crawler.fetch_and_parse(str(server.make_url("/data.json")))
    assert page["errors"] == ["unsupported content type: application/json"]
    assert page["text"] == ""


async def test_page_charset_is_respected(crawler, server):
    page = await crawler.fetch_and_parse(str(server.make_url("/cp1252")))
    assert page["title"] == "Café"
    assert page["text"] == "Crème brûlée"


@pytest.mark.parametrize("name", ENCODING_PAGES)
async def test_page_encoding(crawler, server, name):
    # Priority: byte order mark, header charset, <meta>, then UTF-8. A charset
    # naming a codec that cannot decode the page is ignored.
    page = await crawler.fetch_and_parse(str(server.make_url(f"/encoding/{name}")))
    assert page["title"] == ENCODING_PAGES[name][2]


async def test_http_error_is_raised(crawler, server):
    with pytest.raises(HTTPStatusError):
        await crawler.fetch_and_parse(str(server.make_url("/status/500")))
