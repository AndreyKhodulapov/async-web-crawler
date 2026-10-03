"""Unit tests for SitemapParser: plain sitemaps, sitemap indexes, gzip and limits."""

import asyncio
import gzip
import logging

import pytest
from helpers import SITEMAP_NAMESPACE as NAMESPACE
from helpers import index, urlset

from crawler import HTTPStatusError, NetworkError, SitemapError, SitemapParser


class FakeSite:
    """Serves sitemaps by URL and records what was requested."""

    def __init__(self, files: dict[str, bytes | Exception]) -> None:
        self.files = files
        self.requested: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0

    async def __call__(self, url: str) -> bytes:
        self.requested.append(url)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0)
        self.in_flight -= 1
        answer = self.files.get(url, HTTPStatusError(url, 404, "Not Found"))
        if isinstance(answer, Exception):
            raise answer
        return answer


async def fetch(files: dict[str, bytes | Exception], url: str = "https://site/sitemap.xml", **options) -> list[str]:
    return await SitemapParser(FakeSite(files), **options).fetch_sitemap(url)


class TestUrlset:
    async def test_returns_the_listed_urls_in_order(self):
        files = {"https://site/sitemap.xml": urlset("https://site/b", "https://site/a", "https://site/c")}
        assert await fetch(files) == ["https://site/b", "https://site/a", "https://site/c"]

    async def test_urls_are_normalized_and_deduplicated(self):
        files = {
            "https://site/sitemap.xml": urlset(
                "\n  HTTPS://Site:443/café#top  \n", "https://site/caf%C3%A9", "https://site"
            )
        }
        assert await fetch(files) == ["https://site/caf%C3%A9", "https://site/"]

    async def test_invalid_urls_are_dropped(self):
        files = {"https://site/sitemap.xml": urlset("/relative", "ftp://site/file", "", "https://site/ok")}
        assert await fetch(files) == ["https://site/ok"]

    async def test_escaped_ampersand_is_decoded(self):
        files = {"https://site/sitemap.xml": urlset("https://site/list?a=1&amp;b=2")}
        assert await fetch(files) == ["https://site/list?a=1&b=2"]

    @pytest.mark.parametrize(
        "document",
        [
            b"<urlset><url><loc>https://site/page</loc></url></urlset>",
            b'<urlset xmlns="http://www.google.com/schemas/sitemap/0.84"><url><loc>https://site/page</loc></url></urlset>',
            b'<s:urlset xmlns:s="'
            + NAMESPACE.encode()
            + b'"><s:url><s:loc>https://site/page</s:loc></s:url></s:urlset>',
        ],
        ids=["no namespace", "old namespace", "prefixed"],
    )
    async def test_namespace_spelling_does_not_matter(self, document):
        assert await fetch({"https://site/sitemap.xml": document}) == ["https://site/page"]

    async def test_only_loc_of_url_entries_counts(self):
        document = (
            f'<urlset xmlns="{NAMESPACE}" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
            "<!-- generated -->"
            "<url><loc>https://site/page</loc>"
            "<image:image><image:loc>https://site/picture.png</image:loc></image:image></url>"
            "</urlset>"
        ).encode()
        assert await fetch({"https://site/sitemap.xml": document}) == ["https://site/page"]

    async def test_blank_lines_before_the_xml_declaration_are_ignored(self):
        files = {"https://site/sitemap.xml": b"\n  \r\n" + urlset("https://site/page")}
        assert await fetch(files) == ["https://site/page"]

    async def test_empty_sitemap(self):
        assert await fetch({"https://site/sitemap.xml": urlset()}) == []

    async def test_sitemap_url_is_normalized_before_the_request(self):
        site = FakeSite({"https://site/sitemap.xml": urlset("https://site/page")})
        await SitemapParser(site).fetch_sitemap("HTTPS://Site:443/sitemap.xml#x")
        assert site.requested == ["https://site/sitemap.xml"]


class TestIndex:
    async def test_follows_the_sitemaps_of_an_index(self):
        files = {
            "https://site/sitemap.xml": index("https://site/posts.xml", "https://site/pages.xml"),
            "https://site/posts.xml": urlset("https://site/post/1", "https://site/post/2"),
            "https://site/pages.xml": urlset("https://site/about"),
        }
        assert await fetch(files) == ["https://site/post/1", "https://site/post/2", "https://site/about"]

    async def test_follows_nested_indexes(self):
        files = {
            "https://site/sitemap.xml": index("https://site/2026.xml"),
            "https://site/2026.xml": index("https://site/2026-01.xml"),
            "https://site/2026-01.xml": urlset("https://site/post/1"),
        }
        assert await fetch(files) == ["https://site/post/1"]

    async def test_indexes_listing_each_other_do_not_loop(self):
        site = FakeSite(
            {
                "https://site/a.xml": index("https://site/b.xml", "https://site/a.xml", "https://site/pages.xml"),
                "https://site/b.xml": index("https://site/a.xml", "https://site/pages.xml"),
                "https://site/pages.xml": urlset("https://site/page"),
            }
        )
        assert await SitemapParser(site).fetch_sitemap("https://site/a.xml") == ["https://site/page"]
        assert sorted(site.requested) == ["https://site/a.xml", "https://site/b.xml", "https://site/pages.xml"]

    async def test_pages_listed_twice_are_returned_once(self):
        files = {
            "https://site/sitemap.xml": index("https://site/one.xml", "https://site/two.xml"),
            "https://site/one.xml": urlset("https://site/shared", "https://site/first"),
            "https://site/two.xml": urlset("https://site/second", "https://site/shared"),
        }
        assert await fetch(files) == ["https://site/shared", "https://site/first", "https://site/second"]

    async def test_broken_sitemap_of_an_index_is_skipped(self, caplog):
        files = {
            "https://site/sitemap.xml": index(
                "https://site/missing.xml", "https://site/down.xml", "https://site/broken.xml", "https://site/ok.xml"
            ),
            "https://site/down.xml": NetworkError("https://site/down.xml", "connection refused"),
            "https://site/broken.xml": b"<urlset><url>",
            "https://site/ok.xml": urlset("https://site/page"),
        }
        with caplog.at_level(logging.WARNING, logger="crawler.sitemap"):
            assert await fetch(files) == ["https://site/page"]
        skipped = [record.getMessage() for record in caplog.records]
        assert len(skipped) == 3
        assert "https://site/missing.xml" in skipped[0] and "HTTP 404" in skipped[0]
        assert "NetworkError" in skipped[1]
        assert "SitemapError" in skipped[2]

    async def test_downloads_at_most_concurrency_sitemaps_at_once(self):
        children = [f"https://site/{number}.xml" for number in range(12)]
        site = FakeSite({"https://site/sitemap.xml": index(*children)} | {child: urlset() for child in children})
        await SitemapParser(site).fetch_sitemap("https://site/sitemap.xml")
        assert len(site.requested) == 13
        assert site.max_in_flight == SitemapParser.CONCURRENCY

    async def test_downloads_at_most_max_files(self, monkeypatch, caplog):
        monkeypatch.setattr(SitemapParser, "MAX_FILES", 4)
        children = [f"https://site/{number}.xml" for number in range(10)]
        site = FakeSite(
            {"https://site/sitemap.xml": index(*children)}
            | {child: urlset(child.replace(".xml", "")) for child in children}
        )
        with caplog.at_level(logging.WARNING, logger="crawler.sitemap"):
            urls = await SitemapParser(site).fetch_sitemap("https://site/sitemap.xml")
        assert urls == ["https://site/0", "https://site/1", "https://site/2"]
        assert len(site.requested) == 4
        assert "7 sitemaps left out" in caplog.text


class TestGzip:
    async def test_gzipped_sitemap_is_unpacked(self):
        files = {"https://site/sitemap.xml.gz": gzip.compress(urlset("https://site/page"))}
        assert await fetch(files, "https://site/sitemap.xml.gz") == ["https://site/page"]

    async def test_gzip_is_told_by_content_not_by_name(self):
        files = {"https://site/sitemap.xml": gzip.compress(urlset("https://site/page"))}
        assert await fetch(files) == ["https://site/page"]

    async def test_broken_archive(self):
        files = {"https://site/sitemap.xml": b"\x1f\x8b not an archive"}
        with pytest.raises(SitemapError, match="broken gzip archive"):
            await fetch(files)

    async def test_archive_that_unpacks_over_the_limit_is_rejected(self, monkeypatch):
        monkeypatch.setattr(SitemapParser, "MAX_SIZE", 1000)
        files = {"https://site/sitemap.xml": gzip.compress(urlset(*["https://site/page"] * 100))}
        assert len(files["https://site/sitemap.xml"]) < 1000
        with pytest.raises(SitemapError, match="larger than 1000 bytes"):
            await fetch(files)

    async def test_archive_of_several_members_is_one_document(self):
        document = urlset("https://site/a", "https://site/b")
        archive = gzip.compress(document[:60]) + gzip.compress(document[60:120]) + gzip.compress(document[120:])
        assert await fetch({"https://site/sitemap.xml": archive}) == ["https://site/a", "https://site/b"]

    async def test_members_that_unpack_over_the_limit_together_are_rejected(self, monkeypatch):
        document = urlset(*["https://site/page"] * 30)
        half = len(document) // 2
        monkeypatch.setattr(SitemapParser, "MAX_SIZE", len(document) - 1)
        archive = gzip.compress(document[:half]) + gzip.compress(document[half:])
        with pytest.raises(SitemapError, match="larger than"):
            await fetch({"https://site/sitemap.xml": archive})

    async def test_broken_member_after_a_good_one(self):
        archive = gzip.compress(urlset("https://site/page")) + b"\x1f\x8b not an archive"
        with pytest.raises(SitemapError, match="broken gzip archive"):
            await fetch({"https://site/sitemap.xml": archive})

    async def test_padding_after_the_archive_is_ignored(self):
        archive = gzip.compress(urlset("https://site/page")) + bytes(512)
        assert await fetch({"https://site/sitemap.xml": archive}) == ["https://site/page"]


class TestLimits:
    async def test_sitemap_over_the_size_limit_is_rejected(self, monkeypatch):
        monkeypatch.setattr(SitemapParser, "MAX_SIZE", 100)
        with pytest.raises(SitemapError, match="larger than 100 bytes"):
            await fetch({"https://site/sitemap.xml": urlset("https://site/page", "https://site/other")})

    async def test_returns_at_most_max_urls(self, caplog):
        files = {"https://site/sitemap.xml": urlset(*(f"https://site/{number}" for number in range(5)))}
        with caplog.at_level(logging.WARNING, logger="crawler.sitemap"):
            assert await fetch(files, max_urls=3) == ["https://site/0", "https://site/1", "https://site/2"]
        assert "more than 3 URLs" in caplog.text

    async def test_stops_downloading_once_max_urls_are_found(self):
        children = [f"https://site/{number}.xml" for number in range(12)]
        site = FakeSite(
            {"https://site/sitemap.xml": index(*children)}
            | {child: urlset(child.replace(".xml", "")) for child in children}
        )
        urls = await SitemapParser(site, max_urls=2).fetch_sitemap("https://site/sitemap.xml")
        assert urls == ["https://site/0", "https://site/1"]
        # The index and the first batch of its sitemaps.
        assert len(site.requested) == 1 + SitemapParser.CONCURRENCY

    async def test_exactly_max_urls_is_not_reported_as_cut(self, caplog):
        files = {"https://site/sitemap.xml": urlset("https://site/a", "https://site/b")}
        with caplog.at_level(logging.WARNING, logger="crawler.sitemap"):
            assert len(await fetch(files, max_urls=2)) == 2
        assert not caplog.records

    @pytest.mark.parametrize("max_urls", [0, -1])
    def test_max_urls_must_be_positive(self, max_urls):
        with pytest.raises(ValueError, match="max_urls"):
            SitemapParser(FakeSite({}), max_urls=max_urls)


class TestErrors:
    @pytest.mark.parametrize("url", ["site/sitemap.xml", "ftp://site/sitemap.xml", ""])
    async def test_invalid_sitemap_url(self, url):
        site = FakeSite({})
        with pytest.raises(ValueError, match="not an absolute"):
            await SitemapParser(site).fetch_sitemap(url)
        assert site.requested == []

    async def test_failed_download_of_the_sitemap_itself_is_raised(self):
        with pytest.raises(HTTPStatusError) as raised:
            await fetch({})
        assert raised.value.status == 404

    @pytest.mark.parametrize(
        ("document", "message"),
        [
            (b"", "not an XML document"),
            (b"<urlset><url><loc>https://site/page</loc>", "not an XML document"),
            (b"<html><body>Not found</body></html>", "the root element is <html>"),
            (b'<rss version="2.0"><channel/></rss>', "the root element is <rss>"),
        ],
        ids=["empty", "cut", "html", "rss"],
    )
    async def test_document_that_is_not_a_sitemap(self, document, message):
        with pytest.raises(SitemapError, match=message) as raised:
            await fetch({"https://site/sitemap.xml": document})
        assert raised.value.url == "https://site/sitemap.xml"

    async def test_entity_bomb_is_rejected(self):
        # Each entity is ten of the one before it: expanded, the URL would take a gigabyte.
        entities = '<!ENTITY e0 "aaaaaaaaaa">' + "".join(
            f'<!ENTITY e{level} "{f"&e{level - 1};" * 10}">' for level in range(1, 9)
        )
        document = f"<!DOCTYPE urlset [{entities}]><urlset><url><loc>https://site/&e8;</loc></url></urlset>".encode()
        with pytest.raises(SitemapError, match="not an XML document"):
            await fetch({"https://site/sitemap.xml": document})

    async def test_entities_are_not_expanded(self):
        document = (
            b'<!DOCTYPE urlset [<!ENTITY path "expanded">]>'
            b"<urlset><url><loc>https://site/&path;</loc></url><url><loc>https://site/ok</loc></url></urlset>"
        )
        assert await fetch({"https://site/sitemap.xml": document}) == ["https://site/", "https://site/ok"]

    async def test_external_entity_is_not_loaded(self, tmp_path):
        secret = tmp_path / "secret.txt"
        secret.write_text("password")
        document = (
            f'<!DOCTYPE urlset [<!ENTITY file SYSTEM "{secret.as_uri()}">]>'
            "<urlset><url><loc>https://site/?leak=&file;</loc></url></urlset>"
        ).encode()
        urls = await fetch({"https://site/sitemap.xml": document})
        assert "password" not in "".join(urls)

    async def test_bug_in_the_fetcher_is_not_swallowed(self):
        files = {
            "https://site/sitemap.xml": index("https://site/child.xml"),
            "https://site/child.xml": RuntimeError("bug"),
        }
        with pytest.raises(ExceptionGroup) as raised:
            await fetch(files)
        assert isinstance(raised.value.exceptions[0], RuntimeError)
