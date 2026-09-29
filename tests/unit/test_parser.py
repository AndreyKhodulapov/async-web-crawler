"""Unit tests for HTMLParser on valid, broken and non-HTML input."""

import logging

import pytest
from bs4 import BeautifulSoup

from crawler import HTMLParser

PAGE_URL = "https://shop.example.com/catalog/tools/index.html"


@pytest.fixture
def parser() -> HTMLParser:
    return HTMLParser()


@pytest.fixture
def valid_page(parser, read_fixture):
    return parser.parse(read_fixture("valid_page.html"), PAGE_URL)


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


class TestValidPage:
    def test_required_fields(self, valid_page):
        assert valid_page["url"] == PAGE_URL
        assert valid_page["final_url"] == PAGE_URL
        assert valid_page["title"] == "Garden Tools Catalog"
        assert valid_page["errors"] == []

    def test_metadata(self, valid_page):
        assert valid_page["metadata"] == {
            "title": "Garden Tools Catalog",
            "description": "Tools for every garden.",
            "keywords": ["garden", "tools", "shovels"],
            "language": "en",
            "canonical": "https://shop.example.com/catalog/",
        }

    def test_text_is_main_content_only(self, valid_page):
        text = valid_page["text"]
        assert text.startswith("Garden tools Everything you need to grow vegetables.")
        assert "See steel shovels, the sale, the PDF catalog" in text
        for hidden in ("tracking", "color: green", "a comment", "Enable JavaScript", "Footer", "Home"):
            assert hidden not in text

    def test_links_are_absolute_unique_and_ordered(self, valid_page):
        assert valid_page["links"] == [
            "https://shop.example.com/",
            "https://shop.example.com/catalog/tools/about.html",
            "https://partner.example.org/deals",
            "https://shop.example.com/catalog/tools/shovels/steel.html",
            "https://shop.example.com/catalog/sale/?page=2",
            "https://cdn.example.com/catalog.pdf",
            "https://shop.example.com/about.html",
        ]

    def test_images(self, valid_page):
        assert valid_page["images"] == [
            {"src": "https://shop.example.com/img/shovel.png", "alt": "Steel shovel"},
            {"src": "https://shop.example.com/catalog/tools/img/rake.png", "alt": "Rake"},
        ]

    def test_headings_skip_empty_ones(self, valid_page):
        assert valid_page["headings"] == [
            {"level": 1, "text": "Garden tools"},
            {"level": 2, "text": "Shovels"},
            {"level": 3, "text": "Prices"},
        ]

    def test_tables(self, valid_page):
        price_list, nested, sizes, headerless = valid_page["tables"]
        assert price_list == {
            "caption": "Price list",
            "headers": ["Item", "Price"],
            "rows": [["Shovel", "25"], ["Rake", "inner"]],
        }
        assert nested == {"caption": None, "headers": [], "rows": [["inner"]]}
        assert sizes["headers"] == ["Size", "Weight"]
        assert sizes["rows"] == [["S", "1 kg"]]
        assert headerless == {"caption": None, "headers": [], "rows": [["a", "b"]]}

    def test_lists_keep_nested_items_separate(self, valid_page):
        assert valid_page["lists"] == [
            {"type": "ul", "items": ["Home", "About", "Partner deals"]},
            {"type": "ol", "items": ["Dig", "Plant"]},
            {"type": "ul", "items": ["Loosen soil", "Remove stones"]},
        ]

    async def test_parse_html_matches_sync_parse(self, parser, read_fixture, valid_page):
        page = await parser.parse_html(read_fixture("valid_page.html"), PAGE_URL)
        assert page == valid_page


class TestLinks:
    def test_same_host_only_drops_external_links(self, read_fixture):
        page = HTMLParser(same_host_only=True).parse(read_fixture("valid_page.html"), PAGE_URL)
        assert "https://partner.example.org/deals" not in page["links"]
        assert "https://cdn.example.com/catalog.pdf" not in page["links"]
        assert len(page["links"]) == 5

    def test_base_tag_overrides_page_url(self, parser):
        html = '<head><base href="/static/"></head><a href="a.html">a</a><img src="i.png">'
        page = parser.parse(html, "https://example.com/deep/page")
        assert page["links"] == ["https://example.com/static/a.html"]
        assert page["images"][0]["src"] == "https://example.com/static/i.png"

    def test_same_host_uses_page_host_not_base_tag(self):
        html = '<base href="https://cdn.example.net/"><a href="https://example.com/x">own</a><a href="y">cdn</a>'
        page = HTMLParser(same_host_only=True).parse(html, "https://example.com/")
        assert page["links"] == ["https://example.com/x"]

    def test_same_host_matches_punycode_final_url(self):
        html = '<a href="https://bücher.de/a">a</a><a href="/b">b</a>'
        page = HTMLParser(same_host_only=True).parse(html, "https://xn--bcher-kva.de/")
        assert page["links"] == ["https://xn--bcher-kva.de/a", "https://xn--bcher-kva.de/b"]

    def test_invalid_base_tag_is_ignored(self, parser):
        page = parser.parse('<base href="javascript:x"><a href="a">a</a>', "https://example.com/dir/")
        assert page["links"] == ["https://example.com/dir/a"]

    def test_relative_links_use_final_url(self, parser):
        page = parser.parse('<a href="next">n</a>', "http://example.com/old", final_url="https://example.com/new/")
        assert page["url"] == "http://example.com/old"
        assert page["links"] == ["https://example.com/new/next"]

    def test_extract_links_directly(self, parser):
        html = '<a href="/a">1</a><a href="/a#x">2</a><a href="A">3</a>'
        assert parser.extract_links(soup(html), "https://example.com/") == [
            "https://example.com/a",
            "https://example.com/A",
        ]


class TestTables:
    def test_nested_caption_stays_with_nested_table(self, parser):
        html = "<table><tr><td><table><caption>Inner</caption><tr><td>x</td></tr></table></td></tr></table>"
        outer, inner = parser.extract_tables(soup(html))
        assert outer["caption"] is None
        assert inner["caption"] == "Inner"

    def test_multi_row_thead_uses_last_row(self, parser):
        html = """
            <table>
              <thead>
                <tr><th colspan="2">Group</th></tr>
                <tr><th>A</th><th>B</th></tr>
              </thead>
              <tbody><tr><td>1</td><td>2</td></tr></tbody>
            </table>"""
        [table] = parser.extract_tables(soup(html))
        assert table["headers"] == ["A", "B"]
        assert table["rows"] == [["1", "2"]]

    def test_body_row_equal_to_header_is_kept(self, parser):
        html = "<table><thead><tr><th>A</th></tr></thead><tbody><tr><th>A</th></tr></tbody></table>"
        [table] = parser.extract_tables(soup(html))
        assert table["headers"] == ["A"]
        assert table["rows"] == [["A"]]

    def test_first_row_without_cells(self, parser):
        [table] = parser.extract_tables(soup("<table><tr></tr><tr><td>1</td></tr></table>"))
        assert table["headers"] == []
        assert table["rows"] == [["1"]]


class TestExtractText:
    def test_selector(self, parser):
        html = '<p class="lead">First</p><p>Other</p><p class="lead">Second</p>'
        assert parser.extract_text(soup(html), ".lead") == "First Second"

    def test_selector_without_matches(self, parser):
        assert parser.extract_text(soup("<p>text</p>"), "article") == ""

    def test_single_article_is_main_content(self, parser):
        html = "<nav>Menu</nav><article><p>Story</p></article>"
        assert parser.extract_text(soup(html)) == "Story"

    def test_several_articles_mean_whole_body(self, parser):
        html = "<nav>Menu</nav><article>One</article><article>Two</article>"
        assert parser.extract_text(soup(html)) == "Menu One Two"

    def test_nested_articles_are_one_article(self, parser):
        html = "<nav>menu</nav><article>Post<article>comment</article></article><footer>legal</footer>"
        assert parser.extract_text(soup(html)) == "Post comment"

    def test_nested_selector_matches_are_not_repeated(self, parser):
        html = "<div>a<div>b</div></div><div>c</div>"
        assert parser.extract_text(soup(html), "div") == "a b c"

    def test_inline_and_block_whitespace(self, parser):
        html = "<p>a<b>b</b>, c<br>d</p><div>e</div><span>f</span><span>g</span>"
        assert parser.extract_text(soup(html)) == "ab, c d e fg"


class TestMetadata:
    def test_open_graph_fallback(self, parser):
        html = '<meta property="og:title" content="OG title"><meta property="og:description" content="OG desc">'
        metadata = parser.extract_metadata(soup(html))
        assert metadata["title"] == "OG title"
        assert metadata["description"] == "OG desc"

    def test_empty_meta_is_skipped(self, parser):
        html = '<meta name="description" content="  "><meta name="description" content="Real text">'
        assert parser.extract_metadata(soup(html))["description"] == "Real text"

    def test_missing_metadata(self, parser):
        assert parser.extract_metadata(soup("<p>x</p>")) == {
            "title": None,
            "description": None,
            "keywords": [],
            "language": None,
            "canonical": None,
        }

    def test_svg_title_in_body_is_not_page_title(self, parser):
        assert parser.extract_metadata(soup("<body><svg><title>icon</title></svg></body>"))["title"] is None

    @pytest.mark.parametrize("rel", ["Canonical", "CANONICAL", "alternate canonical"])
    def test_canonical_rel_is_case_insensitive(self, parser, rel):
        metadata = parser.extract_metadata(soup(f'<link rel="{rel}" href="/c">'), "https://example.com/")
        assert metadata["canonical"] == "https://example.com/c"

    def test_canonical_without_base_url_is_kept_raw(self, parser):
        metadata = parser.extract_metadata(soup('<link rel="canonical" href="/c">'))
        assert metadata["canonical"] == "/c"


class TestBrokenHTML:
    def test_broken_page_is_repaired(self, parser, read_fixture):
        page = parser.parse(read_fixture("broken_page.html"), "https://example.com/")
        assert page["errors"] == []
        assert page["title"] == "Broken <b>page"  # <title> content is plain text in HTML
        assert page["headings"] == [{"level": 2, "text": "Heading inside link"}]
        assert page["tables"] == [{"caption": None, "headers": [], "rows": [["cell 1", "cell 2"], ["cell 3"]]}]
        assert page["lists"] == [{"type": "ul", "items": ["one", "two"]}]
        assert page["images"] == [{"src": "https://example.com/pic.png", "alt": "unquoted"}]
        assert "First paragraph never closed Second block" in page["text"]

    def test_invalid_links_are_dropped(self, parser, read_fixture):
        page = parser.parse(read_fixture("broken_page.html"), "https://example.com/")
        assert page["links"] == ["https://example.com/in-heading", "https://example.com/last"]

    def test_deep_nesting_does_not_overflow(self, parser):
        page = parser.parse("<div>" * 20_000 + "deep", "https://example.com/")
        assert page["text"] == "deep"

    @pytest.mark.parametrize("html", ["", "   \n\t"])
    def test_empty_document(self, parser, html, caplog):
        page = parser.parse(html, "https://example.com/")
        assert page["errors"] == ["empty document"]
        assert page["text"] == ""
        assert "empty document" in caplog.text

    def test_plain_text_is_reported_but_kept(self, parser):
        page = parser.parse("just some text", "https://example.com/")
        assert page["errors"] == ["no HTML markup found"]
        assert page["text"] == "just some text"

    def test_binary_garbage(self, parser):
        garbage = bytes(range(256)).decode("latin-1") * 4
        page = parser.parse(garbage, "https://example.com/")
        assert page["url"] == "https://example.com/"

    def test_missing_title_is_logged(self, parser, caplog):
        with caplog.at_level(logging.WARNING):
            parser.parse("<p>no title</p>", "https://example.com/")
        assert "No <title> on https://example.com/" in caplog.text

    @pytest.mark.parametrize("content_type", ["application/json", "image/png"])
    def test_unsupported_content_type(self, parser, content_type):
        page = parser.parse("<p>looks like html</p>", "https://example.com/", content_type=content_type)
        assert page["errors"] == [f"unsupported content type: {content_type}"]
        assert page["text"] == ""

    @pytest.mark.parametrize(
        "content_type",
        ["text/html", "application/xhtml+xml", None, "TEXT/HTML", "text/html; charset=utf-8"],
    )
    def test_html_content_types(self, parser, content_type):
        page = parser.parse("<p>ok</p>", "https://example.com/", content_type=content_type)
        assert page["text"] == "ok"


class TestPartialResults:
    def test_failing_extractor_keeps_other_fields(self, parser, read_fixture, monkeypatch, caplog):
        def broken(*args):
            raise RuntimeError("boom")

        monkeypatch.setattr(parser, "extract_tables", broken)
        page = parser.parse(read_fixture("valid_page.html"), PAGE_URL)

        assert page["tables"] == []
        assert page["errors"] == ["tables: RuntimeError: boom"]
        assert page["title"] == "Garden Tools Catalog"
        assert len(page["links"]) == 7
        assert "Failed to extract tables" in caplog.text
        assert "RuntimeError: boom" in caplog.text  # traceback is logged

    def test_failing_metadata_leaves_empty_title(self, parser, monkeypatch):
        monkeypatch.setattr(parser, "extract_metadata", lambda *args: 1 / 0)
        page = parser.parse("<title>T</title><p>body</p>", "https://example.com/")
        assert page["title"] is None
        assert page["metadata"]["keywords"] == []
        assert page["text"] == "body"
        assert page["errors"] == ["metadata: ZeroDivisionError: division by zero"]

    def test_falls_back_to_html_parser(self, parser, monkeypatch, caplog):
        import crawler.parser as parser_module

        real = parser_module.BeautifulSoup

        def lxml_fails(markup, features):
            if features == "lxml":
                raise ValueError("lxml is broken")
            return real(markup, features)

        monkeypatch.setattr(parser_module, "BeautifulSoup", lxml_fails)
        page = parser.parse("<title>T</title><p>body</p>", "https://example.com/")
        assert page["title"] == "T"
        assert page["errors"] == ["lxml parser failed: ValueError: lxml is broken"]

    def test_all_parsers_fail(self, parser, monkeypatch):
        import crawler.parser as parser_module

        def always_fails(markup, features):
            raise ValueError(features)

        monkeypatch.setattr(parser_module, "BeautifulSoup", always_fails)
        page = parser.parse("<p>body</p>", "https://example.com/")
        assert page["text"] == ""
        assert len(page["errors"]) == 2
