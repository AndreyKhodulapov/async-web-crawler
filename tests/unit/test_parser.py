"""Unit tests for HTMLParser on valid, broken and non-HTML input."""

import gc
import logging

import pytest
from bs4 import BeautifulSoup, Tag
from pages import fixture_html

import crawler.parser as parser_module
from crawler import HTMLParser, ParseError

PAGE_URL = "https://shop.example.com/catalog/tools/index.html"


@pytest.fixture
def parser() -> HTMLParser:
    return HTMLParser()


@pytest.fixture
def valid_page(parser):
    return parser.parse(fixture_html("valid_page.html"), PAGE_URL)


@pytest.fixture
def broken_page(parser):
    return parser.parse(fixture_html("broken_page.html"), "https://example.com/")


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

    async def test_parse_html_matches_sync_parse(self, parser, valid_page):
        page = await parser.parse_html(fixture_html("valid_page.html"), PAGE_URL)
        assert page == valid_page


class TestLinks:
    def test_same_host_only_drops_external_links(self):
        page = HTMLParser(same_host_only=True).parse(fixture_html("valid_page.html"), PAGE_URL)
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

    def test_rows_inside_hidden_elements_are_skipped(self, parser):
        html = """
            <table>
              <thead><template><tr><th>T</th></tr></template><tr><th>A</th></tr></thead>
              <template><tr><td>row template</td></tr></template>
              <noscript><tr><td>no-js row</td></tr></noscript>
              <tr><td>1</td></tr>
            </table>"""
        [table] = parser.extract_tables(soup(html))
        assert table["headers"] == ["A"]
        assert table["rows"] == [["1"]]

    def test_first_row_without_cells(self, parser):
        [table] = parser.extract_tables(soup("<table><tr></tr><tr><td>1</td></tr></table>"))
        assert table["headers"] == []
        assert table["rows"] == [["1"]]


class TestExtractText:
    def test_selector(self, parser):
        html = '<p class="lead">First</p><p>Other</p><p class="lead">Second</p>'
        assert parser.extract_text(soup(html), ".lead") == "First Second"

    def test_selector_skips_hidden_matches(self, parser):
        html = '<noscript><p class="x">hidden</p></noscript><p class="x">shown</p>'
        assert parser.extract_text(soup(html), ".x") == "shown"

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

    def test_ruby_annotations_are_kept(self, parser):
        # bs4 wraps <rt> and <rp> text in its own NavigableString subclasses.
        html = "<p>漢<ruby>字<rp>(</rp><rt>じ</rt><rp>)</rp></ruby>!</p>"
        assert parser.extract_text(soup(html)) == "漢字(じ)!"


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
    def test_broken_page_is_repaired(self, broken_page):
        assert broken_page["errors"] == []
        assert broken_page["title"] == "Broken <b>page"  # <title> content is plain text in HTML
        assert broken_page["headings"] == [{"level": 2, "text": "Heading inside link"}]
        assert broken_page["tables"] == [{"caption": None, "headers": [], "rows": [["cell 1", "cell 2"], ["cell 3"]]}]
        assert broken_page["lists"] == [{"type": "ul", "items": ["one", "two"]}]
        assert broken_page["images"] == [{"src": "https://example.com/pic.png", "alt": "unquoted"}]
        assert broken_page["links"] == ["https://example.com/in-heading", "https://example.com/last"]
        assert "First paragraph never closed Second block" in broken_page["text"]

    def test_deep_nesting_does_not_overflow(self, parser):
        page = parser.parse("<div>" * 20_000 + "deep", "https://example.com/")
        assert page["text"] == "deep"

    @pytest.mark.parametrize("html", ["", "   \n\t"])
    def test_empty_document(self, parser, html):
        with pytest.raises(ParseError, match="empty document"):
            parser.parse(html, "https://example.com/")

    def test_plain_text_is_reported_but_kept(self, parser):
        page = parser.parse("just some text", "https://example.com/")
        assert page["errors"] == ["no HTML markup found"]
        assert page["text"] == "just some text"

    def test_binary_garbage(self, parser):
        garbage = bytes(range(256)).decode("latin-1") * 4
        page = parser.parse(garbage, "https://example.com/")
        assert page["errors"] == []
        assert page["title"] is None
        assert page["links"] == []

    def test_missing_title_is_logged(self, parser, caplog):
        with caplog.at_level(logging.WARNING):
            parser.parse("<p>no title</p>", "https://example.com/")
        assert "No <title> on https://example.com/" in caplog.text

    def test_hidden_content_is_ignored_by_every_extractor(self, parser):
        html = """
            <head><title>Real</title></head>
            <body>
              <p>Shown</p> <a href="page">shown link</a>
              <noscript>
                <title>Hidden</title>
                <meta name="description" content="hidden description">
                <link rel="canonical" href="https://other.example/">
                <img src="https://tracker.example/pixel.gif" alt="">
                <h1>Enable JavaScript</h1>
                <a href="/nojs">no-js version</a>
                <ul><li>hidden item</li></ul>
              </noscript>
              <template>
                <base href="https://cdn.example/">
                <main><article>Template content</article></main>
                <h2>Row template</h2>
                <table><tr><td>cell</td></tr></table>
              </template>
            </body>"""
        page = parser.parse(html, "https://example.com/")
        assert page["title"] == "Real"
        assert page["metadata"]["description"] is None
        assert page["metadata"]["canonical"] is None
        assert page["text"] == "Shown shown link"
        assert page["images"] == []
        assert page["headings"] == []
        assert page["links"] == ["https://example.com/page"]
        assert page["lists"] == []
        assert page["tables"] == []

    @pytest.mark.parametrize("content_type", ["application/json", "image/png"])
    def test_unsupported_content_type(self, parser, content_type):
        with pytest.raises(ParseError, match=f"unsupported content type: {content_type}"):
            parser.parse("<p>looks like html</p>", "https://example.com/", content_type=content_type)

    @pytest.mark.parametrize(
        "content_type",
        ["text/html", "application/xhtml+xml", None, "TEXT/HTML", "text/html; charset=utf-8"],
    )
    def test_html_content_types(self, parser, content_type):
        page = parser.parse("<p>ok</p>", "https://example.com/", content_type=content_type)
        assert page["text"] == "ok"


class TestPartialResults:
    def test_failing_extractor_keeps_other_fields(self, parser, monkeypatch, caplog):
        def broken(*args):
            raise RuntimeError("boom")

        monkeypatch.setattr(parser, "extract_tables", broken)
        page = parser.parse(fixture_html("valid_page.html"), PAGE_URL)

        assert page["tables"] == []
        assert page["errors"] == ["tables: RuntimeError: boom"]
        assert page["title"] == "Garden Tools Catalog"
        assert len(page["links"]) == 7
        assert "Failed to extract tables" in caplog.text
        assert "RuntimeError: boom" in caplog.text  # traceback is logged

    def test_failing_metadata_leaves_empty_title(self, parser, monkeypatch, caplog):
        monkeypatch.setattr(parser, "extract_metadata", lambda *args: 1 / 0)
        page = parser.parse("<title>T</title><p>body</p>", "https://example.com/")
        assert page["title"] is None
        assert page["metadata"]["keywords"] == []
        assert page["text"] == "body"
        assert page["errors"] == ["metadata: ZeroDivisionError: division by zero"]
        assert "No <title>" not in caplog.text  # the failure is already reported

    def test_falls_back_to_html_parser(self, parser, monkeypatch, caplog):
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
        def always_fails(markup, features):
            raise ValueError(features)

        monkeypatch.setattr(parser_module, "BeautifulSoup", always_fails)
        with pytest.raises(ParseError, match="lxml parser failed: ValueError: lxml; html.parser parser failed"):
            parser.parse("<p>body</p>", "https://example.com/")


def test_parsed_tree_is_freed_without_the_garbage_collector():
    def tags() -> int:
        # The roots are few and small; the tags are the tree.
        return sum(isinstance(item, Tag) and not isinstance(item, BeautifulSoup) for item in gc.get_objects())

    parser = HTMLParser()
    html = fixture_html("valid_page.html")
    gc.collect()
    gc.disable()
    try:
        before = tags()
        page = parser.parse(html, "https://example.com/catalog/")
        after = tags()
    finally:
        gc.enable()

    assert page["links"] and page["text"]  # parsed in full before the tree was taken apart
    assert after == before


def test_parse_walks_the_tree_once_for_all_its_tags(parser, monkeypatch):
    walks = []
    find_all = BeautifulSoup.find_all

    def counting(self, *args, **kwargs):
        walks.append(args)
        return find_all(self, *args, **kwargs)

    # Set on the class of the root: searches inside one table or one list are not counted.
    monkeypatch.setattr(BeautifulSoup, "find_all", counting)
    page = parser.parse(fixture_html("valid_page.html"), PAGE_URL)

    assert page["links"] and page["headings"] and page["images"] and page["tables"] and page["lists"]
    assert walks == [(True,)]


def test_extractors_on_their_own_agree_with_parse(parser, valid_page):
    tree = soup(fixture_html("valid_page.html"))

    assert parser.extract_metadata(tree, PAGE_URL) == valid_page["metadata"]
    assert parser.extract_text(tree) == valid_page["text"]
    assert parser.extract_links(tree, PAGE_URL) == valid_page["links"]
    assert parser.extract_headings(tree) == valid_page["headings"]
    assert parser.extract_images(tree, PAGE_URL) == valid_page["images"]
    assert parser.extract_tables(tree) == valid_page["tables"]
    assert parser.extract_lists(tree) == valid_page["lists"]
