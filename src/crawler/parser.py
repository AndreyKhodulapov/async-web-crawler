"""HTML parsing: turns a downloaded page into structured data."""

import asyncio
import logging
import re
import time
from collections.abc import Callable
from typing import Literal, TypedDict, TypeVar

from bs4 import BeautifulSoup
from bs4.element import CData, NavigableString, PageElement, Tag

from crawler.urls import is_same_host, resolve_url

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Content types that are parsed as HTML. A response without a Content-Type
# header is parsed too.
HTML_CONTENT_TYPES = frozenset({"text/html", "application/xhtml+xml"})

# Elements whose text is never shown to the reader as page content.
_NON_CONTENT_TAGS = frozenset({"script", "style", "noscript", "template", "head", "title"})
_LIST_TAGS = frozenset({"ul", "ol"})
# A list item's own text leaves out nested lists: they are reported separately.
_LIST_ITEM_SKIP = _NON_CONTENT_TAGS | _LIST_TAGS
_WHITESPACE = re.compile(r"\s+")
# Elements that start on a new line in a browser. Text on both sides of them
# is separated by a space; text inside inline elements (<b>, <a>, ...) is
# joined as is, so "<b>to</b>, go" stays "to, go".
_BLOCK_TAGS = frozenset(
    {
        "address", "article", "aside", "blockquote", "br", "caption", "dd", "details", "div", "dl", "dt",
        "fieldset", "figcaption", "figure", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "header",
        "hr", "li", "main", "nav", "ol", "option", "p", "pre", "section", "summary", "table", "td", "th",
        "tr", "ul",
    }
)  # fmt: skip


class Metadata(TypedDict):
    title: str | None
    description: str | None
    keywords: list[str]
    language: str | None
    canonical: str | None


class Image(TypedDict):
    src: str
    alt: str


class Heading(TypedDict):
    level: int
    text: str


class Table(TypedDict):
    caption: str | None
    headers: list[str]
    rows: list[list[str]]


class ItemList(TypedDict):
    type: Literal["ul", "ol"]
    items: list[str]


class ParsedPage(TypedDict):
    """Structured data extracted from one page.

    ``url`` is the requested URL, ``final_url`` the one after redirects;
    relative links are resolved against the latter. ``errors`` lists problems
    met while parsing; the fields they affected keep their empty defaults.
    """

    url: str
    final_url: str
    title: str | None
    text: str
    links: list[str]
    metadata: Metadata
    headings: list[Heading]
    images: list[Image]
    tables: list[Table]
    lists: list[ItemList]
    errors: list[str]


class HTMLParser:
    """Extracts text, links, metadata and structured elements from HTML.

    Parsing never raises: malformed markup is repaired by the parser, and if
    one extractor fails, the error is logged and recorded in
    ``ParsedPage["errors"]`` while the other fields are still filled in.

    With ``same_host_only=True``, links to other hosts are dropped.
    """

    def __init__(self, *, same_host_only: bool = False) -> None:
        self.same_host_only = same_host_only

    async def parse_html(
        self,
        html: str,
        url: str,
        *,
        final_url: str | None = None,
        content_type: str | None = None,
    ) -> ParsedPage:
        """Parse a page without blocking the event loop.

        BeautifulSoup is pure-Python, CPU-bound code: a large page takes tens
        of milliseconds, and running it on the loop would stall every other
        request. A worker thread keeps the loop responsive. Because of the GIL
        it gives no parallel speedup; that would need a process pool.
        """
        return await asyncio.to_thread(self.parse, html, url, final_url=final_url, content_type=content_type)

    def parse(
        self,
        html: str,
        url: str,
        *,
        final_url: str | None = None,
        content_type: str | None = None,
    ) -> ParsedPage:
        """Synchronous version of `parse_html`."""
        started = time.perf_counter()
        page = _empty_page(url, final_url or url)

        if content_type is not None and content_type not in HTML_CONTENT_TYPES:
            self._report(page, f"unsupported content type: {content_type}")
            return page
        if not html.strip():
            self._report(page, "empty document")
            return page
        soup = self._make_soup(html, page)
        if soup is None:
            return page
        if "<" not in html:
            self._report(page, "no HTML markup found")

        base_url = self._extract(page, "base_url", _base_url, soup, page["final_url"], default=page["final_url"])
        page["metadata"] = self._extract(
            page, "metadata", self.extract_metadata, soup, base_url, default=page["metadata"]
        )
        page["title"] = page["metadata"]["title"]
        page["text"] = self._extract(page, "text", self.extract_text, soup, default="")
        page["links"] = self._extract(page, "links", self.extract_links, soup, base_url, default=[])
        page["headings"] = self._extract(page, "headings", self.extract_headings, soup, default=[])
        page["images"] = self._extract(page, "images", self.extract_images, soup, base_url, default=[])
        page["tables"] = self._extract(page, "tables", self.extract_tables, soup, default=[])
        page["lists"] = self._extract(page, "lists", self.extract_lists, soup, default=[])

        if page["title"] is None:
            logger.warning("No <title> on %s", url)
        logger.info(
            "Parsed %s: text=%d chars, links=%d, images=%d, errors=%d, elapsed=%.3fs",
            url,
            len(page["text"]),
            len(page["links"]),
            len(page["images"]),
            len(page["errors"]),
            time.perf_counter() - started,
        )
        return page

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Return absolute URLs of all <a href> links, deduplicated, in page order."""
        links: dict[str, None] = {}  # an ordered set
        skipped = 0
        for anchor in soup.find_all("a", href=True):
            link = resolve_url(_attr(anchor, "href"), base_url)
            if link is None or (self.same_host_only and not is_same_host(link, base_url)):
                skipped += 1
                continue
            links[link] = None
        if skipped:
            logger.debug("Skipped %d links on %s", skipped, base_url)
        return list(links)

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        """Return the visible text of the page or of the elements matching `selector`.

        Without a selector the main content is used: <main>, else a single
        <article>, else <body>. Scripts, styles and similar elements are
        skipped and whitespace is collapsed.

        Raises:
            soupsieve.SelectorSyntaxError: `selector` is not valid CSS.
        """
        if selector is not None:
            roots = soup.select(selector)
        else:
            articles = soup.find_all("article", limit=2)
            # Several <article> elements usually mean a listing page, where
            # the whole body is the content.
            article = articles[0] if len(articles) == 1 else None
            roots = [soup.find("main") or article or soup.body or soup]
        return " ".join(filter(None, (_visible_text(root) for root in roots)))

    def extract_metadata(self, soup: BeautifulSoup, base_url: str | None = None) -> Metadata:
        """Return the title, description, keywords, language and canonical URL.

        Open Graph tags are used when the standard ones are missing. The
        canonical URL is resolved against `base_url` when it is given.
        """
        # An inline <svg> may have its own <title> (a tooltip); it is not the
        # page title. Documents without <head> put the real one in <body>.
        title_tag = next((tag for tag in soup.find_all("title") if tag.find_parent("svg") is None), None)
        title = _clean(title_tag.get_text()) if title_tag is not None else None
        keywords = _meta_content(soup, "name", "keywords") or ""

        canonical = None
        canonical_tag = soup.find("link", rel="canonical", href=True)
        if canonical_tag is not None:
            href = _attr(canonical_tag, "href").strip()
            canonical = resolve_url(href, base_url) if base_url else (href or None)

        language = None
        if soup.html is not None and isinstance(lang := soup.html.get("lang"), str):
            language = lang.strip() or None

        return Metadata(
            title=title or _meta_content(soup, "property", "og:title"),
            description=(
                _meta_content(soup, "name", "description") or _meta_content(soup, "property", "og:description")
            ),
            keywords=[word for word in map(str.strip, keywords.split(",")) if word],
            language=language,
            canonical=canonical,
        )

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[Image]:
        """Return every <img> with an absolute `src` and its `alt` text.

        Lazy-loaded images often keep the real address in `data-src`; it is
        used when `src` is missing or is not an http(s) URL (e.g. an inline
        "data:" placeholder). Images without a usable address are skipped.
        """
        images = []
        for img in soup.find_all("img"):
            src = None
            for attribute in ("src", "data-src"):
                if src := resolve_url(_attr(img, attribute), base_url):
                    break
            if src is not None:
                images.append(Image(src=src, alt=_clean(_attr(img, "alt"))))
        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[Heading]:
        """Return non-empty h1-h3 headings in document order."""
        headings = []
        for tag in soup.find_all(["h1", "h2", "h3"]):
            if text := _visible_text(tag):
                headings.append(Heading(level=int(tag.name[1]), text=text))
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[Table]:
        """Return tables as a caption, header cells and rows of cell text.

        Headers come from <thead>, or from the first row when all its cells
        are <th>. Rows of nested tables belong to those tables only.
        Colspan and rowspan are not expanded.
        """
        tables = []
        for table in soup.find_all("table"):
            rows = [row for row in table.find_all("tr") if row.find_parent("table") is table]
            headers: list[str] = []
            if rows:
                first = rows[0]
                cells = first.find_all(["th", "td"], recursive=False)
                in_thead = first.find_parent("thead") is not None
                if cells and (in_thead or all(cell.name == "th" for cell in cells)):
                    headers = [_visible_text(cell) for cell in cells]
                    rows = rows[1:]
            body = [
                [_visible_text(cell) for cell in cells]
                for row in rows
                if (cells := row.find_all(["th", "td"], recursive=False))
            ]
            caption = table.find("caption")
            caption_text = _visible_text(caption) if caption is not None else ""
            tables.append(Table(caption=caption_text or None, headers=headers, rows=body))
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[ItemList]:
        """Return every non-empty <ul>/<ol> with the text of its own items.

        A nested list is returned separately, and its text is not repeated
        in the parent item.
        """
        lists = []
        for tag in soup.find_all(list(_LIST_TAGS)):
            texts = (_visible_text(item, skip=_LIST_ITEM_SKIP) for item in tag.find_all("li", recursive=False))
            items = [text for text in texts if text]
            if items:
                lists.append(ItemList(type="ol" if tag.name == "ol" else "ul", items=items))
        return lists

    def _make_soup(self, html: str, page: ParsedPage) -> BeautifulSoup | None:
        # lxml is fast and lenient; the pure-Python parser is a fallback for
        # the rare input lxml itself cannot handle.
        for features in ("lxml", "html.parser"):
            try:
                return BeautifulSoup(html, features)
            except Exception as exc:
                logger.warning("%s parser failed on %s", features, page["url"], exc_info=True)
                page["errors"].append(f"{features} parser failed: {type(exc).__name__}: {exc}")
        return None

    def _extract(
        self,
        page: ParsedPage,
        field: str,
        extractor: Callable[..., T],
        *args: object,
        default: T,
    ) -> T:
        """Run one extractor; on failure, record the error and return `default`."""
        try:
            return extractor(*args)
        except Exception as exc:
            logger.warning("Failed to extract %s from %s", field, page["url"], exc_info=True)
            page["errors"].append(f"{field}: {type(exc).__name__}: {exc}")
            return default

    @staticmethod
    def _report(page: ParsedPage, problem: str) -> None:
        logger.warning("%s: %s", page["url"], problem)
        page["errors"].append(problem)


def _empty_page(url: str, final_url: str) -> ParsedPage:
    return ParsedPage(
        url=url,
        final_url=final_url,
        title=None,
        text="",
        links=[],
        metadata=Metadata(title=None, description=None, keywords=[], language=None, canonical=None),
        headings=[],
        images=[],
        tables=[],
        lists=[],
        errors=[],
    )


def _base_url(soup: BeautifulSoup, page_url: str) -> str:
    """Return the URL relative links are resolved against: <base href> or the page URL."""
    base = soup.find("base", href=True)
    if base is not None:
        return resolve_url(_attr(base, "href"), page_url) or page_url
    return page_url


def _meta_content(soup: BeautifulSoup, attribute: str, value: str) -> str | None:
    pattern = re.compile(f"^{re.escape(value)}$", re.IGNORECASE)
    tag = soup.find("meta", attrs={attribute: pattern, "content": True})
    if tag is None:
        return None
    return _clean(_attr(tag, "content")) or None


def _visible_text(root: Tag, skip: frozenset[str] = _NON_CONTENT_TAGS) -> str:
    """Collect text under `root`, skipping elements named in `skip`.

    Iterative rather than recursive: broken HTML can nest thousands of
    unclosed tags, which would exceed Python's recursion limit.
    """
    parts: list[str] = []
    # None marks the end of a block element: a space is emitted there.
    stack: list[PageElement | None] = [root]
    while stack:
        node = stack.pop()
        if node is None:
            parts.append(" ")
        elif isinstance(node, Tag):
            if node is root or node.name not in skip:
                if node.name in _BLOCK_TAGS:
                    parts.append(" ")
                    stack.append(None)
                stack.extend(reversed(node.contents))
        # Subclasses such as Comment, Doctype or Script are not page text.
        elif isinstance(node, NavigableString) and type(node) in (NavigableString, CData):
            parts.append(node)
    return _clean("".join(parts))


def _attr(tag: Tag, name: str) -> str:
    """Return an attribute as a string ("" if missing); bs4 may return a list."""
    value = tag.get(name)
    if isinstance(value, list):
        return " ".join(value)
    return value or ""


def _clean(text: str) -> str:
    return _WHITESPACE.sub(" ", text).strip()
