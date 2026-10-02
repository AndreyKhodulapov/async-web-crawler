"""HTML parsing: turns a downloaded page into structured data."""

import asyncio
import logging
import re
import time
from collections.abc import Callable
from typing import TypeVar

from bs4 import BeautifulSoup
from bs4.element import Comment, Declaration, Doctype, NavigableString, PageElement, ProcessingInstruction, Tag

from crawler.exceptions import ParseError
from crawler.models import Heading, Image, ItemList, Metadata, ParsedPage, Table
from crawler.urls import is_same_host, resolve_url

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Elements a browser never renders: everything inside them, from text to a
# <base> or <main>, is left out of every field.
_HIDDEN_TAGS = frozenset({"script", "style", "noscript", "template"})
# The document head holds metadata, not page content: its text, links and
# images are left out too. Metadata extractors look into it on purpose.
_NON_CONTENT_TAGS = _HIDDEN_TAGS | {"head", "title"}
_ARTICLE = frozenset({"article"})
_SVG = frozenset({"svg"})


class HTMLParser:
    """Extracts text, links, metadata and structured elements from HTML.

    Malformed markup is repaired by the parser, and if one extractor fails,
    the error is logged and recorded in `ParsedPage["errors"]` while the
    other fields are still filled in. Only input that is not an HTML
    document at all raises `ParseError`.
    Content inside <noscript>, <template>, <script> and <style> is ignored
    by every extractor.

    With `same_host_only=True`, links to other hosts are dropped.
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

        Raises:
            ParseError: the content type is not HTML, the document is empty,
                or no parser could read it.
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

        if not is_html_content_type(content_type):
            raise ParseError(url, f"unsupported content type: {content_type}")
        if not html.strip():
            raise ParseError(url, "empty document")
        soup = self._make_soup(html, page)
        try:
            if "<" not in html:
                self._report(page, "no HTML markup found")

            base_url = self._extract(page, "<base href>", _base_url, soup, page["final_url"], default=page["final_url"])
            metadata = self._extract(page, "metadata", self.extract_metadata, soup, base_url, default=None)
            if metadata is not None:
                page["metadata"] = metadata
                page["title"] = metadata["title"]
                if page["title"] is None:
                    logger.warning("No <title> on %s", url)
            page["text"] = self._extract(page, "text", self.extract_text, soup, default="")
            page["links"] = self._extract(
                page, "links", self.extract_links, soup, base_url, page["final_url"], default=[]
            )
            page["headings"] = self._extract(page, "headings", self.extract_headings, soup, default=[])
            page["images"] = self._extract(page, "images", self.extract_images, soup, base_url, default=[])
            page["tables"] = self._extract(page, "tables", self.extract_tables, soup, default=[])
            page["lists"] = self._extract(page, "lists", self.extract_lists, soup, default=[])
        finally:
            # A parsed tree is full of reference cycles (parent and child,
            # siblings), so only the garbage collector could free it, at a
            # time of its own choosing: trees of the pages parsed meanwhile
            # would pile up. Taken apart, it is freed right here. (The
            # root's own decompose() does not reach its children.)
            soup.clear(decompose=True)

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

    def extract_links(self, soup: BeautifulSoup, base_url: str, page_url: str | None = None) -> list[str]:
        """Return absolute URLs of all <a href> links, deduplicated, in page order.

        Relative links are resolved against `base_url`. With `same_host_only`,
        links are kept only if they share the host of `page_url`: a <base href>
        may point to another host, such as a CDN, which is not the site itself.
        `page_url` defaults to `base_url`.
        """
        own_url = page_url or base_url
        links: dict[str, None] = {}  # an ordered set
        skipped = 0
        for anchor in _content_tags(soup, "a", href=True):
            link = resolve_url(_attr(anchor, "href"), base_url)
            if link is None or (self.same_host_only and not is_same_host(link, own_url)):
                skipped += 1
                continue
            links[link] = None
        if skipped:
            logger.debug("Skipped %d links on %s", skipped, base_url)
        return list(links)

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        """Return the visible text of the page or of the elements matching `selector`.

        Without a selector the main content is used: <main>, else a single
        top-level <article>, else <body>. Scripts, styles and similar
        elements are skipped and whitespace is collapsed. An element that is
        inside another matched element is not counted twice.

        Raises:
            soupsieve.SelectorSyntaxError: `selector` is not valid CSS.
        """
        if selector is not None:
            matches = [tag for tag in soup.select(selector) if not _inside(tag, _HIDDEN_TAGS)]
            matched = {id(tag) for tag in matches}
            # The text of a nested match is already part of its ancestor's.
            roots = [tag for tag in matches if not any(id(parent) in matched for parent in tag.parents)]
        else:
            # Several top-level <article> elements usually mean a listing
            # page, where the whole body is the content. Nested ones, such as
            # comments inside a post, belong to their article.
            articles = [tag for tag in _content_tags(soup, "article") if not _inside(tag, _ARTICLE)]
            article = articles[0] if len(articles) == 1 else None
            main = _first_content_tag(soup, "main")
            roots = [main or article or soup.body or soup]
        return " ".join(filter(None, (_visible_text(root) for root in roots)))

    def extract_metadata(self, soup: BeautifulSoup, base_url: str | None = None) -> Metadata:
        """Return the title, description, keywords, language and canonical URL.

        Open Graph tags are used when the standard ones are missing. The
        canonical URL is resolved against `base_url` when it is given.
        """
        # An inline <svg> may have its own <title> (a tooltip); it is not the
        # page title. Documents without <head> put the real one in <body>.
        titles = _content_tags(soup, "title", skip=_HIDDEN_TAGS)
        title_tag = next((tag for tag in titles if not _inside(tag, _SVG)), None)
        title = _clean(title_tag.get_text()) if title_tag is not None else None
        keywords = _meta_content(soup, "name", "keywords") or ""

        canonical = None
        # rel values are case-insensitive: "Canonical" is valid too.
        rel = re.compile("^canonical$", re.IGNORECASE)
        canonical_tag = _first_content_tag(soup, "link", skip=_HIDDEN_TAGS, rel=rel, href=True)
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
        for img in _content_tags(soup, "img"):
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
        for tag in _content_tags(soup, ["h1", "h2", "h3"]):
            if text := _visible_text(tag):
                headings.append(Heading(level=int(tag.name[1]), text=text))
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[Table]:
        """Return tables as a caption, header cells and rows of cell text.

        Headers come from <thead>, or from the first row when all its cells
        are <th>. If <thead> has several rows, the last one is used: it names
        the columns, while the rows above usually group them. Rows of nested
        tables belong to those tables only. Colspan and rowspan are not
        expanded.
        """
        tables = []
        for table in _content_tags(soup, "table"):
            head_rows: list[Tag] = []
            rows: list[Tag] = []
            for row in table.find_all("tr"):
                # The table itself is visible, so a hidden ancestor of a row
                # (a <template> with a row template, a <noscript>) is inside it.
                if row.find_parent("table") is not table or _inside(row, _HIDDEN_TAGS):
                    continue
                in_thead = row.parent is not None and row.parent.name == "thead"
                (head_rows if in_thead else rows).append(row)
            if not head_rows and rows:
                first_cells = _cells(rows[0])
                if first_cells and all(cell.name == "th" for cell in first_cells):
                    head_rows, rows = rows[:1], rows[1:]
            headers = [_visible_text(cell) for cell in _cells(head_rows[-1])] if head_rows else []
            row_texts = [[_visible_text(cell) for cell in cells] for row in rows if (cells := _cells(row))]
            # A caption is always a direct child; a deeper one is a nested table's.
            caption = table.find("caption", recursive=False)
            caption_text = _visible_text(caption) if caption is not None else ""
            tables.append(Table(caption=caption_text or None, headers=headers, rows=row_texts))
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[ItemList]:
        """Return every non-empty <ul>/<ol> with the text of its own items.

        A nested list is returned separately, and its text is not repeated
        in the parent item.
        """
        lists = []
        # A list item's own text leaves out nested lists: they are reported separately.
        item_skip = _NON_CONTENT_TAGS | {"ul", "ol"}
        for tag in _content_tags(soup, ["ul", "ol"]):
            texts = (_visible_text(item, skip=item_skip) for item in tag.find_all("li", recursive=False))
            items = [text for text in texts if text]
            if items:
                lists.append(ItemList(type="ol" if tag.name == "ol" else "ul", items=items))
        return lists

    @staticmethod
    def _make_soup(html: str, page: ParsedPage) -> BeautifulSoup:
        # lxml is fast and lenient; the pure-Python parser is a fallback for
        # the rare input lxml itself cannot handle.
        for features in ("lxml", "html.parser"):
            try:
                return BeautifulSoup(html, features)
            except Exception as exc:
                logger.warning("%s parser failed on %s", features, page["url"], exc_info=True)
                page["errors"].append(f"{features} parser failed: {type(exc).__name__}: {exc}")
        raise ParseError(page["url"], "; ".join(page["errors"]))

    @staticmethod
    def _extract(
        page: ParsedPage,
        step: str,
        extractor: Callable[..., T],
        *args: object,
        default: T,
    ) -> T:
        """Run one extractor; on failure, record the error and return `default`."""
        try:
            return extractor(*args)
        except Exception as exc:
            logger.warning("Failed to extract %s from %s", step, page["url"], exc_info=True)
            page["errors"].append(f"{step}: {type(exc).__name__}: {exc}")
            return default

    @staticmethod
    def _report(page: ParsedPage, problem: str) -> None:
        logger.warning("%s: %s", page["url"], problem)
        page["errors"].append(problem)


def is_html_content_type(content_type: str | None) -> bool:
    """Return True for an HTML media type, or when the type is unknown (None).

    Media types are case-insensitive and may carry parameters
    ("text/html; charset=utf-8").
    """
    if content_type is None:
        return True
    return content_type.split(";")[0].strip().lower() in ("text/html", "application/xhtml+xml")


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
    base = _first_content_tag(soup, "base", skip=_HIDDEN_TAGS, href=True)
    if base is not None:
        return resolve_url(_attr(base, "href"), page_url) or page_url
    return page_url


def _meta_content(soup: BeautifulSoup, attribute: str, value: str) -> str | None:
    """Return the first non-empty content of <meta attribute=value>, if any."""
    pattern = re.compile(f"^{re.escape(value)}$", re.IGNORECASE)
    for tag in _content_tags(soup, "meta", skip=_HIDDEN_TAGS, attrs={attribute: pattern, "content": True}):
        if content := _clean(_attr(tag, "content")):
            return content
    return None


def _cells(row: Tag) -> list[Tag]:
    return row.find_all(["th", "td"], recursive=False)


def _content_tags(
    soup: BeautifulSoup, name: str | list[str], *, skip: frozenset[str] = _NON_CONTENT_TAGS, **filters: object
) -> list[Tag]:
    """Find tags by name, leaving out those inside elements named in `skip`.

    `filters` are passed on to `find_all`: attribute values or `attrs`.
    """
    return [tag for tag in soup.find_all(name, **filters) if not _inside(tag, skip)]


def _inside(tag: Tag, names: frozenset[str]) -> bool:
    """Whether an ancestor of `tag` is an element named in `names`.

    A plain walk up the tree: `find_parent` builds a filter on every call,
    which costs more than the walk itself and adds up over every tag of a page.
    """
    return any(parent.name in names for parent in tag.parents)


def _first_content_tag(
    soup: BeautifulSoup, name: str, *, skip: frozenset[str] = _NON_CONTENT_TAGS, **filters: object
) -> Tag | None:
    return next(iter(_content_tags(soup, name, skip=skip, **filters)), None)


def _visible_text(root: Tag, skip: frozenset[str] = _NON_CONTENT_TAGS) -> str:
    """Collect text under `root`, skipping elements named in `skip`.

    Iterative rather than recursive: broken HTML can nest thousands of
    unclosed tags, which would exceed Python's recursion limit.
    """
    # Elements that start on a new line in a browser. Text on both sides of
    # them is separated by a space; text inside inline elements (<b>, <a>, ...)
    # is joined as is, so "<b>to</b>, go" stays "to, go".
    block_tags = {
        "address", "article", "aside", "blockquote", "br", "caption", "dd", "details", "div", "dl", "dt",
        "fieldset", "figcaption", "figure", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "header",
        "hr", "li", "main", "nav", "ol", "option", "p", "pre", "section", "summary", "table", "td", "th",
        "tr", "ul",
    }  # fmt: skip
    parts: list[str] = []
    # None marks the end of a block element: a space is emitted there.
    stack: list[PageElement | None] = [root]
    while stack:
        node = stack.pop()
        if node is None:
            parts.append(" ")
        elif isinstance(node, Tag):
            if node is root or node.name not in skip:
                if node.name in block_tags:
                    parts.append(" ")
                    stack.append(None)
                stack.extend(reversed(node.contents))
        # Markup-level strings are not page text. Other subclasses are: bs4
        # wraps the text of <rt> and <rp> in its own classes.
        elif isinstance(node, NavigableString) and not isinstance(
            node, (Comment, Declaration, Doctype, ProcessingInstruction)
        ):
            parts.append(node)
    return _clean("".join(parts))


def _attr(tag: Tag, name: str) -> str:
    """Return an attribute as a string ("" if missing); bs4 may return a list."""
    value = tag.get(name)
    if isinstance(value, list):
        return " ".join(value)
    return value or ""


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
