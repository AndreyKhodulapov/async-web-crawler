"""Sitemaps (sitemaps.org protocol): downloading, parsing and following sitemap indexes."""

import asyncio
import logging
import zlib
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing
from typing import NamedTuple

from lxml import etree

from crawler.exceptions import FetchError, SitemapError
from crawler.urls import normalize_url

logger = logging.getLogger(__name__)


class _Sitemap(NamedTuple):
    is_index: bool
    locations: list[str]  # normalized URLs: of pages, or of other sitemaps in an index


class SitemapParser:
    """Collects the page URLs a sitemap lists, following sitemap indexes.

    Usage::

        sitemaps = SitemapParser(fetch)               # fetch(url) -> body
        urls = await sitemaps.fetch_sitemap("https://example.com/sitemap.xml")

    `fetch` downloads a URL and returns the body as bytes; it raises
    `FetchError` when there is no successful response.

    A sitemap index lists other sitemaps, which are downloaded too, at most
    `CONCURRENCY` at a time, and may be indexes themselves. Every sitemap is
    downloaded once, so indexes that list each other do not loop, and at
    most `MAX_FILES` of them are downloaded for one call. A sitemap listed
    in an index that cannot be downloaded or read is logged and left out.

    Gzipped sitemaps (.xml.gz) are unpacked, those made of several gzip
    members too. A sitemap over `MAX_SIZE` bytes, the limit of the
    protocol, is rejected, before or after unpacking; the crawler stops
    downloading one at that size. Entities are not expanded and nothing
    outside the document is loaded while parsing it.

    URLs are returned normalized and without duplicates, in the order they
    are listed; those that are not valid http(s) URLs are dropped. No more
    than `max_urls` are returned. `iter_pages` yields them as the files are
    read, for a caller that may need only the first of them.
    """

    MAX_SIZE = 50 * 1024 * 1024
    MAX_FILES = 500
    CONCURRENCY = 5

    def __init__(self, fetch: Callable[[str], Awaitable[bytes]], *, max_urls: int = 50_000) -> None:
        if max_urls < 1:
            raise ValueError(f"max_urls must be >= 1, got {max_urls}")
        self._fetch = fetch
        self.max_urls = max_urls

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """Download a sitemap and return the page URLs it lists, those of nested sitemaps included.

        Raises:
            ValueError: `sitemap_url` is not a valid http(s) URL.
            FetchError: `sitemap_url` could not be downloaded.
            SitemapError: it was downloaded but is not a sitemap.
        """
        pages = []
        async with aclosing(self.iter_pages(sitemap_url)) as batches:
            async for batch in batches:
                pages.extend(batch)
        return pages

    async def iter_pages(self, sitemap_url: str) -> AsyncGenerator[list[str], None]:
        """Download a sitemap and yield the page URLs that `fetch_sitemap` returns, as its files are read.

        After every batch of up to `CONCURRENCY` files, yields the pages of
        the batch that were not yielded before. Nothing more is downloaded
        once the caller stops reading: a caller that needs a few pages
        reads only the first files of a large index (close the generator
        with `contextlib.aclosing`). Raises as `fetch_sitemap` does, before
        the first pages are yielded.
        """
        root = normalize_url(sitemap_url)
        if root is None:
            raise ValueError(f"not an absolute http(s) URL: {sitemap_url!r}")
        pages: dict[str, None] = {}
        known = {root}
        pending = deque([root])
        cut = False
        while pending and len(pages) < self.max_urls:
            batch = [pending.popleft() for _ in range(min(len(pending), self.CONCURRENCY))]
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(self._load(url)) for url in batch]
            new: dict[str, None] = {}
            for url, task in zip(batch, tasks, strict=True):
                sitemap = task.result()
                if isinstance(sitemap, FetchError):
                    if url == root:
                        raise sitemap
                    logger.warning("Skipped sitemap %s: %s: %s", url, type(sitemap).__name__, sitemap.message)
                elif sitemap.is_index:
                    pending.extend(self._new_sitemaps(url, sitemap.locations, known))
                else:
                    new.update(dict.fromkeys(location for location in sitemap.locations if location not in pages))
            taken = list(new)[: self.max_urls - len(pages)]
            cut = len(taken) < len(new)
            if taken:
                pages.update(dict.fromkeys(taken))
                # Out of the task group: a caller that stops here leaves no download running.
                yield taken
        if cut or pending:
            logger.warning("Sitemap %s lists more than %d URLs, the rest are left out", root, self.max_urls)
        logger.info("Sitemap %s: %d URLs in %d files", root, len(pages), len(known))

    def _new_sitemaps(self, index_url: str, locations: list[str], known: set[str]) -> list[str]:
        """The sitemaps of an index that are not known yet, as many as `MAX_FILES` allows; adds them to `known`."""
        new = list(dict.fromkeys(location for location in locations if location not in known))
        allowed = new[: self.MAX_FILES - len(known)]
        if len(allowed) < len(new):
            logger.warning(
                "Sitemap index %s: %d sitemaps left out, over the limit of %d files",
                index_url,
                len(new) - len(allowed),
                self.MAX_FILES,
            )
        known.update(allowed)
        return allowed

    async def _load(self, url: str) -> _Sitemap | FetchError:
        """Download and parse one sitemap; a failure is returned, so it does not cancel the others."""
        try:
            body = await self._fetch(url)
            # A sitemap may hold 50,000 URLs: parsed off the event loop.
            return await asyncio.to_thread(_parse, url, body, self.MAX_SIZE)
        except FetchError as error:
            return error


def _parse(url: str, body: bytes, max_size: int) -> _Sitemap:
    document = _unpack(url, body, max_size)
    parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)
    try:
        # Some sites send blank lines before the XML declaration, which XML does not allow.
        root = etree.fromstring(document.lstrip(), parser)
    except etree.XMLSyntaxError as exc:
        raise SitemapError(url, f"not an XML document: {exc}") from exc
    # Matched by local name: sitemaps in the wild use several versions of
    # the namespace, or none.
    kind = etree.QName(root).localname
    if kind not in ("urlset", "sitemapindex"):
        raise SitemapError(url, f"not a sitemap: the root element is <{kind}>")
    is_index = kind == "sitemapindex"
    entries = root.xpath("*[local-name()=$entry]/*[local-name()='loc']", entry="sitemap" if is_index else "url")
    locations = []
    for entry in entries:
        location = normalize_url(entry.text or "")
        if location is None:
            logger.debug("Sitemap %s: skipped an invalid URL %r", url, entry.text)
        else:
            locations.append(location)
    return _Sitemap(is_index, locations)


def _unpack(url: str, body: bytes, max_size: int) -> bytes:
    """The XML of a sitemap, unpacked if it is gzipped; told by the content, not by the name or headers."""
    if body.startswith(b"\x1f\x8b"):
        packed, parts, size = body, [], 0
        try:
            # An archive may be several gzip members one after another, which
            # unpack into one document. Stops at the limit: a small archive
            # can unpack into gigabytes.
            while packed.startswith(b"\x1f\x8b") and size <= max_size:
                member = zlib.decompressobj(wbits=31)
                parts.append(member.decompress(packed, max_size + 1 - size))
                size += len(parts[-1])
                packed = member.unused_data
        except zlib.error as exc:
            raise SitemapError(url, f"broken gzip archive: {exc}") from exc
        body = b"".join(parts)
    if len(body) > max_size:
        raise SitemapError(url, f"larger than {max_size} bytes")
    return body
