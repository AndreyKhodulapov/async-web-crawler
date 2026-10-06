"""Example: what JavaScript adds to a page, once rendered in a headless browser.

Usage:
    pip install -e .                                    # once: the crawler and Playwright
    playwright install chromium                         # once: the browser
    python examples/render_js.py                        # https://quotes.toscrape.com/js/
    python examples/render_js.py https://example.com/app/

The page is downloaded twice, as robots.txt and the rate limit allow:
without rendering, as the crawler sees it by default, and rendered in
Chromium, as a reader sees it. The quotes of quotes.toscrape.com/js/ are
written by JavaScript: without rendering the page has a header and a
footer, and no quote.
"""

import asyncio
import sys
import textwrap

from crawler import AsyncCrawler, FetchError, ParsedPage, Rendering

URL = "https://quotes.toscrape.com/js/"


async def main(url: str = URL) -> None:
    async with AsyncCrawler() as crawler:
        plain = await crawler.fetch_and_parse(url)
    # The browser starts with the first page to render and ends with the crawler.
    async with AsyncCrawler(rendering=Rendering()) as crawler:
        rendered = await crawler.fetch_and_parse(url)
        render_stats = crawler.render_stats()
    assert render_stats is not None  # the crawler renders

    print(url)
    print(f"  without rendering: {describe(plain)}")
    print(f"  with rendering:    {describe(rendered)}, rendered in {render_stats.avg_render_time:.2f}s")
    new_links = [link for link in rendered["links"] if link not in plain["links"]]
    print(f"  links only JavaScript shows: {', '.join(new_links) or 'none'}")
    print(f"  text: {textwrap.shorten(rendered['text'], 300, placeholder=' ...')}")


def describe(page: ParsedPage) -> str:
    return f"{len(page['text'])} characters of text, {len(page['links'])} links"


if __name__ == "__main__":
    try:
        asyncio.run(main(*sys.argv[1:2]))
    except FetchError as error:
        # The page could not be downloaded, or not rendered: Playwright or Chromium may be missing.
        sys.exit(f"error: {error}")
    except KeyboardInterrupt:
        sys.exit(130)
