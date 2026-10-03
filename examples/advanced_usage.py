"""Example: a crawl set up by a configuration file, with live progress, statistics and a report.

Usage:
    pip install -e .                                    # once, makes `crawler` importable
    python examples/advanced_usage.py                   # examples/config.yaml
    python examples/advanced_usage.py my_config.yaml

The configuration says what to crawl and where the pages, the log and the
statistics go (see examples/config.yaml). The example adds what code is
needed for: it shows the progress, reads the statistics and the pages, and
writes the HTML report next to the other files.
"""

import asyncio
import sys
from pathlib import Path

from crawler import AdvancedCrawler, ConfigError, show_progress

CONFIG = Path(__file__).with_name("config.yaml")


async def main(config_path: str | Path = CONFIG) -> None:
    crawler = AdvancedCrawler.from_config(config_path)
    # crawl() alone is enough; run as a task, it can be watched while it works.
    crawl = asyncio.create_task(crawler.crawl())
    try:
        await show_progress(crawler.crawler, crawl, crawler.config.crawler.max_pages)
        pages = await crawl

        stats = crawler.get_stats()
        print(f"\nProcessed: {stats['total_pages']} pages in {stats['elapsed_seconds']:.1f}s")
        print(f"Successful: {stats['successful']}")
        print(f"Failed: {stats['failed']}")
        print(f"Status codes: {stats['status_codes']}")
        print(f"Top domains: {stats['top_domains']}")

        for url, page in list(pages.items())[:5]:
            print(f"  {page['title']!r}, {len(page['links'])} links: {url}")

        report = Path("out/report.html")
        crawler.export_to_html_report(report)
        print(f"Report: {report}")
        if crawler.config.storage.outputs:
            print(f"Pages: {', '.join(crawler.config.storage.outputs)}")
    finally:
        # Interrupted (Ctrl-C), the crawl is still running: stop it first.
        crawl.cancel()
        await asyncio.gather(crawl, return_exceptions=True)
        await crawler.close()  # writes what the storage still holds, closes the log file


if __name__ == "__main__":
    try:
        asyncio.run(main(*sys.argv[1:2]))
    except ConfigError as error:
        sys.exit(f"error: {error}")
    except KeyboardInterrupt:
        sys.exit(130)
