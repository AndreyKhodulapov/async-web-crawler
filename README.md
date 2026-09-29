# async-web-crawler

An asynchronous web crawler built on `asyncio`, `aiohttp` and BeautifulSoup.
It downloads many pages concurrently over a shared connection pool, limits
concurrency, applies timeouts, and reports failures without stopping the rest
of the batch. Downloaded pages are parsed into structured data: title,
metadata, text, absolute links, images, headings, tables and lists. Starting
from a few URLs, it can crawl a whole site: it follows links breadth-first up
to a given depth, never fetches a page twice, and shows live progress.

## Features

- Concurrent downloads with a global concurrency limit and an optional
  per-domain limit (`SemaphoreManager`)
- Site crawling with a priority queue of URLs (`CrawlerQueue`), a pool of
  workers, depth and page limits, deduplication of normalized URLs, and
  filters: same domain only, include and exclude regular expressions
- Live crawl statistics: pages done, queued, failed, requests in flight,
  pages per second
- Connection pooling and keep-alive via a single `aiohttp.ClientSession`
- Separate connect, read and total timeouts
- Clear error types: `HTTPStatusError`, `NetworkError` (including redirect
  loops), `FetchTimeoutError`, `InvalidURLError`, `CrawlerClosedError` and
  `UnexpectedError`, all subclasses of `FetchError`
- One failing URL never breaks a batch: even unforeseen exceptions are
  logged with a traceback and reported as `UnexpectedError`
- Logging for every request: start, success (status, size, time) and failure
- HTML parsing with `lxml` (falling back to `html.parser`), run in a worker
  thread so that it does not block the event loop
- Relative links resolved against `<base href>` or the final URL after
  redirects, normalized, deduplicated and validated; external links can be
  filtered out
- Page encoding is taken from the byte order mark, else the `Content-Type`
  header, else `<meta charset>`
- Broken HTML is repaired by the parser; a failing extractor is logged and
  reported in `errors`, and the other fields are still returned
- Content that a browser with JavaScript does not render (`<script>`, `<style>`,
  `<noscript>`, `<template>`) is left out of every field, including links and images

## Requirements

Python 3.11+

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt    # runtime + test and lint tools
pip install -e .                       # or: the package alone, runtime deps only
```

## Demo

The demo has three commands: `crawl` follows links from start pages, `parse`
extracts data from pages, and `benchmark` compares sequential and concurrent
fetching. All of them accept `--concurrency`, `--timeout` and `--log-level`.
Logs and progress go to stderr; the report goes to stdout.

### crawl

```bash
python src/main.py crawl                                     # books.toscrape.com, depth 2, 50 pages
python src/main.py crawl https://books.toscrape.com/ --max-depth 1 --max-pages 20 --same-domain
python src/main.py crawl --exclude '/category/' --include '/catalogue/' --json crawl.json
python src/main.py crawl --per-domain 4 --concurrency 20     # at most 4 requests to one host at a time
```

The default start page is a sandbox made for crawling practice. robots.txt
is not checked yet, so point the crawler only at sites that allow crawling.
While it runs, a progress line is updated every second:

```
pages 25 | failed 0 | skipped 0 | queued 15 | in progress 5 | requests 2 | 5.0 pages/s | 5.0s
```

`in progress` counts pages taken by workers; `requests` counts those actually
being downloaded, which never exceeds `--per-domain` for a single site.
Warnings, such as a failed page, are printed above the line; per-request logs
are hidden by default, pass `--log-level INFO` to see them. At the end it prints every page in the order it
was found:

```
=== Crawl (30 pages, 5.57s) ===
DEPTH  RESULT                                LINKS  URL
    0  ok                                       73  https://books.toscrape.com/
    1  ok                                        3  https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html
    ...
    2  ok                                        9  https://books.toscrape.com/catalogue/in-her-wake_980/index.html
Crawled: 30 pages, failed: 0, skipped: 0, left in queue: 15, speed: 5.4 pages/s
```

With `--json`, the parsed pages (with their depth), the failed and skipped
URLs with the reasons and the statistics are saved to a file.

### parse

```bash
python src/main.py parse
python src/main.py parse https://example.com --same-host  # internal links only
python src/main.py parse --preview 10 --json pages.json   # save full results
```

By default it parses real sites of different kinds:

- Wikipedia: a large server-rendered page.
- A scraping sandbox with tables. It answers HTTP 429 when requests come too
  fast.
- A URL that answers HTTP 403, as sites behind anti-bot protection do, which
  shows how a failed page is reported.

For every page it prints a summary, then a table with statistics:

```
=== https://en.wikipedia.org/wiki/Main_Page ===
{
  "url": "https://en.wikipedia.org/wiki/Main_Page",
  "title": "Wikipedia, the free encyclopedia",
  "description": null,
  "text_length": 11609,
  "text_preview": "Main Page Main Page Talk English Read View source View history Tools...",
  "links_count": 632,
  "internal_links": 175,
  "external_links": 457,
  "links": [
    "https://en.wikipedia.org/wiki/Main_Page",
    "https://en.wikipedia.org/wiki/Wikipedia:Contents",
    ...
    "... and 627 more"
  ],
  "images_count": 22,
  "headings": ["h1: Main Page", "h1: Welcome to Wikipedia", ...],
  "tables_count": 1,
  "lists_count": 35,
  "errors": []
}
...
=== Summary (4 pages, 0.91s) ===
URL                                      RESULT                      TEXT     LINKS    IMAGES  HEADINGS    TABLES     LISTS
https://en.wikipedia.org/wiki/Main_Page  ok                         11609       632        22        10         1        35
https://apilearn.tukas.dev/              ok                         12235        28         2        12         0        14
https://apilearn.tukas.dev/exercises/    ok                         35331        40         1         3         6         7
https://httpbin.org/status/403           HTTPStatusError 403
Parsed: 3/4 pages, links: 700, text: 59175 chars
```

### benchmark

```bash
python src/main.py benchmark
```

The benchmark fetches ten URLs twice: once sequentially and once concurrently. The
list includes fast pages, slow `httpbin.org/delay/*` endpoints, HTTP 404/500, a
request that exceeds the timeout and a non-existent domain. For each run it
prints the status, size and time of every request, the total time and the
speedup.

```bash
python src/main.py benchmark --concurrency 3 --timeout 3        # tune the crawler
python src/main.py benchmark https://example.com https://python.org  # custom URLs
python src/main.py benchmark --log-level WARNING                # errors only
```

Sample report (logs omitted):

```
=== Sequential ===
URL                                  STATUS                       SIZE    TIME
https://example.com                  200                          559B   0.28s
https://httpbin.org/delay/2          200                          359B   2.23s
https://httpbin.org/status/404       HTTPStatusError 404            0B   0.18s
https://httpbin.org/delay/10         FetchTimeoutError              0B   5.76s
https://nonexistent-domain.invalid   NetworkError                   0B   0.00s
...
Succeeded: 6/10, total time: 11.59s

=== Concurrent (max_concurrent=10) ===
...
Succeeded: 6/10, total time: 6.00s

Speedup: 1.9x
```

A concurrent run takes about as long as its slowest request. In the default
list that is the request that hits the timeout. `SIZE` is the body size after
content decoding (gzip, deflate), so it can exceed the bytes transferred.

## Usage

```python
import asyncio

from crawler import AsyncCrawler


async def main() -> None:
    async with AsyncCrawler(max_concurrent=5, total_timeout=10) as crawler:
        pages = await crawler.fetch_urls([
            "https://example.com",
            "https://httpbin.org/delay/1",
        ])
        print(f"Fetched {len(pages)} pages")

        page = await crawler.fetch_and_parse("https://en.wikipedia.org/wiki/Main_Page")
        print(page["title"], len(page["links"]), page["links"][:3])

    async with AsyncCrawler(max_concurrent=10, max_depth=2, max_per_domain=2) as crawler:
        results = await crawler.crawl(
            start_urls=["https://books.toscrape.com/"],
            max_pages=50,
            same_domain_only=True,
            exclude_patterns=[r"/category/"],
        )
        print(f"Crawled {len(results)} pages, failed: {len(crawler.failed_urls)}")


asyncio.run(main())
```

| Method | Returns | On failure |
|--------|---------|------------|
| `fetch_url(url)` | page text | raises a `FetchError` subclass |
| `fetch_result(url)` | `FetchResult` | error stored in `result.error` |
| `fetch_urls(urls)` | `{url: text}` for successful pages | failed URLs are logged and skipped |
| `fetch_many(urls)` | `list[FetchResult]` in input order | error stored per result |
| `fetch_and_parse(url)` | `ParsedPage` dict | download errors raise a `FetchError` subclass; parsing problems go to `page["errors"]` |
| `crawl(start_urls, max_pages)` | `{url: ParsedPage}` for fetched pages | failed URLs go to `failed_urls` |
| `close()` | - | safe to call twice; called by `async with` |

Closing the crawler while a batch is running does not break the batch.
Requests still waiting for a free slot fail with `CrawlerClosedError`.
Requests already in flight are not interrupted: one that is still connecting
fails with `NetworkError`, one that is already reading the body runs until it
completes or hits `total_timeout`. Fetching from an already closed crawler
fails the same way: `fetch_url` raises `CrawlerClosedError`, the other methods
report it per URL.

### Crawling

`crawl()` runs `max_concurrent` workers over a priority queue of URLs. A link
found on a page at depth `d` gets depth `d + 1` and is followed only up to
`max_depth`, so the site is walked breadth-first. `max_pages` caps the pages
fetched, failed ones included. URLs are normalized (including their
percent-encoding, so `/café` and `/caf%C3%A9` are one page), and each one is
fetched at most once. The target of a redirect is remembered too; it may still be fetched
twice if a direct link to it is downloaded at the same moment.

| Option | Effect |
|--------|--------|
| `AsyncCrawler(max_depth=2)` | how far from the start pages to go; 0 fetches the start pages only |
| `AsyncCrawler(max_per_domain=None)` | parallel requests to one host; `None` means only `max_concurrent` applies |
| `same_domain_only=False` | follow links on the start hosts only (and on the hosts they redirect to) |
| `include_patterns=()` | regular expressions; a link must match at least one |
| `exclude_patterns=()` | regular expressions; a matching link is skipped, even if included |

Filters apply to discovered links, not to the start URLs. Patterns match the
normalized URL both percent-encoded and decoded, so `r"/café"` works. A link
that passes the filters but redirects to a URL that does not, such as a
sign-in page on another domain, is skipped: it is left out of the results
and listed in `skipped_urls` as `redirected out of scope`.
Invalid start URLs or patterns raise `ValueError` before anything is fetched. After a crawl, and
during one, the crawler exposes its state:

| Attribute | Content |
|-----------|---------|
| `processed_urls` | `{url: ParsedPage}`, the pages returned by `crawl()` |
| `failed_urls` | `{url: "ErrorType: message"}` |
| `skipped_urls` | `{url: reason}` for pages fetched but left out, e.g. redirected out of scope |
| `visited_urls` | every URL taken for fetching, successful or not |
| `url_depths` | depth of every URL accepted into the queue |
| `crawl_stats()` | `CrawlStats`: processed, failed, skipped, queued, in progress, active requests, elapsed, pages per second |

The building blocks can be used on their own: `CrawlerQueue` (priorities,
deduplication, completion detection), `SemaphoreManager` (global and
per-domain limits) and `UrlFilter`.

### Parsed page

`fetch_and_parse` returns a `ParsedPage`, a plain dict with typed keys:

| Key | Content |
|-----|---------|
| `url`, `final_url` | requested URL and the URL after redirects |
| `title` | `<title>`, or `og:title` if it is missing |
| `text` | visible text of `<main>` (or a single `<article>`, or `<body>`) |
| `links` | absolute, normalized, unique `http(s)` links in page order |
| `metadata` | `title`, `description`, `keywords`, `language`, `canonical` |
| `headings` | `h1`-`h3` as `{"level", "text"}` |
| `images` | `{"src", "alt"}`; `data-src` is used for lazy-loaded images |
| `tables` | `{"caption", "headers", "rows"}` |
| `lists` | `<ul>`/`<ol>` as `{"type", "items"}`; nested lists are separate entries |
| `errors` | parsing problems; empty when everything went fine |

Responses whose `Content-Type` is not HTML are not parsed, and their body is
not even downloaded: the page comes back empty with the reason in `errors`.
This keeps a crawl from pulling in archives or videos it finds links to. The parser can be used on its own:
`HTMLParser().parse(html, url)`, or `await HTMLParser().parse_html(html, url)`
in async code. Pass `AsyncCrawler(parser=HTMLParser(same_host_only=True))` to
keep only links to the page's own host.

## Tests

```bash
pytest                      # unit + integration, no internet needed
pytest tests/unit           # parser, URLs, queue, semaphores, filters, client with a fake session
pytest tests/integration    # real HTTP and crawls against a local aiohttp server
pytest -m network           # smoke tests against the real internet
```

```bash
ruff format src tests       # format
ruff check src tests        # lint
```

## Project structure

```
src/
├── main.py                 # demo CLI: `crawl`, `parse` and `benchmark` commands
└── crawler/
    ├── client.py           # AsyncCrawler: fetching, parsing, crawl()
    ├── queue.py            # CrawlerQueue: URL priority queue and statuses
    ├── semaphores.py       # SemaphoreManager: global and per-domain limits
    ├── filters.py          # UrlFilter: host and pattern rules
    ├── parser.py           # HTMLParser
    ├── urls.py             # URL validation, normalization, resolution
    ├── models.py           # FetchResult, ParsedPage, CrawlStats
    └── exceptions.py       # FetchError hierarchy
tests/
├── fixtures/               # valid and broken HTML pages
├── pages.py                # test pages and a small site for crawl tests
├── unit/                   # parser, URLs, queue, semaphores, filters, client
└── integration/            # local HTTP server; live tests marked `network`
docs/
├── asyncio_concepts.md     # notes on async concepts used here
├── concurrency_control.md  # notes on queues, limits and crawl order
└── html_parsing.md         # notes on HTML parsing and URL handling
```
