# async-web-crawler

An asynchronous web crawler built on `asyncio`, `aiohttp` and BeautifulSoup.
It downloads many pages concurrently over a shared connection pool, limits
concurrency, applies timeouts, and reports failures without stopping the rest
of the batch. Downloaded pages are parsed into structured data: title,
metadata, text, absolute links, images, headings, tables and lists.

## Features

- Concurrent downloads with a configurable concurrency limit (`asyncio.Semaphore`)
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

The demo has two commands: `parse` extracts data from pages, `benchmark`
compares sequential and concurrent fetching. Both accept `--concurrency`,
`--timeout` and `--log-level`. Logs go to stderr and the report goes to stdout.

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
- An online-course platform whose HTML is only a JavaScript shell, so little
  text is found.
- A marketplace behind anti-bot protection, which shows how a failed page is
  reported.

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
  "images_count": 23,
  "headings": ["h1: Main Page", "h1: Welcome to Wikipedia", ...],
  "tables_count": 1,
  "lists_count": 35,
  "errors": []
}
...
=== Summary (5 pages, 0.79s) ===
URL                                      RESULT                      TEXT     LINKS    IMAGES  HEADINGS    TABLES     LISTS
https://en.wikipedia.org/wiki/Main_Page  ok                         11609       632        23        10         1        35
https://apilearn.tukas.dev/              ok                         12235        28         2        12         0        14
https://apilearn.tukas.dev/exercises/    ok                         35331        40         1         3         6         7
https://stepik.org/                      ok                           306        17        10         0         0         2
https://www.ozon.ru/                     HTTPStatusError 403
Parsed: 4/5 pages, links: 717, text: 59481 chars
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


asyncio.run(main())
```

| Method | Returns | On failure |
|--------|---------|------------|
| `fetch_url(url)` | page text | raises a `FetchError` subclass |
| `fetch_result(url)` | `FetchResult` | error stored in `result.error` |
| `fetch_urls(urls)` | `{url: text}` for successful pages | failed URLs are logged and skipped |
| `fetch_many(urls)` | `list[FetchResult]` in input order | error stored per result |
| `fetch_and_parse(url)` | `ParsedPage` dict | download errors raise a `FetchError` subclass; parsing problems go to `page["errors"]` |
| `close()` | - | safe to call twice; called by `async with` |

Closing the crawler while a batch is running does not break the batch.
Requests still waiting for a free slot fail with `CrawlerClosedError`.
Requests already in flight are not interrupted: one that is still connecting
fails with `NetworkError`, one that is already reading the body runs until it
completes or hits `total_timeout`. Fetching from an already closed crawler
fails the same way: `fetch_url` raises `CrawlerClosedError`, the other methods
report it per URL.

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

Responses whose `Content-Type` is not HTML are not parsed: the page comes
back empty with the reason in `errors`. The parser can be used on its own:
`HTMLParser().parse(html, url)`, or `await HTMLParser().parse_html(html, url)`
in async code. Pass `AsyncCrawler(parser=HTMLParser(same_host_only=True))` to
keep only links to the page's own host.

## Tests

```bash
pytest                      # unit + integration, no internet needed
pytest tests/unit           # parser and URL edge cases, fake HTTP session
pytest tests/integration    # real HTTP against a local aiohttp server
pytest -m network           # smoke tests against the real internet
```

```bash
ruff format src tests       # format
ruff check src tests        # lint
```

## Project structure

```
src/
├── main.py                 # demo CLI: `parse` and `benchmark` commands
└── crawler/
    ├── client.py           # AsyncCrawler
    ├── parser.py           # HTMLParser and ParsedPage
    ├── urls.py             # URL validation, normalization, resolution
    ├── models.py           # FetchResult
    └── exceptions.py       # FetchError hierarchy
tests/
├── fixtures/               # valid and broken HTML pages
├── unit/                   # parser, URLs, client with a fake session
└── integration/            # local HTTP server; live tests marked `network`
docs/
├── asyncio_concepts.md     # notes on async concepts used here
└── html_parsing.md         # notes on HTML parsing and URL handling
```
