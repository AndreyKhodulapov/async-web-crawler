# async-web-crawler

An asynchronous web crawler built on `asyncio`, `aiohttp` and BeautifulSoup.
It downloads many pages concurrently over a shared connection pool, limits
concurrency, applies timeouts, and reports failures without stopping the rest
of the batch. Downloaded pages are parsed into structured data: title,
metadata, text, absolute links, images, headings, tables and lists. Starting
from a few URLs, it can crawl a whole site: it follows links breadth-first up
to a given depth, never fetches a page twice, and shows live progress. It is
polite by default: it limits the request rate per host, follows robots.txt
and backs off when a site struggles.

## Features

- Concurrent downloads with a global concurrency limit and an optional
  per-domain limit (`SemaphoreManager`)
- Site crawling with a priority queue of URLs (`CrawlerQueue`), a pool of
  workers, depth and page limits, deduplication of normalized URLs, and
  filters: same domain only, include and exclude regular expressions
- Live crawl statistics: pages done, queued, failed, blocked, requests in
  flight, requests per second, average gap between requests to a host
- Rate limiting per host or overall (`RateLimiter`, GCRA): requests per
  second, a minimum delay between requests and random jitter
- robots.txt support (`RobotsParser`, RFC 9309): rules for the crawler's own
  name, wildcards, longest-match precedence, Crawl-delay; one download per
  site, cached; disallowed URLs are logged and never requested
- Retries of timeouts, network errors, HTTP 429 and 5xx with exponential
  backoff and jitter, honoring `Retry-After`; the whole host slows down
  while a retry waits
- Configurable User-Agent, with optional rotation between variants of the
  same bot name
- Connection pooling and keep-alive via a single `aiohttp.ClientSession`
- Separate connect, read and total timeouts
- Clear error types: `HTTPStatusError`, `NetworkError` (including redirect
  loops), `FetchTimeoutError`, `InvalidURLError`, `RobotsDisallowedError`,
  `CrawlerClosedError` and `UnexpectedError`, all subclasses of `FetchError`
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
fetching. All of them accept `--concurrency`, `--timeout` and `--log-level`,
and the politeness options:

| Option | Default | Effect |
|--------|---------|--------|
| `--rps` | 1 | max requests per second to one host; 0 removes the limit |
| `--min-delay` | 0 | min seconds between two requests to one host |
| `--jitter` | 0 | random extra delay of up to this many seconds |
| `--no-robots` | off | do not check robots.txt |
| `--retries` | 2 (0 for `benchmark`) | retries of timeouts, network errors, 429 and 5xx |
| `--user-agent` | `AsyncWebCrawler/0.1 (+repo URL)` | repeat to rotate several; all must share the bot name |

Logs and progress go to stderr; the report goes to stdout.

### crawl

```bash
python src/main.py crawl                                     # two sandboxes, depth 2, 30 pages
python src/main.py crawl https://books.toscrape.com/ --max-depth 1 --max-pages 20 --same-domain
python src/main.py crawl https://books.toscrape.com/ --exclude '/category/' --include '/catalogue/' --json crawl.json
python src/main.py crawl --per-domain 4 --concurrency 20     # at most 4 requests to one host at a time
python src/main.py crawl --rps 2 --min-delay 0.5 --jitter 0.3 --user-agent "MyBot/1.0 (+https://example.com/bot)"
```

The default start pages are sandboxes made for crawling practice, and their
robots.txt shows the rules at work. webscraper.io disallows its pagination
and product pages, one of them with a wildcard rule (`/test-sites/pagination*?page=`),
and web-scraping.dev sets `Crawl-delay: 2`.
While it runs, a progress line is updated every second:

```
pages 9 | failed 0 | skipped 0 | blocked 0 | queued 88 | in progress 6 | in flight 2 | 1.6 req/s | gap 1.09s | 7.0s
```

`in progress` counts pages taken by workers; `in flight` counts those actually
being downloaded, which never exceeds `--per-domain` for a single site.
`req/s` is the request rate over the last 5 seconds, and `gap` the average
time between two requests to the same host.
Warnings, such as a failed page, are printed above the line; per-request logs,
blocked URLs included, are hidden by default, pass `--log-level INFO` to see
them. At the end it prints every page in the order it was found, and then
the requests to every host:

```
=== Crawl (59 pages, 28.31s) ===
DEPTH  RESULT                                LINKS  URL
    0  ok                                       56  https://webscraper.io/test-sites/pagination
    0  ok                                       27  https://web-scraping.dev/products
    ...
    1  blocked, disallowed by robots.txt            https://webscraper.io/test-sites/pagination/BMW
    1  blocked, disallowed by robots.txt            https://webscraper.io/test-sites/pagination?page=2
    1  FetchTimeoutError: request timed out         https://webscraper.io/blog
    1  ok                                       16  https://web-scraping.dev/
    1  HTTPStatusError: HTTP 404 Not Found          https://web-scraping.dev/api/graphql
Crawled: 28 pages, failed: 2, skipped: 0, blocked: 29, left in queue: 232, speed: 1.1 pages/s

=== Requests by host (39 requests, 1.39 req/s) ===
HOST                       REQUESTS  INTERVAL   AVG GAP  BLOCKED
webscraper.io                    24     1.00s     1.00s       25
web-scraping.dev                  5     2.00s     5.05s        0
cloud.webscraper.io               3     1.00s     1.00s        0
...
Average gap between requests to a host: 1.54s, average wait for the rate limit: 1.12s, retries: 0, blocked by robots.txt: 29
```

`INTERVAL` is the minimum gap the crawler keeps for the host: the larger of
`1 / --rps`, `--min-delay` and the site's Crawl-delay. Requests include
robots.txt and retries. With `--json`, the parsed pages (with their depth),
the failed, skipped and blocked URLs with the reasons, the statistics and
the per-host table are saved to a file.

### parse

```bash
python src/main.py parse
python src/main.py parse https://example.com --same-host  # internal links only
python src/main.py parse --preview 10 --json pages.json   # save full results
```

By default it parses real sites of different kinds:

- Wikipedia: a large server-rendered page.
- A scraping sandbox with tables. It answers HTTP 429 when requests come too
  fast, which the default rate limit of one request per second avoids.
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
speedup. Retries are off here, since the list fails on purpose; the rate
limit still applies, so the six httpbin.org requests start a second apart.
The non-existent domain fails with `RobotsDisallowedError`: its robots.txt
cannot be fetched, and an unreachable robots.txt disallows the whole site.
Pass `--no-robots` to see the `NetworkError` itself.

```bash
python src/main.py benchmark --concurrency 3 --timeout 3        # tune the crawler
python src/main.py benchmark https://example.com https://python.org  # custom URLs
python src/main.py benchmark --log-level WARNING                # errors only
```

Sample report (logs omitted):

```
=== Sequential ===
URL                                  STATUS                       SIZE    TIME
https://example.com                  200                          713B   0.07s
https://httpbin.org/delay/2          200                          416B   2.16s
https://httpbin.org/status/404       HTTPStatusError 404            0B   0.16s
https://httpbin.org/delay/10         FetchTimeoutError              0B   5.01s
https://nonexistent-domain.invalid   RobotsDisallowedError          0B   0.00s
...
Succeeded: 6/10, total time: 15.78s

=== Concurrent (max_concurrent=10) ===
...
Succeeded: 6/10, total time: 11.03s

Speedup: 1.4x
```

A concurrent run takes at least as long as its slowest request. In the
default list that is the request that hits the timeout, which also has to
wait for its turn among the httpbin.org requests. `--rps 0` shows the speedup
without the rate limit. `SIZE` is the body size after
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

    async with AsyncCrawler(
        max_concurrent=5,
        requests_per_second=2.0,  # at most 2 requests per second to one host
        respect_robots=True,
        min_delay=0.5,  # and at least 0.5 s between them
        user_agent="MyBot/1.0 (+https://example.com/bot)",
    ) as crawler:
        results = await crawler.crawl(
            start_urls=["https://books.toscrape.com/"],
            max_pages=50,
            same_domain_only=True,
            exclude_patterns=[r"/category/"],
        )
        stats = crawler.crawl_stats()
        print(f"Crawled {len(results)} pages, blocked: {stats.blocked}, avg gap: {stats.avg_delay:.2f}s")


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

Every method checks robots.txt, waits for the rate limit and retries transient
failures; a URL that robots.txt disallows fails with `RobotsDisallowedError`
without being requested.

### Politeness

| Option | Default | Effect |
|--------|---------|--------|
| `requests_per_second` | `1.0` | requests per second to one host; `None` removes the limit |
| `per_domain_rate` | `True` | `False` applies the rate to all hosts together |
| `min_delay` | `0.0` | min seconds between two requests to one host |
| `jitter` | `0.0` | random extra delay of up to this many seconds after each request |
| `respect_robots` | `True` | check robots.txt before every request |
| `max_retries` | `2` | retries of timeouts, network errors, HTTP 408, 429 and 5xx |
| `backoff_base`, `max_backoff` | `1.0`, `30.0` | retry n waits about `backoff_base * 2**n` seconds, at most `max_backoff` |
| `user_agent` | `AsyncWebCrawler/0.1 (+repo URL)` | the User-Agent; robots.txt rules are looked up by its name |
| `user_agents` | `()` | strings to rotate between requests; all must share the name of `user_agent` |

Requests to one host start at least `max(1 / requests_per_second, min_delay,
Crawl-delay)` seconds apart. robots.txt is fetched once per site (scheme,
host and port) and cached for the crawler's lifetime. A missing robots.txt
(HTTP 4xx) allows everything; an unreachable one (HTTP 5xx, 429, network
errors) disallows the whole site. Only the requested URL is checked: the
HTTP client follows redirects on its own, so a redirect can still lead to a
disallowed page. Crawl-delay is capped at 30 seconds. While
a retry waits, the whole host waits with it, since a timeout or a 429 usually
means the site is overloaded.

The building blocks work on their own too:

```python
limiter = RateLimiter(requests_per_second=2.0, per_domain=True, min_delay=0.5, jitter=0.2)
await limiter.acquire("example.com")      # returns when the request may start

robots = RobotsParser(fetch)              # fetch(url) -> (status, body)
await robots.fetch_robots("https://example.com/")
robots.can_fetch("https://example.com/private/", "MyBot/1.0")
robots.get_crawl_delay("https://example.com/", "MyBot/1.0")
```

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
requested, failed ones included; pages that robots.txt disallows are not
requested and do not count. URLs are normalized (including their
percent-encoding, so `/café` and `/caf%C3%A9` are one page), and each one is
fetched at most once. The target of a redirect is remembered too, but only
once the response arrives: if a direct link to it was queued or fetched before
that, the page is downloaded twice and appears in the results under both URLs.

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
| `blocked_urls` | `{url: reason}` for pages robots.txt did not allow to fetch |
| `visited_urls` | every URL taken for fetching, successful or not |
| `url_depths` | depth of every URL accepted into the queue |
| `crawl_stats()` | `CrawlStats`: processed, failed, skipped, blocked, queued, in progress, active requests, elapsed, pages per second; requests, retries, current and average requests per second, average gap between requests to a host, average wait for the rate limit |
| `rate_limiter.get_stats()` | `RateStats`, with requests, interval and average gap per host |

The building blocks can be used on their own: `CrawlerQueue` (priorities,
deduplication, completion detection), `SemaphoreManager` (global and
per-domain limits), `RateLimiter`, `RobotsParser` and `UrlFilter`.

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
pytest tests/unit           # parser, URLs, queue, limits, robots.txt, retries, client with a fake session
pytest tests/integration    # real HTTP, crawls, rate limits and robots.txt against a local aiohttp server
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
    ├── rate_limiter.py     # RateLimiter: requests per second, delays, jitter, rate stats
    ├── robots.py           # RobotsParser, RobotsRules: robots.txt per RFC 9309
    ├── retry.py            # RetryPolicy: which errors to retry, backoff, Retry-After
    ├── filters.py          # UrlFilter: host and pattern rules
    ├── parser.py           # HTMLParser
    ├── urls.py             # URL validation, normalization, resolution
    ├── models.py           # FetchResult, ParsedPage, CrawlStats
    └── exceptions.py       # FetchError hierarchy
tests/
├── fixtures/               # valid and broken HTML pages
├── pages.py                # test pages and a small site for crawl tests
├── helpers.py              # crawler options for tests that skip politeness
├── unit/                   # parser, URLs, queue, limits, robots.txt, retries, filters, client
└── integration/            # local HTTP server; live tests marked `network`
docs/
├── asyncio_concepts.md     # notes on async concepts used here
├── concurrency_control.md  # notes on queues, limits and crawl order
├── html_parsing.md         # notes on HTML parsing and URL handling
└── politeness.md           # notes on rate limiting, robots.txt and backoff
```
