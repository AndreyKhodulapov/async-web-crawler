# async-web-crawler

An asynchronous web crawler built on `asyncio`, `aiohttp` and BeautifulSoup.
It downloads many pages concurrently over a shared connection pool, limits
concurrency, applies timeouts, and reports failures without stopping the rest
of the batch. Downloaded pages are parsed into structured data: title,
metadata, text, absolute links, images, headings, tables and lists. Starting
from a few URLs, it can crawl a whole site: it follows links breadth-first up
to a given depth, never fetches a page twice, and shows live progress. It is
polite by default: it limits the request rate per host, follows robots.txt
and backs off when a site struggles. The pages of a crawl can be saved as it
goes: to a JSON or CSV file, to SQLite or PostgreSQL.

## Features

- Concurrent downloads with a global concurrency limit and an optional
  per-domain limit (`SemaphoreManager`)
- Site crawling with a priority queue of URLs (`CrawlerQueue`), a pool of
  workers, depth and page limits, deduplication of normalized URLs, and
  filters: same domain only, include and exclude regular expressions
- Live crawl statistics: pages done, queued, failed, blocked, unreachable,
  requests in flight, requests per second, average gap between requests to a host
- Statistics of a crawl (`CrawlerStats`): pages in total, successful, failed
  and skipped, pages by status code and by error, top domains, average
  speed and response time, running time; export to JSON and to an HTML
  report with tables and charts
- Rate limiting per host or overall (`RateLimiter`, GCRA): requests per
  second, a minimum delay between requests and random jitter
- robots.txt support (`RobotsParser`, RFC 9309): rules for the crawler's own
  name, wildcards, longest-match precedence, Crawl-delay; one download per
  site, cached; disallowed URLs are logged and never requested; an
  unreachable robots.txt closes the site for a minute, then it is fetched again
- Sitemaps as a source of pages (`SitemapParser`): plain and index sitemaps,
  gzip, the sitemaps named in robots.txt; downloaded through the same
  robots.txt check, limits and retries as pages
- Retries of timeouts, network errors, HTTP 408, 429 and 5xx that usually
  pass, with exponential backoff and jitter, honoring `Retry-After`; the
  whole host slows down while a retry waits or after a Retry-After
- Circuit breaker per host: a host whose requests keep failing is left
  alone for a while, then tested with a single probe request
- Configurable User-Agent, with optional rotation between variants of the
  same bot name
- Connection pooling and keep-alive via a single `aiohttp.ClientSession`
- Separate connect, read and total timeouts that grow with every retry
- Clear error types, all subclasses of `FetchError`, grouped by whether a
  retry can help: `TransientError` (timeouts, HTTP 408, 429, 500, 502, 503,
  504 and Cloudflare's 520-524), `NetworkError` (DNS, refused or reset
  connections), `PermanentError` (other HTTP errors such as 401, 403, 404,
  redirect loops, bad certificates, invalid URLs, pages disallowed by
  robots.txt) and `ParseError` (a response that is not an HTML document); plus
  `RobotsUnreachableError`, `CircuitOpenError`, `CrawlerClosedError` and
  `UnexpectedError`
- One failing URL never breaks a batch: even unforeseen exceptions are
  logged with a traceback and reported as `UnexpectedError`
- Logging for every request: start, success (status, size, time) and failure;
  every retry with the error, the attempt number and the pause before the next one
- Error statistics: failed attempts by kind and class, successful retries,
  average time per retry, pages with permanent errors
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
- Crawled pages saved as the crawl goes, behind one interface (`DataStorage`):
  JSON Lines or an indented JSON array (`JSONStorage`), CSV in any encoding
  (`CSVStorage`), SQLite (`SQLiteStorage`) and PostgreSQL (`PostgresStorage`),
  or several of them at once (`CompositeStorage`)
- The database is chosen by one URL, in code or in `CRAWLER_DATABASE_URL`;
  another database is added as a driver, without changes to the storage
- Asynchronous writes in batches: files through `aiofiles`, databases through
  `aiosqlite` and `asyncpg`, one transaction per batch, saving a URL again
  replaces its row
- Failed writes are retried with backoff; a storage that stays down is
  logged and counted, and the crawl goes on

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

The demo has five commands: `crawl` follows links from start pages, `errors`
crawls a local site that fails on purpose, `save` writes crawled pages to
JSON, CSV and a database, `parse` extracts data from pages,
and `benchmark` compares sequential and concurrent fetching. All of them accept `--concurrency`, `--log-level`, the timeouts
(`--connect-timeout` and `--read-timeout`, 5 s by default, `--total-timeout`,
10 s, and `--timeout-growth`, 1.5, see [Timeouts](#timeouts)) and the
politeness options:

| Option | Default | Effect |
|--------|---------|--------|
| `--rps` | 1 | max requests per second to one host; 0 removes the limit |
| `--min-delay` | 0 | min seconds between two requests to one host |
| `--jitter` | 0 | random extra delay of up to this many seconds |
| `--no-robots` | off | do not check robots.txt; `errors` and `save` do not check it unless given `--robots` |
| `--retries` | 2 (0 for `benchmark`, 3 for `errors` and `save`) | retries of timeouts, network errors, 408, 429, 500, 502-504 and 520-524 |
| `--retry-delay` | 1 (0.2 for `errors` and `save`) | seconds before the first retry, doubled for every next one up to 30 s, see [Retries](#retries) |
| `--breaker-threshold` | 0.5 | block a host once this share of its requests in the last minute (5 at least) failed with a timeout, a network error, 408, 429 or 5xx |
| `--breaker-cooldown` | 30 (1 for `errors` and `save`) | seconds a blocked host is left alone before a probe request |
| `--no-breaker` | off | never block a host |
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
pages 9 | failed 0 | skipped 0 | blocked 0 | unreachable 0 | queued 88 | in progress 6 | in flight 2 | 1.6 req/s | gap 1.09s | 7.0s
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
    1  FetchTimeoutError: read timeout (5.0s)       https://webscraper.io/blog
    1  ok                                       16  https://web-scraping.dev/
    1  PermanentHTTPError: HTTP 404 Not Found       https://web-scraping.dev/api/graphql
Crawled: 28 pages, failed: 2, skipped: 0, blocked: 29, unreachable: 0, left in queue: 232, speed: 1.1 pages/s

=== Requests by host (39 requests, 1.39 req/s) ===
HOST                       REQUESTS  INTERVAL   AVG GAP  BLOCKED  UNREACHABLE
webscraper.io                    24     1.00s     1.00s       25            0
web-scraping.dev                  5     2.00s     5.05s        0            0
cloud.webscraper.io               3     1.00s     1.00s        0            0
...
Average gap between requests to a host: 1.54s, average wait for the rate limit: 1.12s, retries (robots.txt included): 0, blocked by robots.txt: 29, not fetched as robots.txt was unreachable: 0
```

`INTERVAL` is the minimum gap the crawler keeps for the host: the larger of
`1 / --rps`, `--min-delay` and the site's Crawl-delay. Requests include
robots.txt and retries, and so do the retries in the last line, unlike the
retries of the error statistics below. `UNREACHABLE` counts pages not
requested because the site's robots.txt could not be read.

Then come the [error statistics](#error-statistics) and the state of the
[circuit breaker](#circuit-breaker) of every host, here for a crawl of
`httpbin.org/status/503`, `httpbin.org/status/404` and `example.com` with `--retries 1`:

```
=== Errors (3 failed attempts) ===
By kind:  TransientError 2, PermanentError 1, NetworkError 0, ParseError 0, other 0
By class: TransientHTTPError 2, PermanentHTTPError 1
Retries: 1, pages recovered by a retry: 0, average time per retry: 1.08s
Permanent errors (1):
  https://httpbin.org/status/404  PermanentHTTPError: HTTP 404 NOT FOUND

=== Circuit breaker (2 hosts: 0 open, 0 half-open) ===
HOST         STATE      REQUESTS  FAILURES  OPENED  REJECTED
httpbin.org  closed            5         2       0         0
example.com  closed            2         0       0         0
```

`REQUESTS` and `FAILURES` are counted over the breaker's window of the last
minute and include robots.txt; `OPENED` and `REJECTED` count since the crawl
started. With `--json`, the parsed pages (with their depth), the failed,
skipped, blocked and unreachable URLs with the reasons, the statistics, the
per-host table, the error statistics and the circuit breakers are saved to a file.

### errors

```bash
python src/main.py errors                              # the local site only
python src/main.py errors https://httpbin.org/status/503 --robots --rps 1 --json report.json
python src/main.py errors --log-level WARNING          # failed attempts only
```

The command starts a small site on 127.0.0.1 at a free port and crawls its
start page and every page it links to, with 3 retries. The links cover the
errors a crawler meets:

| Page | Answer | What the crawler does |
|------|--------|-----------------------|
| `/articles/1` ... `/articles/8` | ordinary pages | fetches them |
| `/flaky` | HTTP 503 twice, then the page | retries after 0.1–0.2 s, then 0.2–0.4 s, and gets the page |
| `/rate-limited` | HTTP 429 with `Retry-After: 1` once | waits the second the server asked for, then gets the page |
| `/server-error` | always HTTP 500 | retries once, as the rule for 500 says, and gives up |
| `/slow` | the page after 1.2 s | times out at the 1 s read timeout, retries with 1.5 s and gets the page |
| `/missing`, `/private` | HTTP 404, 403 | no retry; listed as permanent errors |
| `/data.json` | JSON | `ParseError`, no retry |
| `localhost:<closed port>/page/1` ... `8` | connection refused | retries until 5 failures open the circuit breaker of `localhost`; the pages not sent yet wait for its probes and are given up once it has opened 3 times |
| `unreachable.invalid` | DNS error | 3 retries, then gives up |

The breaker tells hosts apart by name, so the server that is down, on
`localhost`, does not block the site on `127.0.0.1`. The ordinary pages come
first, so the site's own failures stay under the breaker's threshold. URLs
given on the command line are added to the start page's links; a URL on
`localhost` or `127.0.0.1` shares the circuit breaker with the local site, so
the server that is down may block it. To keep the
run within seconds, the defaults differ from the other commands: `--retries 3`,
`--retry-delay 0.2`, `--read-timeout 1`, `--rps 0`, `--breaker-cooldown 1` and
no robots.txt. Real
URLs get the same defaults; to fetch them politely, add `--robots --rps 1`.
With `--robots`, the server that is down and the domain that does not exist
are not requested at all: their robots.txt is unreachable too.

Every attempt is logged (excerpt):

```
WARNING | crawler.retry | Attempt 1/4 for http://127.0.0.1:50864/flaky failed: TransientHTTPError: HTTP 503 Service Unavailable; retrying in 0.1s
WARNING | crawler.circuit_breaker | Circuit breaker of localhost opened: 5 of 5 requests failed in 60s; requests to it fail for 1s
INFO    | crawler.client | Deferred http://localhost:50865/page/6 for 1.0s: circuit breaker of localhost is open (5 of 5 requests failed in 60s), next probe in 1.0s
WARNING | crawler.retry | Failed http://127.0.0.1:50864/server-error on attempt 2/4 after 0.62s, no retries left for HTTP 500: TransientHTTPError: HTTP 500 Internal Server Error
WARNING | crawler.retry | Attempt 1/4 for http://127.0.0.1:50864/rate-limited failed: TransientHTTPError: HTTP 429 Too Many Requests; retrying in 1.0s
WARNING | crawler.retry | Attempt 1/4 for http://127.0.0.1:50864/slow failed: FetchTimeoutError: read timeout (1.0s); retrying in 0.2s
INFO    | crawler.circuit_breaker | Circuit breaker of localhost is half-open: probing it with http://localhost:50865/page/6
INFO    | crawler.client | Deferred http://localhost:50865/page/7 for 1.0s: circuit breaker of localhost is half-open, waiting for the probe request
INFO    | crawler.retry | Succeeded http://127.0.0.1:50864/flaky on attempt 3/4 after 1.63s
INFO    | crawler.retry | Succeeded http://127.0.0.1:50864/slow on attempt 2/4 after 2.82s
INFO    | crawler.client | Gave up on http://localhost:50865/page/8: circuit breaker of localhost opened 3 times
```

Then come the pages, the error statistics and the circuit breakers:

```
=== Crawl (25 pages, 3.39s) ===
DEPTH  RESULT                                LINKS  URL
    0  ok                                       24  http://127.0.0.1:50864/
    1  ok                                        0  http://127.0.0.1:50864/articles/1
    ...
    1  ok                                        0  http://127.0.0.1:50864/flaky
    1  ok                                        0  http://127.0.0.1:50864/rate-limited
    1  TransientHTTPError: HTTP 500...              http://127.0.0.1:50864/server-error
    1  ok                                        0  http://127.0.0.1:50864/slow
    1  PermanentHTTPError: HTTP 404 Not...          http://127.0.0.1:50864/missing
    1  PermanentHTTPError: HTTP 403...              http://127.0.0.1:50864/private
    1  ParseError: unsupported content...           http://127.0.0.1:50864/data.json
    1  NetworkError:...                             http://localhost:50865/page/1
    ...
    1  NetworkError:...                             http://localhost:50865/page/7
    1  CircuitOpenError: circuit breaker...         http://localhost:50865/page/8
    1  NetworkError:...                             http://unreachable.invalid/
Crawled: 12 pages, failed: 13, skipped: 0, blocked: 0, unreachable: 0, left in queue: 0, speed: 7.4 pages/s

=== Errors (24 failed attempts) ===
By kind:  TransientError 6, PermanentError 2, NetworkError 15, ParseError 1, other 0
By class: NetworkError 15, TransientHTTPError 5, PermanentHTTPError 2, ParseError 1, FetchTimeoutError 1
Retries: 12, pages recovered by a retry: 3, average time per retry: 0.58s
Permanent errors (2):
  http://127.0.0.1:50864/missing  PermanentHTTPError: HTTP 404 Not Found
  http://127.0.0.1:50864/private  PermanentHTTPError: HTTP 403 Forbidden

=== Circuit breaker (3 hosts: 0 open, 1 half-open) ===
HOST                 STATE      REQUESTS  FAILURES  OPENED  REJECTED
127.0.0.1            closed           21         6       0         0
localhost            half-open         0         0       3         8
unreachable.invalid  closed            4         4       0         0

Error report saved to error_report.json
```

The breaker of `localhost` shows no requests, since opening it clears its
window. It opened three times: after the first failures and after two failed
probes; the crawl then gave up on the page still waiting, so the circuit
ends up open, or half-open if its last cooldown is over. The report in `error_report.json` (or `--json PATH`) holds the error
statistics, the circuit breakers, the failed pages with the full errors and
the fetched pages.

### save

```bash
python src/main.py save                                # pages.jsonl, pages.csv and crawler.db
python src/main.py save --indent 2 --csv-encoding utf-8-sig --batch-size 5
python src/main.py save --database-url sqlite:///data/pages.db --append
CRAWLER_DATABASE_URL=postgresql://crawler:crawler@localhost:5432/crawler python src/main.py save
```

The command crawls the local site of [`errors`](#errors), with the same
defaults, and saves every page it gets to three storages at once: a JSON
file, a CSV file and a database. Pages that failed are not saved. The
database is chosen by `--database-url`, else by the `CRAWLER_DATABASE_URL`
environment variable, else it is `sqlite:///crawler.db`; for PostgreSQL see
[Saving pages](#saving-pages). URLs given on the command line are fetched and
saved along with the local site.

| Option | Default | Effect |
|--------|---------|--------|
| `--json` | `pages.jsonl` | JSON file, a record per line |
| `--indent` | off | write `--json` as one JSON array indented by this many spaces |
| `--csv` | `pages.csv` | CSV file |
| `--csv-encoding` | `utf-8` | encoding of the CSV file, e.g. `utf-8-sig` for Excel |
| `--database-url` | `$CRAWLER_DATABASE_URL`, or `sqlite:///crawler.db` | `sqlite:///path` or `postgresql://user:password@host:port/database` |
| `--batch-size` | 10 | pages written to a storage at once |
| `--append` | off | add to the files of an earlier run instead of replacing them |
| `--preview` | 3 | records read back from each storage |

After the pages of the crawl, the command opens the storages again, counts
what they hold and reads the first records back:

```
=== Saved pages (this crawl: 12 saved, 0 not saved) ===
STORAGE        RECORDS       SIZE  BY STATUS         LOCATION
JSONStorage         12     5.8 KB  200: 12           pages.jsonl
CSVStorage          12     4.9 KB  200: 12           pages.csv
SQLiteStorage       12    32.0 KB  200: 12           sqlite:///crawler.db

=== First records in pages.jsonl (JSONStorage) ===
CRAWLED AT (UTC)     STATUS  TYPE         TEXT  LINKS  DEPTH  URL  TITLE
2026-10-02 12:00:24     200  text/html     779     24      0  http://127.0.0.1:50495/  'Unreliable site'
2026-10-02 12:00:24     200  text/html      24      0      1  http://127.0.0.1:50495/articles/1  'Article 1'
2026-10-02 12:00:24     200  text/html      24      0      1  http://127.0.0.1:50495/articles/2  'Article 2'
...
=== Pages found by URL in sqlite:///crawler.db (SQLiteStorage) ===
CRAWLED AT (UTC)     STATUS  TYPE         TEXT  LINKS  DEPTH  URL  TITLE
2026-10-02 12:00:24     200  text/html     779     24      0  http://127.0.0.1:50495/  'Unreliable site'
...
```

A page counts as saved once every storage has written it; when one of them
cannot be written, the table shows which storages have the pages. The files
are read from their start, and the database finds the pages of this crawl by
URL. The files are replaced on every run, unless `--append` is given. The
database is never emptied: saving a URL again replaces its row, but the local
site gets a new port on every run, so its pages are new URLs and the rows add
up; the report says so. A password in the database URL is shown as `***`.

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
https://httpbin.org/status/403           PermanentHTTPError 403
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
The non-existent domain fails with `RobotsUnreachableError`: its robots.txt
cannot be fetched, and while robots.txt is unreachable the site is not requested.
Pass `--no-robots` to see the `NetworkError` itself.

```bash
python src/main.py benchmark --concurrency 3 --read-timeout 3   # tune the crawler
python src/main.py benchmark https://example.com https://python.org  # custom URLs
python src/main.py benchmark --log-level WARNING                # errors only
```

Sample report (logs omitted):

```
=== Sequential ===
URL                                  STATUS                       SIZE    TIME
https://example.com                  200                          713B   0.07s
https://httpbin.org/delay/2          200                          416B   2.16s
https://httpbin.org/status/404       PermanentHTTPError 404         0B   0.16s
https://httpbin.org/delay/10         FetchTimeoutError              0B   5.01s
https://nonexistent-domain.invalid   RobotsUnreachableError         0B   0.00s
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
| `fetch_and_parse(url)` | `ParsedPage` dict | download errors raise a `FetchError` subclass, a response that is not an HTML document raises `ParseError`; problems in parts of the page go to `page["errors"]` |
| `crawl(start_urls, max_pages)` | `{url: ParsedPage}` for fetched pages | failed URLs go to `failed_urls` |
| `close()` | - | safe to call twice; called by `async with`; closes the storage too |

Every method checks robots.txt, waits for the rate limit and retries transient
failures; a URL that robots.txt disallows fails with `RobotsDisallowedError`
without being requested, and a URL of a site whose robots.txt cannot be read
fails with `RobotsUnreachableError`. A request to a host blocked by the
circuit breaker fails with `CircuitOpenError` without being sent.

### Politeness

| Option | Default | Effect |
|--------|---------|--------|
| `requests_per_second` | `1.0` | requests per second to one host; `None` removes the limit |
| `per_domain_rate` | `True` | `False` applies the rate to all hosts together; Crawl-delay and retry pauses stay per host |
| `min_delay` | `0.0` | min seconds between two requests to one host |
| `jitter` | `0.0` | random extra delay of up to this many seconds after each request |
| `respect_robots` | `True` | check robots.txt before every request |
| `retry_strategy` | `RetryStrategy()` | which failures to retry, how many times and how long to wait, see below |
| `circuit_breaker` | `CircuitBreaker()` | when to stop sending requests to a failing host, see below |
| `user_agent` | `AsyncWebCrawler/0.1 (+repo URL)` | the User-Agent; robots.txt rules are looked up by its name |
| `user_agents` | `()` | strings to rotate between requests; all must share the name of `user_agent` |

Requests to one host start at least `max(1 / requests_per_second, min_delay,
Crawl-delay)` seconds apart; with several sites (ports) on one host, the
longest Crawl-delay counts. robots.txt is fetched once per site (scheme,
host and port) and cached for the crawler's lifetime. A missing robots.txt
(HTTP 4xx, or a redirect loop) allows everything; an unreachable one (HTTP 5xx, 429, network
errors, after the retries) disallows the whole site for 60 seconds, then it
is fetched again. Such pages are counted as unreachable, not as blocked:
the site did not forbid them. A crawl does not queue them again, so only
the pages found after the 60 seconds are fetched. Only the requested URL is checked: the
HTTP client follows redirects on its own, so a redirect can still lead to a
disallowed page, and the rate limit of the host it leads to does not apply. Crawl-delay is capped at 30 seconds. While
a retry waits, the whole host waits with it, since a timeout or a 429 usually
means the site is overloaded. A Retry-After header holds back the host even
when the request is not retried, for at most `max_delay` seconds of the
retry strategy; a request whose Retry-After is longer than that is not retried.

### Retries

`RetryStrategy` retries a failed call with exponential backoff and jitter:
retry n waits about `base_delay * backoff_factor**n` seconds, at most
`max_delay`, or longer if the server sends Retry-After. It retries the error
classes in `retry_on` and never a `PermanentError`. `RetryRule`s tune kinds
of errors, keyed by an HTTP status or an exception class.

| Option | Default | Effect |
|--------|---------|--------|
| `max_retries` | `3` | retries in total |
| `backoff_factor` | `2.0` | how much each pause grows |
| `retry_on` | `[TransientError, NetworkError]` | error classes to retry |
| `base_delay`, `max_delay` | `1.0`, `30.0` | the first pause and the longest one, in seconds |
| `rules` | HTTP 500: 1 retry; HTTP 429: 4x longer pauses | per-status or per-class `RetryRule(max_retries, delay_multiplier)`; replaces the defaults |

```python
retry_strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, retry_on=[TransientError, NetworkError])
async with AsyncCrawler(retry_strategy=retry_strategy) as crawler:
    html = await crawler.fetch_url("https://example.com")      # retried inside

# Any coroutine function can be retried on its own.
html = await retry_strategy.execute_with_retry(fetch_page, "https://example.com")
```

Every failed attempt is logged as a warning with the error, the attempt
number and the pause before the next one, and so is the final failure,
with the reason it was not retried; a success after retries is logged at
the INFO level:

```
WARNING | crawler.retry | Attempt 1/3 for https://httpbin.org/status/503 failed: TransientHTTPError: HTTP 503 SERVICE UNAVAILABLE; retrying in 0.9s
WARNING | crawler.retry | Attempt 2/3 for https://httpbin.org/status/503 failed: TransientHTTPError: HTTP 503 SERVICE UNAVAILABLE; retrying in 1.6s
WARNING | crawler.retry | Failed https://httpbin.org/status/503 on attempt 3/3 after 4.46s, no retries left: TransientHTTPError: HTTP 503 SERVICE UNAVAILABLE
WARNING | crawler.retry | Failed https://httpbin.org/status/404 on attempt 1/3 after 0.82s, permanent error: PermanentHTTPError: HTTP 404 NOT FOUND
```

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

### Circuit breaker

`CircuitBreaker` keeps a circuit per host. While it is closed, requests go
through and their outcomes are counted over the last `window` seconds;
once at least `min_requests` are counted and `failure_threshold` of them
failed, it opens. For `cooldown` seconds requests to the host then fail at
once with `CircuitOpenError`: they are not sent, not retried and not
counted in `error_stats()`. After that one request goes through as a probe
(half-open): its success closes the circuit, its failure opens it again.

Failures are timeouts, network errors, HTTP 408, 429 and any 5xx, even
one that is not retried, such as 501; any other response, a 404 too, is a
success, so broken links do not block a site. Every attempt counts,
retries and robots.txt downloads included. An outcome counts for the host
of the requested URL: the HTTP client follows redirects on its own, so a
link that redirects to a failing host counts against the host of the link.
The circuit is checked before a request waits for the rate limit, where a
half-open one gives its probe to one request and refuses the rest, once
more when its turn comes, and a last time once it holds a concurrency slot:
a request that was already waiting when the circuit opened is not sent, but
it is refused only when its turn comes, and a request that waited for the
slot of a host behind the one that opened its circuit is refused too. A
retry the breaker would refuse is not made, so the request fails with the
error of its last attempt, not with `CircuitOpenError`. When the breaker
refuses the download of robots.txt, the page fails with `CircuitOpenError`
under its own URL, and robots.txt is not cached as unreachable.

In a crawl, a page the breaker refuses does not count toward `max_pages`
and is not failed: it is put off until the circuit may let a probe through,
or for a second while the probe is in flight, and the workers go on with
other pages meanwhile. So the pages of a host that went down for a moment
are fetched once it is back. After the circuit of a host has opened
`AsyncCrawler.MAX_CIRCUIT_OPENINGS` (3) times in the crawl, no more probes
are sent: its remaining pages go to `failed_urls` with `CircuitOpenError`,
and a host that stays down holds the crawl for about two cooldowns.

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `failure_threshold` | `0.5` | share of failed requests that opens the circuit; `None` turns the breaker off |
| `min_requests` | `5` | requests in the window before the share counts |
| `window` | `60.0` | seconds over which outcomes are counted |
| `cooldown` | `30.0` | seconds the circuit stays open before a probe |

```python
breaker = CircuitBreaker(failure_threshold=0.5, min_requests=5, cooldown=30.0)
async with AsyncCrawler(circuit_breaker=breaker) as crawler:
    await crawler.crawl(["https://example.com"])
stats = breaker.get_stats()["example.com"]  # CircuitStats
print(stats.state, stats.times_opened, stats.rejected)  # e.g. "closed 0 0"
```

`get_stats()` gives, per host, the state, the requests and failures in the
window, how many times it opened and how many requests it refused. A
crawl resets the two counters but keeps the states.

### Error statistics

`crawler.error_stats()` returns `ErrorStats` for page requests since the
latest `crawl()` started, or since the crawler was created; robots.txt
downloads, the URLs it blocks and the requests the circuit breaker refused
are not counted.

| Field | Content |
|-------|---------|
| `by_kind` | failed attempts by kind: `TransientError`, `PermanentError`, `NetworkError`, `ParseError`, `other`; attempts a retry made good count too |
| `by_class` | failed attempts by exception class, e.g. `FetchTimeoutError` |
| `total` | all failed attempts |
| `retries`, `successful_retries` | retries made, and pages they recovered |
| `avg_retry_time` | average seconds from a failed attempt to the end of the next one: the pause plus the request |
| `permanent_errors` | `{url: "ErrorType: message"}` for pages that failed with a `PermanentError` |

`error_kind(error)` gives the kind of any exception by the same rules.

### Timeouts

| Option | Default | Effect |
|--------|---------|--------|
| `connect_timeout` | `10.0` | DNS, TCP and TLS, including the wait for a pooled connection |
| `read_timeout` | `20.0` | the longest pause between two chunks of the response |
| `total_timeout` | `30.0` | the whole request, body included |
| `timeout_growth` | `1.5` | each retry multiplies all three by this, up to 4 times the initial values; `1` keeps them fixed |

With the defaults the read timeout is 20 s on the first attempt and 30, 45
and 67.5 s on the three retries: a page that is only slow gets through, a
server that does not answer at all is not waited for forever. A timeout
fails with `FetchTimeoutError` that says which timeout fired, e.g.
`read timeout (20.0s)`, and is retried like any other transient error.

### Crawling

`crawl()` runs `max_concurrent` workers over a priority queue of URLs. A link
found on a page at depth `d` gets depth `d + 1` and is followed only up to
`max_depth`, so the site is walked breadth-first. `max_pages` caps the pages
requested, failed ones included; pages that robots.txt disallows are not
requested and do not count, and neither do pages the circuit breaker
refuses: they wait for their host, see [Circuit breaker](#circuit-breaker). URLs are normalized (including their
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
| `sitemap_urls=()` | sitemaps whose pages are crawled too |
| `robots_sitemaps=False` | also read the sitemaps that robots.txt of the start URLs' sites names; needs `respect_robots` |

Filters apply to discovered links, not to the start URLs. Patterns match the
normalized URL both percent-encoded and decoded, so `r"/café"` works. A link
that passes the filters but redirects to a URL that does not, such as a
sign-in page on another domain, is skipped: it is left out of the results
and listed in `skipped_urls` as `redirected out of scope`.
Invalid start URLs, sitemap URLs or patterns raise `ValueError` before anything is fetched.

```python
async with AsyncCrawler(max_depth=1) as crawler:
    pages = await crawler.crawl(
        ["https://example.com/"],                          # may be empty when sitemaps are given
        sitemap_urls=["https://example.com/sitemap.xml"],
        robots_sitemaps=True,
        same_domain_only=True,
    )
    crawler.failed_sitemaps                                # {sitemap URL: "ErrorType: message"}
```

Sitemaps are read before the first page is fetched: indexes are followed,
gzipped files unpacked (see `SitemapParser` for the limits). A sitemap is
downloaded like a page: robots.txt, the rate limit, retries and the circuit
breaker apply, and its requests count in `crawl_stats().requests`, but not
in `max_pages` or `error_stats()`. A page a sitemap lists has depth 0, like
a start URL, so its links are followed up to `max_depth`; unlike a start
URL, it must pass the filters, and a redirect does not bring another host
into the crawl. `same_domain_only` keeps the hosts of `sitemap_urls` as well
as those of the start URLs. A sitemap that cannot be downloaded or read is
logged and listed in `failed_sitemaps`, and the crawl goes on.

After a crawl, and during one, the crawler exposes its state:

| Attribute | Content |
|-----------|---------|
| `processed_urls` | `{url: ParsedPage}`, the pages returned by `crawl()` |
| `failed_urls` | `{url: "ErrorType: message"}` |
| `skipped_urls` | `{url: reason}` for pages fetched but left out, e.g. redirected out of scope |
| `blocked_urls` | `{url: reason}` for pages robots.txt did not allow to fetch |
| `unreachable_urls` | `{url: reason}` for pages not fetched because robots.txt of their site was unreachable |
| `failed_sitemaps` | `{sitemap url: "ErrorType: message"}` for sitemaps that could not be read |
| `visited_urls` | every URL taken for fetching, successful or not |
| `url_depths` | depth of every URL accepted into the queue; 0 for start URLs and pages listed in sitemaps |
| `crawl_stats()` | `CrawlStats`: processed, failed, skipped, blocked, unreachable, queued, in progress, active requests, elapsed, pages per second; requests, retries, current and average requests per second, average gap between requests to a host, average wait for the rate limit; pages saved and not saved, see [Saving pages](#saving-pages) |
| `stats.get_stats()` | the pages by outcome, status code and domain, see [Page statistics](#page-statistics) |
| `error_stats()` | `ErrorStats`, see [Error statistics](#error-statistics) |
| `rate_limiter.get_stats()` | `RateStats`, with requests, interval and average gap per host |
| `circuit_breaker.get_stats()` | `{host: CircuitStats}`, see [Circuit breaker](#circuit-breaker) |

The building blocks can be used on their own: `CrawlerQueue` (priorities,
deduplication, completion detection), `SemaphoreManager` (global and
per-domain limits), `RateLimiter`, `RobotsParser`, `SitemapParser`, `RetryStrategy`,
`CircuitBreaker` and `UrlFilter`.

### Page statistics

`crawler.stats` is a `CrawlerStats`: it counts every page the crawl is done
with, once, however many attempts it took. `crawl_stats()` tells how the
crawl is going (the queue, requests in flight, the request rate);
`stats.get_stats()` tells what it got, as a plain dict ready for a report:

```python
stats = crawler.stats.get_stats()
print(f"{stats['successful']} of {stats['total_pages']} pages in {stats['elapsed_seconds']:.1f}s")
```

| Key | Content |
|-----|---------|
| `total_pages` | pages the crawl is done with: `successful + failed + skipped` |
| `successful` | pages fetched and parsed, the ones `crawl()` returns |
| `failed` | pages in `failed_urls` |
| `skipped` | pages in `skipped_urls`: fetched, but redirected out of scope |
| `elapsed_seconds` | running time of the crawl, up to now while it runs |
| `pages_per_second` | `total_pages / elapsed_seconds` |
| `avg_response_time` | average time of a page request (of its last attempt, if retried) |
| `status_codes` | `{status: pages}`, e.g. `{200: 41, 404: 2}`; pages that got no response are not here |
| `errors` | `{error class: pages}` for the failed pages, most frequent first |
| `top_domains` | `{host: pages}`, the 10 hosts with the most pages, largest first |
| `started_at`, `finished_at` | UTC times in ISO 8601; `finished_at` is `None` while the crawl runs |

Pages that were never requested (disallowed by robots.txt, or of a site
whose robots.txt is unreachable) are not counted; they are in `blocked_urls`
and `unreachable_urls`. The statistics are reset when the next `crawl()`
starts. `CrawlerStats(top_domains=20)` can also be used on its own: `start()`,
`record_page(url, status=..., elapsed=..., error=..., skipped=...)`, `finish()`.

The statistics can be written to a file, during the crawl or after it:

```python
crawler.stats.export_to_json("stats.json")           # get_stats() as JSON
crawler.stats.export_to_html_report("report.html")   # title="Crawl report" by default
```

The JSON file holds the same keys; the status codes are strings there, as
JSON has no other keys. The HTML report is a single file with a summary and,
for status codes, top domains and errors, a bar chart and a table. It needs
no network and no other files to be viewed: the styles are inline, the charts
(drawn with matplotlib) are embedded images, and there are no scripts. Both
methods replace the file if it exists and raise `OSError` if it cannot be
written.

### Saving pages

Give the crawler a storage, and `crawl()` saves every page it has processed:

```python
from crawler import AsyncCrawler, CompositeStorage, CSVStorage, JSONStorage, SQLiteStorage, storage_from_env

storage = JSONStorage("pages.jsonl")              # a record per line
storage = JSONStorage("pages.json", indent=2)     # or one indented JSON array
storage = CSVStorage("pages.csv", encoding="utf-8-sig")
storage = SQLiteStorage("crawler.db")
storage = storage_from_env()                      # the database of CRAWLER_DATABASE_URL
storage = CompositeStorage(JSONStorage("pages.jsonl"), SQLiteStorage("crawler.db"))  # both

crawler = AsyncCrawler(storage=storage)
await crawler.crawl(start_urls=["https://example.com"])
await crawler.close()                             # writes what is left and closes the storage

async with SQLiteStorage("crawler.db") as storage:
    async for record in storage.read():           # oldest first, one at a time
        print(record["url"], record["status_code"], record["crawled_at"])
    print(await storage.count(), await storage.status_counts(), await storage.get("https://example.com/"))
```

Every storage keeps the same record, `PageRecord`, and gives it back with
the same types:

| Key | Content |
|-----|---------|
| `url` | the requested URL, normalized |
| `title` | the page title; an empty string if it has none |
| `text` | visible text of the page |
| `links` | absolute links found on the page |
| `metadata` | `description`, `keywords`, `language`, `canonical`, plus `final_url` (the URL after redirects) and `depth` in the crawl |
| `crawled_at` | when the page was processed: a `datetime` in UTC |
| `status_code` | HTTP status of the response |
| `content_type` | media type of the response; an empty string if the server sent none |

| Storage | Keeps the pages in | Notes |
|---------|--------------------|-------|
| `JSONStorage(path, indent=None)` | a JSON Lines file, or one indented array with `indent` | records are added without reading the file, and read back in pieces; the array is valid JSON after every write |
| `CSVStorage(path, encoding="utf-8")` | a CSV file with a header row | the header comes from the first record, or from the file if it exists; `links` and `metadata` are JSON in a cell; quoting per RFC 4180; a character the encoding lacks is written as `?` |
| `SQLiteStorage(path)` | the `pages` table of an SQLite file | `links` and `metadata` as JSON text, `crawled_at` as ISO 8601 in UTC |
| `PostgresStorage(dsn)` | the `pages` table of a PostgreSQL database | `links` and `metadata` as `JSONB`, `crawled_at` as `TIMESTAMPTZ`; a connection pool |
| `CompositeStorage(*storages)` | each of the storages | a page counts as written once all of them have it; one failing does not stop the others |

All of them share the behavior of `DataStorage`:

- `save(record)` puts the record into a buffer; the buffer is written once
  it holds `batch_size` records (100 by default), on `flush()` and on
  `close()`. Several workers may save at once.
- A failed write is retried by the storage's own `retry_strategy`: by default
  3 times with exponential backoff from 0.1 s, for the errors a retry can
  cure (I/O errors, a locked SQLite database, a lost PostgreSQL connection).
  When the retries run out, `StorageError` is raised and the records stay in
  the buffer, so the next write takes them along. A repeated write does not
  duplicate records. For `cooldown` seconds after that (5 by default)
  `save()` only buffers, so a storage that is down does not slow the crawl
  down; `flush()` and `close()` write at once all the same. Any other error (e.g. a value the database refuses) is
  raised as it is and its batch is dropped, so that one bad record does not
  fail every later write.
- `read()` iterates over the saved records, oldest first, without loading
  them all; `pending` and `written` count the records in the buffer and those
  written out.

A database storage creates its table on the first use (`init_db()`), with
`url` unique and indexes on `crawled_at` and `status_code`. A batch is one
transaction: all of its pages are saved or none. Saving a URL again replaces
its row. `count()`, `status_counts()` and `get(url)` query the table.

In a crawl, a failed save never stops the crawler: it is logged, the page
stays in the results, and `crawl_stats()` counts `saved` and `save_failed`.
`saved` counts the pages actually written out, so pages still in the buffer
are in neither while the crawl runs; `crawl()` flushes the storage before it
returns, and what could not be written by then is `save_failed`. Only
processed pages are saved, not the failed or skipped ones. A storage that
stays down does not slow the crawl: after a write runs out of retries, saves
only buffer for `cooldown` seconds before the storage tries again; the pages
are written at the end of the crawl, or counted as `save_failed`.

The database is chosen by a URL: `storage_from_url(url)` takes it as an
argument, `storage_from_env()` reads it from `CRAWLER_DATABASE_URL` and
falls back to `sqlite:///crawler.db`.

| URL | Storage |
|-----|---------|
| `sqlite:///crawler.db` | SQLite file relative to the working directory |
| `sqlite:////var/data/crawler.db` | SQLite file at an absolute path |
| `postgresql://user:password@host:5432/database` (or `postgres://`) | PostgreSQL |

A URL with another scheme, or without one, raises `ValueError`. To start a
PostgreSQL server for the crawler, use the compose file of the repository:

```bash
docker compose up -d --wait                  # PostgreSQL 17 on localhost:5432
export CRAWLER_DATABASE_URL=postgresql://crawler:crawler@localhost:5432/crawler
python src/main.py save
CRAWLER_POSTGRES_PORT=55432 docker compose up -d --wait   # if port 5432 is taken
```

Another database needs a driver and a few lines of storage: `DatabaseStorage`
holds the SQL and talks to the database through `DatabaseDriver` (connect,
execute, fetch, placeholders and column types), and `register_database` makes
its URL scheme known:

```python
class MySQLStorage(DatabaseStorage):
    WRITE_ERRORS = (OSError, aiomysql.OperationalError)   # what a retry can cure

    def __init__(self, url: str, **options) -> None:
        super().__init__(MySQLDriver(url), **options)      # MySQLDriver(DatabaseDriver)

register_database("mysql", MySQLStorage)
storage = storage_from_url("mysql://user:password@host/database")
```

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
not even downloaded: `fetch_and_parse` fails with `ParseError`, and so does
an empty document; a crawl lists such pages as failed. This keeps a crawl
from pulling in archives or videos it finds links to. The parser can be used on its own:
`HTMLParser().parse(html, url)`, or `await HTMLParser().parse_html(html, url)`
in async code. Pass `AsyncCrawler(parser=HTMLParser(same_host_only=True))` to
keep only links to the page's own host.

## Tests

```bash
pytest                      # unit + integration, no internet needed
pytest tests/unit           # parser, URLs, queue, limits, robots.txt, retries, circuit breaker, storages, client with a fake session
pytest tests/integration    # real HTTP, crawls, sitemaps, rate limits, robots.txt, retries, the circuit breaker and saving against a local aiohttp server
pytest -m network           # smoke tests against the real internet
pytest -m postgres          # the database tests and the save demo against PostgreSQL
```

The database tests run on SQLite by default. With the marker `postgres` the
same checks run on a PostgreSQL server: start it with `docker compose up -d
--wait`, or point `CRAWLER_TEST_DATABASE_URL` at another one (the default is
`postgresql://crawler:crawler@localhost:5432/crawler`). The tests drop and
create the `pages` table.

```bash
ruff format src tests       # format
ruff check src tests        # lint
```

## Project structure

```
src/
├── main.py                 # demo CLI: `crawl`, `errors`, `save`, `parse` and `benchmark` commands
├── demo_site.py            # DemoSite: a local site that fails on purpose, for `errors` and `save`
└── crawler/
    ├── client.py           # AsyncCrawler: fetching, parsing, crawl()
    ├── queue.py            # CrawlerQueue: URL priority queue and statuses
    ├── semaphores.py       # SemaphoreManager: global and per-domain limits
    ├── rate_limiter.py     # RateLimiter: requests per second, delays, jitter, rate stats
    ├── robots.py           # RobotsParser, RobotsRules: robots.txt per RFC 9309
    ├── sitemap.py          # SitemapParser: sitemaps and sitemap indexes, gzip, limits
    ├── retry.py            # RetryStrategy: which errors to retry, backoff, Retry-After
    ├── circuit_breaker.py  # CircuitBreaker: blocks a failing host for a while
    ├── error_stats.py      # ErrorTracker: counts errors, retries and their outcomes
    ├── stats.py            # CrawlerStats: pages by outcome, status code and domain, speed, running time
    ├── report.py           # the statistics as JSON and as an HTML report with charts
    ├── filters.py          # UrlFilter: host and pattern rules
    ├── parser.py           # HTMLParser
    ├── urls.py             # URL validation, normalization, resolution
    ├── models.py           # FetchResult, ParsedPage, PageRecord, CrawlStats, ErrorStats, RateStats, CircuitStats
    ├── exceptions.py       # FetchError hierarchy, StorageError
    └── storage/
        ├── base.py         # DataStorage: buffer, batches, retries of failed writes
        ├── json_file.py    # JSONStorage: JSON Lines or an indented array
        ├── csv_file.py     # CSVStorage: header, quoting, encodings
        ├── database.py     # DatabaseStorage and the DatabaseDriver interface: table, indexes, upsert
        ├── sqlite.py       # SQLiteDriver, SQLiteStorage (aiosqlite)
        ├── postgres.py     # PostgresDriver, PostgresStorage (asyncpg)
        ├── composite.py    # CompositeStorage: several storages at once
        └── factory.py      # storage_from_url, storage_from_env, register_database
docker-compose.yml          # PostgreSQL for the crawler and its tests
tests/
├── fixtures/               # valid and broken HTML pages
├── pages.py                # test pages and a small site for crawl tests
├── helpers.py              # test bot name, crawler options for tests that skip politeness, sitemaps, page records, a storage in memory
├── unit/                   # parser, URLs, queue, limits, robots.txt, sitemaps, retries, circuit breaker, error and page stats, reports, filters, storages, client
└── integration/            # local HTTP server, databases; live tests marked `network`, PostgreSQL ones `postgres`
docs/
├── asyncio_concepts.md     # notes on async concepts used here
├── concurrency_control.md  # notes on queues, limits and crawl order
├── data_storage.md         # notes on saving data: files, databases, batching, failed writes
├── error_handling.md       # notes on error kinds, retries, timeouts and circuit breakers
├── html_parsing.md         # notes on HTML parsing and URL handling
└── politeness.md           # notes on rate limiting, robots.txt and backoff
```
