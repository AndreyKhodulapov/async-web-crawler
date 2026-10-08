# Demo commands

The demo commands show the parts of the crawler one by one, on real sites
and on local ones that fail on purpose:

```bash
python src/demo_main.py crawl      # or errors, save, parse, benchmark, scale
python src/demo_main.py crawl --help
```

`crawl` follows links from start pages, `errors` crawls a local site that
fails on purpose, `save` writes crawled pages to JSON, CSV and a database,
`parse` extracts data from pages, `benchmark` compares sequential and
concurrent fetching, and `scale` measures the crawler against a synchronous
one on sites of growing size.
All but `scale` accept `--concurrency`, `--log-level`, the timeouts
(`--connect-timeout` and `--read-timeout`, 5 s by default, `--total-timeout`,
10 s, and `--timeout-growth`, 1.5, see [Timeouts](api.md#timeouts)) and the
politeness options:

| Option | Default | Effect |
|--------|---------|--------|
| `--rps` | 1 | max requests per second to one host; 0 removes the limit |
| `--min-delay` | 0 | min seconds between two requests to one host |
| `--jitter` | 0 | random extra delay of up to this many seconds |
| `--no-robots` | off | do not check robots.txt; `errors` and `save` do not check it unless given `--robots` |
| `--retries` | 2 (0 for `benchmark`, 3 for `errors` and `save`) | retries of timeouts, network errors, 408, 429, 500, 502-504 and 520-524 |
| `--retry-delay` | 1 (0.2 for `errors` and `save`) | seconds before the first retry, doubled for every next one up to 30 s, see [Retries](api.md#retries) |
| `--breaker-threshold` | 0.5 | block a host once this share of its requests in the last minute (5 at least) failed with a timeout, a network error, 408 or 5xx |
| `--breaker-cooldown` | 30 (1 for `errors` and `save`) | seconds a blocked host is left alone before a probe request |
| `--no-breaker` | off | never block a host |
| `--user-agent` | `AsyncWebCrawler/0.1 (+repo URL)` | repeat to rotate several; all must share the bot name |

Logs and progress go to stderr; the report goes to stdout.

`crawl`, `parse` and `benchmark` take URLs as arguments. Without them each
uses its list in [`src/demo_urls.yaml`](../src/demo_urls.yaml); edit the file to
change the defaults. `errors` and `save` crawl a local site.

## crawl

```bash
python src/demo_main.py crawl                                     # two sandboxes, depth 2, 30 pages
python src/demo_main.py crawl https://books.toscrape.com/ --max-depth 1 --max-pages 20 --same-domain
python src/demo_main.py crawl https://books.toscrape.com/ --exclude '/category/' --include '/catalogue/' --json crawl.json
python src/demo_main.py crawl --per-domain 4 --concurrency 20     # at most 4 requests to one host at a time
python src/demo_main.py crawl --rps 2 --min-delay 0.5 --jitter 0.3 --user-agent "MyBot/1.0 (+https://example.com/bot)"
```

The default start pages are sandboxes made for crawling practice, and their
robots.txt shows the rules at work. webscraper.io disallows its pagination
and product pages, one of them with a wildcard rule (`/test-sites/pagination*?page=`),
and web-scraping.dev sets `Crawl-delay: 2`.
While it runs, a progress line is updated every second (when stderr is a
file or a pipe, a line is printed every 30 seconds):

```
[######--------------]  30% | 9/30 pages, 1 failed | 1.6 pages/s | ETA 14s | active 6 (2 in flight) | queued 15 | 7s
```

The percent is the share of `--max-pages` done, `pages/s` the speed over the
last 10 seconds and `ETA` the time the remaining pages take at that speed.
`active` counts pages taken by workers; `in flight` counts those actually
being downloaded, which never exceeds `--per-domain` for a single site.
See [Live progress](api.md#live-progress).
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

Then come the [error statistics](api.md#error-statistics) and the state of the
[circuit breaker](api.md#circuit-breaker) of every host, here for a crawl of
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
minute and include robots.txt; a request made good by a retry is one success,
one that failed after its retries one failure. `OPENED` and `REJECTED` count
since the crawl started. With `--json`, the parsed pages (with their depth), the failed,
skipped, blocked and unreachable URLs with the reasons, the statistics, the
per-host table, the error statistics and the circuit breakers are saved to a file.

## errors

```bash
python src/demo_main.py errors                              # the local site only
python src/demo_main.py errors https://httpbin.org/status/503 --robots --rps 1 --json report.json
python src/demo_main.py errors --log-level WARNING          # failed attempts only
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
| `/empty` | an HTML page with nothing in it | `ParseError`, no retry |
| `/data.json` | JSON | skipped as not HTML, its body not downloaded; not an error |
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
INFO    | crawler.crawl_run | Deferred http://localhost:50865/page/6 for 1.0s: circuit breaker of localhost is open (5 of 5 requests failed in 60s), next probe in 1.0s
WARNING | crawler.retry | Failed http://127.0.0.1:50864/server-error on attempt 2/4 after 0.62s, no retries left for HTTP 500: TransientHTTPError: HTTP 500 Internal Server Error
WARNING | crawler.retry | Attempt 1/4 for http://127.0.0.1:50864/rate-limited failed: TransientHTTPError: HTTP 429 Too Many Requests; retrying in 1.0s
WARNING | crawler.retry | Attempt 1/4 for http://127.0.0.1:50864/slow failed: FetchTimeoutError: read timeout (1.0s); retrying in 0.2s
INFO    | crawler.circuit_breaker | Circuit breaker of localhost is half-open: probing it with http://localhost:50865/page/6
INFO    | crawler.crawl_run | Deferred http://localhost:50865/page/7 for 1.0s: circuit breaker of localhost is half-open, waiting for the probe request
INFO    | crawler.retry | Succeeded http://127.0.0.1:50864/flaky on attempt 3/4 after 1.63s
INFO    | crawler.retry | Succeeded http://127.0.0.1:50864/slow on attempt 2/4 after 2.82s
INFO    | crawler.crawl_run | Gave up on http://localhost:50865/page/8: circuit breaker of localhost opened 3 times
```

Then come the pages, the error statistics and the circuit breakers:

```
=== Crawl (26 pages, 3.39s) ===
DEPTH  RESULT                                LINKS  URL
    0  ok                                       25  http://127.0.0.1:50864/
    1  ok                                        0  http://127.0.0.1:50864/articles/1
    ...
    1  ok                                        0  http://127.0.0.1:50864/flaky
    1  ok                                        0  http://127.0.0.1:50864/rate-limited
    1  TransientHTTPError: HTTP 500...              http://127.0.0.1:50864/server-error
    1  ok                                        0  http://127.0.0.1:50864/slow
    1  PermanentHTTPError: HTTP 404 Not...          http://127.0.0.1:50864/missing
    1  PermanentHTTPError: HTTP 403...              http://127.0.0.1:50864/private
    1  ParseError: empty document                   http://127.0.0.1:50864/empty
    1  skipped, not HTML: application/json          http://127.0.0.1:50864/data.json
    1  NetworkError:...                             http://localhost:50865/page/1
    ...
    1  NetworkError:...                             http://localhost:50865/page/7
    1  CircuitOpenError: circuit breaker...         http://localhost:50865/page/8
    1  DNSError:...                                 http://unreachable.invalid/
Crawled: 12 pages, failed: 13, skipped: 1, blocked: 0, unreachable: 0, left in queue: 0, speed: 7.4 pages/s

=== Errors (24 failed attempts) ===
By kind:  TransientError 6, PermanentError 2, NetworkError 15, ParseError 1, other 0
By class: NetworkError 11, TransientHTTPError 5, DNSError 4, PermanentHTTPError 2, ParseError 1, FetchTimeoutError 1
Retries: 12, pages recovered by a retry: 3, average time per retry: 0.58s
Permanent errors (2):
  http://127.0.0.1:50864/missing  PermanentHTTPError: HTTP 404 Not Found
  http://127.0.0.1:50864/private  PermanentHTTPError: HTTP 403 Forbidden

=== Circuit breaker (3 hosts: 0 open, 1 half-open) ===
HOST                 STATE      REQUESTS  FAILURES  OPENED  REJECTED
127.0.0.1            closed           17         1       0         0
localhost            half-open         0         0       3         8
unreachable.invalid  closed            1         1       0         0

Error report saved to error_report.json
```

The breaker of `localhost` shows no requests, since opening it clears its
window. It opened three times: after the first failures and after two failed
probes; the crawl then gave up on the page still waiting, so the circuit
ends up open, or half-open if its last cooldown is over. The report in `error_report.json` (or `--json PATH`) holds the error
statistics, the circuit breakers, the failed pages with the full errors and
the fetched pages.

## save

```bash
python src/demo_main.py save                                # pages.jsonl, pages.csv and crawler.db
python src/demo_main.py save --indent 2 --csv-encoding utf-8-sig --batch-size 5
python src/demo_main.py save --database-url sqlite:///data/pages.db --append
CRAWLER_DATABASE_URL=postgresql://crawler:crawler@localhost:5432/crawler python src/demo_main.py save
```

The command crawls the local site of [`errors`](#errors), with the same
defaults, and saves every page it gets to three storages at once: a JSON
file, a CSV file and a database. Pages that failed are not saved. The
database is chosen by `--database-url`, else by the `CRAWLER_DATABASE_URL`
environment variable, else it is `sqlite:///crawler.db`; for PostgreSQL see
[Saving pages](api.md#saving-pages). URLs given on the command line are fetched and
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
cannot be written, the table shows which storages have the pages. A storage
that cannot be opened at all (a file in a directory that does not exist, a
database that cannot be reached) stops the command before the crawl, with
an error on stderr. The files
are read from their start, and the database finds the pages of this crawl by
URL. The files are replaced on every run, unless `--append` is given. The
database is never emptied: saving a URL again replaces its row, but the local
site gets a new port on every run, so its pages are new URLs and the rows add
up; the report says so. A password in the database URL is shown as `***`.

## parse

```bash
python src/demo_main.py parse
python src/demo_main.py parse https://example.com --same-host  # internal links only
python src/demo_main.py parse --preview 10 --json pages.json   # save full results
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

## benchmark

```bash
python src/demo_main.py benchmark
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
python src/demo_main.py benchmark --concurrency 3 --read-timeout 3   # tune the crawler
python src/demo_main.py benchmark https://example.com https://python.org  # custom URLs
python src/demo_main.py benchmark --log-level WARNING                # errors only
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

## scale

```bash
python src/demo_main.py scale                    # sites of 100, 500 and 1000 pages
python src/demo_main.py scale 200 2000 --delay 0.1 --concurrency 50
python src/demo_main.py scale --no-memory --json scale.json
```

Compares the crawler with a synchronous one that fetches a page at a time
(`SyncCrawler` in `src/demo_scale.py`: `urllib` and the same parser). Both
crawl a local site whose every response takes `--delay` seconds, as a
remote server's would. The time is measured first, then the peak memory in
runs of their own (allocation tracing slows a crawl down); the last column
is the crawler with `keep_pages=False`. The default run takes about three
minutes, most of it the synchronous crawler.

```
=== Scale: one request at a time vs 20 at once (the site answers in 50 ms) ===
PAGES  SYNC TIME  SYNC PAGES/S  ASYNC TIME  ASYNC PAGES/S  SPEEDUP  SYNC MEMORY  ASYNC MEMORY  PAGES NOT KEPT
  100      5.50s          18.2       0.56s          178.6     9.8x       1.4 MB        2.3 MB          2.2 MB
  500     27.57s          18.1       2.12s          235.4    13.0x       4.7 MB        6.1 MB          2.5 MB
 1000     55.00s          18.2       4.13s          242.2    13.3x       8.8 MB       10.3 MB          3.0 MB
```

What the numbers mean, the bottlenecks they showed and what was done about
them is in [performance.md](performance.md).

### scale --workers

```bash
export CRAWLER_DATABASE_URL=postgresql://crawler:crawler@localhost:5432/crawler  # docker compose up -d postgres
python src/demo_main.py scale 1000 --workers 1 2 4
```

Compares a crawl of one process with crawl jobs in PostgreSQL crawled by 1,
2 and 4 worker processes, in place of the synchronous crawler. Each crawl
has a local site of its own; each worker is a process of the command line,
`python src/main.py worker`, with `--concurrency` requests in flight, and
the local crawl has as many. The job, named `scale`, is created anew for
every crawl, with no rate limit, robots.txt or retries, as the local crawl.
The time of a job is that of its database, from the start of its first
worker to its end: starting Python in every process is not counted.
`SPEEDUP` is against the job of the fewest workers; the line under the
rows of a site is what the database costs a page of one worker. A crawl
job that requests a page more than once, or a crawl that misses pages, is
reported there as well. Memory is not measured.

In compose, next to the database (the output below):

```bash
docker compose run --rm --no-deps --entrypoint python worker src/demo_main.py scale 1000 --workers 1 2 4
```

```
=== Scale: one process vs crawl jobs of 1, 2, 4 worker processes, 20 requests at once in each (the site answers in 50 ms) ===
PAGES  CRAWL            TIME   PAGES/S  SPEEDUP
 1000  local           6.23s     160.4        -
 1000  1 worker        6.97s     143.5     1.0x
 1000  2 workers       4.48s     223.0     1.6x
 1000  4 workers       4.19s     238.8     1.7x
  a page of one worker takes 0.7 ms more than a local one
The time of a job is that of its database, from the start of its first worker to its end; SPEEDUP is against the job of the fewest workers.
```

The database must be PostgreSQL: `--database-url`, or `CRAWLER_DATABASE_URL`.
Ctrl-C stops the workers, which put their pages back. Why four workers
are no faster than two is in
[performance.md](performance.md#crawl-jobs-of-several-workers).
