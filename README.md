# async-web-crawler

An asynchronous web crawler built on `asyncio`, `aiohttp` and BeautifulSoup.
Starting from a few URLs or a sitemap, it crawls a site breadth-first with
many requests at once, parses every page into structured data and saves it
as it goes: to JSON, CSV, SQLite or PostgreSQL. It is polite by default (a
rate limit per host, robots.txt, backoff when a site struggles), survives
failures (retries, a circuit breaker per host) and reports what it did:
live progress, statistics, an HTML report, a log. A crawl is set up by a
configuration file, by command-line options, or from Python.

## Features

- **Crawling**: a priority queue of URLs and a pool of workers, depth and
  page limits, deduplication of normalized URLs, filters by domain, by
  regular expressions and by file extension (by default the crawl stays on
  the start hosts and their subdomains, and documents, images and archives
  are not followed);
  guards against endless URL spaces: tracking parameters dropped, a URL
  length limit, `<link rel="canonical">` for variants of a page, a page
  limit per host; a queue bounded by the page limit, so memory does not
  grow with the size of the site
- **Sitemaps** as a source of pages: plain and index sitemaps, gzip, the
  sitemaps named in robots.txt
- **Concurrency**: one connection pool, a global limit of requests in
  flight and an optional limit per host
- **Politeness**: requests per second per host or overall, minimum delay
  and jitter, robots.txt per RFC 9309 with Crawl-delay, `nofollow` and
  `noindex` of links, robots meta tags and `X-Robots-Tag` (also those
  that name the crawler), a
  configurable User-Agent with rotation
- **Sessions**: cookies kept between requests as a browser keeps them,
  taken from a `cookies.txt` file exported by the browser and saved back
  after the crawl; extra headers of every request; their values never
  reach the log or the reports
- **Proxies**: http and https proxies with a password, a site kept on one
  proxy or the proxies taking turns per request, `HTTP_PROXY` and
  `NO_PROXY` of the environment; a proxy that keeps failing is taken out
  of rotation for a while without blocking the sites behind it; requests
  and failures per proxy in the summary and the reports, passwords hidden
- **JavaScript rendering** in a headless Chromium (Playwright):
  every HTML page or those matching patterns, waiting for an event or a
  CSS selector; the page itself downloaded as without a browser, images
  and fonts not loaded, navigations of a page checked against robots.txt
  and the filters like redirects; the browser shares the cookies, the
  headers and the proxy of the crawler, and the cookies its scripts set
  are saved; pages rendered and the time they took in the summary and
  the reports
- **Retries** of timeouts, network errors, HTTP 408, 429 and 5xx with
  exponential backoff and jitter, honoring `Retry-After`; timeouts that
  grow with every retry
- **Circuit breaker** per host: a host that keeps failing is left alone
  for a while, then tested with a single probe request
- **Clear error types** grouped by whether a retry can help; one failing
  URL never breaks a crawl; a size limit on every body, gzip bombs included
- **HTML parsing** into title, metadata, text, links, images, headings,
  tables and lists; broken HTML and any encoding are handled; runs in a
  worker thread
- **Storage** behind one interface: JSON Lines or a JSON array, CSV,
  SQLite, PostgreSQL, or several at once; asynchronous writes in batches
  with retries; a file is added to by the next run, or started anew with
  `--overwrite`
- **Configuration file** in YAML or JSON, checked on load with every
  problem reported by the path of its key
- **Command line** with live progress, a summary, exit codes for scripts
  and a clean stop on Ctrl-C that keeps the pages fetched so far; start
  URLs from a list file or stdin
- **Statistics and reports**: pages by outcome, status code, error and
  domain, speed and response time; export to JSON and to a self-contained
  HTML report with charts
- **Logging** to the console and to a rotated file of JSON Lines
- **Live progress**: percent of the page limit, current speed, time left,
  active tasks
- **Measured performance**: 13x faster than a synchronous crawler on 1000
  pages, memory that does not grow with the crawl
  ([docs/performance.md](docs/performance.md))

## Requirements

Python 3.11+

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .                       # the crawler and its dependencies, Playwright among them
pip install -r requirements-dev.txt    # test and lint tools, for development
playwright install chromium            # the browser that renders JavaScript pages
```

`make install` runs the three commands in `.venv`. The Chromium of
Playwright is a download of its own (about 600 MB on disk); only
rendering needs it, the crawler does without it otherwise.

## Quick start

```bash
python src/main.py --urls https://books.toscrape.com/ --max-pages 20 --output pages.jsonl --report report.html
```

Or keep the settings in a file:

```yaml
# config.yaml
urls:
  - https://books.toscrape.com/
crawler:
  max_pages: 20
  rate_limit: 2.0           # requests per second
storage:
  outputs: [pages.jsonl, pages.csv]
report:
  html: report.html
```

```bash
python src/main.py --config config.yaml
```

[config.example.yaml](config.example.yaml) lists every key with its default;
the [configuration guide](docs/configuration.md) explains them.

## Command line

```bash
python src/main.py --config config.yaml
python src/main.py --urls https://example.com --max-pages 100 --output results.json
python src/main.py --config config.yaml --max-pages 500 --report report.html
python src/main.py --config config.yaml --urls-file urls.txt
some_tool | python src/main.py --config config.yaml --urls-file -
python src/main.py --urls https://quotes.toscrape.com/js/ --render --output quotes.jsonl
```

A crawl is set up by a configuration file, by options, or by both. An
option wins over the file; an option left out keeps the value of the file,
or the default without a file.

A long list of start URLs can be kept in a text file, one URL per line
([examples/urls.txt](examples/urls.txt)): `#` starts a comment line, blank
lines are skipped, a URL given twice is crawled once. One file of settings
then serves many lists. A line that is not an http(s) URL, or has a space
inside (a comment after the URL), stops the run before anything is
requested; the error counts the valid and the invalid lines and lists the
first 20 of the invalid ones by number. A list without URLs is no error
when the configuration has sitemaps to crawl.

| Option | Configuration key | Effect |
|--------|-------------------|--------|
| `--config PATH` | | configuration file, YAML or JSON |
| `--urls URL [URL ...]` | `urls` | start URLs, in place of those of the file |
| `--urls-file PATH` | `urls` | start URLs from a text file, one per line; `-` reads them from stdin. With `--urls`, both are crawled, those of `--urls` first; together they replace the `urls` of the file and keep its `sitemaps.urls` |
| `--max-pages N` | `crawler.max_pages` | pages to request, failed ones included |
| `--max-depth N` | `crawler.max_depth` | links followed from a start URL; 0 crawls the start URLs only |
| `--output PATH` | `storage.outputs` | where to save the pages: a `.jsonl`, `.json`, `.csv` or `.db` file, or a database URL; repeat for several, in place of those of the file |
| `--overwrite`, `--no-overwrite` | `storage.overwrite` | start output files anew, or add to them (the default; the log warns about a file that is not empty); a database keeps a row per URL either way |
| `--cookies-file PATH` | `session.cookies_file` | send the cookies of a Netscape `cookies.txt` file, as a browser extension or `curl -c` exports it |
| `--save-cookies PATH` | `session.save_cookies` | write the cookies to a `cookies.txt` file after the crawl, readable by its owner only |
| `--proxy URL` | `proxy.urls` | send the requests through a proxy, `http://[user:password@]host:port`; repeat for several, in place of those of the file (`proxy.from_env` is turned off) |
| `--render` | `rendering.mode` | render every HTML page in a headless Chromium, so that links and text JavaScript makes are found; sets `mode: always` and clears `rendering.include` of the file. Without Chromium, exit code 2 with the command to install it |
| `--respect-robots`, `--no-respect-robots` | `crawler.respect_robots` | follow robots.txt, `nofollow` and `noindex`, or do not |
| `--same-domain-only`, `--no-same-domain-only` | `filters.same_domain_only` | follow links on the start hosts only (the default), or on any host |
| `--rate-limit RPS` | `crawler.rate_limit` | max requests per second to one host; 0 lifts the limit |
| `--stats-json PATH` | `report.stats_json` | write the statistics of the crawl to a JSON file |
| `--report PATH` | `report.html` | write an HTML report with charts |
| `--log-level LEVEL` | `logging.level` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` |
| `--log-file PATH` | `logging.file` | also write the log to a file, as JSON Lines |
| `--no-progress` | | do not show the progress line |

Everything else (sitemaps, the other filters, retries, the circuit breaker, timeouts,
the rotation of proxies, what and how to render) is set in the file. The command line never keeps the pages in memory
(`crawler.keep_pages` is off whatever the file says): they go to `--output`.
The log and the progress line go to stderr, the summary to stdout:

```
[####################] 100% | 8/8 pages, 0 failed | 1.1 pages/s | done | active 0 (0 in flight) | queued 0 | 8s

=== Crawl finished (8.06s) ===
Pages: 8 (8 successful, 0 failed, 0 skipped), 1.0 pages/s, average response time 2.37s
Status codes: 200: 8
Top domains: books.toscrape.com: 8
Saved: 8 pages to out/pages.jsonl, out/pages.csv
Reports: out/stats.json, out/report.html
Log: out/crawler.log
```

At the default level `INFO` the log has a line per request; `--log-level
WARNING` leaves the progress line and the failures. A password in a database
or a proxy URL is shown as `***`. With proxies, the summary has a line of
them: `Proxies: http://user:***@proxy-1:3128 (41 sent, 0 failed), ...`;
with rendering, a line of the pages rendered: `Rendering: 12 pages
rendered, 1 failed, average 0.84s` (the average of the rendered ones).

| Exit code | Meaning |
|-----------|---------|
| 0 | the crawl ran, fetched at least one page and saved every page it should |
| 1 | no page was fetched, some could not be saved, or a directory, the log file, an output file or the database could not be opened; an output that cannot be opened is reported before anything is requested |
| 2 | wrong options or configuration; nothing was requested or written |
| 130 | interrupted with Ctrl-C |

A crawl interrupted with Ctrl-C stops its requests, saves the pages fetched
by then, writes the reports of them and prints the summary. The summary names
the reports that were written: one that could not be is an error in the log.

## Usage from Python

`AdvancedCrawler` puts everything together by a configuration: the crawler,
the storage, the statistics, the reports and the log.

```python
import asyncio

from crawler import AdvancedCrawler


async def main():
    crawler = AdvancedCrawler.from_config("config.yaml")

    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"Processed: {stats['total_pages']} pages")
    print(f"Successful: {stats['successful']}")
    print(f"Failed: {stats['failed']}")

    crawler.export_to_html_report("report.html")
    await crawler.close()


asyncio.run(main())
```

[examples/advanced_usage.py](examples/advanced_usage.py) is this example
with live progress, ready to run:

```bash
python examples/advanced_usage.py      # crawls by examples/config.yaml, writes to out/
PYTHONPATH=src python examples/advanced_usage.py   # the same without `pip install -e .`
```

[examples/render_js.py](examples/render_js.py) shows what rendering adds
to a page: it downloads one without and with the browser and prints the
text and the links of both (needs the Chromium of Playwright):

```bash
python examples/render_js.py                                   # https://quotes.toscrape.com/js/
python examples/render_js.py https://example.com/app/
```

```
https://quotes.toscrape.com/js/
  without rendering: 74 characters of text, 5 links
  with rendering:    1485 characters of text, 5 links, rendered in 1.45s
  links only JavaScript shows: none
  text: Quotes to Scrape Login “The world as we have created it is a process of our thinking. ...
```

`AsyncCrawler` is the crawler itself, without files or configuration:

```python
from crawler import AsyncCrawler, JSONStorage

async with AsyncCrawler(max_concurrent=5, requests_per_second=2.0, storage=JSONStorage("pages.jsonl")) as crawler:
    page = await crawler.fetch_and_parse("https://example.com")       # one page
    print(page["title"], len(page["links"]))

    pages = await crawler.crawl(["https://books.toscrape.com/"], max_pages=50, same_domain_only=True)
    print(f"Crawled {len(pages)} pages, failed: {len(crawler.failed_urls)}")
```

The parts work on their own too: `RateLimiter`, `RobotsParser`,
`SitemapParser`, `RetryStrategy`, `CircuitBreaker`, `HTMLParser`, the
storages. All of it is described in the [API reference](docs/api.md).

## Limitations

- **`max_pages` counts requests, not saved pages.** Every page requested
  counts: one that failed, one that turned out not to be HTML, one marked
  `noindex`, one that is a variant of another page by its canonical URL.
  So `--max-pages 100` may save fewer than 100 pages; the summary shows how
  many were skipped and why. Pages that robots.txt disallows are not
  requested and do not count. A page put off after its request (a
  Retry-After too long to retry, a circuit breaker that opened, a redirect
  to a host that cannot be asked yet) counts once, when it is crawled, but
  is requested each time it comes back, so a crawl may send more than
  `max_pages` page requests.
- **A site is a host name.** `same_domain_only` keeps the crawl on the
  start hosts and their subdomains, `www.example.com` and `example.com`
  being one host. There is no public suffix list: a start URL on
  `docs.example.com` does not bring in `example.com`, and a site spread over
  unrelated domains needs `same_domain_only: false` with an `include`
  pattern for each of them. The limits go by the exact host name: the rate
  limit, the circuit breaker and `max_pages_per_host` are kept per host, so
  a site that spreads its links over `example.com`, `www.example.com` and
  `docs.example.com` is asked at up to three times the rate, as one server.
- **A crawl cannot be resumed.** Ctrl-C keeps the pages fetched so far,
  but the queue is lost: the next run starts from the start URLs again,
  adding to the output files or starting them anew with `--overwrite`.
- **A storage that keeps failing fills memory.** Pages that could not be
  written stay buffered and are retried; a database that is down for long
  holds every page since the outage in memory.
- **One host at a time under a rate limit.** The workers take pages from
  one queue in the order of depth and wait for the turn of their host in
  the rate limiter; while the pages of the first host last, those of the
  other hosts wait in the queue. A crawl of several hosts with a rate limit
  takes about the sum of their times, not the longest of them (two hosts
  of 30 pages at 2 requests per second: 25 s instead of 15), and the time
  left on the progress line does not know it. One site never notices; for
  the sitemaps of several hosts, a list of start URLs on several hosts,
  `same_domain_only: false` or a site on many subdomains, set `rate_limit`
  higher or to `null` and rely on `max_per_domain`. See [docs/concurrency_control.md](docs/concurrency_control.md).
- **No login form.** The crawler does not fill in and send a login form.
  A site behind a login is crawled with the cookies of a browser session:
  log in in the browser, export its cookies to `cookies.txt` and pass it
  with `--cookies-file`. `--save-cookies` keeps the session the site
  renewed for the next run.
- **No cookies for IP addresses.** aiohttp keeps cookies of host names
  only, so a site reached as `http://127.0.0.1:8080/` cannot keep a session
  of the crawler; reach it as `http://localhost:8080/`.
- **Extra headers go to every host.** The headers of `session.headers`
  are sent to every request, robots.txt and other sites included: an
  `Authorization` header reaches a third-party site that the crawl follows
  a link or a redirect to. Use it with `same_domain_only`, the default.
- **No SOCKS proxies.** Only http and https proxies; `socks5://` is a
  configuration error. A local bridge from HTTP to SOCKS (such as
  `gost` or `privoxy`) makes a SOCKS proxy usable.
- **A timeout through a proxy is the site's.** The crawler cannot tell a
  slow proxy from a slow site, so a timeout counts against the site, in
  its circuit breaker, and never takes the proxy out of rotation. A proxy
  that loses packets looks like slow sites; a proxy that cannot be reached
  or refuses the password is taken out as it should be.
- **An error page of an http proxy is the site's.** An `http://` URL is
  asked of the proxy itself, which answers for the site: its own 502 or
  503, when it cannot reach the site or fails, cannot be told from the
  site's. Such a response is retried and counts against the site, in its
  circuit breaker, and as a response through the proxy it clears the
  failures of the proxy, so a proxy that fails this way is never taken
  out of rotation.
- **A proxy sees what plain HTTP carries.** Through an http proxy the
  cookies and the headers of `session` reach `http://` sites in the clear,
  as the proxy reads the request; `https://` sites go through a tunnel the
  proxy cannot read. Use proxies you trust with the session.
- **`https://` proxies are not tested end to end.** The tests run an
  http proxy, with CONNECT for https sites; a proxy reached over TLS is
  left to aiohttp.
- **A rendered page is fetched by two clients.** The document is
  downloaded by aiohttp, its scripts, styles and data by Chromium: a site
  that compares the fingerprints of TLS and headers sees two clients.
- **The browser shares the session, not all of it.** The cookies go
  between the crawler and the browser around every page, but `SameSite`
  is not kept (as in `cookies.txt`): a cookie the browser gets from the
  crawler has the default of Chromium, `Lax`. A cookie a script sets for
  an IP address stays in the browser and never reaches `save_cookies`.
  With `keep_cookies: false` every page has a browser context of its own,
  so its scripts and styles are downloaded again for every page.
- **The browser goes through the proxy of the page.** The requests of a
  rendered page go through the proxy its document came through: with
  `per_request` one page is one address, the next page another. Every
  proxy has a browser context of its own for the whole crawl: many
  proxies with `per_request` cost memory. With `from_env` the requests of
  a page all go through the proxy of the scheme of its document, and the
  ranges of addresses in `NO_PROXY` (`10.0.0.0/8`) are not read, by the
  browser as by the crawler. The browser's requests are not counted in
  the statistics of the proxies, and their failures never take a proxy
  out of rotation.
- **A rendered page cannot reach a private network of another origin.**
  The browser gets the document from the crawler, not from its address,
  so Chromium takes the page for a public one and refuses its requests to
  another origin on a loopback or private address (Local Network Access).
  A site on an intranet whose scripts come from another private host is
  rendered without them. The page's own origin is not affected.
- **The browser's own requests are not checked against robots.txt**, as
  no browser checks them: scripts, styles, data, the requests of a page
  after it loaded. They are not counted against the rate limit either; a
  rendered page costs the site its scripts and data too. Images, fonts
  and media are not loaded by default.
- **What a page does after the wait is not seen.** A page is read once
  `wait_until` and `wait_for` are met: content loaded later, and a
  navigation started later, are missed. A page that changes its URL with
  `history.pushState` keeps the URL it was downloaded from. In the
  browser the User-Agent is `user_agent`; `user_agents` are not rotated
  there. The rendering `timeout` does not grow with retries, as the
  timeouts of requests do.
- **Not for URLs from strangers.** Links to private addresses
  (`127.0.0.1`, `10.0.0.0/8`, the cloud metadata address) are followed
  like any other. The crawler is a command-line tool for sites you choose,
  not a service that takes URLs from users.
- **Parsing is bound by one CPU.** HTML is parsed in a worker thread of
  one process; a few heavy pages per second is the ceiling whatever
  `max_concurrent` says. With a rate limit per host it never matters; a
  crawl of many hosts without one is parsing-bound. A page costs about
  forty times its size in memory and a couple of seconds per megabyte to
  parse, so pages over `max_page_size` (3 MiB) are not read and at most
  `max_parsing` (2) are parsed at once; raising both for a site of huge
  pages costs memory accordingly.
- **Some guards are constants, not options.** The URL length limit (2048),
  the redirect limit (10), the queue size (3 times the page limit), the
  wait at which a host's pages are put off (1 second), the times a
  robots.txt that cannot be read is downloaded again (3), how often its
  pages look in on that download (2 seconds) and the times a page waits
  for a Retry-After too long to retry (3) are class attributes of
  `AsyncCrawler`.

## Documentation

| Document | Content |
|----------|---------|
| [docs/api.md](docs/api.md) | API reference: fetching and crawling, politeness, retries, the circuit breaker, timeouts, cookies and headers, proxies, rendering, statistics, `AdvancedCrawler`, progress, logging, storages, the parsed page, the internal layers |
| [docs/configuration.md](docs/configuration.md) | configuration guide: every key with its type and default, validation, overrides, recipes |
| [docs/demo.md](docs/demo.md) | the demo commands and their output |
| [docs/performance.md](docs/performance.md) | measurements against a synchronous crawler, memory, bottlenecks found and fixed |
| [config.example.yaml](config.example.yaml) | every configuration key with its default |
| [examples/](examples/) | a crawl from Python by a configuration file; what rendering adds to a page |

Notes on the concepts behind the crawler:
[asyncio](docs/asyncio_concepts.md),
[concurrency control](docs/concurrency_control.md),
[HTML parsing](docs/html_parsing.md),
[politeness](docs/politeness.md),
[error handling](docs/error_handling.md),
[data storage](docs/data_storage.md),
[advanced features](docs/advanced_features.md),
[sessions, proxies and rendering](docs/sessions_proxies_rendering.md),
[architecture](docs/architecture.md).

## Demo

The demo commands show the parts of the crawler one by one: `crawl` follows
links from start pages, `errors` crawls a local site that fails on purpose,
`save` writes crawled pages to JSON, CSV and a database, `parse` extracts
data from pages, `benchmark` compares sequential and concurrent fetching,
and `scale` measures the crawler against a synchronous one on sites of 100,
500 and 1000 pages.

```bash
python src/demo_main.py crawl https://books.toscrape.com/ --max-depth 1 --max-pages 20 --same-domain
python src/demo_main.py errors
python src/demo_main.py scale
```

Their options and output are described in [docs/demo.md](docs/demo.md).

## Tests

```bash
pytest                      # unit + integration, no internet needed
pytest tests/unit           # parser, URLs, queue, limits, robots.txt, retries, circuit breaker, storages, client with a fake session
pytest tests/integration    # real HTTP, crawls, sitemaps, rate limits, robots.txt, retries, the circuit breaker, saving, AdvancedCrawler, the command line, the examples and the scale demo against a local aiohttp server
pytest -m network           # smoke tests against the real internet
pytest -m postgres          # the database tests and the save demo against PostgreSQL
pytest -m browser           # rendering in a headless Chromium; skipped without Playwright or Chromium
```

The test of `examples/render_js.py` against the real site has both
markers, `network` and `browser`, so `-m browser` reaches the internet
for it; `pytest -m "browser and not network"` leaves it out.

The database tests run on SQLite by default. With the marker `postgres` the
same checks run on a PostgreSQL server: start it with `docker compose up -d
--wait`, or point `CRAWLER_TEST_DATABASE_URL` at another one (the default is
`postgresql://crawler:crawler@localhost:5432/crawler`). The tests drop and
create the `pages` table. A server of the compose file on another port is
found by the same variable that moved it:

```bash
export CRAWLER_POSTGRES_PORT=55432          # port 5432 is taken
docker compose up -d --wait
pytest -m ""                                # every test: the default ones, network, postgres and browser
```

```bash
ruff format src tests       # format
ruff check src tests        # lint
```

The [Makefile](Makefile) keeps these commands short, with the tools of `.venv`:

```bash
make install                # the crawler, the tools and the Chromium of Playwright
make test                   # the default tests
make test-all               # every test, network, postgres and browser too
make lint                   # ruff check and a format check
make db                     # start the PostgreSQL of docker-compose.yml
make check                  # lint and every test: what a change must pass
make db check CRAWLER_POSTGRES_PORT=55432   # the same with the server on another port
```

## Project structure

```
src/
├── main.py                 # command line of the crawler: a configuration file and options over it
├── cli_options.py          # checks of command-line values shared by main.py and demo_main.py
├── demo_main.py            # demo CLI: `crawl`, `errors`, `save`, `parse`, `benchmark` and `scale` commands
├── demo_urls.yaml          # URLs the demo commands use when none are given
├── demo_site.py            # DemoSite: a local site that fails on purpose, for `errors` and `save`
├── demo_scale.py           # ScaleSite, SyncCrawler and the measurements of the `scale` command
└── crawler/
    ├── advanced.py         # AdvancedCrawler: the crawler, storage, statistics, reports and log by a configuration
    ├── client.py           # AsyncCrawler: the public API — fetch_*, crawl(), statistics, close()
    ├── crawl_run.py        # CrawlRun: one crawl — filters, deferred pages, counters, saving
    ├── frontier.py         # Frontier contract; MemoryFrontier: the pages of a crawl, their outcomes, max_pages
    ├── fetching.py         # Fetcher: one URL with robots.txt, circuit breaker, rate limit, retries and redirects
    ├── transport.py        # Transport contract; HttpTransport: single GET requests over an aiohttp session, decoding, size limits
    ├── rendering.py        # Rendering, BrowserTransport, Renderer: HTML pages rendered in a headless Chromium (Playwright)
    ├── session.py          # cookies.txt files, the cookie jar, checks of cookies and headers
    ├── proxy.py            # ProxyPool, Proxy: rotation, proxies out of rotation, their statistics
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
    ├── config.py           # CrawlerConfig, load_config, load_urls: YAML or JSON file, defaults, validation, URL lists
    ├── logging_setup.py    # configure_logging: text on the console, JSON Lines in a rotated file
    ├── progress.py         # ProgressTracker, show_progress: percent, speed, time left, active tasks
    ├── filters.py          # UrlFilter: host, pattern and file extension rules
    ├── parser.py           # HTMLParser
    ├── urls.py             # URL validation, normalization, resolution
    ├── models.py           # FetchResult, ParsedPage, PageRecord, CrawlStats, ErrorStats, RateStats, CircuitStats, ProxyStats, RenderStats
    ├── exceptions.py       # FetchError hierarchy, StorageError, ConfigError
    └── storage/
        ├── base.py         # DataStorage: buffer, batches, retries of failed writes
        ├── json_file.py    # JSONStorage: JSON Lines or an indented array
        ├── csv_file.py     # CSVStorage: header, quoting, encodings
        ├── database.py     # DatabaseStorage and the DatabaseDriver interface: table, indexes, upsert
        ├── sqlite.py       # SQLiteDriver, SQLiteStorage (aiosqlite)
        ├── postgres.py     # PostgresDriver, PostgresStorage (asyncpg)
        ├── composite.py    # CompositeStorage: several storages at once
        └── factory.py      # storage_from_output, storage_from_url, storage_from_env, register_database
examples/
├── advanced_usage.py       # a crawl by a configuration file: progress, statistics, report
├── config.yaml             # the configuration of the example
└── urls.txt                # a list of start URLs for --urls-file
config.example.yaml         # every configuration key with its default
docker-compose.yml          # PostgreSQL for the crawler and its tests
Makefile                    # install, test, lint and database commands
tests/
├── fixtures/               # valid and broken HTML pages
├── pages.py                # test pages and a small site for crawl tests
├── helpers.py              # test bot name, crawler options for tests that skip politeness, sitemaps, page records, a storage in memory
├── unit/                   # links of the documentation, parser, URLs, queue, limits, robots.txt, sitemaps, retries, circuit breaker, error and page stats, reports, configuration, logging, progress, filters, storages, client
└── integration/            # local HTTP server, databases; live tests marked `network`, PostgreSQL ones `postgres`
docs/
├── api.md                  # API reference
├── configuration.md        # configuration guide: every key, validation, recipes
├── demo.md                 # the demo commands and their output
├── advanced_features.md    # notes on sitemaps, configuration, logging, monitoring and integration
├── architecture.md         # notes on the layers of the crawler and refactoring without changing behaviour
├── asyncio_concepts.md     # notes on async concepts used here
├── concurrency_control.md  # notes on queues, limits and crawl order
├── data_storage.md         # notes on saving data: files, databases, batching, failed writes
├── error_handling.md       # notes on error kinds, retries, timeouts and circuit breakers
├── html_parsing.md         # notes on HTML parsing and URL handling
├── performance.md          # sync vs async measurements, memory, bottlenecks found and fixed
├── politeness.md           # notes on rate limiting, robots.txt and backoff
└── sessions_proxies_rendering.md  # notes on cookies, proxies and a headless browser
```
