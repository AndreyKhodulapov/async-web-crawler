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
  the start hosts, and documents, images and archives are not followed);
  guards against endless URL spaces: tracking parameters dropped, a URL
  length limit, `<link rel="canonical">` for variants of a page, a page
  limit per host
- **Sitemaps** as a source of pages: plain and index sitemaps, gzip, the
  sitemaps named in robots.txt
- **Concurrency**: one connection pool, a global limit of requests in
  flight and an optional limit per host
- **Politeness**: requests per second per host or overall, minimum delay
  and jitter, robots.txt per RFC 9309 with Crawl-delay, `nofollow` and
  `noindex` of links, `<meta name="robots">` and `X-Robots-Tag`, a
  configurable User-Agent with rotation
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
  and a clean stop on Ctrl-C that keeps the pages fetched so far
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
pip install -e .                       # the crawler and its dependencies
pip install -r requirements-dev.txt    # test and lint tools, for development
```

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
```

A crawl is set up by a configuration file, by options, or by both. An
option wins over the file; an option left out keeps the value of the file,
or the default without a file.

| Option | Configuration key | Effect |
|--------|-------------------|--------|
| `--config PATH` | | configuration file, YAML or JSON |
| `--urls URL [URL ...]` | `urls` | start URLs, in place of those of the file |
| `--max-pages N` | `crawler.max_pages` | pages to request, failed ones included |
| `--max-depth N` | `crawler.max_depth` | links followed from a start URL; 0 crawls the start URLs only |
| `--output PATH` | `storage.outputs` | where to save the pages: a `.jsonl`, `.json`, `.csv` or `.db` file, or a database URL; repeat for several, in place of those of the file |
| `--overwrite`, `--no-overwrite` | `storage.overwrite` | start output files anew, or add to them (the default; the log warns about a file that is not empty); a database keeps a row per URL either way |
| `--respect-robots`, `--no-respect-robots` | `crawler.respect_robots` | follow robots.txt, `nofollow` and `noindex`, or do not |
| `--same-domain-only`, `--no-same-domain-only` | `filters.same_domain_only` | follow links on the start hosts only (the default), or on any host |
| `--rate-limit RPS` | `crawler.rate_limit` | max requests per second to one host; 0 lifts the limit |
| `--stats-json PATH` | `report.stats_json` | write the statistics of the crawl to a JSON file |
| `--report PATH` | `report.html` | write an HTML report with charts |
| `--log-level LEVEL` | `logging.level` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` |
| `--log-file PATH` | `logging.file` | also write the log to a file, as JSON Lines |
| `--no-progress` | | do not show the progress line |

Everything else (sitemaps, the other filters, retries, the circuit breaker, timeouts)
is set in the file. The command line never keeps the pages in memory
(`crawler.keep_pages` is off whatever the file says): they go to `--output`.
The log and the progress line go to stderr, the summary to stdout:

```
[####################] 100% | 8/8 pages, 0 failed | 1.1 pages/s | done | active 0 (0 in flight) | queued 66 | 8s

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
URL is shown as `***`.

| Exit code | Meaning |
|-----------|---------|
| 0 | the crawl ran and fetched at least one page |
| 1 | no page was fetched, or a directory or the log file could not be opened |
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

## Documentation

| Document | Content |
|----------|---------|
| [docs/api.md](docs/api.md) | API reference: fetching and crawling, politeness, retries, the circuit breaker, timeouts, statistics, `AdvancedCrawler`, progress, logging, storages, the parsed page |
| [docs/configuration.md](docs/configuration.md) | configuration guide: every key with its type and default, validation, overrides, recipes |
| [docs/demo.md](docs/demo.md) | the demo commands and their output |
| [docs/performance.md](docs/performance.md) | measurements against a synchronous crawler, memory, bottlenecks found and fixed |
| [config.example.yaml](config.example.yaml) | every configuration key with its default |
| [examples/](examples/) | a crawl from Python by a configuration file |

Notes on the concepts behind the crawler:
[asyncio](docs/asyncio_concepts.md),
[concurrency control](docs/concurrency_control.md),
[HTML parsing](docs/html_parsing.md),
[politeness](docs/politeness.md),
[error handling](docs/error_handling.md),
[data storage](docs/data_storage.md),
[advanced features](docs/advanced_features.md).

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
pytest tests/integration    # real HTTP, crawls, sitemaps, rate limits, robots.txt, retries, the circuit breaker, saving, AdvancedCrawler, the command line, the example and the scale demo against a local aiohttp server
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
├── main.py                 # command line of the crawler: a configuration file and options over it
├── cli_options.py          # checks of command-line values shared by main.py and demo_main.py
├── demo_main.py            # demo CLI: `crawl`, `errors`, `save`, `parse`, `benchmark` and `scale` commands
├── demo_urls.yaml          # URLs the demo commands use when none are given
├── demo_site.py            # DemoSite: a local site that fails on purpose, for `errors` and `save`
├── demo_scale.py           # ScaleSite, SyncCrawler and the measurements of the `scale` command
└── crawler/
    ├── advanced.py         # AdvancedCrawler: the crawler, storage, statistics, reports and log by a configuration
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
    ├── config.py           # CrawlerConfig, load_config: YAML or JSON file, defaults, validation
    ├── logging_setup.py    # configure_logging: text on the console, JSON Lines in a rotated file
    ├── progress.py         # ProgressTracker, show_progress: percent, speed, time left, active tasks
    ├── filters.py          # UrlFilter: host, pattern and file extension rules
    ├── parser.py           # HTMLParser
    ├── urls.py             # URL validation, normalization, resolution
    ├── models.py           # FetchResult, ParsedPage, PageRecord, CrawlStats, ErrorStats, RateStats, CircuitStats
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
└── config.yaml             # the configuration of the example
config.example.yaml         # every configuration key with its default
docker-compose.yml          # PostgreSQL for the crawler and its tests
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
├── asyncio_concepts.md     # notes on async concepts used here
├── concurrency_control.md  # notes on queues, limits and crawl order
├── data_storage.md         # notes on saving data: files, databases, batching, failed writes
├── error_handling.md       # notes on error kinds, retries, timeouts and circuit breakers
├── html_parsing.md         # notes on HTML parsing and URL handling
├── performance.md          # sync vs async measurements, memory, bottlenecks found and fixed
└── politeness.md           # notes on rate limiting, robots.txt and backoff
```
