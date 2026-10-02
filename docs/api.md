# API reference

The classes and functions of the `crawler` package: what they take, what
they return and how they behave. For the settings of a crawl kept in a file
see the [configuration guide](configuration.md); for the command line, the
[README](../README.md#command-line).

| Section | What it covers |
|---------|----------------|
| [Fetching and crawling](#fetching-and-crawling) | `AsyncCrawler` and its methods |
| [Politeness](#politeness) | rate limit, robots.txt, User-Agent |
| [Retries](#retries) | `RetryStrategy`, `RateLimiter` and `RobotsParser` on their own |
| [Circuit breaker](#circuit-breaker) | `CircuitBreaker` |
| [Error statistics](#error-statistics) | `error_stats()` |
| [Timeouts](#timeouts) | connect, read and total timeouts |
| [Crawling](#crawling) | `crawl()`: depth, filters, sitemaps, state of a crawl |
| [Page statistics](#page-statistics) | `CrawlerStats`, export to JSON and HTML |
| [AdvancedCrawler](#advancedcrawler) | the crawler set up by a configuration |
| [Live progress](#live-progress) | `show_progress`, `ProgressTracker` |
| [Configuration](#configuration) | `load_config`, `CrawlerConfig` |
| [Logging](#logging) | `configure_logging` |
| [Saving pages](#saving-pages) | the storages, `PageRecord`, databases by URL |
| [Parsed page](#parsed-page) | `ParsedPage`, `HTMLParser` |

## Fetching and crawling

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

## Politeness

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

## Retries

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

## Circuit breaker

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

## Error statistics

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

## Timeouts

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

## Crawling

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

`crawl()` returns every parsed page, so it holds them all in memory until
it ends. A large crawl that saves its pages to a storage does not need
that: `AsyncCrawler(storage=..., keep_pages=False)` lets a page go once it
is saved and its links are queued. `crawl()` then returns an empty dict,
the pages are in the storage and the counts in `stats` and `crawl_stats()`;
memory stays nearly flat (see [performance.md](performance.md)).

After a crawl, and during one, the crawler exposes its state:

| Attribute | Content |
|-----------|---------|
| `processed_urls` | `{url: ParsedPage}`, the pages returned by `crawl()`; empty with `keep_pages=False` |
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

## Page statistics

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

## AdvancedCrawler

`AdvancedCrawler` puts everything together by a configuration: the crawler
with its limits, retries and circuit breaker, the sitemaps, the filters, the
storage, the statistics, the reports and the log.

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

| Member | What it does |
|--------|--------------|
| `AdvancedCrawler(config)` | takes a `CrawlerConfig`; the defaults without one |
| `AdvancedCrawler.from_config(path, overrides)` | reads a YAML or a JSON file, see the [configuration guide](configuration.md) |
| `await crawl()` | crawls the start URLs and the sitemaps of the configuration, saves the pages, writes the reports of the `report` section; returns the pages by URL |
| `write_reports()` | writes the reports of the `report` section and returns their paths; `crawl()` calls it, call it yourself after a crawl that was cancelled |
| `get_stats()` | the statistics of the latest crawl, see [Page statistics](#page-statistics) |
| `export_to_json(filename)`, `export_to_html_report(filename, title=)` | write the statistics to a file; the title is `report.title` by default |
| `await close()` | closes the crawler, writes what the storage still holds, stops logging to the file; `async with` does it too |
| `config`, `crawler`, `storage`, `stats` | the configuration, the `AsyncCrawler` that does the work, its storage (`None` without outputs) and its `CrawlerStats` |

Directories of the log, the reports and the files of the storage are created
if they are missing. A configuration with neither `urls` nor `sitemaps.urls`
makes `crawl()` raise `ConfigError`. A report that cannot be written is
logged and does not fail the crawl. Logging is set up when the crawler is
made (see [Logging](#logging)) and belongs to the whole process: with two
crawlers at once the log is written as the later one says.

To show the progress of the crawl, run it as a task and pass the inner
crawler to `show_progress`:

```python
crawl = asyncio.create_task(crawler.crawl())
await show_progress(crawler.crawler, crawl, crawler.config.crawler.max_pages)
pages = await crawl
```

## Live progress

`show_progress` prints a line about a running crawl every second, until the
crawl ends:

```python
import asyncio

from crawler import AsyncCrawler, show_progress

async with AsyncCrawler() as crawler:
    crawl = asyncio.create_task(crawler.crawl(["https://example.com/"], max_pages=100))
    await show_progress(crawler, crawl, max_pages=100)
    pages = await crawl
```

```
[######--------------]  30% | 30/100 pages, 1 failed | 1.6 pages/s | ETA 44s | active 6 (2 in flight) | queued 88 | 19s
```

| Part | Meaning |
|------|---------|
| bar, percent | pages done of `max_pages`, rounded down |
| `30/100 pages, 1 failed` | pages requested and finished (processed, failed, skipped), and the failed among them |
| `pages/s` | the speed over the last 10 seconds |
| `ETA` | the time the remaining pages take at that speed; `--` while the speed is 0, `done` once the crawl has ended |
| `active`, `in flight` | pages taken by workers, and the HTTP requests being made |
| `queued` | pages waiting in the queue |
| the last value | the time since the crawl started |

The percent and the time left are measured against `max_pages`: a site with
fewer pages ends sooner, with the last line below 100%. The line goes to
stderr (or to `stream`). In a terminal it is redrawn in place, and log
records are printed above it; in a file or a pipe every update takes a line.

For another output, `ProgressTracker(max_pages).update(crawler.crawl_stats())`
returns the same numbers as a `Progress` (`done`, `total`, `percent`,
`pages_per_second`, `eta`, `active`, ...), and `format_progress` makes the
line of it.

## Configuration

`load_config(path, overrides)` reads a YAML or a JSON file into a
`CrawlerConfig`, checked as it is loaded; `CrawlerConfig.from_dict(mapping)`
does the same for a mapping. The keys, their limits, the errors and the
overrides are described in the [configuration guide](configuration.md).

## Logging

Every module logs to a logger named after it (`crawler.client`,
`crawler.retry`, ...). `configure_logging` sends the records to the console
and, given a file, to that file as well:

```python
from crawler import configure_logging

configure_logging("INFO", "crawler.log", max_bytes=10 * 1024 * 1024, backup_count=5)
```

The console (stderr) gets a line of text per record:

```
19:41:40 | INFO    | crawler.client | Fetched https://example.com/: status=200 size=1256B elapsed=0.10s
```

The file gets JSON Lines, an object per record, so it can be read by
`jq` or loaded by a log collector as it is:

```json
{"time": "2026-10-02T16:41:40.438+00:00", "level": "INFO", "logger": "crawler.client", "message": "Fetched https://example.com/: status=200 size=1256B elapsed=0.10s"}
```

`time` is UTC in ISO 8601; a record logged with an exception has its
traceback under `exception`. The file is appended to. Once it reaches
`max_bytes` it becomes `crawler.log.1` (the older ones `crawler.log.2` and
so on, `backup_count` of them are kept) and a new one is started; with
either of the two set to 0 the file is never rotated. A record is never
split between files, so one longer than `max_bytes` makes a file larger
than that.

The level is `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` and applies
to both the console and the file. The root logger is configured, so the
records of other libraries are written too. A second call replaces the
handlers of the first; handlers added by other code are left alone. The
directory of the file is not created: `OSError` is raised if the file cannot
be opened, and the logging stays as it was. The arguments are the keys of
the `logging` section of the configuration file.

## Saving pages

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

`storage_from_output(output)` chooses the storage by the name of a file:
`.jsonl` (or `.ndjson`) is JSON Lines, `.json` an indented array, `.csv` CSV,
`.db` (or `.sqlite`, `.sqlite3`) SQLite; a string with `://` is a database
URL. This is what the `storage.outputs` of a configuration file go through.

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
python src/demo_main.py save
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

## Parsed page

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
