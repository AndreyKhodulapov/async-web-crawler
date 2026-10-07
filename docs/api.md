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
| [Cookies and headers](#cookies-and-headers) | the session of the crawler, `cookies.txt` files |
| [Proxies](#proxies) | `ProxyPool`: rotation, proxies taken out of it, their errors |
| [Rendering](#rendering) | `Rendering`: pages rendered in a headless Chromium, their errors |
| [Crawling](#crawling) | `crawl()`: depth, filters, sitemaps, state of a crawl |
| [Page statistics](#page-statistics) | `CrawlerStats`, export to JSON and HTML |
| [AdvancedCrawler](#advancedcrawler) | the crawler set up by a configuration |
| [Crawl jobs](#crawl-jobs) | `create_job`, `run_worker`, `job_stats`, `job_progress`: a crawl in PostgreSQL that workers share, its statistics and progress |
| [Live progress](#live-progress) | `show_progress`, `ProgressTracker` |
| [Configuration](#configuration) | `load_config`, `load_urls`, `CrawlerConfig` |
| [Logging](#logging) | `configure_logging` |
| [Saving pages](#saving-pages) | the storages, `PageRecord`, databases by URL |
| [Parsed page](#parsed-page) | `ParsedPage`, `HTMLParser` |
| [Internals](#internals) | the layers behind `AsyncCrawler` and what each is responsible for |

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
| `crawl(start_urls, max_pages)` | `{url: ParsedPage}` for fetched pages | failed URLs go to `failed_urls`; a storage that cannot be opened raises `StorageError` before anything is requested |
| `close()` | - | safe to call twice; called by `async with`; closes the storage too |

Every method checks robots.txt, waits for the rate limit and retries transient
failures; a URL that robots.txt disallows fails with `RobotsDisallowedError`
without being requested, and a URL of a site whose robots.txt cannot be read
fails with `RobotsUnreachableError`. A request to a host blocked by the
circuit breaker fails with `CircuitOpenError` without being sent, and one
for which every proxy is out of rotation with `NoProxyError` (see
[Proxies](#proxies)). With `rendering`, an HTML page is rendered in a
headless browser before it is returned (see [Rendering](#rendering)).

## Politeness

| Option | Default | Effect |
|--------|---------|--------|
| `requests_per_second` | `1.0` | requests per second to one host; `None` removes the limit |
| `per_domain_rate` | `True` | `False` applies the rate to all hosts together; Crawl-delay and retry pauses stay per host |
| `min_delay` | `0.0` | min seconds between two requests to one host |
| `jitter` | `0.0` | random extra delay of up to this many seconds after each request |
| `respect_robots` | `True` | check robots.txt before every request; in a crawl, also follow `nofollow` and `noindex` (see [Crawling](#crawling)) |
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
the site did not forbid them. In a crawl they are put off until robots.txt
is fetched again, so a site whose robots.txt failed for a moment is crawled
once it is back; only after the `AsyncCrawler.MAX_ROBOTS_RETRIES` (3)
downloads after the first have failed too, about three minutes, do its
pages go to `unreachable_urls`. A failure that does not pass by itself, a
bad certificate or a host name that does not exist (`DNSError`), is not
waited for at all: the pages go there at once. A resolver that fails for
now (`EAI_AGAIN`) is an outage, not `DNSError`, and is waited for; with
aiodns installed, which aiohttp then uses and which gives no such code,
every DNS failure is `DNSError`. A page that redirects to such a site
waits the same way and is requested again, and so does a
sitemap of the site (see [Crawling](#crawling)); they all share the
downloads of the site. Each download after the first is a single attempt,
without the retries and their growing timeouts (an attempt that failed
in a proxy is still made again through another one), and no page of a crawl
waits for a download longer than `AsyncCrawler.ROBOTS_POLL` (2) seconds:
the download goes on, and the page is put off for that long at a time
until it is over, so a site that is slow to fail holds no worker back
(outside a crawl, `fetch_url` and the others wait for the download). A
site given up on is not downloaded again for the rest of the crawl; once
it is over, `fetch_url` and the next `crawl()` on the same crawler try it
again.
Redirects are followed by
the crawler, one request at a time: the target of each is checked against
robots.txt of its own site and waits for the rate limit of its own host, as
a link to it would. Up to `AsyncCrawler.MAX_REDIRECTS` (10) redirects in a
row are followed; one more fails with `TooManyRedirectsError`, its target
not requested. A disallowed target fails the request with
`RobotsDisallowedError` before it is sent. Crawl-delay is capped at 30 seconds. While
a request that got HTTP 429, a Retry-After header or a timeout waits for
its retry, the whole host waits with it: such a failure usually means the
site is overloaded. After any other failure (HTTP 500, a reset connection)
only that request waits, and the other pages of the host are fetched
meanwhile. A Retry-After header holds back the host for
as long as it asks, up to `AsyncCrawler(max_retry_after=600.0)` seconds
(10 minutes), even when the request is not retried; a request whose
Retry-After is longer than `max_delay` of the retry strategy is not
retried. Later `fetch_url()` calls to the host wait for that time too.

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
success, so broken links do not block a site. A request counts once,
however many attempts it takes, robots.txt downloads included: its first
failure counts at once, so a host that goes down is spotted after its
first failed requests; a failed retry adds nothing; a retry that
succeeds turns the failure into a success. So one broken URL retried
three times does not open the circuit, and neither does a slow host whose
pages come through on the second attempt, as long as the retries land
before `min_requests` first attempts have failed: with that many requests
to the host in flight at once, their timeouts open the circuit before any
retry, and the probe after the cooldown, a first attempt with the base
timeout, may open it again. Each request of a redirect
chain counts for its own host: a link that redirects to a failing host
counts against that host, not the host of the link.
The circuit is checked before a request waits for the rate limit, where a
half-open one gives its probe to one request and refuses the rest, once
more when its turn comes, and a last time once it holds a concurrency slot:
a request that was already waiting when the circuit opened is not sent, but
it is refused only when its turn comes, and a request that waited for the
slot of a host behind the one that opened its circuit is refused too. A
retry the breaker would refuse is not made, so the request fails with the
error of its last attempt, not with `CircuitOpenError`. When the breaker
refuses the download of robots.txt, the page fails with `CircuitOpenError`
under its own URL, and robots.txt is not cached as unreachable. The same
goes for a download of robots.txt that failed in a proxy (`ProxyError`):
it says nothing of the site.

Errors of proxies (`ProxyError`) are not counted at all: a dead proxy
must not open the circuits of healthy sites. Proxies have states of their
own, see [Proxies](#proxies). A page the browser took too long to render
(`RenderTimeoutError`) is not counted either: its document came in time,
and a few pages with slow scripts must not block the site.

In a crawl, a page the breaker refuses is not failed: it is put off until
the circuit may let a probe through, or for a second while the probe is in
flight, and the workers go on with other pages meanwhile. So is a page
whose request was sent and failed while the circuit opened, on its own
failure or on those of the other requests in flight: the breaker refused
the retries it would have had, so it is requested again when the host may
be probed, instead of being the page lost to the outage. The probe is
such a retry: a page whose probe failed is failed with its own error and
not put off again, so that one broken page does not probe a healthy host
until it is given up. A page with an error that is never retried, such as
HTTP 501, fails at once too: the breaker took nothing from it. So the pages of
a host that went down for a moment are fetched once it is back, even when
the page refused was the last one `max_pages` allowed. A page put off
does not count toward `max_pages` until it is taken again, whether it was
refused before its request, failed with it, or was refused at the target of
its redirect: the request was not answered with the page, which is
requested again when it comes back. After the circuit of a host has opened
`AsyncCrawler.MAX_CIRCUIT_OPENINGS` (3) times in the crawl, no more probes
are sent: its remaining pages go to `failed_urls`, with `CircuitOpenError`
if they were never requested and with the error of their request if they
were, and a host that stays down holds the crawl for about two cooldowns.
A page given up on counts toward `max_pages` if its request was sent, as
any page that failed does.

The same goes for a host held back longer than
`AsyncCrawler.MIN_PENALTY_TO_DEFER` (1 second), by a Retry-After or the
pause before the retry of a request that found it overloaded (HTTP 429, a
timeout): its pages are put off until the host may be asked
again, instead of holding workers in the rate limiter, and count toward
`max_pages` only when they are taken again. A Retry-After longer than
`max_delay` of the retry strategy is logged as a warning once per host
(`example.com asked to wait 300s (Retry-After); its pages are put off until
then`): with one host in the crawl, nothing is requested until it ends.
The page that got such a Retry-After is not retried by its request, as
`RetryStrategy` says, but it comes back with the host, up to
`AsyncCrawler.MAX_WAITS_PER_PAGE` (3) times; then it goes to `failed_urls`.
A page that got it with a permanent error (HTTP 403 with a Retry-After)
goes there at once: the host is held back all the same, but the page would
fail the same way when it came back.

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
| `max_page_size` | `3145728` | bytes of a page body (3 MiB); a larger one fails with `PageTooLargeError` (a permanent error) and the rest is not downloaded; `None` lifts the limit |
| `max_parsing` | `2` | pages parsed at once, whatever `max_concurrent` says |

With the defaults the read timeout is 20 s on the first attempt and 30, 45
and 67.5 s on the three retries: a page that is only slow gets through, a
server that does not answer at all is not waited for forever. A timeout
fails with `FetchTimeoutError` that says which timeout fired, e.g.
`read timeout (20.0s)`, and is retried like any other transient error.

The body is read in chunks and given up once it is over its limit:
`max_page_size` for a page, 50 MB for a sitemap. The limit is on the body
unpacked from gzip or deflate, so a gzip bomb fails too, and a
Content-Length over the limit fails the request before the body is read.
robots.txt is cut at 500 KiB, the size RFC 9309 asks crawlers to read.

The size limit bounds the download, `max_parsing` the parsing: a page
takes about forty times its size in memory and a couple of seconds per
megabyte to parse (a 10 MiB page full of links measured 20 s and 400 MB),
and the GIL runs the parses one at a time anyway. With the defaults no
more than two pages of 3 MiB are parsed at once, about 250 MB; a site of
larger pages needs a larger `max_page_size`, and the memory grows with it.

## Cookies and headers

The crawler keeps the cookies sites set and sends them back, as a browser
does; robots.txt and sitemaps share them with the pages. Starting cookies,
extra headers and the cookies of a `cookies.txt` file are given to the
crawler, and its cookies are taken back after the crawl:

```python
from crawler import AsyncCrawler, load_cookies_file, make_cookie, save_cookies_file

cookies = load_cookies_file("cookies.txt")  # exported from the browser
cookies.append(make_cookie("consent", "yes", ".example.com"))  # the host and its subdomains

async with AsyncCrawler(cookies=cookies, headers={"Accept-Language": "en"}) as crawler:
    await crawler.crawl(["https://example.com/account/"], same_domain_only=True)
    save_cookies_file(crawler.export_cookies(), "cookies.txt")
```

| Name | What it does |
|------|--------------|
| `AsyncCrawler(headers=)` | headers sent with every request to every host; `User-Agent`, `Cookie`, `Host` and `Proxy-Authorization` are refused (`ValueError`) |
| `AsyncCrawler(cookies=)` | `http.cookiejar.Cookie` objects sent from the first request, each to its own domain |
| `AsyncCrawler(keep_cookies=False)` | no cookies sent or kept (aiohttp's `DummyCookieJar`); with `cookies` it is a `ValueError` |
| `export_cookies()` | the cookies the crawler keeps, those sites set included, as `http.cookiejar.Cookie`; also after `close()`. A cookie is for its host only if aiohttp sends it so: a cookie set without `Domain`, until the host sets it again with one |
| `make_cookie(name, value, domain, path=, secure=, expires=, http_only=)` | a cookie; `example.com` is that host only, `.example.com` also its subdomains |
| `load_cookies_file(path)` | the cookies of a Netscape `cookies.txt` file; expired ones are left out, session ones kept, those the crawler cannot send (of an IP address, with an invalid name) left out with a warning. A malformed file raises `ValueError` whose message does not quote it |
| `save_cookies_file(cookies, path)` | writes a `cookies.txt` file with mode `0600`, session cookies included; an existing file is replaced whole, so it gets that mode too |

The `cookies.txt` format is read and written by `http.cookiejar`, never with
`pickle`, which aiohttp's `CookieJar.save()` and `load()` use: loading a
pickle runs the code it holds. A cookie of an IP address is a `ValueError`:
aiohttp keeps cookies of host names only.

`AdvancedCrawler` takes all of it from the `session` section (see the
[configuration guide](configuration.md#session)) and writes `save_cookies`
when the crawl ends; `save_cookies()` writes it after a cancelled crawl.
Why it works this way: [the note on sessions](sessions_proxies_rendering.md#cookies-and-sessions).

## Proxies

With a `ProxyPool`, every request goes through a proxy of the pool: pages,
robots.txt and sitemaps alike.

```python
from crawler import AsyncCrawler, ProxyPool

proxies = ProxyPool(
    ["http://user:secret@proxy-1.example:3128", "http://proxy-2.example:3128"],
    rotation="per_host",
    max_failures=3,
    cooldown=60.0,
)
async with AsyncCrawler(proxies=proxies) as crawler:
    await crawler.crawl(["https://example.com/"], same_domain_only=True)
for label, stats in crawler.proxy_stats().items():
    print(label, stats.state, stats.requests, stats.failures)  # http://user:***@proxy-1.example:3128 active 41 0
```

| Name | What it does |
|------|--------------|
| `ProxyPool(urls, rotation=, max_failures=, cooldown=)` | the proxies, `http://` or `https://` with a port; `ValueError` for a URL that is not one, a SOCKS proxy or a proxy listed twice; the message never shows a password |
| `ProxyPool.from_env(max_failures=, cooldown=)` | a pool of the proxies of `HTTP_PROXY` and `HTTPS_PROXY`, with `NO_PROXY`; `None` if neither is set |
| `AsyncCrawler(proxies=)` | send the requests through the pool; `None`, the default, sends them directly |
| `crawler.proxies` | the pool, `None` without one |
| `proxy_stats()` | `{label: ProxyStats}`: `state` (`"active"` or `"out"`), `requests`, `failures`, `times_removed`; empty without a pool |
| `pick(url)`, `record(proxy, url, error)` | the proxy for a request (`None`: directly), and how it went; for a transport of your own |

`rotation="per_host"`, the default, sends every host through a proxy of
its own, chosen by a hash of the host that is the same in every run, so a
site sees one address and its session does not move between addresses.
`"per_request"` lets the proxies take turns, one request each.

A proxy that fails `max_failures` requests in a row is out of rotation for
`cooldown` seconds, logged as a warning, and its return as INFO; any
response through it clears the count, and once back, one more failure
takes it out again. A request through a proxy fails with:

| Error | When | Retried |
|-------|------|---------|
| `ProxyNetworkError` (a `ProxyError` and a `NetworkError`) | the proxy cannot be reached, its name does not resolve, the TLS of an `https://` proxy fails, or it answers HTTP 407 to CONNECT or to the request of an `http://` URL (inside the tunnel of an `https://` URL the site answers) | yes, through the next proxy at once; with `per_host` the host stays on that proxy |
| `NoProxyError` (a `ProxyError`) | every proxy for the URL is out of rotation: the request is not sent; the message says when the first is back | no |
| `NetworkError` | the proxy answered CONNECT with another status: it cannot or may not reach the site | yes, as any network error of the site |
| `FetchTimeoutError` | a timeout: the proxy and the site cannot be told apart | yes, as any timeout of the site |

Whatever the site answers through a proxy (a 404, a 503) is the site's,
and the proxy is up. The circuit breaker counts no `ProxyError`: a dead
proxy does not open the circuits of healthy sites. A download of robots.txt
that a proxy failed is retried through the other proxies as the request
of a page is; one that fails with a `ProxyError` all the same is not
cached: the page fails with the error of the proxy. `error_stats()` counts these errors under their own classes,
`ProxyNetworkError` as a `NetworkError`.

The rate limit, robots.txt, Crawl-delay, `max_per_domain` and the circuit
breaker go by the host of the URL, whatever proxy a request goes through.
`crawl()` counts `requests`, `failures` and `times_removed` anew, as it
does the counters of the breaker; the proxies out of rotation stay out.

The user name and the password of a proxy are taken out of its URL:
aiohttp gets the URL without them, and the password goes in the
`Proxy-Authorization` header, with CONNECT for an `https://` site, so the
site never sees it, and with the request itself for an `http://` one, which
the proxy takes out. A proxy is named by its `label`, the URL with the
password hidden (`http://user:***@host:port`), in the log, the errors and
the statistics; `repr()` of a `Proxy` leaves the header out.

`from_env()` reads the variables once, in either case, without
`ALL_PROXY`, `~/.netrc` or the proxies of the system settings, unlike
aiohttp's `trust_env`: a URL goes through the proxy of its scheme, or
directly when its scheme has none or `NO_PROXY` names its host. A proxy
without a scheme is an `http://` one. Each scheme has one proxy, so the
rotation does not matter. A variable that is not a proxy URL raises
`ValueError` with the name of the variable, not its value, and so does
one proxy in both variables with different passwords. The host of a proxy
is lowercased: `Proxy.example` and `proxy.example` are one proxy.

SOCKS proxies are not supported; an http proxy, or a local bridge from
HTTP to SOCKS, does instead.

`AdvancedCrawler` makes the pool of the `proxy` section (see the
[configuration guide](configuration.md#proxy)). Why it works this way:
[the note on proxies](sessions_proxies_rendering.md#proxies).

## Rendering

With `Rendering`, HTML pages are rendered in a headless Chromium before
they are parsed, so the links and the text that JavaScript makes are
found. It needs the browser of Playwright, `playwright install chromium`;
Playwright is imported when the first page is rendered, so the crawler
works without the browser until then.

```python
from crawler import AsyncCrawler, Rendering

rendering = Rendering(wait_until="load", wait_for=".quote", timeout=20.0)
async with AsyncCrawler(rendering=rendering) as crawler:
    page = await crawler.fetch_and_parse("https://quotes.toscrape.com/js/")
    print(len(page["text"]))  # about 1500 characters of quotes; without rendering, 74 of the header and footer
```

| Name | What it does |
|------|--------------|
| `Rendering(include=, wait_until=, wait_for=, timeout=, max_open_pages=, block_resources=)` | which pages are rendered and how long they are waited for; `ValueError` for a value out of its range or a pattern that is not a regular expression, `TypeError` for a string in place of a list |
| `include` | regular expressions searched in the URL, as in `UrlFilter`; given, only the pages that match one are rendered; empty (the default), every HTML page |
| `wait_until` | `"load"` (the default), `"domcontentloaded"` or `"networkidle"` (no request for half a second) |
| `wait_for` | a CSS selector to wait for after that; `None` by default |
| `timeout` | seconds the browser has for a page, the waits included; `30.0` |
| `max_open_pages` | pages rendered at once, each a browser tab of 50 to 100 MB; `2` |
| `block_resources` | types of requests the browser does not make (`RESOURCE_TYPES` of `crawler.rendering`: `"image"`, `"script"`, `"xhr"` ...); images, fonts and media by default |
| `renders(url)` | whether the page at `url` is rendered, if it is HTML |
| `AsyncCrawler(rendering=)` | render pages as it says; `None`, the default, renders none |
| `crawler.rendering` | the settings, `None` without rendering |
| `render_stats()` | a `RenderStats`: `rendered` (pages the browser loaded to the end, those that went elsewhere on their own included), `failed` (a timeout, a `RenderError`), `avg_render_time` (seconds per rendered page, from a free tab in a running browser to the HTML); `None` without rendering. `crawl()` counts anew |
| `browser_problem()`, `playwright_problem()` of `crawler.rendering` | why pages cannot be rendered here (Playwright or Chromium is not installed), with the command to install it; `None` if they can. `browser_problem()` is a coroutine that starts the driver of Playwright for a moment |

Every request is made as without a browser first: the page is downloaded
by aiohttp, through the proxies, with the cookies and the headers, within
`max_page_size`. Only a response that is HTML, for a URL that
`renders()`, goes to the browser: robots.txt, sitemaps, redirects and
other types never do. The browser gets the document as downloaded, not
again from the site, runs its JavaScript and loads what it asks for
itself, but `block_resources`. The page counts once against `max_pages`
and the rate limit, and is rendered within the concurrency slot of its
request; its response time includes the rendering. `fetch_url()` and the
other methods render an HTML page too.

A page that goes to another URL on its own (JavaScript setting
`location`, `<meta http-equiv="refresh">`, a form sent) comes back as a
redirect to that URL, with the status of the download: the browser is
stopped, and the crawler checks the target against robots.txt and the
filters, waits for the rate limit of its host and requests it, within
`MAX_REDIRECTS`, as it does after an HTTP redirect. A disallowed target
fails with `RobotsDisallowedError` before it is requested. Navigations of
frames and pop-up windows are refused: what they show is not in the HTML
of the page. Service workers are blocked.

The rendered page keeps the status, the headers and the final URL of its
download; its text is the HTML of the page once rendered (the DOM, as
`page.content()` serializes it), and its size the bytes of that HTML in
UTF-8. A tab is opened for every page and closed after it.

The browser shares the cookies, the headers and the proxies of the
crawler. A page is rendered in the browser context of the proxy its
document came through (one context per proxy, and one for the pages
without a proxy, each for the life of the browser), so all its requests
go through that proxy, with the password of it; the hosts of `NO_PROXY`
of a `ProxyPool.from_env()` are reached directly. The requests carry the
`user_agent` and the `headers`. Before a page, the context gets the
cookies the crawler changed since the last page, and after it, the
crawler gets those the context changed: cookies set by JavaScript and by
the responses to the page's requests go with the next download, to
`export_cookies()` and to `save_cookies`, and a cookie a script deletes
is deleted. Only the changes go each way, so two tabs do not undo each
other; when both sides change one cookie, the browser wins. Not shared:
`SameSite`, which `http.cookiejar` does not keep (Chromium gives such a
cookie its default, `Lax`), and the cookies of IP addresses or with a
value the crawler cannot send, which stay in the browser. A cookie
Chromium refuses, such as a `__Secure-` one without `Secure`, is left
out of the browser with a warning, the others go. With
`keep_cookies=False`, every page has a context of its own, without
cookies, closed after it. The requests of the browser are not those of
the crawler: `proxy_stats()` does not count them, and their failures
neither take a proxy out of rotation nor count in the circuit breaker.

| Error | When | Retried | Circuit breaker |
|-------|------|---------|-----------------|
| `RenderTimeoutError` (a `FetchTimeoutError`) | the page took the browser longer than `timeout`: `rendering timeout (30.0s)` | yes, with the same `timeout`; the other requests to the host do not wait for the retry | does not count: the host answered in time, the page is slow in the browser |
| `PageTooLargeError` | the rendered HTML is over `max_page_size` bytes | no | does not count, as without a browser |
| `RenderError` | Playwright or Chromium is not installed (the message has the command to install it), the browser could not start, crashed, or the page crashed in it | no | does not count: the browser failed, not the site |
| `CrawlerClosedError` | the crawler was closed while the page was rendered | no | does not count |

The browser is started for the first page to render, so a crawl without
such a page never starts it, and closed by `close()`. A browser that
crashes fails the pages it was rendering and is started again for the
next one, once (`Renderer.MAX_LAUNCHES`, 2 launches in all); after that,
or after a browser that could not start, every page to render fails with
`RenderError` at once. `error_stats()` counts `RenderError` as `other`.

`AdvancedCrawler` makes the settings of the `rendering` section (see the
[configuration guide](configuration.md#rendering)). Why it works this way:
[the note on the headless browser](sessions_proxies_rendering.md#headless-browser).

## Crawling

`crawl()` runs `max_concurrent` workers over a priority queue of URLs. A link
found on a page at depth `d` gets depth `d + 1` and is followed only up to
`max_depth`, so the site is walked breadth-first. `max_pages` caps the pages
requested, failed and skipped ones included; pages that robots.txt disallows are not
requested and do not count, and neither do pages the circuit breaker
refuses: they wait for their host, see [Circuit breaker](#circuit-breaker). URLs are normalized (including their
percent-encoding, so `/café` and `/caf%C3%A9` are one page), and each one is
fetched at most once. The target of a redirect is remembered before it is
requested: a later link to it is not fetched, and a redirect to a page
already seen is not followed (the page is listed in `skipped_urls` as
`redirected to a page already seen`), so a page is never saved under two
URLs. If the page fails on the way (the target answers an error, the chain
is too long), its targets are forgotten: a later link to one of them is
fetched as any other. A page whose redirect leads to a URL robots.txt disallows is listed in
`blocked_urls`; it counts toward `max_pages`, as its own request was sent.

Some sites have endless URL spaces: a listing under every sort order and
filter, a calendar, a session ID in every link. Against them a crawl:

- drops tracking parameters (`utm_*`, `fbclid`, `gclid`, `dclid`,
  `msclkid`, `yclid`) from every URL it queues, so `/a?utm_source=x` is
  requested as `/a` and is the same page; the other parameters are kept as
  they are (`strip_tracking_params`);
- does not follow a link longer than `MAX_URL_LENGTH` (2048 characters);
- skips a page whose `<link rel="canonical">` is the same URL with another
  query and is processed or still to be crawled, such as
  `/list?page=2&sort=price` with the canonical `/list?page=2`: it is listed
  in `skipped_urls` as `duplicate of <canonical>`, not returned or saved,
  and its links, being variants too, are not followed. A canonical URL with
  another path is not trusted: a site that points every page to its home
  page would lose them all. Nor is one that failed or was left out: the
  variant is kept then. A page that names itself, also under the URL it
  was redirected to, is not a duplicate;
- with `max_pages_per_host`, requests at most that many pages of one host:
  the other pages of the host are listed in `skipped_urls` without a
  request and do not count toward `max_pages`, nor toward the progress and
  the speed of the crawl (`crawl_stats().over_host_limit` counts them).

The first two happen before a request, the canonical URL is known only
after it: a variant of a page still counts toward `max_pages`.

With `respect_robots`, a crawl also does what pages ask of crawlers, see
[politeness.md](politeness.md#robots-directives-of-pages-and-links): links
marked `rel="nofollow"` are not followed, nor are the links of a page whose
robots meta tag or `X-Robots-Tag` header says `nofollow`; a page
that says `noindex` is not returned or saved, and is listed in
`skipped_urls` as `noindex in X-Robots-Tag` or
`noindex in a robots meta tag`, but its links are followed. `none` means
both. A robots meta tag is `<meta name="robots">` or a `<meta>` named
after the crawler: the robots.txt name of its `user_agent`,
`<meta name="asyncwebcrawler">` by default. A meta tag or an
`X-Robots-Tag` header that names another crawler is ignored.

| Option | Effect |
|--------|--------|
| `AsyncCrawler(max_depth=2)` | how far from the start pages to go; 0 fetches the start pages only |
| `AsyncCrawler(max_per_domain=None)` | parallel requests to one host; `None` means only `max_concurrent` applies |
| `max_pages_per_host=None` | pages requested from one host; `None` means only `max_pages` applies |
| `same_domain_only=False` | follow links on the start hosts only (and on the hosts their redirects end on, not those they pass through) and on their subdomains: `docs.example.com` for a start URL on `example.com`, not the other way round; `www.example.com` and `example.com` are one host. The configuration turns it on by default |
| `include_patterns=()` | regular expressions; a link must match at least one |
| `exclude_patterns=()` | regular expressions; a matching link is skipped, even if included |
| `exclude_extensions=()` | file extensions such as `"pdf"`; a link to such a file is skipped. Only the last extension of the URL path counts, in any case, the query does not. The configuration sets a list of documents, images, archives and media by default |
| `sitemap_urls=()` | sitemaps whose pages are crawled too |
| `robots_sitemaps=False` | also read the sitemaps that robots.txt of the start URLs' sites names; needs `respect_robots` |

Filters apply to discovered links, not to the start URLs. Patterns match the
normalized URL both percent-encoded and decoded, so `r"/café"` works. A link
that passes the filters but redirects to a URL that does not, such as a
sign-in page on another domain, is skipped without requesting the target:
it is left out of the results and listed in `skipped_urls` as
`redirected out of scope`.
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

Sitemaps are read before the first page is fetched, one after another and
only until the queue is full (see below): the rest of a sitemap and the
sitemaps after it are not downloaded, so a crawl of 10 pages reads an index
and its first few files, not the hundreds of files it may list. Indexes are
followed, gzipped files unpacked (see `SitemapParser` for the limits; a
sitemap over 50 MB is not downloaded to the end). A sitemap is
downloaded like a page: robots.txt, the rate limit, retries and the circuit
breaker apply, and its requests count in `crawl_stats().requests`, but not
in `max_pages` or `error_stats()`. A page a sitemap lists has depth 0, like
a start URL, so its links are followed up to `max_depth`; unlike a start
URL, it must pass the filters, and a redirect does not bring another host
into the crawl. `same_domain_only` keeps the hosts of `sitemap_urls` as well
as those of the start URLs; when a start URL redirects to another host
("example.org" to "example.com"), the sitemap pages on that host are
crawled too. A sitemap that cannot be downloaded or read is logged and
listed in `failed_sitemaps`, and the crawl goes on. A sitemap of a site
whose robots.txt cannot be read waits for it to be downloaded again, within
the `AsyncCrawler.MAX_ROBOTS_RETRIES` (3) repeat downloads of the site, as a page
does, and so do the sitemaps that such a robots.txt names under `robots_sitemaps`: the first
page is fetched after that wait, so that a crawl fed by sitemaps alone does
not end empty after a 503 of a few seconds.

`seed(frontier, start_urls, ...)` does this first step alone: it queues the
start URLs and the pages of the sitemaps in a `Frontier` and crawls nothing.
It takes the arguments of `crawl()` but for the limits, which are those of
the frontier, and returns the sitemaps that could not be read. It is how a
crawl job of distributed workers is filled, see [Crawl jobs](#crawl-jobs).
Under `same_domain_only`, the sitemap pages of hosts out of scope are held
in the frontier (`hold_out_of_scope`); a start URL that redirects to their
host brings them in (`widen_scope`), whichever process crawls it.

`crawl_frontier(frontier, start_urls, ...)` is the second step: it crawls
the pages of a frontier that `seed()` filled, by the rules of `crawl()`,
until `take` hands out no page, and leaves the frontier open. It takes the
arguments `seed()` was given, but for `robots_sitemaps`: the sitemaps are
not read again, and under `same_domain_only` `sitemap_urls` only give
their hosts to the scope. The start URLs are queued again, which queues
only those the frontier has not seen. This is how a worker of a crawl job
crawls (see `run_worker` in [Crawl jobs](#crawl-jobs)).

`crawl()` returns every parsed page, so it holds them all in memory until
it ends. A large crawl that saves its pages to a storage does not need
that: `AsyncCrawler(storage=..., keep_pages=False)` lets a page go once it
is saved and its links are queued. `crawl()` then returns an empty dict,
the pages are in the storage and the counts in `stats` and `crawl_stats()`;
memory stays nearly flat (see [performance.md](performance.md)).

The queue is bounded by `max_pages` as well: a large site can have far more
links than a crawl will ever request, and each one queued costs memory and
work. Once the pages queued, in progress and requested reach
`FRONTIER_FACTOR` (3) times `max_pages`, new links and sitemap pages are not
queued, nor remembered, so a page found again later is queued if there is
room by then. The spare room is for pages that do not count toward
`max_pages` (disallowed by robots.txt, over `max_pages_per_host`); a crawl
whose queue is mostly such pages may end before `max_pages`. With
`max_pages_per_host`, a host gets at most `FRONTIER_FACTOR` times that many
pages queued in the whole crawl, so a large site cannot fill the queue
with pages it would skip and crowd out the other hosts. How many links
were left out, and why, is logged at the end of the crawl.

After a crawl, and during one, the crawler exposes its state:

| Attribute | Content |
|-----------|---------|
| `processed_urls` | `{url: ParsedPage}`, the pages returned by `crawl()`; empty with `keep_pages=False` |
| `failed_urls` | `{url: "ErrorType: message"}` |
| `skipped_urls` | `{url: reason}` for pages fetched but left out: not HTML, redirected out of scope or to a page already seen, `noindex`, or a duplicate by the canonical URL; also pages not requested over `max_pages_per_host` |
| `blocked_urls` | `{url: reason}` for pages robots.txt did not allow to fetch |
| `unreachable_urls` | `{url: reason}` for pages not fetched because robots.txt of their site was unreachable |
| `failed_sitemaps` | `{sitemap url: "ErrorType: message"}` for sitemaps that could not be read |
| `visited_urls` | every URL taken for fetching, successful or not |
| `url_depths` | depth of every URL accepted into the queue; 0 for start URLs and pages listed in sitemaps |
| `crawl_stats()` | `CrawlStats`: processed, failed, skipped (`over_host_limit` of them not requested over `max_pages_per_host`), blocked, unreachable, queued, in progress, active requests, elapsed, pages per second; requests, retries, current and average requests per second, average gap between requests to a host, average wait for the rate limit; pages saved and not saved, see [Saving pages](#saving-pages) |
| `stats.get_stats()` | the pages by outcome, status code and domain, see [Page statistics](#page-statistics) |
| `error_stats()` | `ErrorStats`, see [Error statistics](#error-statistics) |
| `rate_limiter.get_stats()` | `RateStats`, with requests, interval and average gap per host |
| `circuit_breaker.get_stats()` | `{host: CircuitStats}`, see [Circuit breaker](#circuit-breaker) |

After `crawl_frontier()` on a frontier that keeps the pages elsewhere, such
as `PostgresFrontier`, `visited_urls`, `failed_urls`, `skipped_urls`,
`blocked_urls`, `unreachable_urls` and `url_depths` are empty: the outcomes
are in the frontier.

The building blocks can be used on their own: `CrawlerQueue` (priorities,
deduplication, completion detection), `MemoryFrontier` (a `CrawlerQueue`
with the limits on the pages of a crawl), `PostgresFrontier` (the same
contract in PostgreSQL, shared by the workers of a job made by
`create_job`, see [Crawl jobs](#crawl-jobs)), `SemaphoreManager` (global and
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
| `skipped` | pages in `skipped_urls`: fetched, but not HTML, redirected out of scope or to a page already seen, `noindex`, or a duplicate by the canonical URL; or not requested over `max_pages_per_host` |
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
| `AdvancedCrawler(config, configure_logging=True, worker="local")` | takes a `CrawlerConfig`; the defaults without one. `worker` stands for `{worker}` in the paths of the files written (`config.for_worker(worker)`) |
| `AdvancedCrawler.from_config(path, overrides, configure_logging=True)` | reads a YAML or a JSON file, see the [configuration guide](configuration.md) |
| `await crawl()` | crawls the start URLs and the sitemaps of the configuration, saves the pages, writes the reports of the `report` section and the cookies of `session.save_cookies`; returns the pages by URL |
| `await crawl_frontier(frontier)` | crawls the pages of `frontier`, which `seed()` filled, as `crawl()` crawls those of the configuration, see `AsyncCrawler.crawl_frontier`; writes the reports and the cookies as `crawl()` does |
| `await seed(frontier, sitemaps=True)` | queues the start URLs and the pages of the sitemaps of the configuration in `frontier` without crawling them, see `AsyncCrawler.seed`; `sitemaps=False` leaves the sitemaps unread |
| `check_start()` | raises `ConfigError` if the configuration has neither `urls` nor `sitemaps.urls`; `crawl()`, `crawl_frontier()` and `seed()` call it |
| `write_reports()` | writes the reports of the `report` section and returns their paths; `crawl()` calls it, call it yourself after a crawl that was cancelled |
| `get_stats()` | the statistics of the latest crawl, see [Page statistics](#page-statistics); with proxies, `proxies` too: `{label: {state, requests, failures, times_removed}}`, also in the JSON and as a table in the HTML report; with rendering, `rendering`: `{rendered, failed, avg_render_time}` (see `render_stats()` in [Rendering](#rendering)), also in the JSON and the HTML report |
| `export_to_json(filename)`, `export_to_html_report(filename, title=)` | write the statistics to a file; the title is `report.title` by default |
| `await close()` | closes the crawler, writes what the storage still holds, stops logging to the file; `async with` does it too |
| `config`, `crawler`, `storage`, `stats` | the configuration, the `AsyncCrawler` that does the work, its storage (`None` without outputs) and its `CrawlerStats`; `crawler.proxies` and `crawler.rendering` are the pool and the settings of rendering, `None` without them |
| `reports` | the report files the latest `write_reports()` wrote |
| `save_cookies()`, `cookie_file` | writes the cookies to `session.save_cookies` and returns the file, `None` without one or when it cannot be written (logged); `crawl()` calls it, call it yourself after a crawl that was cancelled. `cookie_file` is the file it wrote |

`{worker}` in a path of the storage, the log, the reports or
`session.save_cookies`, such as `pages-{worker}.jsonl`, is "local" in a
crawl of its own and the name of the worker in a worker of a crawl job.
Directories of the log, the reports and the files of the storage are created
if they are missing. A configuration with neither `urls` nor `sitemaps.urls`
makes `crawl()` raise `ConfigError`. A report that cannot be written is
logged and does not fail the crawl. Logging is set up when the crawler is
made (see [Logging](#logging)) and belongs to the whole process: with two
crawlers at once the log is written as the later one says. A program that
sets up logging itself passes `configure_logging=False`: the crawler then
leaves logging alone, `close()` too, and the `logging` section is ignored.

To show the progress of the crawl, run it as a task and pass the inner
crawler to `show_progress`:

```python
crawl = asyncio.create_task(crawler.crawl())
await show_progress(crawler.crawler, crawl, crawler.config.crawler.max_pages)
pages = await crawl
```

## Crawl jobs

A crawl job is one crawl in PostgreSQL that workers in other processes,
or on other machines, share: its frontier, its limits and its scope are
in the database (see [architecture.md](architecture.md#the-frontier-in-a-database)).
`create_job` creates and seeds it:

```python
from crawler import load_config
from crawler.distributed import JobMode, PostgresFrontier, create_job

dsn = "postgresql://crawler:crawler@localhost/crawler"
failed_sitemaps = await create_job(load_config("config.yaml"), "shop", dsn=dsn)

frontier = await PostgresFrontier.open(dsn, job="shop")   # a worker of the job, by hand
```

A worker takes no more than the name of the job and a configuration of its
own; run as many as you like, in other processes or on other machines:

```python
from crawler import CrawlerConfig
from crawler.distributed import run_worker

config = CrawlerConfig.from_dict(
    {
        "crawler": {"max_concurrent": 20},
        "storage": {"outputs": ["out/pages-{worker}.jsonl"]},
        "distributed": {"database_url": dsn},
    }
)
stats = await run_worker(config, "shop")
```

The statistics of the whole job, of all its workers, come from the
database, at any time:

```python
from crawler.distributed import export_job_stats, job_stats

stats = await job_stats(dsn, "shop")   # the keys of CrawlerStats.get_stats(), and the workers
export_job_stats(stats, stats_json="out/stats.json", html="out/report.html", title="Shop")
```

So does its progress, as a line like that of [Live progress](#live-progress):

```python
from crawler.distributed import format_job_progress, job_progress, watch_job

print(format_job_progress(await job_progress(dsn, "shop")))
await watch_job(dsn, "shop", interval=2)   # the line every 2 seconds until the job is finished
```

| Name | What it is |
|------|------------|
| `create_job(config, name, *, dsn, mode=JobMode.NEW, configure_logging=False)` | makes the tables if they are missing, creates the job and seeds it with the start URLs and the sitemaps of `config` (see `seed` in [Crawling](#crawling)); returns the sitemaps that could not be read; `FrontierError` if the database cannot be reached or fails; with `configure_logging`, logs by the `logging` section of `config` meanwhile |
| `JobMode.NEW` | the name must be free, or `JobError` is raised |
| `JobMode.RESUME` | goes on with the job of that name: its start URLs are seeded again (only those never queued are), a finished job runs again, the sitemaps are not read again unless the seeding did not finish; `config` must not differ from that of the job, or `JobError` names the keys that do |
| `JobMode.RESTART` | deletes the job and its pages, then creates it anew |
| `job_config(config)` | the part of a configuration the job keeps and every worker shares: the sections of `JOB_SECTIONS` (`urls`, `sitemaps`, `crawler`, `retry`, `circuit_breaker`, `filters`, `rendering`) without `crawler.max_concurrent` |
| `PostgresFrontier.open(dsn, *, job, worker=None, ...)` | connects a worker to the job, with the limits of the job; `JobError` if there is no such job |
| `run_worker(config, job, *, worker=None, configure_logging=True)` | crawls the pages of the job with `AdvancedCrawler.crawl_frontier` until none is left, then closes the frontier and the crawler; returns the statistics of this worker (`AdvancedCrawler.get_stats()` with `worker`, its name, `saved` and `save_failed`, the pages its storage wrote and could not); `FrontierError` if the database fails; cancelled, it writes the buffer of the storage, queues the pages it had in flight again, writes its reports and cookies and raises `CancelledError` |
| `job_stats(dsn, job, *, top_domains=10)` | the statistics of the job from the database, in one snapshot: the keys of `CrawlerStats.get_stats()` over the pages of all workers, and `job`, `state`, `queued`, `in_progress` and `workers` (name -> `state`, `started_at`, `active_seconds`, `pages`, `successful`, `failed`, `skipped`, `pages_per_second`); `JobError` if there is no such job, `FrontierError` if the database fails |
| `job_progress(dsn, job, *, window=30.0)` | the progress of the job from the database, in one snapshot: a `JobProgress` with `state`, `done` (pages requested and finished, by any worker), `total` (`max_pages` of the job), `failed`, `percent`, `pages_per_second` (pages done in the last `window` seconds), `eta`, `workers` (running), `lost`, `in_progress`, `queued` and `elapsed`; `JobError` if there is no such job, `FrontierError` if the database fails |
| `watch_job(dsn, job, *, interval=2.0, window=30.0, stream=None)` | prints the line of `format_job_progress` to `stream` (stdout) every `interval` seconds until the job is finished; redrawn in place in a terminal, a line per update in a file or a pipe |
| `export_job_stats(stats, *, stats_json=None, html=None, title="Crawl report")` | writes the statistics of `job_stats` to a JSON file and an HTML report with a table of the workers, those given, creating their directories; returns the files written; `OSError` if one cannot be written |
| `host_interval(config.crawler)` | the seconds between two pages of a host that the workers of a job take: `1 / rate_limit` or `min_delay`, whichever is longer, with `per_domain_rate`; 0 without it |

A job is `seeding` while `create_job` fills it: workers started meanwhile
wait, and take its first page once it is `running`. Once nothing is left
to hand out and no page is in progress or pending its save, or once
`max_pages` pages are requested and done, it is `finished`. A job whose
seeding failed stays `seeding`; resumed, it is seeded again. The sitemaps
are read with the session and the proxies of the configuration given to
`create_job`; its storage is not opened.

The other sections of a configuration, `session`, `proxy`, `storage`,
`logging`, `report` and `distributed`, and `crawler.max_concurrent`, are
those of each worker; the secrets of a configuration (cookies, headers, the
passwords of proxies and of the database) are all in them, so the job keeps
none. Each worker has its own cookies and proxies.

`run_worker` takes the part of the job from the database. A key of that
part that the configuration of the worker sets otherwise is ignored, and
the keys are named in one warning; a worker can be given the file the job
was created with. The database is `distributed.database_url`, or the one of
`CRAWLER_DATABASE_URL`; without either, `ConfigError`. `worker` names the
worker in the database and in its files; by default it is made of the host
name, the process id and a random part. The pages are not kept in memory
whatever `crawler.keep_pages` says: they go to the storage of the worker
(a warning is logged if it has none).

A page is crawled at least once, not exactly once: the page of a worker
that stopped goes back to the queue once its lease expires, and another
worker crawls it again, following its redirect again if it had one. So a database storage, which keeps a row per URL,
suits workers best. Files are each worker's own: every file of the
storage, SQLite databases included, must have `{worker}` in its name, or
`run_worker` raises `ConfigError`; a page may then be in the files of two
workers. The log and the reports of a worker without `{worker}` in their
names are written over by the last worker.

The workers ask a host together at the rate of the job: the frontier hands
out a page of a host every `host_interval` seconds, or every Crawl-delay of
the host if that is longer, whichever worker takes it, and each worker
keeps its own requests apart as well. A worker tells the frontier a
Crawl-delay longer than the one it knew (the fetcher calls
`on_crawl_delay`, the crawl `Frontier.set_host_interval`); `create_job`
tells those of the start sites whose robots.txt it reads for sitemaps.
With `per_domain_rate: false` the rate for all hosts together is that of
each worker. A worker with nothing to take, while other workers still crawl,
writes out the pages its storage buffers before it waits: they may wait
for those in turn (see `on_waiting` of `Frontier.take`).

A host that asks one worker to wait (Retry-After), or makes it pause before
a retry (HTTP 429, a timeout), is left alone by all of them for as long:
the fetcher tells `on_host_held`, and the crawl holds the host back in the
frontier (`Frontier.hold_host`, done only by a frontier that is `shared`).
So is a host whose circuit has opened in one worker, until its probe is
due, and one whose robots.txt one worker found unreachable, until it is
downloaded again; the breaker and robots.txt stay each worker's own. The
page put off comes back with its host, without a delay of its own; a page
that redirects to the held host waits as long itself. A
worker that took a page of the host before the hold reached the database
may still send one request for it. If the hold cannot be written, a warning
is logged and the host is held back by the worker that was answered only.

A page whose host is held back by its own worker while the page waits for
its turn goes back to the queue rather than keep the worker waiting: the
crawl passes `max_wait=MIN_PENALTY_TO_DEFER` to `Fetcher.fetch`, whose
request then fails with `HostHeldBackError` without being sent (not retried,
not counted in the errors), and the page is put back uncounted. A retry
waits out its own pause as before. The waits of a page
(`Frontier.waits`, counted by `put_back(..., waited=True)`) are kept in the
database, so `MAX_WAITS_PER_PAGE` counts them for the whole job; a page held
back after it was taken that has waited its last waits in the rate limiter,
as in a local crawl.

A host is given up for the whole job, not for each worker: the openings of
the circuits of all workers count toward `MAX_CIRCUIT_OPENINGS`, and their
failed downloads of robots.txt toward `MAX_ROBOTS_RETRIES` (counted from zero
again once a worker reads it). Each worker tells the frontier of its new
failures (`Frontier.count_host_failures`, which returns the counts of the
job); the one whose failure reaches the limit gives the host up
(`Frontier.give_up_host`). Its queued pages are finished at once, failed or
unreachable with the reason, and a page of it taken later says so
(`Frontier.given_up`) and is finished without a request. Pages of the host
other workers have in progress then cost at most one request each. A local
crawl counts as before: its frontier counts nothing.

A worker whose storage cannot write, once a write has failed after its
retries (`DataStorage.write_failed`), takes no pages until the storage
writes again: it writes the buffer again after the storage's `cooldown`,
then after twice as long each time, up to `AsyncCrawler.MAX_STORAGE_PAUSE`
(60 s), and the pages of the buffer stay leased meanwhile. Nor does it stop
before they are written. A local crawl goes on taking pages as before.

A worker whose database fails stops at once: `run_worker` raises
`FrontierError`, with the error of the database as its cause. An operation
of the frontier failing with one of `PostgresFrontier.ERRORS` (`Frontier.ERRORS`
of a frontier kept outside the process) stops the crawl rather than fail
the page; the storage writes what it buffers first. The pages the worker had
in progress come back to the other workers once their leases expire, and
the worker is meant to be started again by whatever runs it. A failure of
the heartbeat or of a call that only tells the others something
(`saved`, `hold_host`, `set_host_interval`) is logged as a warning.

A worker stopped, that is its task cancelled, as Ctrl-C and SIGTERM cancel
the command line, leaves nothing to its leases: the crawl writes the buffer
of the storage and the pages are `saved` while the frontier is still open,
then `close` queues the pages it had in flight again, uncounted. The
requests in flight are not finished. A buffer the storage cannot write is
logged as an error, and its pages stay `saving` until their leases expire.

The statistics of a job count its pages as a crawl of one process counts
them (see [Page statistics](#page-statistics)): a worker passes the status
and time of each response and the class of each error to
`Frontier.finish`, which a frontier in a database keeps with the page; a
host given up keeps the class its pages fail with (`CircuitOpenError`,
see `GivenUp`), and a page whose lease expired `max_attempts` times fails
with `LeaseExpired`. A worker joins the table of workers with its first
`take`, and its heartbeat counts the time it runs; run again under the same
name, it does not count the pause. A worker is `running` while it renews
its lease, `stopped` once it closed its frontier, `lost` if its lease ran
out without a stop (killed, or cut off from the database). The time of the
job runs from the start of its first worker to its end, or to now: the
pauses of a job resumed are a part of it. A page counts for the worker
that finished it; the pages a host given up finishes in the database, and
those failed as their leases expired, count for the job only. The
statistics and reports of each worker (`run_worker`) are those of its own
pages.

The command line runs them: `python src/main.py job create --config
config.yaml --name shop`, `python src/main.py worker --job shop` and
`python src/main.py report --job shop --report report.html` and
`python src/main.py status --job shop --watch`, see the
[README](../README.md#crawl-jobs-on-the-command-line).

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
[######--------------]  30% | 30/100 pages, 1 failed | 1.6 pages/s | ETA 44s | active 6 (2 in flight) | queued 64 | 19s
```

| Part | Meaning |
|------|---------|
| bar, percent | pages done of `max_pages`, rounded down |
| `30/100 pages, 1 failed` | pages requested and finished (processed, failed, skipped; not the pages over `max_pages_per_host`, which are skipped without a request), and the failed among them |
| `pages/s` | the speed over the last 10 seconds |
| `ETA` | the time the remaining pages take at that speed; `--` while the speed is 0, `done` once the crawl has ended |
| `active`, `in flight` | pages taken by workers, and the HTTP requests being made |
| `queued` | pages waiting in the queue, but no more than `max_pages` leaves to request: the rest will not be fetched |
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
does the same for a mapping. `load_urls(path)` reads start URLs from a
text file, one per line (`"-"` is stdin), and raises `ConfigError` listing
every line that is not an http(s) URL or has a space inside. The keys, their limits, the errors
and the overrides are described in the [configuration guide](configuration.md).

## Logging

Every module logs to a logger named after it (`crawler.fetching` for
requests, `crawler.crawl_run` for the pages of a crawl, `crawler.retry`, ...). `configure_logging` sends the records to the console
and, given a file, to that file as well:

```python
from crawler import configure_logging

configure_logging("INFO", "crawler.log", max_bytes=10 * 1024 * 1024, backup_count=5)
```

The console (stderr) gets a line of text per record:

```
19:41:40 | INFO    | crawler.fetching | Fetched https://example.com/: status=200 size=1256B elapsed=0.10s
```

The file gets JSON Lines, an object per record, so it can be read by
`jq` or loaded by a log collector as it is:

```json
{"time": "2026-10-02T16:41:40.438+00:00", "level": "INFO", "logger": "crawler.fetching", "message": "Fetched https://example.com/: status=200 size=1256B elapsed=0.10s"}
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
| `metadata` | `description`, `keywords`, `language`, `canonical`, `robots`, plus `final_url` (the URL after redirects) and `depth` in the crawl |
| `crawled_at` | when the page was processed: a `datetime` in UTC |
| `status_code` | HTTP status of the response |
| `content_type` | media type of the response; an empty string if the server sent none |

| Storage | Keeps the pages in | Notes |
|---------|--------------------|-------|
| `JSONStorage(path, indent=None, overwrite=False)` | a JSON Lines file, or one indented array with `indent` | records are added without reading the file, and read back in pieces; the array is valid JSON after every write |
| `CSVStorage(path, encoding="utf-8", overwrite=False)` | a CSV file with a header row | the header comes from the first record, or from the file if it exists; `links` and `metadata` are JSON in a cell; quoting per RFC 4180; a character the encoding lacks is written as `?` |
| `SQLiteStorage(path)` | the `pages` table of an SQLite file | `links` and `metadata` as JSON text, `crawled_at` as ISO 8601 in UTC |
| `PostgresStorage(dsn)` | the `pages` table of a PostgreSQL database | `links` and `metadata` as `JSONB`, `crawled_at` as `TIMESTAMPTZ`; a connection pool |
| `CompositeStorage(*storages)` | each of the storages | a page counts as written once all of them have it; one failing does not stop the others |

All of them share the behavior of `DataStorage`:

- `open()` opens the file or the connection ahead of the first write and
  checks that it can be written to: a file of another layout, a path that
  cannot be written, a database that cannot be reached raise `StorageError`
  at once, before a crawl has anything to save. Nothing is written by it.
  `crawl()` calls it before its first request and lets the error through;
  without it, the first write opens the storage the same way.
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
  down; `flush()` and `close()` write at once all the same. `write_failed`
  tells that the buffer holds records a write could not take, until one
  does: a worker of a crawl job takes no pages meanwhile. Any other error
  (e.g. a value the database refuses, text that is not valid UTF-8) is one
  no retry cures: the batch is written again a record at a time, and only
  the records that fail on their own are dropped, each logged with its URL,
  so that one bad record neither fails every later write nor takes its batch
  along. The error of the first record dropped is raised as it is.
- `read()` iterates over the saved records, oldest first, without loading
  them all; `pending` and `written` count the records in the buffer and those
  written out.
- `on_settled`, if set, is awaited with the URLs of the records written
  out, and of those dropped, after every write. Records still in the buffer
  are reported once a later write takes them; those lost when `close()`
  cannot write them are not. `crawl()` sets it for its run, so that a page
  is done in its frontier only once its record is stored (see
  `Frontier.saved`), and clears it at the end. An error of the callback is
  logged and does not fail the write.

A database storage creates its table on `open()` or the first use (`init_db()`), with
`url` unique and indexes on `crawled_at` and `status_code`. A batch is one
transaction: all of its pages are saved or none. Saving a URL again replaces
its row. `count()`, `status_counts()` and `get(url)` query the table.

A file storage adds to the file if it exists, so a second crawl with the
same file keeps the pages of the first one, and a page fetched by both is
in the file twice; adding to a file that is not empty is logged as a
warning. With `overwrite=True` the file is started anew: what it held is
dropped on the first write, not on `open()` (a crawl that saves nothing
leaves it as it was), and a file that could not be added to, such as one
of the other JSON layout, is replaced too.

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
URL. `overwrite=True` reaches the file storages only. This is what the
`storage.outputs` of a configuration file go through.

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
CRAWLER_POSTGRES_PORT=55432 docker compose up -d --wait   # if port 5432 is taken; the URL then has :55432
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
| `links` | absolute, normalized, unique `http(s)` links in page order; without those marked `rel="nofollow"` when the crawler follows robots.txt |
| `metadata` | `title`, `description`, `keywords`, `language`, `canonical`, `robots` (directives of `<meta name="robots">` and, in a crawl, of the `<meta>` named after the crawler; lower case) |
| `headings` | `h1`-`h3` as `{"level", "text"}` |
| `images` | `{"src", "alt"}`; `data-src` is used for lazy-loaded images |
| `tables` | `{"caption", "headers", "rows"}` |
| `lists` | `<ul>`/`<ol>` as `{"type", "items"}`; nested lists are separate entries |
| `errors` | parsing problems; empty when everything went fine |

Responses whose `Content-Type` is not HTML are not parsed, and their body is
not even downloaded: `fetch_and_parse` fails with `ParseError`, and so does
an empty document. A crawl lists a page that is not HTML as skipped
(`not HTML: application/pdf`), not as failed: nothing went wrong with it, and
it is not counted in `error_stats()`; its request counts toward `max_pages`.
An empty document is a failed page. This keeps a crawl from pulling in
archives or videos it finds links to; `exclude_extensions` keeps it from even
asking for those whose URL tells what they are. The parser can be used on its own:
`HTMLParser().parse(html, url)`, or `await HTMLParser().parse_html(html, url)`
in async code. Pass `AsyncCrawler(parser=HTMLParser(same_host_only=True))` to
keep only links to the page's own host. `HTMLParser(skip_nofollow=True)`
leaves out links marked `rel="nofollow"`; the crawler's own parser does so
when `respect_robots` is on, a parser passed in decides for itself.
`HTMLParser(robots_name="mybot")` adds the directives of
`<meta name="mybot">` to those of `<meta name="robots">`; the crawler's own
parser takes the robots.txt name of its `user_agent`.

## Internals

`AsyncCrawler` is a facade over three layers, each in a module of its own.
They are not part of the public API: they are not exported from `crawler`
and may change. A layer calls only the one below it.

| Layer | Module | Class | Responsible for | Knows nothing of |
|-------|--------|-------|-----------------|------------------|
| Facade | `client.py` | `AsyncCrawler` | the public API: checks the arguments, builds the layers and shares them, parses pages (at most `max_parsing` at once), keeps the latest crawl for its properties, closes the session and the storage | how a request or a crawl is made |
| Crawl | `crawl_run.py` | `CrawlRun` | one crawl, of `crawl()` or `crawl_frontier()`: what is done with every page its `Frontier` hands out — filters, depth, the seeding (`seed`: the start URLs, then the sitemaps read before the first page until the frontier is full, also alone for `AsyncCrawler.seed`), the scope a redirecting start URL widens and the hosts other processes brought into it, pages put off while robots.txt, a Retry-After or an open circuit holds their host back, duplicates, saving pages (with a shared frontier, none taken while the storage cannot write), a crawl stopped by the `ERRORS` of its frontier, the counters of `crawl_stats()` | how a URL is fetched, how the pages are kept |
| Crawl, frontier | `frontier.py` | `Frontier`, `MemoryFrontier`, `HostFailures`, `GivenUp` | `Frontier` is the contract the crawl layer takes its pages through: the queue and the URLs seen (`mark_seen` checks and remembers in one call, the page whose redirect led to the URL is told it is new again; `forget` by that page only), the outcomes of the pages (a page saved is done once the storage reports its record written: `pending_save`, `saved`), `max_pages` and `max_pages_per_host` counted as pages are admitted and uncounted when they go back unanswered, the bound of `FRONTIER_FACTOR`, the scope (sitemap pages held out of it: `hold_out_of_scope`; a host brought in: `widen_scope`, `scope_hosts`), `on_waiting` for the buffer of the storage before a wait for other processes, `shared` and `hold_host` for a host held back for all of them, `set_host_interval` for its Crawl-delay, `count_host_failures`, `give_up_host` and `given_up` for a host given up for all of them (nothing in memory), the status, time and error of a page finished for the statistics of all of them (not kept in memory), the waits of a page for its host (`waits`, `put_back(..., waited=True)`), `ERRORS` that stop the crawl rather than fail a page (none in memory). `MemoryFrontier` keeps them in memory, in a `CrawlerQueue` | how a page is fetched or what is done with it |
| Crawl, shared frontier | `distributed/frontier.py`, `distributed/schema.py` | `PostgresFrontier` | a `Frontier` in PostgreSQL that the workers of one job share: the limits and the scope of the job kept in the database, nothing handed out while the job is seeding, the job finished by the worker that finds nothing left, pages leased to a worker and renewed by its heartbeat, an expired lease queued again and uncounted (failed after `max_attempts`), the target of a redirect seen from its page (`frontier.seen_from`; followed again by the page whoever takes it, queued again once the page fails), pages `saving` until `saved`, one turn of a host for all workers (`next_allowed_at`, `FOR UPDATE SKIP LOCKED`), a host held back for all workers (`hold_host`, `hold_reason`), the Crawl-delay of a host for all workers (`hosts.interval`, never shorter), the failures of a host counted for the job and a host given up (`circuit_openings`, `robots_failures`, `given_up_outcome`; its pages handed out whatever the hold, with the outcome), the waits of a page counted for the job (`frontier.waits`), the response and the error of a page finished (`status`, `elapsed`, `error`), the workers that joined the job and their time (`workers`), waiting by polling, the stats as a snapshot of the job (`refresh_stats()`), `close()` putting the pages in progress back (left to their leases if the database is gone), `ERRORS` of a database that fails | what is done with a page; how a job is created |
| Crawl jobs | `distributed/job.py`, `distributed/worker.py`, `distributed/stats.py`, `distributed/progress.py` | `create_job`, `JobMode`, `job_config`, `run_worker`, `job_stats`, `export_job_stats`, `job_progress`, `watch_job`, `JobProgress` | a job created, resumed or restarted, its part of the configuration kept, its seeding through `AdvancedCrawler.seed`; a worker: the configuration of the job with its own, `{worker}` in its files, the interval of a host, its crawl through `AdvancedCrawler.crawl_frontier`, `FrontierError` when the database fails; the statistics of a job from its tables, its reports through `render_json` and `render_html`, its progress line | how a page is crawled |
| Request | `fetching.py` | `Fetcher` | one URL fetched politely: robots.txt, the circuit breaker, the rate limit and the concurrency limits, retries with growing timeouts, redirects one hop at a time, Retry-After; a host held back told to `on_host_held` (`tell_host_held`), a longer Crawl-delay of a host to `on_crawl_delay`; with `max_wait`, a request not sent to a host held back longer (`HostHeldBackError`); every outcome reported in a `FetchResult` | the queue of a crawl |
| HTTP | `transport.py` | `Transport`, `HttpTransport` | `Transport` is the contract the request layer sends through; `HttpTransport` makes a single GET without redirects over one aiohttp session: TLS with the system and certifi CAs, rotating User-Agents, the cookies and headers, the proxy of the request and its outcome told to the `ProxyPool`, the size limit of a body, decoding; every failure raised as a `FetchError` | robots.txt, retries, limits |
| HTTP, rendered | `rendering.py` | `BrowserTransport`, `Renderer` | `BrowserTransport` is a `Transport` over `HttpTransport`: it hands the HTML pages that `Rendering` names to the `Renderer`, with the proxy their document came through (`Response.proxy`), a page that goes elsewhere on its own back as a redirect, and checks the size of the rendered HTML. `Renderer` runs one headless Chromium: launches it for the first page, a context per proxy, the cookies kept in step with those of `HttpTransport` (`CookieSync`, `Transport.update_cookies`), a tab per page within `max_open_pages`, the routing of the browser's requests, the waits, the errors of Playwright as `FetchError`s, one restart after a crash, the counters of `render_stats()` | robots.txt, filters, retries, limits |

Who owns what:

- `AsyncCrawler` creates the shared objects — `SemaphoreManager`,
  `RateLimiter`, `RetryStrategy`, `CircuitBreaker`, `HttpTransport`
  (wrapped in a `BrowserTransport` with `rendering`),
  `Fetcher`, `HTMLParser`, `CrawlerStats` — and exposes some of them as
  its attributes (`rate_limiter`, `circuit_breaker`, `stats` ...). The
  settings and the components of a crawler are read-only, as the layers
  got them when it was made; only `storage` may be replaced between
  crawls.
- `Fetcher` creates `RobotsParser` and `SitemapParser`, which download
  through it, so robots.txt and sitemaps get the same politeness as pages;
  `AsyncCrawler.robots` and `.sitemaps` are the same objects.
- Every `crawl()` makes a new `MemoryFrontier` with its limits and a new
  `CrawlRun` over it, so the state of a crawl is never reset field by
  field: the previous run stays readable until the next one starts. The
  run knows only the `Frontier` contract; the facade made the frontier and
  reads `visited_urls`, `failed_urls`, `url_depths` ... from it. Before
  the first crawl the properties read an empty run and frontier. The
  rate limits, the robots.txt cache and the states of the circuit breaker
  live in the shared objects and carry over between crawls.
- The crawl constants (`ROBOTS_POLL`, `MAX_ROBOTS_RETRIES` ...) are
  defined by `CrawlRun` and read from the crawler when a run is made, and
  `FRONTIER_FACTOR` is defined by `Frontier` and read when its frontier is
  made, so one set on an `AsyncCrawler` or on a subclass applies to its
  crawls. The request constants
  (`MAX_REDIRECTS`, `MAX_TIMEOUT_GROWTH`, `REDIRECT_STATUSES`) are
  defined by `Fetcher` and `HttpTransport` and read from the crawler when
  it is made: one set on a subclass applies, one set on a crawler later
  does not. `sitemaps.MAX_SIZE` is read on every download of a sitemap.
