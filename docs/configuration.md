# Configuration guide

The settings of a crawl can be kept in a YAML (`.yaml`, `.yml`) or a JSON
(`.json`) file. Every key is optional and has a default;
[config.example.yaml](../config.example.yaml) lists them all, and
[examples/config.yaml](../examples/config.yaml) is a small working file.

```yaml
urls:
  - https://example.com/
crawler:
  max_pages: 500
  rate_limit: 2.0           # requests per second
filters:
  exclude: ['/login']
storage:
  outputs: [pages.jsonl]    # files by extension, or database URLs
```

```bash
python src/main.py --config config.yaml
```

## Where a value comes from

1. An option of the command line, if it was given.
2. Else the key of the file.
3. Else the default.

The options and the keys they stand for are listed in the
[README](../README.md#command-line). An option that takes a list (`--urls`,
`--output`) replaces the whole list of the file.

The start URLs come from `urls` of the file, or from the command line:
`--urls` and `--urls-file` (a text file with a URL per line, or `-` for
stdin; see [examples/urls.txt](../examples/urls.txt)). A comment takes a
line of its own, as a space inside a URL is an error, and lines may end in
`\n`, `\r\n` or a lone `\r`. Given together, both
are crawled, those of `--urls` first, and a URL given twice is crawled
once; either of them replaces `urls` of the file, and `sitemaps.urls` stay.
A list file is an option of the command line only: the configuration has
no key for it, so one file of settings serves many lists. In code,
`load_urls(path)` reads such a file. Sitemaps, filters, retries,
the circuit breaker and timeouts have no options: they are set in the file.
The command line always turns `crawler.keep_pages` off, whatever the file
says: it saves the pages and does not need them in memory.

Times are in seconds. Paths are relative to the working directory, not to
the configuration file; `~` is expanded. Directories of the outputs, the
log and the reports are created if they are missing.

## Keys

### `urls`

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `urls` | list of URLs | `[]` | the start URLs, `http://` or `https://`; may be empty when `sitemaps.urls` is not |

### `sitemaps`

Sitemaps whose pages are crawled along with the start URLs; see
[Crawling](api.md#crawling) for how such pages are treated.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `urls` | list of URLs | `[]` | sitemaps or sitemap indexes, plain or gzipped |
| `from_robots` | true or false | `false` | also read the sitemaps that robots.txt of the start URLs' sites names; needs `crawler.respect_robots` |
| `max_urls` | whole number, >= 1 | `50000` | pages taken from one sitemap, its index included; a crawl stops reading sitemaps sooner once its queue is full |

### `crawler`

How much to crawl and how fast; see [Politeness](api.md#politeness) and
[Timeouts](api.md#timeouts).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `max_pages` | whole number, >= 1 | `100` | pages requested, failed ones included |
| `max_pages_per_host` | whole number, >= 1, or `null` | `null` | pages requested from one host; the others of that host are skipped without a request, and no more than 3 times as many are queued; `null` for no limit of its own |
| `max_depth` | whole number, >= 0 | `2` | links followed from a start URL; 0 crawls the start URLs only |
| `max_concurrent` | whole number, >= 1 | `10` | requests in flight |
| `max_per_domain` | whole number, >= 1, or `null` | `null` | requests in flight to one host; `null` for no limit of its own |
| `rate_limit` | number, > 0, or `null` | `1.0` | requests per second; `null` lifts the limit |
| `per_domain_rate` | true or false | `true` | the rate limit is for each host, not for all of them together |
| `min_delay` | number, >= 0 | `0.0` | pause between two requests to a host |
| `jitter` | number, >= 0 | `0.0` | random addition to the pause, up to this much |
| `respect_robots` | true or false | `true` | check robots.txt before every request, and follow `nofollow` and `noindex` of pages and links |
| `user_agent` | string, one line | `AsyncWebCrawler/0.1 (+repo URL)` | the User-Agent; robots.txt rules are looked up by its name; spaces and line breaks around it are dropped |
| `user_agents` | list of strings | `[]` | variants to rotate; each must have the same name as `user_agent` |
| `total_timeout` | number, > 0 | `30.0` | the whole request, body included |
| `connect_timeout` | number, > 0 | `10.0` | DNS, TCP and TLS |
| `read_timeout` | number, > 0 | `20.0` | the longest pause between two chunks of the response |
| `timeout_growth` | number, >= 1 | `1.5` | the timeouts grow by this factor on every retry |
| `max_page_size` | whole number, >= 1, or `null` | `3145728` | bytes of a page body (3 MiB); a larger page fails unread; `null` lifts the limit |
| `max_parsing` | whole number, >= 1 | `2` | pages parsed at once; parsing takes about 40 times the size of a page in memory and a couple of seconds per megabyte, so this times `max_page_size` bounds the memory of parsing |
| `max_retry_after` | number, > 0 | `600.0` | the longest wait a `Retry-After` header is obeyed for (10 minutes); a host that asks for more is asked again after this long |
| `keep_pages` | true or false | `true` | `false` drops a page from memory once it is saved, for large crawls |

### `retry`

Repeated attempts of a request that failed for a while; see
[Retries](api.md#retries).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `max_retries` | whole number, >= 0 | `3` | retries of one request; 0 turns them off |
| `backoff_factor` | number, >= 1 | `2.0` | the pause grows by this factor on every attempt |
| `base_delay` | number, > 0 | `1.0` | the first pause |
| `max_delay` | number, > 0 | `30.0` | the longest pause |

### `circuit_breaker`

Stops asking a host that keeps failing; see
[Circuit breaker](api.md#circuit-breaker).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `failure_threshold` | number, > 0 and <= 1, or `null` | `0.5` | share of failed requests that opens the circuit; `null` turns the breaker off |
| `min_requests` | whole number, >= 1 | `5` | requests in the window before the share counts |
| `window` | number, > 0 | `60.0` | seconds over which outcomes are counted |
| `cooldown` | number, >= 0 | `30.0` | how long the host is left alone before a probe |

### `filters`

Which links to follow. The filters apply to links and to pages of sitemaps,
not to the start URLs.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `same_domain_only` | true or false | `true` | follow links on the hosts of the start URLs and of `sitemaps.urls` only, and on their subdomains; `false` follows links to any host |
| `include` | list of regular expressions | `[]` | a link must match at least one; empty means any link |
| `exclude` | list of regular expressions | `[]` | a matching link is skipped, even if included |
| `exclude_extensions` | list of file extensions | documents, images, archives, media, programs, `css`, `js` (see `config.example.yaml`) | a link to a file with one of them is not followed; `[]` follows every link |

A pattern is searched anywhere in the URL. In YAML write patterns in single
quotes, where a backslash is a backslash: `'\.pdf$'`.

An extension is compared with the last one of the file the URL path names,
in any case and with or without the dot: `pdf` rejects `/files/Manual.PDF`
and `/report.pdf?v=2`, but not `/view?file=report.pdf`. Write `gz`, not
`tar.gz`. A page that turns out not to be HTML anyway, such as a PDF behind
a link without an extension, is requested but not downloaded: it is listed
as skipped and counts toward `max_pages`.

A site is its host name: with a start URL on `example.com`, links to
`www.example.com` (the same host) and to `docs.example.com` (a subdomain)
are followed, links to `example.org` are not. A start URL on
`docs.example.com` keeps the crawl there: `example.com` and `blog.example.com`
are outside. A site spread over unrelated domains needs
`same_domain_only: false` with an `include` pattern for each of them.
The limits are per exact host name, not per site: `rate_limit`,
the circuit breaker and `max_pages_per_host` count `example.com`,
`www.example.com` and `docs.example.com` apart, so a site that links to
all three is asked at up to three times `rate_limit`. Most sites redirect
the apex to `www.` (or back) and are not affected.

The library itself, `AsyncCrawler.crawl()`, follows links to any host and
to files unless given `same_domain_only=True` and `exclude_extensions`; the
configuration turns both on, so that a crawl stays on the site it was
started on.

### `session`

The cookies and the headers of the requests; see
[Cookies and headers](api.md#cookies-and-headers), and for the ideas
behind them, [the note on sessions](sessions_proxies_rendering.md#cookies-and-sessions).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `keep_cookies` | true or false | `true` | keep the cookies sites set and send them back, as a browser does; `false` sends and keeps none, so no site can keep a session of the crawler |
| `cookies` | list of cookies | `[]` | cookies sent from the first request: `{name, value, domain, path, secure}`, see below |
| `cookies_file` | string or `null` | `null` | a Netscape `cookies.txt` file to take cookies from, as browser extensions and `curl -c` export it |
| `save_cookies` | string or `null` | `null` | write the cookies to this `cookies.txt` file after the crawl, those sites set included; only its owner can read it |
| `headers` | mapping of names to values | `{}` | headers sent with every request, such as `Authorization` or `Accept-Language` |

A cookie of `cookies` needs `name`, `value` and `domain`; `path` is `/` and
`secure` (sent over https only) is `false` unless given. The domain is
required so that a cookie never goes to a host it is not for:
`example.com` is that host only, `.example.com` the host and its subdomains,
as in a `cookies.txt` file. The cookies of `cookies_file` come first, those
of `cookies` win over them. A cookie of an expired date in the file is left
out; one without a date lasts for the crawl. aiohttp keeps no cookies of IP
addresses, so a cookie for `127.0.0.1` is an error here and is left out of
the file with a warning: reach such a site by its name, e.g. `localhost`.

```yaml
session:
  cookies_file: cookies.txt    # exported from the browser after logging in
  save_cookies: cookies.txt    # the session the site renewed, for the next run
  cookies:
    - {name: consent, value: "yes", domain: .example.com}
  headers:
    Accept-Language: en
    Authorization: "Bearer ..."
```

The headers go to every request: pages, robots.txt and sitemaps, the hosts
of other sites and the targets of redirects to them included. So an
`Authorization` header reaches a third-party site the crawl follows a link
or a redirect to. Keep `filters.same_domain_only: true`, the default, with
such a header. `User-Agent`, `Cookie`, `Host` and `Proxy-Authorization`
cannot be set here: they come from `crawler.user_agent`, the cookies, the
URL and the password in `proxy.urls`, which goes to the proxy only.

Rotated `crawler.user_agents` with cookies show a site one session in
several browsers; a site that ties its session to the browser may end it.

The values of the cookies and the headers are secrets: they are not
written to the log, the summary, the reports or the messages of the
validation, and `repr()` of the configuration leaves them out.
`CrawlerConfig.to_dict()` keeps them, so that `from_dict()` can read it
back. Keep a file with them private, or keep them in `cookies_file`.

### `proxy`

The proxies the requests go through; without any, requests go directly.
See [Proxies](api.md#proxies), and for the ideas behind them,
[the note on proxies](sessions_proxies_rendering.md#proxies).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `urls` | list of URLs | `[]` | the proxies, `http://[user:password@]host:port` or `https://...`; the port is required |
| `rotation` | `per_host` or `per_request` | `per_host` | `per_host`: a host always goes through one proxy, chosen by a hash of the host; `per_request`: the proxies take turns, one request each |
| `from_env` | true or false | `false` | take the proxies of `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` instead of `urls` |
| `max_failures` | whole number, >= 1 | `3` | failures in a row that take a proxy out of rotation |
| `cooldown` | number, > 0 | `60.0` | seconds a proxy stays out of rotation |

```yaml
proxy:
  urls:
    - http://user:password@proxy-1.example:3128
    - http://proxy-2.example:3128
  rotation: per_host
```

Every request goes through a proxy: pages, robots.txt and sitemaps.
`per_host` keeps the session of a site on one address: its cookies do not
move between the addresses of several proxies, which a site may take for
a stolen session. `per_request` spreads the requests of one site over all
the proxies.

A proxy fails a request when it cannot be reached, its name does not
resolve, the TLS of an `https://` proxy fails, or it asks for a password
(HTTP 407, to CONNECT or to the request of an `http://` URL; a 407 from
inside the tunnel of an `https://` URL is the site's). After `max_failures` such
failures in a row it is out of rotation for `cooldown` seconds, and the
requests go through the other proxies; any response through it clears
the count. Once back, one more failure takes it out again. The request
that failed is retried through the next proxy at once, and with
`per_host` its host stays on that proxy. When every proxy is out, a page
fails at once with "no proxy available" and is not retried, and the crawl
ends instead of waiting. A proxy that answers, whatever the site says
through it, is up: a 404, a 503 or a refused CONNECT (the proxy cannot or
may not reach the site) count against the site, and so does a timeout,
since the proxy and the site cannot be told apart then.

`from_env` reads the variables once, when the crawler is made, in either
case (`https_proxy` too): an `https://` URL goes through `HTTPS_PROXY`,
an `http://` one through `HTTP_PROXY`, and a host of `NO_PROXY` or a
scheme without a proxy goes directly. A proxy without a scheme
(`proxy:3128`) is an `http://` one, as curl takes it. `ALL_PROXY`,
`~/.netrc` and the proxies of the system settings are not read, unlike
aiohttp's `trust_env`. Neither variable set is no error: the log warns
and the requests go directly. `rotation` does not apply: each scheme has
one proxy. The same proxy in both variables with different passwords is
an error.

SOCKS proxies are not supported: `socks5://` is an error that suggests an
http proxy or a local bridge from HTTP to SOCKS. `--proxy` on the command
line replaces `urls` and turns `from_env` off.

The passwords of proxies are secrets like the values of `session`: a
proxy is shown as `http://user:***@host:port` in the log, the summary,
the reports and the errors, and `repr()` of the configuration leaves the
URLs out. The password goes to the proxy in the `Proxy-Authorization`
header, never to the site.

Politeness stays with the sites: the rate limit, robots.txt, Crawl-delay,
`max_per_domain` and the circuit breaker go by the host of the URL,
whatever proxy a request goes through. Proxies are not a way around the
limits of a site.

### `rendering`

Pages rendered in a headless Chromium, for sites whose links and text
JavaScript makes: without it, such a page is an empty shell. Off by
default. See [Rendering](api.md#rendering), and for the ideas behind
it, [the note on the headless browser](sessions_proxies_rendering.md#headless-browser).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `mode` | `off`, `always` or `patterns` | `off` | `always`: every HTML page is rendered; `patterns`: only the pages whose URL matches `include`; write `"off"` in quotes, YAML reads a bare `off` as `false` |
| `include` | list of regular expressions | `[]` | with `mode: patterns`, the URLs to render, searched anywhere in the URL as in `filters`; required there, an error with another `mode` |
| `wait_until` | `load`, `domcontentloaded` or `networkidle` | `load` | the event of the page to wait for; `networkidle` is no request for half a second |
| `wait_for` | CSS selector or `null` | `null` | an element to wait for after that, e.g. `"#content"` |
| `timeout` | number, > 0 | `30.0` | seconds the browser has for a page, the waits included |
| `max_open_pages` | whole number, >= 1 | `2` | pages rendered at once; a browser tab takes 50 to 100 MB |
| `block_resources` | list of resource types | `[image, font, media]` | requests the browser does not make: `image`, `font`, `media`, `stylesheet`, `script`, `xhr`, `fetch`, `websocket`, `eventsource`, `manifest`, `texttrack`, `other` |

```yaml
rendering:
  mode: patterns
  include: ['^https://example\.com/app/']
  wait_for: "#content"
```

Rendering needs the browser of Playwright, a download of its own:

```bash
playwright install chromium
```

Playwright comes with the crawler; in an environment without the package,
a `mode` other than `"off"` is a configuration error with the command to
install it. Without Chromium, the command line
stops before the crawl with the other command and exit code 2; from
Python, every page to render fails with `RenderError` that says the same.
`--render` on the command line sets `mode: always` and clears `include`,
so it renders every page whatever the file says.

The page itself is downloaded as without a browser: through the proxies,
with the cookies and the headers of `session`, within `max_page_size`.
Only an HTML page goes to the browser; robots.txt, sitemaps, redirects
and other types never do. The browser gets the document as it was
downloaded, runs its JavaScript and loads its scripts, styles and data
itself; robots.txt is not asked about them, as no browser asks it. A
page counts once against `max_pages` and the rate limit, whatever it
loads, and is rendered within the slot of its request, so
`max_concurrent` and `max_per_domain` bound the browser too.

The browser shares the session of the crawler. Before every page it
gets the cookies the crawler keeps, and after it the crawler gets those
the page set, by JavaScript or in the responses to its requests: they go
with the next download and to `save_cookies`. Only the changes go each
way, so two pages rendered at once do not undo each other's cookies; when
both change the same cookie, the browser wins. The requests of the
browser carry `crawler.user_agent` and `session.headers`, and go through
the proxy the document of the page came through: every proxy has a
browser context of its own (cookies, cache), so with `per_host` a site
and its scripts stay on one address. The hosts of `NO_PROXY` are reached
directly with `from_env`. With `keep_cookies: false` every page is
rendered in a context of its own, closed after it: nothing goes from one
page to the next, the cache neither. The browser's requests are not
counted in the statistics of the proxies, and their failures neither take
a proxy out of rotation nor count against the site.

A page that goes to another URL on its own (JavaScript setting
`location`, `<meta http-equiv="refresh">`) is a redirect: the browser is
stopped, and the crawler checks the target against robots.txt and the
filters and requests it as it would after an HTTP redirect, within the
limit of redirects. Frames and pop-up windows get nothing: their content
is not in the HTML of the page.

The rendered HTML is what is parsed and saved; the status, the headers
(`X-Robots-Tag`) and the final URL are those of the download. A page
over `max_page_size` once rendered fails with `PageTooLargeError`, one
the browser takes longer than `timeout` to render fails with a timeout
(`RenderTimeoutError`) and is retried as one, with the same `timeout`
(it does not grow with the retries); the site is not held to blame for
it, since the document came in time. A browser that cannot start or
crashes fails the pages
with `RenderError`, which is not retried and not held against the site;
a crashed browser is started again for the next page, once.

`wait_until: load` is enough for a page that builds itself from its own
scripts; a page that loads its data afterwards needs `networkidle` or,
better, `wait_for` with an element the data makes. Rendering costs: a
page takes seconds and tens of megabytes instead of milliseconds, and
the site serves its scripts and data too. Use `patterns` when only some
pages need it.

The summary, the JSON statistics and the HTML report count the pages
rendered and failed (a timeout, a browser that failed) and the average
time the browser took for a rendered page, without the wait for a free
tab.

### `storage`

Where the crawled pages are saved; see [Saving pages](api.md#saving-pages).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `outputs` | list of strings | `[]` | files or database URLs; the pages go to each of them; empty saves nothing |
| `batch_size` | whole number, >= 1 | `100` | pages written at once |
| `csv_encoding` | string | `utf-8` | encoding of CSV files, e.g. `utf-8-sig` for Excel |
| `overwrite` | true or false | `false` | true starts the files anew on the first write; false adds to them and logs a warning if a file is not empty. A database keeps a row per URL either way |

| Output | Storage |
|--------|---------|
| `pages.jsonl`, `pages.ndjson` | JSON Lines, a record per line |
| `pages.json` | one indented JSON array |
| `pages.csv` | CSV with a header row |
| `pages.db`, `pages.sqlite`, `pages.sqlite3` | SQLite |
| `sqlite:///pages.db`, `postgresql://user:password@host:5432/database` | the database of the URL |

`{worker}` in a file name, such as `pages-{worker}.jsonl`, is the name of
the worker of a crawl job, and `local` in a crawl of its own; so it is in
`logging.file`, the files of `report` and `session.save_cookies`. A worker
refuses a file of the storage, SQLite included, without it: workers write
side by side, and one page may be saved by two of them (see
[Crawl jobs](api.md#crawl-jobs)).

### `logging`

See [Logging](api.md#logging).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `level` | string | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`, in any case; for the console and the file. matplotlib, which draws the report charts, logs from `WARNING` up whatever the level |
| `file` | string or `null` | `null` | also write the log to this file, as JSON Lines; the console gets it either way |
| `max_bytes` | whole number, >= 0 | `10485760` | the file is rotated at this size; 0 never rotates it |
| `backup_count` | whole number, >= 0 | `5` | rotated files that are kept; 0 never rotates the file |

### `report`

Files the statistics are written to after the crawl; see
[Page statistics](api.md#page-statistics). A worker of a crawl job writes
those of its own pages after its crawl; the command `report` writes those
of the whole job by the same keys, its title followed by the name of the
job (see [Crawl jobs](api.md#crawl-jobs)).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `stats_json` | string or `null` | `null` | the statistics as JSON |
| `html` | string or `null` | `null` | an HTML report with tables and charts |
| `title` | string | `Crawl report` | the title of the HTML report |
| `top_domains` | whole number, >= 1 | `10` | hosts listed in the statistics |

### `distributed`

The database of the crawl jobs and how a worker holds its pages; see
[Crawl jobs](api.md#crawl-jobs). Each worker has its own section: it is not
a part of the job, and a crawl of its own ignores it. On the command line,
`job create --config`, `worker --config`, `report --config` and
`status --config` read it (see the
[README](../README.md#crawl-jobs-on-the-command-line)); a worker,
`report` and `status` need no file when `CRAWLER_DATABASE_URL` is set.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `database_url` | string or `null` | `null` | `postgresql://user:password@host:5432/database`; `null` takes the one of `CRAWLER_DATABASE_URL`. A secret: never shown in messages |
| `lease_seconds` | number, > 0 | `60.0` | a page of a worker that stopped is crawled again after this long |
| `heartbeat_seconds` | number, > 0 | `20.0` | how often a worker renews the leases of its pages and its own; less than `lease_seconds` |
| `max_attempts` | whole number, >= 1 | `3` | leases of a page that may expire before it fails |
| `poll_interval` | number, > 0 | `1.0` | how often a worker with no page looks for one at least |

## Validation

The file is checked as it is loaded, before anything is requested or
written. A problem is reported by the path of its key:

- an unknown key, with the closest known one suggested;
- a value of the wrong type (`max_pages: yes` is not a number, `"10"` is
  not one either) or out of its limits; `true` or `false` for a string
  suggests quotes, as YAML reads a bare `off`, `on`, `no` or `yes` so;
- an invalid URL or regular expression, an unknown log level or encoding;
  a URL with a space or a control character inside, which a client would
  send as `%20` or drop, so it is not the URL that was meant;
- an output with an unknown extension, or a URL of an unknown database;
- a User-Agent with a line break or another control character in it, a path
  with a null character or in the home directory of an unknown user;
- a cookie without a name, a value or a domain, a cookie of an IP address,
  a header that has a key of its own (`User-Agent`, `Cookie`, `Host`,
  `Proxy-Authorization`) or is
  given twice in different case; the values of cookies and headers are not
  shown;
- a proxy URL that is not `http://` or `https://`, has no port, or has a
  path; a SOCKS proxy; the same proxy listed twice, its host in any case;
  the URL is not shown,
  a repeated one with its password hidden;
- an unknown `rendering.mode`, `wait_until` or resource type, an empty
  `wait_for`;
- a key written twice in YAML (plain YAML would keep the last one silently);
- keys that do not go together: `sitemaps.from_robots` without
  `crawler.respect_robots`, a `user_agents` entry with another bot name,
  `session.cookies`, `cookies_file` or `save_cookies` with
  `session.keep_cookies: false`, `proxy.from_env` with `proxy.urls`,
  `rendering.mode: patterns` without `rendering.include`, `include` with
  another `mode`;
- rendering on, without the package of Playwright, with the command to
  install it;
- with `proxy.from_env`, a variable that is not the URL of a proxy, or
  `HTTP_PROXY` and `HTTPS_PROXY` naming one proxy with different
  passwords, when the crawler is made, reported by the names of the
  variables;
- a file that cannot be read, has another extension or is not valid YAML or JSON;
- in the file of `--urls-file`, a line that is not an http(s) URL, reported
  by its number, or a file that cannot be read or is not UTF-8; `-` with
  stdin closed.

All the problems are listed at once:

```
Invalid configuration: config.yaml: 2 problems
  - crawler.max_page: unknown key; did you mean "max_pages"?
  - filters.exclude[0]: not a regular expression: missing ), unterminated subpattern at position 0, got "("
```

A list of start URLs is checked the same way; its error counts the lines:

```
Invalid configuration: urls.txt: 9870 URLs are valid, 130 lines are not
  - urls.txt:12: expected an http:// or https:// URL, got "example.com"
  ...
  - ... and 110 more
```

The message shows the first 20 problems; `error.problems` holds all of
them. The command line prints the message and exits with code 2. In code it is a
`ConfigError` (a `ValueError`) with the list in `error.problems` and the
file in `error.source`.

An empty file, or a section with every key commented out, gives the defaults.

## In code

```python
from crawler import AdvancedCrawler, ConfigError, CrawlerConfig, load_config

try:
    config = load_config("config.yaml")
except ConfigError as error:
    print(error)            # every problem, each with the path of its key

config.crawler.max_pages    # 500
config.filters.exclude      # ("\\.pdf$",)
storage = config.storage.build()   # JSONStorage here; None without outputs

crawler = AdvancedCrawler(config)                      # or straight from the file:
crawler = AdvancedCrawler.from_config("config.yaml", {"crawler": {"max_pages": 5}})
```

`load_config(path, overrides)` applies `overrides`, a mapping shaped like the
file, over the file key by key: `{"crawler": {"max_pages": 5}}` replaces
that one key, a list replaces the whole list. The result is checked as a
whole. This is how the command line applies its options.
`CrawlerConfig.from_dict(mapping)` checks a mapping without a file, and
`config.to_dict()` gives one back. A configuration cannot be changed once
made; to change a value, make another one with `overrides`.

## Recipes

A whole site by its sitemap, saved to a database, quiet console:

```yaml
urls: [https://example.com/]
sitemaps:
  from_robots: true
crawler:
  max_pages: 10000
  max_depth: 3
  rate_limit: 2.0
  keep_pages: false         # memory does not grow with the crawl
filters:
  exclude: ['/login']
storage:
  outputs: ['sqlite:///site.db']
logging:
  level: WARNING
  file: logs/crawler.log
report:
  html: reports/site.html
```

One section of a site, to files for a spreadsheet:

```yaml
urls: [https://example.com/blog/]
crawler:
  max_depth: 5
filters:
  include: ['^https://example\.com/blog/']
storage:
  outputs: [blog.csv, blog.jsonl]
  csv_encoding: utf-8-sig   # opens in Excel
```

A site that is slow or fails often:

```yaml
urls: [https://example.com/]
crawler:
  max_concurrent: 4
  max_per_domain: 2
  rate_limit: 0.5           # a request every two seconds
  jitter: 0.5
  read_timeout: 40
  total_timeout: 60
retry:
  max_retries: 5
  base_delay: 2.0
  max_delay: 60.0
circuit_breaker:
  cooldown: 120
```

A site behind a login, with the session of a browser: log in in the
browser and export its cookies to `cookies.txt` with a browser extension,
then:

```yaml
urls: [https://example.com/account/]
crawler:
  max_concurrent: 2
  rate_limit: 1.0
filters:
  exclude: ['/log-?out', '/sign-?out']   # the links that end the session
session:
  cookies_file: cookies.txt   # exported from the browser after logging in
  save_cookies: cookies.txt   # the session the site renewed, for the next run
```

The crawl must not follow the link that logs out: the site would end the
session, and the rest of the crawl would get the login page. Leave
`crawler.user_agents` empty: a site that ties its session to the browser
may end it when the User-Agent changes. A site that takes a token instead
of a cookie gets it in a header, `session.headers: {Authorization:
"Bearer ..."}`, with `filters.same_domain_only: true`, the default, so that
the token does not reach other sites. The file is the session: it is
written readable by its owner only; keep it out of version control.

Through a few proxies, each site on one of them, a dead proxy left alone
for five minutes:

```yaml
urls: [https://example.com/]
proxy:
  urls:
    - http://user:password@proxy-1.example:3128
    - http://user:password@proxy-2.example:3128
  max_failures: 2
  cooldown: 300
report:
  html: reports/site.html   # the requests and failures of every proxy
```

With `rotation: per_request` the requests of a site take turns over the
proxies instead. The proxies of the environment, `HTTP_PROXY` and
`HTTPS_PROXY`, with the hosts of `NO_PROXY` reached directly:

```yaml
urls: [https://example.com/]
proxy:
  from_env: true
```

A single-page application, rendered in a browser where it needs to be:

```yaml
urls: [https://example.com/app/]
crawler:
  max_concurrent: 4
  rate_limit: 1.0
rendering:
  mode: patterns
  include: ['^https://example\.com/app/']
  wait_for: "#content"      # an element the data of the page makes
  timeout: 20
  max_open_pages: 2
```

Look at a page without the browser first: many sites send their content
in the HTML, and rendering costs seconds and tens of megabytes a page;
[examples/render_js.py](../examples/render_js.py) shows what it adds to a
page. `wait_for` with an element the data makes is surer and faster than
`wait_until: networkidle`, which waits for every request of the page,
analytics included. A page is rendered within the slot of its request,
so `max_concurrent` bounds the browser too, and `max_open_pages` the tabs
open at once. For one run, `--render` renders every page instead. The
recipes add up: with `session` and `proxy`, the browser gets the cookies,
the headers and the proxy of the crawler.
