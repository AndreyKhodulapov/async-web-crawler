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
`--output`) replaces the whole list of the file. Sitemaps, filters, retries,
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
| `max_urls` | whole number, >= 1 | `50000` | pages taken from one sitemap, its index included |

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
| `max_page_size` | whole number, >= 1, or `null` | `10485760` | bytes of a page body (10 MiB); a larger page fails unread; `null` lifts the limit |
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
[Page statistics](api.md#page-statistics).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `stats_json` | string or `null` | `null` | the statistics as JSON |
| `html` | string or `null` | `null` | an HTML report with tables and charts |
| `title` | string | `Crawl report` | the title of the HTML report |
| `top_domains` | whole number, >= 1 | `10` | hosts listed in the statistics |

## Validation

The file is checked as it is loaded, before anything is requested or
written. A problem is reported by the path of its key:

- an unknown key, with the closest known one suggested;
- a value of the wrong type (`max_pages: yes` is not a number, `"10"` is
  not one either) or out of its limits;
- an invalid URL or regular expression, an unknown log level or encoding;
- an output with an unknown extension, or a URL of an unknown database;
- a User-Agent with a line break or another control character in it, a path
  with a null character or in the home directory of an unknown user;
- a key written twice in YAML (plain YAML would keep the last one silently);
- keys that do not go together: `sitemaps.from_robots` without
  `crawler.respect_robots`, a `user_agents` entry with another bot name;
- a file that cannot be read, has another extension or is not valid YAML or JSON.

All the problems are listed at once:

```
Invalid configuration: config.yaml: 2 problems
  - crawler.max_page: unknown key; did you mean "max_pages"?
  - filters.exclude[0]: not a regular expression: missing ), unterminated subpattern at position 0, got "("
```

The command line prints the list and exits with code 2. In code it is a
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
