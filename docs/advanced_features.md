# Advanced features: sitemaps, configuration, logging, monitoring and integration

Short notes on what turns a crawling library into a tool
that can be run, configured and watched.

## Sitemaps

- A **sitemap** (`urlset`) lists the pages a site wants crawled; a **sitemap
  index** (`sitemapindex`) lists other sitemaps. One file holds at most
  50 000 URLs and 50 MB, so large sites always use indexes. Where to find
  them: `Sitemap:` lines of robots.txt, or `/sitemap.xml` by convention.
- Why use one: it finds pages no link leads to and skips the discovery
  phase. It is a hint, not the truth: it may be stale, list pages robots.txt
  disallows, or miss pages. So its URLs go through the same filters and
  robots.txt check as links.
- **Treat it as untrusted input**:
  - XML entities: turn entity resolution and network access off in the
    parser (XXE, "billion laughs").
  - gzip: unpack with a size limit and stop there (a gzip bomb); detect gzip
    by its magic bytes, not by the file name or headers. An archive may be
    several gzip members in a row: the limit covers them all.
  - the download itself: the HTTP client undoes `Content-Encoding: gzip`
    while reading, so read the body in chunks up to the limit rather than
    whole.
  - indexes: cap the depth or the number of files, remember what was
    fetched (an index may list itself), cap the URLs taken.
- Real files are sloppy: different namespace versions or none, blank lines
  before the XML declaration. Match elements by local name.
- Download sitemaps **through the crawler's own request path**, so the rate
  limit, retries and the circuit breaker apply to them as to pages.
- A broken nested sitemap is logged and skipped; only the failure of the
  one that was asked for is an error.

## Configuration

- **Layers, in order**: defaults in code, then the file, then command-line
  options (and environment variables for secrets). The same merge serves
  all of them: overrides shaped like the file, applied key by key.
- **Validate on load, fail fast, report everything**: an error found after
  an hour of crawling costs an hour. Collect all the problems, name each by
  the path of its key (`crawler.max_pages`, `urls[1]`), suggest the closest
  key for a typo.
- **Reject unknown keys.** A silently ignored `max_page: 10` is the worst
  outcome: the user believes the limit is set.
- YAML pitfalls: `yes`/`no` are booleans, a key written twice keeps the last
  one silently, `yaml.load` without a safe loader runs code. Use
  `SafeLoader` and reject duplicates.
- Type pitfalls in Python: `True` is an `int`, so check `bool` first;
  accept an int where a float is expected; reject `inf` and `nan`.
- **One source of truth**: typed dataclasses with the defaults and limits
  next to the fields; the example file is tested against them, so it cannot
  go stale.
- **Immutable** once built (`frozen=True`): a running crawl cannot be
  changed halfway by another part of the program.
- Validation opens no files and no connections: checking a configuration
  has no side effects.

## Structured logging

- **Text for people, JSON for machines.** The console gets
  `time | level | logger | message`; the file gets JSON Lines, one object
  per record, which `jq` and log collectors read without parsing rules.
- A record must stay **one line**: newlines of messages and tracebacks are
  escaped inside the JSON string.
- Timestamps in **UTC, ISO 8601**: sortable, no daylight-saving gaps.
- **Levels**: DEBUG for what a developer needs, INFO for the normal course
  (a line per request), WARNING for what recovered or was skipped (a retry,
  a failed page), ERROR for what was lost (a report not written).
- **Rotation** by size (`RotatingFileHandler`) or by time: the disk does not
  fill up; `backup_count` old files are kept. Under several processes
  rotate outside the program (logrotate) or log to stdout and let the
  platform collect it.
- Loggers are named after modules (`logging.getLogger(__name__)`), and only
  the application configures handlers; a library never does. Configuring
  twice must replace the handlers, not add to them, or every record is
  written twice.
- Logging is not free: at INFO it cost about 10% of the crawl speed here,
  the file another 10%. Use `logger.info("... %s", value)`, not f-strings:
  the message is not built when the level is off.

## Live monitoring

- **Percent** needs a known total. A crawl does not know how many pages a
  site has, so the percent is measured against the page limit; a smaller
  site ends below 100%.
- **Current speed is a moving window** (the last 10 seconds), not the
  average since the start: the average hides a crawl that has stalled.
- **ETA = remaining / current speed**; undefined at zero speed, so show
  `--` rather than infinity. An estimate from the queue size jumps around,
  since the queue is small at the start and grows.
- Show what explains the speed: active tasks, requests in flight, the queue.
  Few requests in flight with a long queue means the rate limit is the
  bottleneck, not the network.
- In a terminal redraw one line (`\r`, clear to the end of the line) and
  print log records above it; in a pipe or a file print a line per update
  (check `isatty()`).
- Keep the numbers apart from the text: a tracker returns a dataclass, a
  formatter makes the line. The tracker takes time from the statistics, so
  tests need no sleep.

## Statistics and reports

- Two questions, two objects: **how the crawl is going** (queue, in flight,
  request rate: a snapshot) and **what it got** (pages by outcome, status
  code, domain: totals for a report).
- Count a **page once**, however many attempts it took; count attempts
  separately (error statistics). Otherwise retries inflate the failures.
- The statistics are a plain dict: the same data goes to the console
  summary, the JSON file and the HTML report.
- The HTML report is **one self-contained file**: inline styles, charts as
  embedded images, no scripts, no CDN. It opens anywhere and can be mailed.
- **Escape everything that came from the crawl** (hosts, error texts): a
  report is HTML built from data of other sites.
- Reports are written on interruption too: a crawl stopped with Ctrl-C
  still saves its pages and says what it did.

## Integration: a facade over the components

- `AdvancedCrawler` is a **facade by composition**: it builds the crawler,
  the storage, the statistics and the logging from one configuration and
  owns their lifetime. The crawler class itself knows nothing about files
  or configuration and stays usable as a library.
- **Layers**: command line -> facade -> components. Each layer adds one
  thing: the command line parses options and prints, the facade wires, the
  components work.
- **Dependency injection** makes the parts testable: the sitemap and
  robots.txt parsers get a fetch function, the retry strategy a wait
  function, the statistics a clock.
- **Resource ownership**: whoever creates a resource closes it, in the
  reverse order, in `finally` or `async with`. Set up the logging last in
  the constructor, so a failed construction leaves no open file.
- **Exit codes** make a tool scriptable: 0 success, 1 the run failed, 2 the
  usage or configuration is wrong, 130 interrupted. Results go to stdout,
  logs and progress to stderr.
- **Graceful shutdown**: on cancellation stop the workers, flush the
  storage, write the reports, then let the cancellation go on.
