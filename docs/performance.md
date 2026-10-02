# Performance: synchronous vs asynchronous crawling, memory and bottlenecks

Short, interview-ready notes on what concurrency buys a crawler, where it
stops helping and what the measurements of this project show.

## How it was measured

- `python src/demo_main.py scale` crawls a local site of 100, 500 and 1000
  pages twice: with `SyncCrawler` (one request at a time, `urllib`) and with
  `AsyncCrawler` (20 requests at once). Both use the same parser and fetch
  the same pages.
- The site (`ScaleSite`) answers every request after a **fixed delay** of
  50 ms, standing in for the network and a remote server. Without a delay a
  local server answers in microseconds and there is no waiting to overlap,
  so the comparison would measure nothing a real crawl has.
- The server runs in a **thread of its own**: a synchronous client blocks
  its thread, and a server in the same thread could never answer it.
- No rate limit, robots.txt or retries: they bound the speed on purpose and
  would hide the crawler's own.
- **Time and memory are measured in separate runs.** `tracemalloc` slows
  Python down several times, so the timed run goes without it. The memory
  run needs no delay (what a crawler holds does not depend on how slow the
  server is). The number is the peak of what Python allocated during the
  crawl, the local server included, not the RSS of the process.

## Results

Python 3.14, Intel Core i7 2.3 GHz, pages of 6 KB, 50 ms per response,
20 concurrent requests:

| Pages | Sync time | Sync pages/s | Async time | Async pages/s | Speedup | Sync memory | Async memory | Async, pages not kept |
|------:|----------:|-------------:|-----------:|--------------:|--------:|------------:|-------------:|----------------------:|
| 100   | 5.50 s    | 18.2         | 0.56 s     | 179           | 9.8x    | 1.4 MB      | 2.3 MB       | 2.2 MB                |
| 500   | 27.57 s   | 18.1         | 2.12 s     | 235           | 13.0x   | 4.7 MB      | 6.1 MB       | 2.5 MB                |
| 1000  | 55.00 s   | 18.2         | 4.13 s     | 242           | 13.3x   | 8.8 MB      | 10.3 MB      | 3.0 MB                |

Throughput of the asynchronous crawler on 500 pages by concurrency:

| Concurrent requests | 1    | 5    | 10    | 20    | 50    |
|---------------------|-----:|-----:|------:|------:|------:|
| Pages per second    | 18.1 | 85.2 | 159.4 | 223.2 | 247.1 |

## What the numbers say

- **The synchronous crawler pays the delay for every page**: 1000 / 18.2 is
  55 ms a page, 50 of them spent waiting. Its time grows linearly and no
  code change makes it faster: the time is not the crawler's.
- **The asynchronous crawler overlaps the waits.** Up to 10 concurrent
  requests the speed grows almost linearly (5 requests, 4.7 times faster):
  the crawl is **I/O-bound**, and concurrency is what it needs.
- **Then it hits a ceiling of about 250 pages per second**, and 50 requests
  at once are little faster than 20. One page costs about 4 ms of CPU
  (parsing 2 ms, the HTTP client and the event loop the rest), and one
  Python process runs on one core: the crawl has become **CPU-bound**.
  Amdahl's law in practice: concurrency removes the waiting, not the work.
- With one request at a time both crawlers do 18 pages per second: asyncio
  itself costs nothing measurable next to a network round trip.
- A small site gains less (9.8x at 100 pages): the crawl starts from one
  page, and until links are found there is nothing to fetch in parallel.
- Against a real site the picture is simpler: the rate limit (1 request per
  second per host by default) is the bottleneck long before the CPU is. The
  ceiling matters for crawls of many hosts at once.

## Pages of a real size

The pages of the measurement are small: 6 KB and 42 tags. A real page is
ten times that, and the CPU ceiling moves with it. A page of 78 KB (1300
tags, 290 links, a table of 80 rows) takes 56 ms to parse, and a site of
such pages is crawled at about **10 pages per second** with 20 requests at
once: the same ceiling, 25 times lower. With the default rate limit, a
dozen hosts crawled at once are enough to reach it.

Of the 56 ms, about 34 are BeautifulSoup building its tree: `lxml` itself
parses the page in 2 ms, the rest is a Python object made for every tag and
string. The extractors take the other 20, links and text the most.

## Bottlenecks found and what was done

1. **Parsed trees waiting for the garbage collector.** A BeautifulSoup tree
   is full of reference cycles (parent and child, siblings), so reference
   counting cannot free it; it lives until the cyclic collector gets to it.
   Meanwhile the trees of the next pages pile up: the peak memory of a
   crawl that kept nothing still grew with its size (10 MB at 1000 pages).
   Fix: the parser takes the tree apart as soon as the page is extracted
   (`soup.clear(decompose=True)`), and the memory is freed at once: under
   3 MB at 1000 pages. Note that `soup.decompose()` on the root does not do it:
   it does not reach the children.
2. **Every parsed page held until the end of the crawl.** `crawl()` returns
   the pages, so it keeps them all: about 6 KB a page here, more on real
   pages, and memory linear in the size of the crawl. A crawl that saves
   its pages to a storage does not need them in memory. Fix: the option
   `keep_pages=False` (`crawler.keep_pages` in the configuration; the
   command line always sets it). Memory then stays nearly flat: what is
   left is the set of seen URLs, about 1 KB a page.
3. **A filter object built for every tag.** The extractors asked
   `tag.find_parent(names)` for every tag to skip hidden content; each call
   builds a matcher. A plain walk up `tag.parents` does the same check:
   parsing a page went from 4.2 ms to 3.2 ms.
4. **The tree walked for every kind of tag.** The extractors called
   `find_all` about 15 times a page, and each call visits every node.
   Worse, bs4 has a fast path only for a search by one name: by a list of
   names or by an attribute (`find_all("a", href=True)`) it is 3 to 6 times
   slower. Fix: `parse` walks the tree once and keeps its tags by name; the
   extractors pick theirs from that index and check the attributes
   themselves. Parsing went from 3.2 ms to 2.2 ms on the small page and
   from 90 ms to 56 ms on the page of 78 KB (with the next fix).
5. **The same URL normalized again and again.** A link was normalized when
   it was found, again when it was filtered and again when it was queued;
   the host of a page was worked out nine times in one request. Each time
   is 10 to 25 microseconds of `urlsplit`, percent-encoding and joining,
   and the filtering and queuing happen **on the event loop**: the 290
   links of a large page held it for 12 ms, when no other request, timeout
   or rate limit could move. Fix: `normalize_url` and `get_host` remember
   their answers for the latest 4096 URLs (`functools.lru_cache`): 12 ms
   became 1 ms, and a link to the site's menu, repeated on every page, is
   normalized once. The price is memory, up to about 2 MB however large
   the crawl; it is a part of the last column of the table.

Together the last two moved the ceiling from 200 to 250 pages per second on
the small pages, and from 7.6 to 10.5 on the large ones.

## What was left alone, and why

- **Parsing in a worker thread** (`asyncio.to_thread`). Because of the GIL
  it gives no parallelism, and for small pages parsing right on the event
  loop was about 20% faster (no handoff between threads). But a large page
  takes tens of milliseconds, and on the loop that would freeze every
  other request, timeout and rate limit for that long. The thread keeps
  the loop responsive; throughput was traded for predictable latency.
- **A process pool for parsing** would use several cores and lift the
  ceiling. It costs pickling every page both ways and a pool to manage.
  The next step if one process is not enough is several crawler processes
  sharing a queue: parsing, the HTTP client and the event loop all scale
  that way, not the parser alone.
- **BeautifulSoup itself.** Its tree is now most of the cost of a page
  (34 ms of 56 on a large one), and the extractors working on `lxml`
  directly would be several times faster. That is a rewrite of the parser
  and a change of its interface (`extract_links(soup)` and the others take
  a BeautifulSoup tree), for a gain that only shows where the rate limit
  does not: wide crawls of many hosts. Processes help there as well, and
  keep the parser as it is.
- **Logging.** At the `INFO` level the crawler writes four records a page,
  each formatted and flushed to stderr on the event loop: about 10% of the
  time of a small page, and as much again with a log file. Against a real
  site that is nothing next to the network. For a crawl that is CPU-bound,
  `logging.level: WARNING` is the cheap fix; a queue with a writer thread
  would move the work, not remove it.
- **The server shares the process with the crawler** in this measurement,
  and so shares its GIL: a real remote server would leave the crawler a
  little more CPU. The numbers are a lower bound.

## Rules of thumb

- Measure before optimizing: the two fixes that mattered most (trees,
  kept pages) were found with `tracemalloc` snapshots, not by reading code.
- Measure on input of a real size. On the small pages the event loop looked
  free; the 12 ms it spent on the links of a page only showed on large ones.
- A library call that looks like one operation may be a walk over the whole
  input. Count the calls per item and find out what each one costs.
- I/O-bound work scales with concurrency, CPU-bound work with processes.
  Find out which one the program is at the load you care about.
- A reference cycle is not a leak, but it delays freeing memory until the
  garbage collector runs; break cycles in objects made by the thousand.
- Memory that grows with the input is a limit on the input. Stream results
  to storage instead of collecting them.
- More concurrency is not free even when it does not help: 50 requests at
  once load the remote site 50 times as much for the same speed.
