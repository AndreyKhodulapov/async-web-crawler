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
| 100   | 5.65 s    | 17.7         | 0.60 s     | 167           | 9.4x    | 1.4 MB      | 2.4 MB       | 2.2 MB                |
| 500   | 27.99 s   | 17.9         | 2.48 s     | 201           | 11.3x   | 4.6 MB      | 5.9 MB       | 2.3 MB                |
| 1000  | 55.93 s   | 17.9         | 5.03 s     | 199           | 11.1x   | 8.5 MB      | 10.1 MB      | 2.7 MB                |

Throughput of the asynchronous crawler on 500 pages by concurrency:

| Concurrent requests | 1    | 5    | 10    | 20    | 50    |
|---------------------|-----:|-----:|------:|------:|------:|
| Pages per second    | 17.9 | 82.0 | 156.2 | 206.5 | 210.8 |

## What the numbers say

- **The synchronous crawler pays the delay for every page**: 1000 / 17.9 is
  56 ms a page, 50 of them spent waiting. Its time grows linearly and no
  code change makes it faster: the time is not the crawler's.
- **The asynchronous crawler overlaps the waits.** Up to 10 concurrent
  requests the speed grows almost linearly (5 requests, 4.6 times faster):
  the crawl is **I/O-bound**, and concurrency is what it needs.
- **Then it hits a ceiling of about 200 pages per second**, and 50 requests
  at once are no faster than 20. One page costs about 5 ms of CPU (parsing
  3 ms, the HTTP client and the event loop the rest), and one Python
  process runs on one core: the crawl has become **CPU-bound**. Amdahl's law
  in practice: concurrency removes the waiting, not the work.
- With one request at a time both crawlers do 17.9 pages per second: asyncio
  itself costs nothing measurable next to a network round trip.
- A small site gains less (9.4x at 100 pages): the crawl starts from one
  page, and until links are found there is nothing to fetch in parallel.
- Against a real site the picture is simpler: the rate limit (1 request per
  second per host by default) is the bottleneck long before the CPU is. The
  ceiling matters for crawls of many hosts at once.

## Bottlenecks found and what was done

1. **Parsed trees waiting for the garbage collector.** A BeautifulSoup tree
   is full of reference cycles (parent and child, siblings), so reference
   counting cannot free it; it lives until the cyclic collector gets to it.
   Meanwhile the trees of the next pages pile up: the peak memory of a
   crawl that kept nothing still grew with its size (10 MB at 1000 pages).
   Fix: the parser takes the tree apart as soon as the page is extracted
   (`soup.clear(decompose=True)`), and the memory is freed at once: 2.7 MB
   at 1000 pages. Note that `soup.decompose()` on the root does not do it:
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
- **The server shares the process with the crawler** in this measurement,
  and so shares its GIL: a real remote server would leave the crawler a
  little more CPU. The numbers are a lower bound.

## Rules of thumb

- Measure before optimizing: the two fixes that mattered most (trees,
  kept pages) were found with `tracemalloc` snapshots, not by reading code.
- I/O-bound work scales with concurrency, CPU-bound work with processes.
  Find out which one the program is at the load you care about.
- A reference cycle is not a leak, but it delays freeing memory until the
  garbage collector runs; break cycles in objects made by the thousand.
- Memory that grows with the input is a limit on the input. Stream results
  to storage instead of collecting them.
- More concurrency is not free even when it does not help: 50 requests at
  once load the remote site 50 times as much for the same speed.
