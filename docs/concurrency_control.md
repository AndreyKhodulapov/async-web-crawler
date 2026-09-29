# Crawling: queues and concurrency control

Short, interview-ready notes on how the crawler walks a site and keeps its
load under control.

## Producer–consumer with a worker pool

- A crawl is a **producer–consumer** problem: every fetched page produces new
  URLs, and workers consume them. The two sides meet in a queue.
- Instead of one task per URL, a **fixed pool of N workers** loops
  `take URL -> fetch -> parse -> queue new links`. Memory stays bounded (the
  queue holds strings, not thousands of pending tasks), and N is the natural
  concurrency knob.
- Workers run in an `asyncio.TaskGroup`: the crawl returns only when all of
  them have finished, and no worker outlives it.

## `asyncio.Queue` and friends

| Class | Order | Notes |
|-------|-------|-------|
| `asyncio.Queue` | FIFO | `put`/`get` wait when full/empty; `maxsize` gives backpressure |
| `asyncio.PriorityQueue` | smallest item first | items are tuples `(priority, seq, value)`; `seq` breaks ties so values are never compared |
| `asyncio.LifoQueue` | LIFO | depth-first order |

- `heapq` is the data structure behind `PriorityQueue`: `heappush` and
  `heappop` are O(log n).
- These queues are for coroutines on one event loop. They are not thread-safe;
  between threads use `queue.Queue`.

## When is a crawl finished?

- An empty queue does **not** mean the end: a page still being fetched may
  add new links. The crawl is over only when the queue is empty **and** no
  URL is in progress.
- Classic solution with `asyncio.Queue`: `task_done()` after each item plus
  `await queue.join()`, then cancel the workers, which are blocked forever in `get()`.
- This crawler counts URLs in progress instead. `get_next()` waits on an
  `asyncio.Event` while work is in flight, and returns `None` to every worker
  once nothing is left. The workers then exit on their own, without being cancelled.
- The in-progress counter must go down on **every** path, including
  unexpected exceptions. A single leaked URL makes all workers wait forever.
  That is why a worker catches `Exception` around each page and marks it failed.

## Synchronization primitives

- `Lock`: one holder at a time.
- `Semaphore(n)`: up to n holders. `BoundedSemaphore` also raises if it is
  released more times than acquired, which catches bugs.
- `Event`: a flag that wakes *all* waiters when set. Good for "something
  changed, re-check your condition".
- `Condition`: a lock plus wait/notify. `notify()` needs the lock to be held,
  so it cannot be called from plain (non-async) methods. The queue's `add_url`
  and `mark_*` are plain methods, so they use an `Event`.
- No locks are needed around plain Python state in asyncio: a task can be
  switched only at an `await`, so code between two `await`s runs atomically.

## Global and per-host limits

- A **global limit** protects the crawler (sockets, memory). A **per-host limit**
  protects each site and keeps the crawler from being rate-limited or banned
  (HTTP 429, 403).
- `SemaphoreManager` keeps one global semaphore and one per host, created
  on first use and dropped when idle.
- **Acquisition order matters.** Take the host slot first, then the global
  one. In the reverse order, tasks waiting for a busy host would hold global
  slots and block requests to idle hosts.
- A fixed order also rules out deadlock: two tasks never hold one lock
  each while waiting for the other's.
- aiohttp's `TCPConnector(limit_per_host=...)` limits *connections*. A
  semaphore limits *requests* at the application level and makes the active
  ones countable.
- **Head-of-line blocking**: when every worker holds a URL for the same busy
  host, URLs for other hosts wait in the queue. Fixes include more workers than
  slots, or one queue per host (as in large crawlers).

## Traversal order and depth

- **BFS** (queue) visits pages level by level: with a page limit, you get the
  pages closest to the start, usually the most important ones. **DFS** (stack)
  dives into one branch and can get lost in endless pagination or calendars.
- Here the priority *is* the depth, so the priority queue gives BFS even
  though several workers take URLs at once.
- `max_depth` bounds the crawl's shape; `max_pages` bounds its cost. Use both.

## Deduplication

- A URL is checked against a "seen" set **when it is queued**, not when it is
  fetched. Then the queue never holds duplicates, and two workers never race
  for the same page.
- Deduplication is only as good as normalization: `HTTP://Site:80/a#top`
  and `http://site/a` are one page.
- Redirects: the target is added to the seen set after the response. If a
  direct link to the target is already being fetched at the same time, the page
  is downloaded twice. Nothing reveals the duplicate before the response arrives.
- Some duplicates cannot be detected by URL at all (`/` and `/index.html`).
  `<link rel="canonical">` or content hashing handles those.

## URL filters

- Host filter: stay on the start hosts. A start URL that redirects
  (`example.com` -> `www.example.com`) adds its final host too.
- Include/exclude regular expressions, compiled once up front so that a
  bad pattern fails fast. Exclude wins over include.
- Filters apply to discovered links only: the start URLs are an explicit choice.

## Measuring progress

- Throughput = finished pages / elapsed time. Report it together with queue
  size, errors and requests in flight. A growing queue with flat throughput
  means the limits, not the site, set the pace.
- Report progress from a separate task (`asyncio.wait({crawl}, timeout=1)`
  in a loop) rather than from inside the workers. The workers stay simple, and
  the report ticks even when no page finishes for a while.
