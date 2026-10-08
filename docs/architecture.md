# Architecture: the layers of the crawler

Short notes on how the crawler is split into layers, and on moving code
between them without changing what it does. The layers themselves are
listed in the [API reference](api.md#internals).

## Why layers

- One class that does everything (a session, retries, robots.txt, a
  queue, counters) grows until every change touches all of it. Split it
  by **reason to change**: how bytes are fetched, how one URL is fetched
  politely, how a crawl walks a site, what the user calls.
- The layers here, from the bottom:
  - **HTTP** (a `Transport`, such as `HttpTransport`): one GET, no redirects.
  - **Request** (`Fetcher`): robots.txt, rate limit, circuit breaker, retries, redirects.
  - **Crawl** (`CrawlRun`): filters, deferred pages, counters; the pages
    come from a `Frontier`: the queue, the URLs seen, the outcomes and the
    limits on how many pages are requested.
  - **Facade** (`AsyncCrawler`): the public API.
- **Calls go down only.** A lower layer knows nothing of the one above:
  `Fetcher` has no idea a crawl exists, so `fetch_url()` and `crawl()`
  share one request path, and the robots.txt and sitemap downloads go
  through it as well.
- A feature then lands in one layer:
  - proxies and cookies change the HTTP session: the transport picks the
    proxy of every request and tells the pool how it went, and the
    request layer sees only the class of the error (`ProxyNetworkError`
    is retried, `NoProxyError` is not), never a proxy;
  - rendering JavaScript in a headless browser is a second transport with
    the same contract (`BrowserTransport`), a wrapper of the first: the
    page is downloaded as before and only then handed to the browser. A
    page that goes to another URL on its own comes back as a redirect, so
    the request layer checks robots.txt, the filters and the redirect
    limit for it as for any redirect; the transport never calls up to ask.
    The response names the proxy it came through, so the browser renders
    the page through the same one, and the cookies the page set go back
    into the cookie jar of the first transport, which stays the one the
    crawler keeps;
  - a crawl shared by several machines replaces the frontier of the crawl
    layer: `MemoryFrontier` with one kept in a database.

## Contracts between layers

- The HTTP layer has one method: "make a single GET and return the
  response; a redirect comes back as a response; every failure is a
  typed `FetchError`". It does not follow redirects. Following them is the
  request layer's job, because every hop needs robots.txt, the rate limit
  and the circuit breaker of its own host.
- Failures cross layers as **typed exceptions** below the request layer
  and as **results** above it (`FetchResult.error`). A crawl worker has to
  go on after any failure, so it should not have to catch exceptions.
- **Add an abstraction when the second implementation comes.** With one
  transport, a `Protocol` would be a guess at the interface, so the
  contract lived in the docstring of `HttpTransport`. The `Protocol`
  (`Transport`: structural typing, no base class to inherit) is made for
  the browser transport, shaped by what both need: `get()`, `close()`,
  `reset_stats()`, `cookies()` and `update_cookies()` (the browser gives
  back the cookies its pages set). The request layer knows only
  `Transport`; the facade builds the transports and knows what they are.
- The `Frontier` came one step ahead of its second implementation, a
  frontier in a database that several worker processes share, because
  that one shapes the contract more than the first: its methods are
  `async`, found links are added in one call per page, and a page goes
  back to the queue and is uncounted from `max_pages` in the same call,
  which is one transaction in a database. The counts of `stats()` are the
  exception: progress is shown from a synchronous call, so they are what
  the process knows without asking: all of them in memory, a snapshot of
  the job refreshed now and then in the database. It is an ABC, not a `Protocol`: the
  implementations share the limits they are made with. The contract
  tests run against every implementation.
- Two more points of that contract come from the database one, though
  the frontier in memory has no use for them:
  - "is this URL seen" and "remember it" are one call, `mark_seen`, that
    tells whether the URL was new. In memory two calls cannot interleave
    with another worker; in a database they can, and two workers would
    both follow a redirect to the same page. One `INSERT ... ON CONFLICT`
    answers both questions at once. The call names the page whose
    redirect led to the URL, and that page is told the URL is new
    again (below);
  - a page is **done only once it is stored**. The storage writes in
    batches, so a page processed may wait in its buffer: a process that
    stops then leaves it done in a shared queue with no record, and the
    next run never fetches it again. The crawl finishes such a page with
    `pending_save`; the storage reports the records it has written
    through `on_settled`, and those it dropped as ones no write can take
    through `on_dropped`; the crawl passes them on to `Frontier.saved`
    and `Frontier.dropped`. Until then the database frontier keeps the
    page leased, and hands it out again if the lease runs out. A page
    whose record is dropped fails: counted processed, it would stand for
    a record the job does not have, and left `saving`, the heartbeat
    would renew its lease and keep the job from finishing. The storage knows nothing of the frontier: a callback of
    URLs is all it offers, so any storage works, not only a table in the
    same database as the queue.

## The frontier in a database

`PostgresFrontier` (`crawler/distributed/`) is the second implementation:
the frontier of one crawl job, shared by workers in other processes or
on other machines. It keeps the contract, and the same contract tests
run against it; what it adds is what sharing needs.

- **Tables**, made by the first process that connects: `crawl_jobs`
  (the part of the configuration a job keeps, its limits and the counts
  they are checked against: pages requested, pages unfinished, links
  dropped), `frontier` (every URL of a job once: the primary key
  `(job, url)` is the deduplication; a page finished keeps its worker,
  the status and time of its response and the class of its error, for
  the report, and the moment it was finished, for the speed of the
  job), `workers` (when each worker started, its lease and the
  time it ran), `hosts` (when a host may be
  requested next, its Crawl-delay and, if it is held back, why; how many of its pages
  were accepted and requested; its failures that count toward giving it
  up, and the outcome of a host given up),
  `job_scope` and `out_of_scope` (below). Workers take the limits from
  the job, not from their own configuration, so that they all count
  against the same ones.
- **A job is created, then seeded.** `create_job` makes the row of the
  job, `seeding`, and fills it the way a local crawl starts: the start
  URLs, then the sitemaps read until the frontier is full, by a
  `CrawlRun` that crawls nothing (`seed`). Sitemaps are read once, by
  the process that creates the job, not by every worker. `take` hands out
  nothing while the job is seeding, so workers may start at any time: one
  that started early would crawl a start URL that redirects before the
  sitemap pages it brings into the scope are held. A worker never makes a
  job: one started with a mistyped name fails instead of crawling an
  empty one.
- **The scope is the job's.** Under `same_domain_only`, a start URL that
  redirects ("example.org" to "example.com") brings its target host into
  the crawl, and with it the sitemap pages of that host, read before any
  page was fetched. One worker crawls the start URL, all of them filter
  links: the host goes to `job_scope` and bumps the version of the scope
  in the job row, which `take` returns with every page. A worker that
  sees a new version reads the hosts before it crawls the page, so no
  links of the new host are filtered out by a worker that has not heard
  of it yet. The sitemap pages out of scope wait in `out_of_scope`, not
  in `frontier`: they are not seen, and once their host joins they are
  added as any page found, with the bounds checked.
- **A job ends by itself.** A worker that finds nothing to hand out and
  nothing to wait for marks the job `finished`, if no page of any worker
  is in progress or pending its save; so does one that closes after its
  last pages are saved. A job that reached `max_pages` is finished with
  pages left in the queue, as a local crawl ends.
- **A page is leased, not handed over.** `take` makes it `leased` until
  `lease_until`, and a heartbeat renews the leases of the worker's pages.
  A worker that is killed renews nothing: its pages go back to the queue
  when their leases expire, uncounted from `max_pages`, and fail after
  `max_attempts` expiries. So a page is crawled **at least once**, not
  exactly once, and the storage must take a page twice: a table with a
  row per URL does. A lease that expired while the worker was slow, not
  gone, may come back to that worker through another of its tasks: the
  page is left to the task that crawls it, not crawled a second time.
  So a task lets go of a page it puts back before the database queues
  it: another task of the worker may take it before the answer comes,
  and would otherwise leave it to the first one, done with it, while
  the heartbeat kept it leased for good.
- **The target of a redirect is seen from its page.** A redirect marks
  its target seen, so that a link to it is not crawled a second time,
  and the row keeps the page it was seen from (`frontier.seen_from`). A
  page may go back to the queue after it followed its redirect: the
  host of the target asked to wait, its circuit is open, its robots.txt
  is down, or the lease of the worker expired. Whoever takes it next
  follows the redirect again, as `mark_seen` is true for the page that
  marked the URL; with the source kept in the memory of one worker,
  another one found the target seen and skipped the page, and the
  target was never crawled. A page that fails lets its targets be
  queued again (`forget`, by the same page only); so do the pages the
  database fails itself, those of a host given up and those whose lease
  expired `max_attempts` times, in the statement that fails them.
- **Saved, then done.** A page processed with `pending_save` is
  `saving`, still leased and renewed, until the storage reports its
  record written and the crawl calls `saved`. A worker killed with a
  full buffer leaves its pages `saving`; their leases expire and another
  worker crawls them again. A worker stopped (its task cancelled: Ctrl-C,
  SIGTERM) writes the buffer first, so that those pages are `saved`, then
  closes the frontier, which queues the pages it had in flight again,
  uncounted: nothing is left to the leases but a buffer the storage
  cannot write, which is logged as an error. A page is made `saving` and
  its record handed to the storage at one go, which a cancellation waits
  for: cancelled between the two, the page would be `saving` with no
  record in the buffer, and crawled again once its lease expired (a
  stopped worker left one so in about one stop of six). `take` waits for the
  `saving` pages of other workers (they may come back) but not for its
  own: the buffer is written after the last page is taken, and waiting
  for it would never end.
  Nor would two workers that wait for the `saving` pages of each other,
  each with its own in its buffer, as the heartbeats keep the leases: the
  first run of workers with a storage hung so. So before `take` waits for
  the pages of others with nothing queued, it calls `on_waiting`, and the
  crawl writes out the buffer of its storage. It is not called while a
  worker only waits for the turn of a host: the batches of the storage
  would shrink to a page.
- **A worker whose storage cannot write takes no pages.** A local crawl
  goes on and keeps the pages in the buffer; a worker doing so would
  hold more and more pages `saving` that it may never store, while the
  other workers could crawl them. So once a write fails after its
  retries (`DataStorage.write_failed`), the worker stops taking pages:
  one of its tasks writes the buffer again after the storage's
  `cooldown`, then after twice as long each time, up to
  `MAX_STORAGE_PAUSE` (60 s), for as long as it takes, while the
  heartbeat keeps the pages of the buffer leased. The pages already in
  progress are finished into the buffer. The worker does not stop either
  before its buffer is written: its pages would wait for their leases,
  and there may be no other worker to take them then.
- **A database that fails stops the worker.** An operation of the
  frontier that fails with `FrontierDatabaseError`, its one
  `PostgresFrontier.ERRORS` (the database is down, the connection broke,
  a query was refused), is not tried again and does not fail the page.
  Only the operations of the frontier raise it, so an `OSError` of the
  crawl itself fails its page alone. On a failure of the database the
  crawl stops all its tasks, the storage writes what it buffers, and
  `run_worker` raises `FrontierError`, the error of the database as its
  cause. The pages in progress stay `leased` and come back to the other
  workers once their leases expire; `close` tries to put them back at once and
  only logs if it cannot. Starting the worker again is the business of
  whatever runs it (a restart policy of compose, a Kubernetes Job), as
  in crash-only software: a worker that tried to ride out an outage would
  need to tell a short one from a long one, and a page from a bug, and
  the leases already bring back what it had. The calls that only tell
  the others something (the heartbeat, `saved`, `hold_host`,
  `set_host_interval`) log their errors: what they lose comes back
  through the leases too, and if the database is down, the next
  operation stops the worker anyway.
- **A host has one turn for all workers.** `take` picks the ready host
  with the shallowest page, locks the host row with
  `FOR UPDATE SKIP LOCKED` and moves its `next_allowed_at` on by the
  interval in the same statement. Two workers never take a page of one
  host at once, and a worker never waits for a host that another one
  holds: it takes a page of another host. The order is breadth-first
  among the ready hosts: a page at depth 2 of a ready host comes before
  one at depth 1 of a host that has to wait. To pick the host, `take`
  reads every host of the job whose turn has come, so a take costs more
  the more hosts the job has met (see
  [performance.md](performance.md#many-hosts)).
- **The interval of a host is the longer of the job's and its
  Crawl-delay.** The job's comes from its rate limit (`host_interval`);
  the Crawl-delay is in `hosts.interval`, and holds under
  `per_domain_rate: false` too, as it does in the rate limiter. A worker
  learns it from robots.txt and tells it when it is longer than the one
  it knew (`Fetcher.on_crawl_delay`, `set_host_interval`): once per host
  and worker. The interval never goes down, as a host may serve several
  sites with a robots.txt each, and the next page of the host waits the
  delay from then, as the worker is about to send its request.
  - The pages of the host taken before the delay reached the database
    are not called back: a request per page in progress, as with a hold.
    A crawl from one start URL has one page in progress then. Seeding
    closes the gap for the start sites whose robots.txt it reads
    (`sitemaps.from_robots`): their delay is in the job before any
    worker takes a page.
  - The requests are paced twice: by the database as pages are taken,
    and by the rate limiter of each worker, as in a local crawl. The
    limiter stays: robots.txt and sitemaps are downloaded without `take`,
    the jitter is its own, and a delay that cannot reach the database is
    logged and still keeps the requests of that worker apart. The cost is
    a request that waits in the limiter after its page is taken: the
    first page of a worker waits the delay after the worker downloads
    robots.txt, so it may come close to the next request of another
    worker, while the rate of the host holds over any run of requests.
- **A host held back is held back for all workers.** A host that asks
  to wait (Retry-After), or answers so that the whole host waits out the
  pause before a retry (HTTP 429, a timeout), is held back in the rate
  limiter of the worker that was answered, as in a local crawl; with a
  shared frontier the fetcher also tells it to `on_host_held`, and the
  crawl calls `hold_host`. That moves `hosts.next_allowed_at` to the end
  of the hold, never back (`greatest`), with the reason in `hold_reason`,
  and `take` hands out no page of the host until then. A page put off for
  its host goes back without a delay of its own and comes back with the
  host, however long another worker holds it meanwhile. A page that
  redirects to a held host waits out the hold itself: its own host is
  not held back, and it would be handed out at once and redirect to the
  held one again. The host may have no row yet (the target of a
  redirect): the hold makes one.
  - A host whose circuit has opened, or whose robots.txt is unreachable,
    is held back the same way, by the crawl: until the probe is due, or
    until robots.txt is downloaded again. So a host that is down costs a
    few log lines and statements, not one per page of it every second: its
    pages stay in the queue. A circuit opened again by a failed probe holds
    the host back again, although its page fails rather than waits.
  - Only a hold of a known length goes to the database. A page refused
    while the probe of the host is in flight, or while its robots.txt is
    being downloaded, is put off on its own: when that ends is not known,
    and robots.txt may well be read.
  - The circuit breaker and robots.txt are each worker's own. A worker
    knows of a host that is down from the hold alone; once it ends, the
    first worker that takes a page of the host asks it, with a closed
    circuit or a probe of its own: one or two requests from several
    workers where a local crawl sends one probe. robots.txt is downloaded
    by every worker, and held back for by host, though it belongs to an
    origin (`http` and `https` of a host are one host here).
  - Giving a host up is the job's. The circuit openings of all workers
    and their failed downloads of robots.txt count together in `hosts`.
    A worker tells the database of the failures it has not told yet, in
    one statement that adds them and returns the counts of the job; of
    the workers that fail at once, one sees a count reach
    `MAX_CIRCUIT_OPENINGS` or pass `MAX_ROBOTS_RETRIES`, and gives the
    host up (`give_up_host`). Failures told within half a cooldown of the
    circuit breaker, or half of `UNREACHABLE_TTL` for robots.txt, of the
    last ones counted are not counted (`hosts.circuit_counted_at`,
    `robots_counted_at`): a wave of workers that fail at once is one
    failure. A circuit opens at most once per cooldown and robots.txt is
    downloaded again at most once per `UNREACHABLE_TTL`, so the limits
    keep the time a host fails in a local crawl; half, as the failures of
    one worker reach the database a little more or less than that apart.
    A host name that does not resolve is given up at its first failure. The queued pages of the host are finished
    in one statement, failed or unreachable with the reason; a page of it
    handed out later (put back, its lease expired, a link found since)
    comes with that outcome, whatever the hold, and is finished without a
    request. robots.txt read counts its failures from zero again: the
    worker that read it says so when it sees a page of the host allowed,
    or with its next failure; a read at the target of a redirect goes
    unseen until then.
  - The pages of a host that other workers have in progress when it is
    given up are not called back: each costs at most one request. A
    worker follows a redirect to a host given up for as long as its own
    breaker and robots.txt let it: the database is not asked at every
    redirect.
  - The pages of the host that are already taken are not called back. A
    worker that took one before the hold reached the database knows of
    the hold from its own rate limiter and puts the page back, holding
    the host again in case its hold is the first to arrive (with no
    reason, so the one the fetcher gave stays). It does so even when the
    hold comes while the page waits for its turn in the rate limiter: a
    request of the crawl waits there `MIN_PENALTY_TO_DEFER` at most for
    a penalty of its host (`max_wait` of `Fetcher.fetch`), then fails
    with `HostHeldBackError`, unsent, and the worker takes a page of
    another host instead of sleeping out the hold. Nothing was sent, so
    the page is uncounted and the put-back is not a wait; a page that was
    answered with a redirect to the held host waits, and that counts. A
    retry waits out its own pause however long: a page put back would
    start its retries anew, and an overloaded host would get more
    requests than `max_retries` allows. Another worker that took a page
    of the host knows nothing of the hold and sends the request: at most
    one request per page in progress, whatever the length of the queue.
  - The waits of a page are counted in the database (`frontier.waits`),
    so `MAX_WAITS_PER_PAGE` holds for the job, not for each worker: a page
    asked to wait comes back three times in all, whichever workers take
    it. `take` hands the count out with the page; an expired lease is
    not a wait. A page asked to wait fails after its last wait; a page
    whose host is held back after it was taken waits in the rate limiter
    after its last one, as in a local crawl.
  - `take` cannot undo a hold: it moves `next_allowed_at` on only for a
    host its condition finds ready, and locks the row of the host in the
    same statement. A row changed after the snapshot of the statement is
    checked against the condition again before it is locked (READ
    COMMITTED), so a hold that commits in between takes the host out of
    the statement. A stress check of 1500 holds made alongside a `take`
    of the same host found none undone.
  - A hold that cannot reach the database is logged: the host stays held
    back in the worker that was answered, and the crawl goes on.
- **One round trip per operation on a page.** Taking a page, admitting
  it, putting it back, finishing it and adding the links found on it are
  each one call of a PL/pgSQL function (`distributed/procedures.py`,
  replaced as the tables are made). Every page needs the row of the job
  three times (admitted, its links added, finished), and a transaction
  made from the client held the row across its 4 to 8 round trips: some
  7 ms of every page, for all the workers of the job together, so about
  130 pages per second however many workers ran. A function holds it
  while the database works alone. Plain statements with CTEs would not
  do: admitting a page ends in one of three ways, and expired leases are
  taken back only when there are some. A connection that goes back to
  the pool is not reset either, which would be one more round trip: the
  frontier leaves nothing in a session.
- **Commits that do not wait for the disk, and few connections.** A
  function runs in a fraction of a millisecond, but its transaction keeps
  the row of the job until COMMIT, and COMMIT waits for the WAL to be
  flushed (slow in Docker on macOS). With four workers some 26 sessions
  waited for that one row at a time, and the more of them wait, the
  longer each hand-over of the lock takes: a lock convoy, slower than
  one worker. So the connections of the frontier set
  `synchronous_commit` off: a crash of the database itself (not of a
  worker) loses up to 3 × `wal_writer_delay` (0.6 s by default) of
  frontier changes and stays consistent; those pages are handed out or
  finished once more, which "at least once" allows already. The storage
  keeps synchronous commits, and its records are written before a page is
  `saved`: a `saved` lost leaves the page `saving` until its lease
  expires. A worker has 4 connections by default
  (`distributed.pool_size`): its other tasks wait for one in the process
  rather than for the row of the job in the database.
- **Locks in one order, and no waits where the order cannot be kept.**
  Every operation on a page locks the row of the page, then the job,
  then the host; adding links locks the job first and only inserts new
  rows. Giving a host up locks the job, the host, then its queued pages:
  a worker that holds a page of the host waits for the job before it
  locks anything else, and the page it holds is not queued in the
  snapshot of the statement, which does not wait for it. `take` reads its page from the snapshot of its statement: another
  worker may have taken the page since and hold its row while it waits
  for the host `take` holds. So `take` locks the row of the page skipping
  too, and looks again a moment later if it was taken. The expired
  leases are taken back skipping the rows other workers hold. The stress
  check that found this deadlock (32 tasks in 4 workers, pages put off,
  given up and uncounted at random) now ends with the counts of the job
  equal to those of its rows, and so does one that gives hosts up meanwhile.
  - The job is locked `FOR NO KEY UPDATE`, not `FOR UPDATE`. A statement
    of its own that inserts a row of the job (a redirect target seen, a
    host held back, a worker joining) checks its foreign key by locking
    the job `FOR KEY SHARE`, after the row is inserted. `FOR UPDATE`
    would make it wait for the worker adding links while it holds the new
    row, and that worker may insert the same row next: each waits for
    the other. Two workers on the demo site deadlocked so in 5 runs of 15.
    `FOR NO KEY UPDATE` still lets one worker at a time change the counts
    of the job; an `UPDATE` of the job takes the same lock.
- **Waiting is polling.** A worker with nothing to take asks the
  database what it waits for: a page put off, a host's turn, a lease of
  another worker that may expire, and sleeps until the nearest of them,
  at most `poll_interval`. One task of the worker polls; its other tasks
  with nothing to take wait behind it in the process and look once it
  has a page, so an idle worker asks as often as one task does, whatever
  `max_concurrent`. Operations of its own frontier wake it at once:
  each sets the event the waiting task saw before it looked and starts
  a new one, so a wakeup meanwhile is not lost. `LISTEN/NOTIFY` could
  wake it on the operations of others; it is worth it only if the
  measurements show the polls cost too much.
- **Times are those of the database**: one clock for all workers, so a
  host interval holds whatever the clocks of the machines say. The
  interval is held as pages are taken: a request starts a little after
  its page is taken, the first of a worker later, as it opens its
  connections, so two requests of different workers may come closer than
  the interval, while the rate of a host holds over any run of them.
- **A worker is a crawl of the frontier.** `run_worker` reads the part of
  the configuration the job keeps, puts the worker's own sections over it
  (the database, the session, the proxies, the storage, the log, the
  reports, `max_concurrent`), opens a `PostgresFrontier` and runs the
  same `CrawlRun` as a local crawl through `crawl_frontier`, which reads
  no sitemaps. The interval of a host is that of the rate limit of the
  job (`host_interval`) or its Crawl-delay, see above. Its
  files have `{worker}` in their names, as two workers may save one page.
  The order at the end matters, and it is the same when the worker is
  stopped: the storage writes its buffer and the pages are `saved`, then
  the frontier is closed (its leases put back, the job finished if
  nothing is left), then the crawler.
- **The stats are a snapshot.** Counting every state change in the job
  row would make that row the one every worker of the job writes on
  every page. Instead the heartbeat (and `refresh_stats()`) counts the
  rows of the job by state; `requested` is also taken from `admit`.
- **The report of a job is read from its tables.** Each worker counts
  its own pages, as a local crawl does; the job has no process that saw
  them all. So the worker passes what `CrawlerStats` records of a page
  (the status, the time of the response, the class of the error) to
  `finish`, which stores it in the row of the page, and `job_stats`
  makes the statistics of the job with a few `GROUP BY` queries in one
  read-only snapshot (REPEATABLE READ), while the workers go on. Equal
  counts are ordered by name in byte order (`COLLATE "C"`), as Python
  orders them, so the statistics of a job equal those of a local crawl
  of the same pages. The workers are rows of their own: each joins with
  its first `take` (the process that seeds a job takes nothing and is
  no worker), renews its lease with its heartbeat and adds the time
  since it was last seen to `active_seconds`. A worker whose lease ran
  out without a stop is `lost`, and the pause of a worker run again
  under its name is not counted. Reading the report costs the workers
  nothing: no counter is written on their path.
- **The speed of a job is read from when its pages finished.** The local
  progress line measures its speed between the snapshots it took over
  the last seconds; `status` is one query by a process that saw nothing
  before, and the average since the first worker started counts the
  pauses of a job stopped and resumed. So `finish` stamps the row of a
  page with `finished_at`, and `job_progress` counts the pages requested
  and finished in the last 30 seconds (of the job, if it has ended). A
  job no worker runs has a speed of 0 and no time left, instead of the
  average of a crawl long gone.

## State of a unit of work

- The state of one crawl (queue, seen URLs, counters, timestamps) lives
  in an object made for that crawl, not in fields of the long-lived
  crawler that are reset at the start. Resetting field by field breaks as
  soon as a new field is added and someone forgets to reset it. A new
  object starts clean.
- What must outlive a crawl stays in shared objects: rate limits, the
  robots.txt cache, the states of the circuit breaker, the HTTP session.
  The run receives them; it does not own them.
- The facade keeps the latest run and its frontier for its properties
  (`visited_urls`, `crawl_stats()` ...). An empty run before the first
  crawl saves a `None` check in every property.
- A guard against two crawls at once on one crawler is a property of the
  run (`running`), not a pair of timestamps checked by the caller.

## Refactoring without changing behaviour

- **The tests are the specification.** Only tests that reach into private
  members may change, each for a stated reason; everything else must pass
  as it is.
- **Move code verbatim**, one layer per commit, bottom up, so that every
  step is small and green. Moved lines then show as moved
  (`git diff --color-moved`), and a script listing the lines of a new
  module that the old one did not have shows exactly what was written
  rather than moved.
- Keep the old names of attributes in the new class (`self.robots`,
  `self.circuit_breaker`), and the method bodies move without edits. Keep
  public constants on the facade as references to their new home.
- Look for every way a member is used, not just calls: tests that patch
  methods, and plain assignments on an instance
  (`crawler.ROBOTS_POLL = 0.01`). After a move, an assignment like that
  silently stops working: the test still passes but no longer tests what
  it says.
- Shared components are passed to the layers once. Reassigning
  `crawler.circuit_breaker` after construction would no longer reach
  requests already wired to the old one, so the settings of the facade
  are read-only properties: a new value fails loudly with
  `AttributeError` instead of doing nothing.
- **Logger names are an interface**: log filters, alerts and tests depend
  on them. A layer moved to a new module logs under a new name. Tests
  should listen to the package logger (`crawler`), not to a module's.
- **Measure speed in interleaved pairs** (after, before, after, ...) on
  the same machine. Runs in a row drift with the load of the machine more
  than a refactoring changes anything.
