# Distributed crawling: a shared frontier, leases and workers

Short notes on how one crawl is shared by many processes, what it costs
and how such crawlers run in production. How the crawl jobs of this
crawler do it, statement by statement, is in
[architecture.md](architecture.md#the-frontier-in-a-database); how fast
they are, in [performance.md](performance.md#crawl-jobs-of-several-workers).

## Why more than one process

- **One process is bound by one core.** Parsing, the HTTP client and the
  event loop share it: about 250 small pages or 10 pages of a real size
  per second, however many requests are in flight. Several processes, or
  machines, use several cores.
- **A crawl in memory dies with its process.** A queue kept elsewhere
  survives a crash, a deploy or a stop, and the crawl goes on.
- **Hosts can be crawled side by side.** With one queue in the order of
  depth, a rate limit per host makes the pages of the other hosts wait;
  a frontier that hands out the pages of hosts that are ready does not.

## The frontier is the shared state

- A crawler's state is its **frontier**: the URLs seen, those to crawl,
  and when each host may be asked next. Share the frontier and any
  process can be a worker; the workers themselves keep nothing that
  matters.
- **Deduplication is a unique key**: `INSERT ... ON CONFLICT DO NOTHING`
  on `(job, url)`. Two workers that find the same link at once insert it
  once, without a lock or a check first.
- **Handing out without a queue of waiters**: `SELECT ... FOR UPDATE SKIP
  LOCKED` lets every worker take a different row at once; a row another
  worker holds is skipped, not waited for. It is how PostgreSQL serves as
  a job queue.
- **Limits live with the frontier.** `max_pages`, the page limit per
  host, the scope of the crawl and the rate of a host are checked in the
  database, against counts all workers share; a limit counted in each
  worker would be multiplied by their number.

## Leases and "at least once"

- A worker can die at any moment: killed, out of memory, its machine
  gone. A page handed to it **for good** would be lost with it. So a page
  is **leased**: it is the worker's until a time, and a **heartbeat**
  renews the lease while the page is in work. A lease that expires puts
  the page back in the queue for another worker.
- So a page is crawled **at least once**, not exactly once: the worker
  may have fetched and stored the page before it died. Exactly once
  would need the fetch, the store and the "done" in one transaction, and
  a fetch is a request to someone else's server: it cannot be rolled
  back. The usual answer is **idempotent effects**: storing a page twice
  leaves one record (a row per URL, an upsert), and a page crawled twice
  costs one more request.
- **Saved, then done.** A page is marked done only after its record is
  written; until then its lease is renewed. A worker killed with a full
  buffer loses only work: its pages are crawled again. Marking first and
  writing later would lose the pages.
- **How long a lease is** trades speed of recovery for false alarms: a
  short one brings the pages of a dead worker back sooner, but a worker
  paused longer than the lease (a long garbage collection, a stalled
  disk) loses its pages to another one and they are crawled twice. The
  heartbeat must come several times within a lease (here 20 s and 60 s),
  and does nothing else: the counts of the job, a scan of all its pages,
  are refreshed by a task of their own every minute, so a slow scan of a
  large job never makes a lease late.
- **A page that kills its workers** (a parser crash, a page too big for
  memory) would come back forever. Expiries are counted, and a page fails
  after a few (`max_attempts`).

## Politeness with many workers

- **The rate of a host is the job's, not each worker's.** Four workers
  each keeping one request a second to a site send four. Here a host has
  one turn for all: taking a page moves the host's next allowed time on
  in the same statement, and no other worker takes a page of that host
  until then. A worker never waits for a host: it takes a page of
  another one.
- **A host that asks to wait is held back for everybody.** Retry-After,
  a 429, a timeout, an open circuit breaker or an unreachable robots.txt
  move the host's next allowed time in the database; the pages of the
  host stay in the queue instead of being taken and put back. A host
  that keeps failing is given up for the job, by counts all workers add
  to. Failures several workers meet at once count as one: a host down for
  a few seconds is not given up because many workers saw it.
- **What is cheap to repeat stays in each worker**: robots.txt is
  downloaded and cached by every worker once per host, each worker
  has its own circuit breaker, and each slows a host down after the 429
  it gets. A host gets a few more requests than from
  one process; sharing them would cost a database round trip at every
  request. The proxies are each worker's too: a page none of them was
  left for goes back to the queue for any worker, without holding its
  host back.

## Failures

- **A worker killed** (`kill -9`, OOM): its leases expire, its pages come
  back. Nothing else needs to notice.
- **A worker stopped** (SIGTERM, Ctrl-C): it writes what its storage
  buffers, puts the pages it had in flight back at once and exits with
  143 or 130. Orchestrators send SIGTERM first and kill after a grace
  period (`docker stop`: 10 s by default, 30 s in this compose file).
- **The database fails**: the worker stops with exit code 1 rather than
  trying to ride out the outage (crash-only design). Its leases bring
  its pages back, and starting it again is the job of whatever runs it: a
  restart policy, a Kubernetes Job.
- **The job is restarted under its workers** (`job create --restart`):
  each worker of the old job stops the same way, exit code 1, once the
  database tells it the job is gone. It writes what its storage buffers
  first, and touches nothing of the new job; started again, it joins it.
- **The database crashes itself**: the frontier commits without waiting
  for the disk, so up to 0.6 s of its changes may be lost; those pages
  are handed out or finished once more, which "at least once" allows.
  The storage commits as usual.

## Why PostgreSQL, and the alternatives

| Frontier in | Good at | Costs |
|-------------|---------|-------|
| PostgreSQL | transactions over several rows (a page, its job and its host at once), `SKIP LOCKED`, a unique key for deduplication, SQL for reports; often there already | a row every page updates is a lock every worker waits for: a few hundred pages a second per job |
| Redis (scrapy-redis) | a sorted set as the queue and a set of seen URLs, tens of thousands of operations a second; the standard for Scrapy | memory for every URL seen; no transaction over a page, its host and its job; persistence and the "done" of a page are up to you |
| A message broker (Kafka, RabbitMQ, SQS) | throughput, redelivery of unacknowledged messages, many consumers | no deduplication, no "when may this host be asked"; needs a store beside it |
| Files or a key-value store on a cluster (Hadoop, HBase) | billions of URLs, as in Apache Nutch or the crawler of a search engine | a cluster to run; crawls in batches or with a custom scheduler |

A crawl of thousands to millions of pages fits in PostgreSQL. The choice
changes when the frontier no longer fits one machine, or when the
operations a second on one job exceed what one row allows (below).

## The ceiling of one job

- Admitting, finishing and adding the links of a page update the counts
  of the job (`max_pages` and the pages unfinished), one row of
  `crawl_jobs`, one transaction at a time. That row caps a job at about
  240 pages per second on the machine measured: two workers are 1.6 to
  1.7 times as fast as one, four no faster than two.
- Real sites seldom reach it: with a rate limit of one request a second
  per host, a crawl needs hundreds of ready hosts to send 240 requests a
  second, and a page of a real size is parsed at about 10 per second per
  process.
- A very wide crawl meets another limit first: to hand out a page, the
  frontier looks at every host of the job whose turn has come, so a take
  costs 6 ms at 1 000 hosts and 137 ms at 20 000 (see
  [performance.md](performance.md#many-hosts)). A crawl of a few sites
  keeps to the hosts of its start URLs.
- Large crawlers lift it by not touching a global count on every page:
  workers admit and finish pages in batches, take their share of the
  page limit in chunks and give back the rest, and keep counts split
  over several rows. The price is counts that lag and limits kept with
  a margin. This crawler keeps exact counts instead.

## In production

- **A crawl is a batch job**, not a service: it starts, works through its
  frontier and ends. In Kubernetes, a `Job` with `parallelism: N` runs N
  workers and is complete when they all exit with 0; a `CronJob` creates
  the crawl and its workers on a schedule (`job create --restart` for a
  fresh crawl, `--resume` to go on). In compose, `docker compose up
  --scale worker=N` (see the [README](../README.md#docker)).
- **Workers are cattle**: identical, with no state of their own, started
  and stopped at will. Their names only tell their pages and files apart.
- **Scale by the length of the queue**: an autoscaler (KEDA, an HPA on an
  external metric) adds workers while pages wait and removes them as the
  queue drains. Scale down by SIGTERM, which puts the pages back.
- **Logs to stdout, one JSON object a line** (`logging.console_format:
  json`); the platform collects them (Loki, Elasticsearch, CloudWatch).
  A file in the container dies with it.
- **Metrics**: pages per second, pages queued and in progress, leases
  expired, hosts held back and given up, storage failures. Here `status`
  and `report` read the first of them from the database, and only read:
  a role that may not write is enough for them, and they never change
  the tables or functions the workers run on. A production setup exports
  them to Prometheus and alerts on a queue that stops shrinking or
  leases that keep expiring.
- **Secrets in the environment**, not in files baked into the image:
  `CRAWLER_DATABASE_URL` here.
- **One image, many roles**: the same image creates the job, runs the
  workers and builds the report, with different commands.
