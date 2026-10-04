# Politeness: rate limits, robots.txt and backoff

Short notes on how a crawler keeps from overloading the
sites it visits and follows their rules.

## Concurrency is not rate

- A semaphore limits requests **in flight**; a rate limit limits requests
  **started per second**. With `max_per_domain=1` and 50 ms responses, a
  semaphore alone still allows 20 requests per second to one site.
- Crawlers limit the rate **per host**: each site has its own budget, and a
  slow site does not slow down the others. A **global** limit protects the
  crawler's own bandwidth or an API quota instead.

## Rate limiting algorithms

| Algorithm | Idea | Trade-off |
|-----------|------|-----------|
| Fixed window | count requests per calendar second/minute | up to 2x the limit around a window boundary |
| Sliding window log | keep a timestamp per request, count the last N seconds | exact, but memory grows with the rate |
| Sliding window counter | weight the previous window's count by overlap | cheap approximation; common for API quotas in Redis |
| Token bucket | tokens refill at `rate` up to `burst`; a request spends one | allows bursts; the classic API limiter |
| Leaky bucket | requests leave a queue at a constant rate | smooth output, no bursts |
| GCRA | keep only the "theoretical arrival time" (TAT) of the next request | equivalent to a token bucket, one number of state |

## GCRA in asyncio (`RateLimiter`)

- State per host: `next_start`. To book a request:
  `start = max(now, next_start)`, `next_start = start + interval`, then sleep
  until `start`.
- **No lock needed**: there is no `await` between reading and writing
  `next_start`, and asyncio switches tasks only at an `await`.
- **FIFO fairness**: every caller gets its own start time at once. A polling
  loop (`while no_tokens: await sleep(x)`) wakes all waiters together
  (thundering herd) and serves them in random order. The order is strict
  only while starts come on time: after one that is late by more than an
  interval (a busy event loop, a late slot), the requests due in the
  meantime may be overtaken. The rate holds either way.
- **Bursts**: a tolerance `tau` turns GCRA into a token bucket:
  `start = max(now, next_start - tau)`. Here `tau = 0`: a minimum delay
  between requests rules out bursts by definition.
- A task cancelled while sleeping keeps its booking. The schedule only errs
  on the slow side, which is the safe side for politeness.
- **Distributed crawlers** use the same algorithm with `next_start` in Redis,
  updated atomically by a Lua script (or the redis-cell module).
- **Where to wait**: before taking the concurrency slot, so a request
  waiting for its host does not hold a slot another host could use. The
  slot can come later than the booked time, so inside it the interval is
  checked once more against the request that actually started last: the
  interval holds between requests sent, not only between bookings. Nothing
  sleeps inside the slot: a request that finds the interval not yet passed
  lets the slot go and books again. Sleeping there would hold a slot, and a
  request waking inside it would race the ones waking outside for the same
  moment and could lose again and again. Trade-off: a request sent back
  books a new time, so later ones may overtake it; the rate still holds,
  only the start order is not strict. The
  server sees arrival times, which also carry network jitter, e.g. the first
  request also opens the connection.
- **Penalties** (HTTP 429, timeouts) move `next_start`, but requests that
  booked earlier already hold their times. Each one checks after its sleep:
  if a penalty came after its booking, it books again, behind the penalty.
  Trade-off: it goes to the end of the host's queue. With hundreds of URLs
  of one host booked at once (`fetch_many`), those requests move far back;
  in `crawl()` the queue is no longer than the number of workers.
  Under a global limit, a request penalized while it waits for its host
  does not book the shared schedule at all; one that has already booked
  it loses that turn, as GCRA never gives a booked time back.
- **Global limit with per-host rules**: a host's own Crawl-delay or penalty
  must not stop other hosts. The host's schedule is waited for first, then
  the shared one is booked: booking both at once would park a far-future
  time in the shared schedule, and every host would wait for it.
  Trade-off: while the shared queue is long, a request's turn may come
  before its host's Crawl-delay has passed since the host's last request.
  It waits out the rest outside the slot; if it then has to book again,
  the shared turn it held is lost.

## Delays

- Interval per host = max(`1 / requests_per_second`, `min_delay`, Crawl-delay).
  robots.txt is per origin, the limit per host: when one host serves
  several origins (ports), the longest Crawl-delay wins.
- **Jitter** adds a random 0..jitter seconds to each interval, so the load
  does not look like a metronome. Adding it only on top keeps `min_delay` a
  guarantee; Scrapy instead multiplies the delay by 0.5-1.5, which keeps the
  average.
- **Crawl-delay** is not in RFC 9309. Bing and Yandex honor it, Google
  ignores it. Cap it: `Crawl-delay: 86400` would freeze a crawl.

## robots.txt (RFC 9309)

- Groups start with one or more `User-agent` lines. The crawler matches its
  **product token** ("MyBot/1.0 (+url)" is "mybot") case-insensitively.
  Groups for the same agent are merged. With no group of its own, the
  crawler follows `*`.
- The **longest matching rule wins**, and `Allow` wins a tie; the order of
  lines does not matter. `*` matches any characters, `$` anchors the end.
  Paths are compared percent-encoded, query string included; an escaped
  unreserved character is the character itself (`%7E` is `~`), or
  `Disallow: /~joe/` would let `/%7Ejoe/` through. Resolve dot segments
  too: the HTTP client sends `/x/../private` as `/private`.
- **Wildcards without backtracking**: turning each `*` into `.*` makes a
  regex that tries every way to split the URL between the wildcards, so a
  few of them in a site's robots.txt can freeze the event loop for
  minutes. Matching each part at its leftmost occurrence and never
  backtracking gives the same answer in linear time (here atomic groups
  `(?>.*?part)`; Google's matcher uses dynamic programming).
- **Status codes**: 2xx means parse; 4xx means there are no rules, so
  everything is allowed, and so may a redirect loop (the RFC counts too
  many redirects as unavailable); 5xx or no answer means unreachable, so
  everything is disallowed. 429 is best treated like 5xx: the site asks crawlers to back off.
- An **unreachable** robots.txt is an outage, not a rule: cache it briefly
  (here 60 seconds) and fetch it again, or one timeout closes the site for
  the whole crawl. The TTL alone helps only the URLs that come later; the
  pages refused during the outage must wait for it too, or a crawl of that
  one site ends empty after a 503 of a few seconds. The same goes for every
  way into the site: a start URL that redirects there and a sitemap of it,
  or the fix covers the direct links only. Budget the waiting per site
  (here three repeat downloads), not per page or per sitemap, or a dead
  site is waited for once by its sitemaps and again by its pages; and do
  not wait at all for a failure that does not pass by itself: a host name
  that does not exist is a typo, and three minutes change nothing about
  it (but a resolver that answers "try again" is an outage). Mind the
  cost of each try, too: a host that accepts the connection and never
  answers fails only by timeout, and a download with retries
  and growing timeouts takes minutes, so download again with a single
  attempt, and let no page wait for a download for long (here two
  seconds; the download goes on by itself and the page comes back
  later), or every worker that takes a page of that site stands still
  with it. Count such pages apart from the
  disallowed ones, so the report does not blame robots.txt for a network
  failure.
- Rules apply to one **origin** (scheme, host, port) and are cached per
  origin. The RFC allows caching for up to 24 hours. Parse at least 500 KiB,
  and stop downloading there: a huge or endless file must not fill the memory.
- **Single flight**: when many workers reach a new site at once, they must
  share one robots.txt download. Cache the `Task` rather than its result, and
  `await asyncio.shield(task)` so that one cancelled caller does not cancel
  the download for everyone.
- Python's `urllib.robotparser` gained wildcards and longest-match only in
  3.14. Before that it used the first matching rule, so results depend on
  the Python version.
- robots.txt is a convention, not access control: it tells polite crawlers
  what to skip, and it does not protect anything.

## Robots directives of pages and links

- robots.txt speaks for a whole site before a request; a page can speak
  for itself after it: `<meta name="robots" content="noindex, nofollow">`
  in its HTML, or an `X-Robots-Tag` response header, which works for any
  file type. Either may speak to one crawler: `<meta name="mybot"
  content="noindex">`, `X-Robots-Tag: mybot: noindex`. The name is the one
  robots.txt knows the crawler by; directives for all crawlers and for
  this one add up.
- `nofollow` on a page: do not follow its links. `rel="nofollow"` on a
  link: do not follow that one. `noindex`: do not keep the page; its links
  may still be followed (`noindex, follow` is common on listing pages).
  `none` is `noindex, nofollow`.
- Here they are honored together with robots.txt (`respect_robots`): a
  `noindex` page is listed as skipped (`noindex in X-Robots-Tag`) and not
  saved, `nofollow` links are dropped by the parser. Not part of RFC 9309,
  but search engines follow them, and site owners expect a polite crawler
  to do the same.

## Backing off a struggling site

- Which errors to retry and how long to wait between retries is in
  [error_handling.md](error_handling.md); this section is about the load on
  the site.
- A 429 or a timeout usually means the **whole site** is struggling:
  penalize the host in the rate limiter, so that every worker slows down,
  not only the one that failed. A Retry-After is a request to the whole
  crawler: it holds back the host for as long as it asks, even when the
  failed URL is not retried and the wait is longer than any retry pause.
  Coming back after 30 seconds when asked for 2 minutes is what gets a bot
  blocked. Cap it all the same (here 10 minutes by default,
  `max_retry_after`): a misconfigured server must not stop the crawl for a
  day. And say so in the log: a crawl that makes no requests for minutes
  looks stuck to the user.
- A 500 or 502 on one URL, or a reset connection, more often means **one
  page** or one backend is broken. Let only that request wait for its
  retry: holding the host for every retry lets a few broken pages stall
  the whole site for minutes (two pages answering 503 among 40 took a
  crawl from 0.2 s to 10 s). A host that fails everywhere is caught by the
  circuit breaker instead.
- While a host is held back for long, put its pages aside rather than let
  workers wait for it: otherwise one host blocks the crawl of all the
  others (head-of-line blocking).
  Scrapy's AutoThrottle adapts the delay to latency the same way.

## User-Agent

- Identify the bot with a name and a contact URL: sites can then limit or
  contact you instead of blocking you.
- robots.txt is keyed by the bot name. Rotating several strings of the
  **same** bot (desktop and mobile variants) is fine. Rotating browser
  User-Agents is evasion: robots.txt no longer knows who you are, and
  anti-bot systems look at TLS and header fingerprints anyway.

## Measuring politeness

- Current requests per second over a sliding window (the last 5 seconds),
  average gap between requests to one host, average wait in the limiter,
  retries, URLs blocked by robots.txt and URLs left unfetched because
  robots.txt was unreachable.
- An average gap close to the configured interval means the limiter, not the
  site's speed, sets the pace.
