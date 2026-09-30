# Politeness: rate limits, robots.txt and retries

Short, interview-ready notes on how a crawler keeps from overloading the
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
  (thundering herd) and serves them in random order.
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
  moment and could lose again and again. The
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
  Trade-off: while the shared queue is long, a request may reach its slot
  before its host's Crawl-delay has passed since the host's last request,
  and it waits out the rest holding the slot.

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
  `Disallow: /~joe/` would let `/%7Ejoe/` through.
- **Status codes**: 2xx means parse; 4xx means there are no rules, so
  everything is allowed, and so may a redirect loop (the RFC counts too
  many redirects as unavailable); 5xx or no answer means unreachable, so
  everything is disallowed. 429 is best treated like 5xx: the site asks crawlers to back off.
- An **unreachable** robots.txt is an outage, not a rule: cache it briefly
  (here 60 seconds) and fetch it again, or one timeout closes the site for
  the whole crawl. The TTL helps the URLs that come later: pages refused
  during the outage are not queued again, so a crawl of that one site
  still ends empty. Count such pages apart from the disallowed ones, so the
  report does not blame robots.txt for a network failure.
- Rules apply to one **origin** (scheme, host, port) and are cached per
  origin. The RFC allows caching for up to 24 hours. Parse at least 500 KiB.
- **Single flight**: when many workers reach a new site at once, they must
  share one robots.txt download. Cache the `Task` rather than its result, and
  `await asyncio.shield(task)` so that one cancelled caller does not cancel
  the download for everyone.
- Python's `urllib.robotparser` gained wildcards and longest-match only in
  3.14. Before that it used the first matching rule, so results depend on
  the Python version.
- robots.txt is a convention, not access control: it tells polite crawlers
  what to skip, and it does not protect anything.

## Retries and backoff

- Retry only **transient** failures: timeouts, connection errors, 408, 429,
  500, 502, 503, 504. A 404 or 403 fails the same way again. Retrying is safe
  for idempotent requests such as GET.
- Not every network error is transient: a redirect loop or a certificate
  that fails verification is a property of the site. Retrying a redirect
  loop is costly too: each attempt follows the whole chain (10 requests in
  aiohttp) outside the rate limiter.
- **Exponential backoff**: wait `base * 2**n`, capped. **Jitter** keeps
  clients that failed together from retrying in lockstep. "Full jitter"
  (0..delay) spreads retries best but can retry almost at once; "equal
  jitter" (delay/2 + 0..delay/2) keeps a minimum pause.
- Honor **Retry-After** (seconds or an HTTP date). When it asks for longer
  than the backoff cap, do not retry: coming back early earns another 429.
- A 429 or a timeout usually means the **whole site** is struggling:
  penalize the host in the rate limiter, so that every worker slows down,
  not only the one that failed. A Retry-After is a request to the whole
  crawler: it holds back the host even when the failed URL is not retried. Scrapy's AutoThrottle adapts the delay to
  latency the same way.

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
