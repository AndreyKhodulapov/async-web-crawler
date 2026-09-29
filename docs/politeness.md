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
- **Where to wait**: right before the request, inside the concurrency slot,
  so that the interval holds between requests actually sent. The cost is a
  slot held while waiting. The server sees arrival times, which also carry
  network jitter, e.g. the first request also opens the connection.

## Delays

- Interval per host = max(`1 / requests_per_second`, `min_delay`, Crawl-delay).
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
  Paths are compared percent-encoded, query string included.
- **Status codes**: 2xx means parse; 4xx means there are no rules, so
  everything is allowed; 5xx or no answer means unreachable, so everything
  is disallowed. 429 is best treated like 5xx: the site asks crawlers to back off.
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
- **Exponential backoff**: wait `base * 2**n`, capped. **Jitter** keeps
  clients that failed together from retrying in lockstep. "Full jitter"
  (0..delay) spreads retries best but can retry almost at once; "equal
  jitter" (delay/2 + 0..delay/2) keeps a minimum pause.
- Honor **Retry-After** (seconds or an HTTP date), up to a cap.
- A 429 or a timeout usually means the **whole site** is struggling:
  penalize the host in the rate limiter, so that every worker slows down,
  not only the one that failed. Scrapy's AutoThrottle adapts the delay to
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
  retries and URLs blocked by robots.txt.
- An average gap close to the configured interval means the limiter, not the
  site's speed, sets the pace.
