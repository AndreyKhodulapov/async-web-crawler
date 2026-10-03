# Error handling: retries, timeouts and circuit breakers

Short notes on how a crawler tells failures that pass from
failures that stay, retries the first kind without making things worse, and
leaves a failing site alone.

## Classify by whether a retry can help

| Kind | Examples | Retry? |
|------|----------|--------|
| Transient | timeouts, HTTP 408, 429, 500, 502, 503, 504, Cloudflare's 520-524 | yes, with backoff |
| Network | DNS failure, connection refused or reset | yes |
| Permanent | HTTP 401, 403, 404, 410, 501, a redirect loop, a bad certificate, an invalid URL, a page over the size limit | no |
| Parse | the body is not an HTML document | no: the same bytes come back |

- Classify by **what a retry would do**, not by where the error comes from.
  A certificate that fails verification is raised by the network layer, but
  every attempt fails the same way. Retrying a redirect loop is costly too:
  each attempt follows the whole chain (up to 10 requests).
- Translate transport exceptions (aiohttp, asyncio) into one hierarchy of
  your own, in one place. Callers then depend on `TransientError` and
  `PermanentError`, not on the HTTP library, and the retry decision is an
  `isinstance` check, not lists of status codes spread over the code.
- **Grey zones**:
  - 500 often comes from a bug, not from load: retry it once, not three times.
  - 429 means "too fast": retry, but with longer pauses (here 4x).
  - 501 and 505 are server errors that never pass.
  - A DNS failure is permanent for a mistyped domain, but a resolver timeout
    looks the same to the client; a retry costs little.
- Some failures are not about the request at all: the crawler is closed, the
  host's circuit is open, robots.txt is unreachable. Retrying the request
  cannot fix them.
- An unforeseen exception (a bug) must not break the batch: catch it at the
  boundary of one URL, log the traceback and report it as that URL's error.

## When a retry is safe

- **Idempotency**: GET, HEAD, PUT and DELETE can be repeated; POST cannot,
  unless the API takes an idempotency key (e.g. Stripe's `Idempotency-Key`).
  A request that timed out may still have been processed: the client cannot
  tell. A crawler sends only GETs.
- **Retry at one layer**. If the HTTP client, the crawler and the job
  scheduler each make 3 retries, one failing page costs 4 x 4 x 4 = 64
  requests. Here only `RetryStrategy` retries; aiohttp does not.
- **Cap the retries**: in total per request (`max_retries`) and per kind of
  error (`RetryRule`). Large systems add a **retry budget**: retries may be at
  most a share of all requests (Finagle, Envoy), so when a whole service
  fails, the load on it does not multiply.

## Backoff and jitter

- **Exponential backoff**: retry n waits `base * factor**n` seconds, capped
  by `max_delay`. A fixed delay keeps hammering a struggling server; a growing
  one gives it more time with every round.
- **Jitter**: clients that failed at the same moment (a server restart) retry
  in lockstep and fail together again. A random part spreads them out.

| Jitter | Delay | Trade-off |
|--------|-------|-----------|
| none | `d` | retries stay synchronized |
| full | `random(0, d)` | the best spread, but a retry may come almost at once |
| equal | `d/2 + random(0, d/2)` | a good spread and a minimum pause; used here |
| decorrelated | `min(cap, random(base, previous * 3))` | grows from the previous delay rather than the attempt number |

- **Retry-After** (seconds or an HTTP date, with 429 or 503) tells when to
  come back: wait `max(backoff, Retry-After)`. When it asks for longer than
  the cap, do not retry: coming back early earns another refusal. The rest of
  the crawler honors it too, see [politeness.md](politeness.md#backing-off-a-struggling-site).
- **Make the wait injectable**. `RetryStrategy(wait=...)` sleeps by default;
  the crawler instead holds back the whole host in the rate limiter, and
  tests pass a wait that only records the delays.

## Timeouts

| Timeout | Catches | aiohttp |
|---------|---------|---------|
| connect | DNS, TCP and TLS, a wait for a pooled connection | `connect` |
| read | a server that stops sending in the middle of a response | `sock_read`: the longest pause between two chunks |
| total | the whole request; a body that trickles in without long pauses | `total` |

- Each one covers a hole in the others: without a read timeout a dead
  connection is found only by the total one, without a total timeout a
  server that sends a byte every few seconds holds a worker forever.
- **Grow the timeouts with retries** (here x1.5 per retry, at most 4x): a page
  that is only slow gets through, while the first attempt stays short and a
  dead server is not waited for forever.
- Since Python 3.11 `asyncio.TimeoutError` is the built-in `TimeoutError`, and
  `asyncio.timeout()` limits any block of code. aiohttp's `ServerTimeoutError`
  is both a `ClientError` and a `TimeoutError`, so the order of `except`
  clauses decides how it is reported.
- A timeout cancels the code under it: that code must clean up in `finally`
  or `async with`.
- Say which timeout fired: a read timeout points to a slow server, a connect
  timeout to the network or a host that is down.

## Circuit breaker

- A retry helps when one request fails. When a **whole host** is down, the
  breaker stops sending requests to it for a while, instead of letting every
  page fail slowly through all of its retries.
- **States**:
  - closed: requests go through, their outcomes are counted over a sliding
    window. Once there are at least `min_requests` and the share of failures
    reaches the threshold, the circuit opens.
  - open: requests fail at once with `CircuitOpenError`, without being sent,
    for `cooldown` seconds.
  - half-open: one probe request goes through, the rest are still refused.
    Its success closes the circuit with an empty window, its failure opens it
    for another cooldown. A recovering host does not get the whole backlog at once.
- **A failure rate with a minimum volume**, rather than N failures in a row:
  one failed request out of one is not a broken site. Hystrix used a rolling
  percentage; resilience4j offers count-based and time-based windows.
- **What counts as a failure**: only what says the host is in trouble, i.e.
  timeouts, network errors, 429 and any 5xx, even a 501 that is not
  retried. A 404 is a healthy server answering; counting it would block a
  site for its broken links.
- **Per host**: one dead site must not stop the crawl of the others, and a
  host that is down fails all of its pages, so a circuit per URL learns too late.
- **Retries under a breaker**: every attempt counts, retries included, so a
  dead host opens its circuit after a few pages. A retry the breaker would
  refuse is not made, and the page fails with the error of its last attempt,
  not with `CircuitOpenError`.
- Check the circuit before a request waits for the rate limit (no point in
  queueing for a blocked host) and once more when its turn comes (the circuit
  may have opened meanwhile).
- **In a crawl, defer rather than fail**. A refused page has not been tried:
  failing it turns a host that was down for half a minute into a crawl that
  lost all of its pages in milliseconds. Put it back in the queue until the
  probe may go, and let the workers fetch other hosts meanwhile. Cap it: after
  a few openings (3 here) give up on the host, or a dead site holds the crawl forever.
- Use `time.monotonic` and make the clock injectable: the wall clock can jump,
  and tests move a fake clock instead of sleeping through cooldowns.

## Retry storms

- A service slows down, clients time out and retry, the load grows by the
  number of retries, and the service slows down further. Retries turn a short
  incident into a lasting outage (a metastable failure).
- No single defense is enough; they work together:
  - backoff and jitter spread retries out in time;
  - retries at one layer, capped per request or by a budget;
  - a timeout or a 429 holds back the whole host, not only the failed request;
  - Retry-After is honored;
  - a circuit breaker stops the traffic while the host is down, and a single
    probe tests its recovery.
- The server-side counterpart is load shedding: a fast 503 with Retry-After
  instead of a slow timeout.

## Observability

- Log every failed attempt with the error type, the URL, the attempt number
  (`2/4`) and the pause before the next one, and the final outcome with the
  reason it is not retried: "permanent error", "no retries left",
  "Retry-After of 120s is longer than max_delay of 30s". A success that took
  retries is worth an INFO line.
- Keep statistics: failed attempts by kind and by class, retries made, pages
  they recovered (do retries pay off?), average time per retry, and the pages
  with permanent errors: broken links to fix, not to retry.
- Count the requests the breaker refused apart from the errors: they were
  never sent.

## Testing error handling

- A deterministic unreliable server: a page that fails N times with 503 and
  then answers, fixed statuses, a slow page, a closed port for refused
  connections, a `.invalid` domain (reserved by RFC 2606, it never resolves).
- Inject time: a fake wait for the retry strategy, a fake clock for the
  breaker. Backoff and cooldowns are then tested in microseconds, and delays
  are checked exactly with the jitter fixed.
- Test what must not happen too: a 404 is requested exactly once, a
  Retry-After beyond the cap is not retried, a refused request is not counted
  as an error.
