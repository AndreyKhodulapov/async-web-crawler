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
  - **Crawl** (`CrawlRun`): queue, filters, limits, deferred pages, counters.
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
  - a crawl shared by several machines replaces the queue and the set of seen URLs of the crawl layer.

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

## State of a unit of work

- The state of one crawl (queue, seen URLs, counters, timestamps) lives
  in an object made for that crawl, not in fields of the long-lived
  crawler that are reset at the start. Resetting field by field breaks as
  soon as a new field is added and someone forgets to reset it. A new
  object starts clean.
- What must outlive a crawl stays in shared objects: rate limits, the
  robots.txt cache, the states of the circuit breaker, the HTTP session.
  The run receives them; it does not own them.
- The facade keeps the latest run for its properties (`visited_urls`,
  `crawl_stats()` ...). An empty run before the first crawl saves a
  `None` check in every property.
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
