# Sessions, proxies and rendering: cookies, proxies and a headless browser

Short notes on how a crawler keeps a session with a site, sends its
requests through other addresses and sees the pages JavaScript builds.

## Cookies and sessions

- HTTP has no state; a **session** is a cookie the site sets
  (`Set-Cookie`) and the client sends back (`Cookie`). A crawler that
  keeps cookies behaves like one browser; one that keeps none
  (`DummyCookieJar`) is a new visitor on every request.
- **A cookie belongs to a domain.** Without `Domain` it is host-only:
  that host and no other. With `Domain=example.com` it goes to the host
  and its subdomains. `Path` narrows it to a part of the site, `Secure`
  to https, `HttpOnly` hides it from JavaScript, `SameSite` keeps it out
  of requests other sites start. A jar that ignores the domain sends a
  session to every host the crawl reaches.
- **Lifetime**: `Expires` is a date, `Max-Age` seconds from the moment
  the cookie arrived; neither makes a session cookie, which lasts until
  the browser closes (for a crawler, the crawl). To save a cookie for
  later, turn `Max-Age` into a date when it arrives: afterwards nobody
  knows when that was.
- aiohttp's jar **keeps no cookies of IP addresses** unless made
  `unsafe`; a test server is reached as `localhost`, not `127.0.0.1`.
- **Shared by everything**: pages, robots.txt and sitemaps go with the
  same cookies, as in a browser.

## Logging in

- Filling in a login form is a project of its own: CSRF tokens, captchas,
  two factors, JavaScript. The simple way that works: **log in in a
  browser, export its cookies** to a Netscape `cookies.txt` (browser
  extensions and `curl -c` write it) and give the file to the crawler.
  Save the cookies after the crawl: the site may have renewed the session.
- Do not follow the logout link: the crawl would end its own session.
- A session tied to the browser (its User-Agent, its address) may end
  when the crawler rotates User-Agents or proxies.
- An API token goes in a header (`Authorization`). Extra headers go to
  every request, so with links to other sites the token leaks: keep the
  crawl on its own domain.

## Cookies are secrets

- A session cookie **is** the login: whoever has it is the user. So its
  value never goes to the log, the console, the reports, the error
  messages or `repr()` of the configuration.
- **Never `pickle`** for a file of cookies (`aiohttp.CookieJar.save()`
  uses it): loading a pickle runs the code inside it, so a cookie file
  someone slipped in runs their code. `cookies.txt` is text, read by
  `http.cookiejar`.
- The saved file is created with mode `0600` (only its owner reads it),
  written to a temporary file and moved over the old one with
  `os.replace`: the old file would keep its old mode, and a crash midway
  would leave half a file.
- An error about a malformed file must not quote the line: the line is a
  cookie.

## Proxies

- **Why**: a network that only lets traffic out through a proxy, a
  country or an address the site serves, spreading the load of a large
  crawl over addresses.
- **Two ways through an http proxy**:
  - an `http://` URL: the request goes to the proxy whole, with the
    absolute URL (`GET http://site/page`); the proxy reads it all,
    cookies and headers included;
  - an `https://` URL: `CONNECT site:443` opens a tunnel, and TLS goes
    through it end to end; the proxy sees the host name only.
- **The password** of the proxy goes in `Proxy-Authorization`: to the
  proxy only, never to the site. It also hides in places one forgets:
  the proxy URL in exception messages of the HTTP client, the
  configuration in a report. Show a proxy as `http://user:***@host:port`.
- **Rotation**:
  - `per_host`: a site always goes through one proxy, chosen by a hash of
    the host. Its session stays on one address; a session that jumps
    between addresses looks stolen.
  - `per_request`: the proxies take turns; the load on each is even, but
    a site sees all of them.
- **A proxy has its own health**, like a circuit breaker per proxy: after
  N failures in a row it is out for a cooldown, and the request is
  retried through another one at once. All out: fail the pages at once
  and end the crawl instead of waiting.
- **Blame the right party.** A dead proxy must not open the circuits of
  the sites behind it, so a proxy error is not the site's. Only what is
  surely the proxy's counts against it: cannot connect, its name does not
  resolve, HTTP 407. A timeout, an error page of the proxy, a refused
  CONNECT may be the site's: they count against the site.
- **Politeness goes by the site, not by the proxy**: rate limits,
  robots.txt and the circuit breaker are per host of the URL, whatever
  address the request leaves from. A proxy pool is not a way around a
  site's limits.
- `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`: read them yourself rather than
  with aiohttp's `trust_env`, which also reads `~/.netrc` (and sends its
  passwords to sites) and, on macOS, the proxies of the system settings.

## Headless browser

- **When it is needed**: a page whose HTML is an empty shell that
  JavaScript fills (a single-page application, `document.write`). Look
  first: many sites render on the server, and their HTML already has
  everything. Sometimes the data comes from a JSON API the page calls,
  which is cheaper to request directly.
- **Playwright or Selenium**: Selenium speaks WebDriver to a browser
  through a driver, a request at a time, and is blocking in Python.
  Playwright drives the browser through a driver process of its own
  (the DevTools protocol for Chromium), has an asyncio
  API, cheap isolated **contexts** (cookies and cache of their own, like
  an incognito window), routing of every request (`page.route`) and
  waits for events and selectors; it installs its own browsers.
- **Download the document, let the browser run it.** The crawler
  downloads the page as always (proxies, cookies, size limit, errors,
  retries) and hands the HTML to the browser (`route.fulfill`); the
  browser loads the scripts and data itself. The browser never decides
  what to request: the crawler does.
- **A navigation is a redirect.** JavaScript setting `location` or a
  `<meta refresh>` would take the browser to a page robots.txt and the
  filters never saw. Stop it and give the target back to the crawler as
  a redirect: it is checked and requested like one. The lower layer
  reports, the upper layer decides: calls go down only.
- **Two cookie stores**: the crawler's jar and the browser context. Copy
  only the changes, both ways, around every page; when both changed a
  cookie, the browser wins. `SameSite` is lost on the way (`cookies.txt`
  does not keep it).
- **A context per proxy**: the proxy of a browser is set on the context,
  not on a request, so every proxy gets one; a page renders in the
  context of the proxy its document came through.
- **Waiting**: `load` (the page and its resources), `domcontentloaded`
  (the HTML parsed), `networkidle` (no requests for 500 ms; never comes on
  a page that polls), or best, a CSS selector the data makes.
- **The cost**: seconds and 50 to 100 MB a tab instead of milliseconds;
  limit the open tabs, do not load images, fonts and media, render only
  the pages that need it. The site serves the scripts and data too, and
  no browser asks robots.txt for them.
- **The pitfalls**:
  - a browser that crashes: restart it, but not forever;
  - Chromium's Local Network Access: a page the browser got from the
    crawler looks public, so its requests to another origin on a
    loopback or private address are refused;
  - two clients: the document comes from aiohttp, the scripts from
    Chromium, and a site that compares TLS fingerprints sees both;
  - what happens after the wait (a later fetch, `history.pushState`) is
    not seen.
