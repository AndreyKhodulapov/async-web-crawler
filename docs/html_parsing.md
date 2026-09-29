# HTML parsing: key concepts

Short, interview-ready notes on how the crawler turns pages into data.

## Parsers behind BeautifulSoup

BeautifulSoup is a tree API on top of a pluggable parser:

| Parser | Speed | Broken HTML | Notes |
|--------|-------|-------------|-------|
| `lxml` | fast (C, libxml2) | lenient, repairs most markup | extra dependency; used here by default |
| `html.parser` | slower (pure Python) | lenient, different repairs | stdlib, no dependency; used here as a fallback |
| `html5lib` | slowest | exactly like a browser (HTML5 spec) | use when fidelity matters more than speed |

- Parsers **repair** invalid markup instead of failing: they close open tags,
  add missing `<html>`/`<body>` and move misplaced elements. The same broken
  input can produce slightly different trees with different parsers.
- Raw `lxml.html` with XPath is faster still. BeautifulSoup trades speed for
  a convenient, forgiving API.

## Finding elements

- `find` / `find_all(name, attrs)` search by tag and attributes.
  `recursive=False` looks only at direct children, e.g. the `<li>` of one
  list without the items of nested lists.
- `select(css)` / `select_one(css)` take CSS selectors (`"main .price"`,
  `"a[href^='/catalog']"`). They are concise and familiar from front-end work.
  An invalid selector raises `SelectorSyntaxError`.
- `get_text()` joins all text nodes. For clean text you still skip `<script>`,
  `<style>`, `<noscript>` and `<template>`, collapse whitespace, and add
  spaces only between block elements: `a<b>b</b>` is "ab", not "a b".

## Links: relative → absolute

- `urllib.parse.urljoin(base, href)` implements RFC 3986 resolution:
  `"../a"`, `"./a"`, `"/a"`, `"?q=1"` and protocol-relative `"//host/a"`.
- **Which base?** The `<base href>` tag if the page has one, otherwise the
  **final URL after redirects**, not the requested one. `/docs` redirected
  to `/docs/` changes where `href="intro"` points.
- **Normalization** lets you deduplicate: lowercase the scheme and host, drop
  the default port and the `#fragment` (it is the same page), turn an empty
  path into `/`.
- **Filtering**: only `http(s)` URLs are crawlable. Skip `mailto:`, `tel:`,
  `javascript:`, `data:`, empty and fragment-only hrefs. Reject malformed
  hosts and ports.
- **Internal vs external**: compare hostnames. `www.example.com` and
  `example.com` are different hosts unless you decide otherwise.

## Parsing inside an async program

- Parsing is **CPU-bound**. Run on the event loop, a 50 ms parse freezes every
  other download for 50 ms.
- `asyncio.to_thread(parse, html)` moves it to a worker thread. The loop stays
  responsive, because the GIL is handed between threads every few
  milliseconds. There is no parallel speedup, though: only one thread runs
  Python code at a time.
- For real parallelism on many cores use a `ProcessPoolExecutor`
  (`loop.run_in_executor`). The cost: arguments and results are pickled
  between processes.

## Robust extraction and partial results

- Real pages are messy, so extraction must **degrade, not fail**. Run each
  extractor (text, links, tables...) separately. If one raises, log a warning
  with the traceback, record the error and keep the other fields.
- Watch for inputs that are not HTML at all: check `Content-Type` before
  parsing JSON, images or PDFs, and handle empty bodies.
- Avoid recursion over the tree: broken markup can nest thousands of unclosed
  tags. An explicit stack has no recursion limit.

## What a static parser cannot see

- **JavaScript-rendered pages (SPA)**: the HTML is a shell and the content
  arrives later via JS and API calls. Options: call the site's JSON API
  directly, or render with a headless browser (Playwright).
- **Anti-bot protection**: a challenge page or HTTP 403 instead of content.
  Respect it; do not try to bypass it.
- **Rate limits**: HTTP 429 with `Retry-After` means "slow down". A polite
  crawler honours it, limits requests per host, follows `robots.txt` and sends
  a User-Agent with contact details.
