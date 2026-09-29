# async-web-crawler

An asynchronous web crawler built on `asyncio` and `aiohttp`. It downloads many
pages concurrently over a shared connection pool, limits concurrency, applies
timeouts, and reports failures without stopping the rest of the batch.

## Features

- Concurrent downloads with a configurable concurrency limit (`asyncio.Semaphore`)
- Connection pooling and keep-alive via a single `aiohttp.ClientSession`
- Separate connect, read and total timeouts
- Clear error types: `HTTPStatusError`, `NetworkError` (including redirect
  loops), `FetchTimeoutError`, `InvalidURLError`, `CrawlerClosedError` and
  `UnexpectedError`, all subclasses of `FetchError`
- One failing URL never breaks a batch: even unforeseen exceptions are
  logged with a traceback and reported as `UnexpectedError`
- Logging for every request: start, success (status, size, time) and failure

## Requirements

Python 3.11+

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt        # runtime only
pip install -r requirements-dev.txt    # runtime + test and lint tools
```

## Demo

```bash
python src/main.py
```

The demo fetches ten URLs twice: once sequentially and once concurrently. The
list includes fast pages, slow `httpbin.org/delay/*` endpoints, HTTP 404/500, a
request that exceeds the timeout and a non-existent domain. For each run it
prints the status, size and time of every request, the total time and the
speedup. Logs go to stderr and the report goes to stdout.

```bash
python src/main.py --concurrency 3 --timeout 3        # tune the crawler
python src/main.py https://example.com https://python.org  # custom URLs
python src/main.py --log-level WARNING                # errors only
```

Sample report (logs omitted):

```
=== Sequential ===
URL                                  STATUS                       SIZE    TIME
https://example.com                  200                          559B   0.28s
https://httpbin.org/delay/2          200                          359B   2.23s
https://httpbin.org/status/404       HTTPStatusError 404            0B   0.18s
https://httpbin.org/delay/10         FetchTimeoutError              0B   5.76s
https://nonexistent-domain.invalid   NetworkError                   0B   0.00s
...
Succeeded: 6/10, total time: 11.59s

=== Concurrent (max_concurrent=10) ===
...
Succeeded: 6/10, total time: 6.00s

Speedup: 1.9x
```

A concurrent run takes about as long as its slowest request. In the default
list that is the request that hits the timeout. `SIZE` is the body size after
content decoding (gzip, deflate), so it can exceed the bytes transferred.

## Usage

```python
import asyncio

from crawler import AsyncCrawler


async def main() -> None:
    async with AsyncCrawler(max_concurrent=5, total_timeout=10) as crawler:
        pages = await crawler.fetch_urls([
            "https://example.com",
            "https://httpbin.org/delay/1",
        ])
        print(f"Fetched {len(pages)} pages")


asyncio.run(main())
```

| Method | Returns | On failure |
|--------|---------|------------|
| `fetch_url(url)` | page text | raises a `FetchError` subclass |
| `fetch_result(url)` | `FetchResult` | error stored in `result.error` |
| `fetch_urls(urls)` | `{url: text}` for successful pages | failed URLs are logged and skipped |
| `fetch_many(urls)` | `list[FetchResult]` in input order | error stored per result |
| `close()` | - | safe to call twice; called by `async with` |

Closing the crawler while a batch is running does not break the batch.
Requests already in flight fail with `NetworkError`, and requests still
waiting for a free slot fail with `CrawlerClosedError`. Fetching from an
already closed crawler fails the same way: `fetch_url` raises
`CrawlerClosedError`, the other methods report it per URL.

## Tests

```bash
pytest                      # unit + integration, no internet needed
pytest tests/unit           # edge cases with a fake HTTP session
pytest tests/integration    # real HTTP against a local aiohttp server
pytest -m network           # smoke tests against the real internet
```

```bash
ruff format src tests       # format
ruff check src tests        # lint
```

## Project structure

```
src/
├── main.py                 # demo: sequential vs concurrent fetching
└── crawler/
    ├── client.py           # AsyncCrawler
    ├── models.py           # FetchResult
    ├── exceptions.py       # FetchError hierarchy
    └── logging_config.py   # log format setup
tests/
├── unit/                   # session replaced by fakes: closing, error mapping
└── integration/            # local HTTP server; live tests marked `network`
docs/
└── asyncio_concepts.md     # notes on async concepts used here
```
