# Asynchronous Python: key concepts

Short notes on the ideas this crawler is built on.

## Concurrency vs parallelism

- **Concurrency**: several tasks make progress in overlapping time periods.
  One worker can switch between them. It is about *structure*.
- **Parallelism**: several tasks run at the *same instant* on different CPU
  cores. It is about *execution*.
- asyncio gives concurrency on a single thread. For a crawler this is enough:
  most of the time is spent waiting for the network, not computing.

## I/O-bound vs CPU-bound, and the GIL

- **I/O-bound** work (HTTP, disk, DB) waits on external systems. A single
  thread can serve thousands of such waits → use **asyncio**, or threads.
- **CPU-bound** work (parsing huge documents, ML inference) needs real cores →
  use **multiprocessing** / `ProcessPoolExecutor`.
- The **GIL** lets only one thread execute Python bytecode at a time, so
  threads do not speed up CPU-bound code in CPython. They still help with I/O,
  because the GIL is released while waiting. Python 3.13+ has an optional
  free-threaded build without the GIL.
- Threads vs asyncio for I/O: threads are preemptive and cost memory per
  thread, and shared state needs locks. asyncio is cooperative: a task switches
  only at `await`, so it scales to many connections cheaply and races are
  easier to reason about.

## Event loop, coroutines, tasks, futures

- **Event loop**: a scheduler that runs ready callbacks and watches sockets
  (via `select`/`epoll`/`kqueue`). It resumes a coroutine when its I/O is ready.
- **Coroutine**: the object an `async def` function returns. It does nothing
  until awaited or wrapped in a task.
- **`await`**: suspends the current coroutine and gives control back to the
  loop until the awaited object completes.
- **Task**: a coroutine scheduled to run concurrently on the loop
  (`asyncio.create_task`, `TaskGroup.create_task`). Keep a reference to it,
  otherwise it may be garbage-collected.
- **Future**: a low-level placeholder for a result that will appear later.
  A Task is a Future that drives a coroutine.
- **Blocking the loop**: any synchronous call (`time.sleep`, `requests.get`,
  heavy CPU work) freezes *all* tasks. Use async libraries, or offload with
  `asyncio.to_thread` / `run_in_executor`.

## Running many tasks

| Tool | Behavior on error | When to use |
|------|-------------------|-------------|
| `asyncio.gather(*aws)` | first exception propagates, other tasks keep running; `return_exceptions=True` returns errors as values | simple fan-out, results in input order |
| `asyncio.TaskGroup` (3.11+) | first failure cancels the remaining tasks and raises an `ExceptionGroup` | structured concurrency: no task outlives the block |
| `asyncio.as_completed(aws)` | yields results as they finish | process results as early as possible |
| `asyncio.wait(aws, return_when=...)` | returns `(done, pending)` sets | fine-grained control |

This crawler uses `TaskGroup` and catches per-URL errors *inside* each
task, so one broken URL never cancels the others. Expected failures are
mapped to domain exceptions; anything else is caught by a last-resort
`except Exception`, logged with its traceback (`logger.exception`) and
reported as `UnexpectedError`, so bugs stay visible without killing the
batch. `CancelledError` is a `BaseException` and passes through.

## Limiting concurrency: Semaphore

- `asyncio.Semaphore(n)` lets at most `n` coroutines into a section at once;
  others wait in `async with semaphore:`.
- Why limit at all: to avoid overloading target servers, running out of file
  descriptors, or getting banned. Unlimited fan-out is also no faster once the
  network is saturated.
- Per-host limits, queues and crawl order are covered in
  [concurrency_control.md](concurrency_control.md).

## Connection pooling

- Opening a TCP connection (+ TLS handshake) costs several round trips.
  A **pool** keeps connections open (HTTP keep-alive) and reuses them.
- In aiohttp the pool lives in `TCPConnector`, owned by a `ClientSession`.
  **Create one session per application, not per request**, and close it at the
  end (`async with` or `await session.close()`).
- `TCPConnector(limit=..., limit_per_host=...)` caps open connections;
  `ttl_dns_cache` avoids repeated DNS lookups.

## Timeouts

- Without a timeout, a slow or dead server can hang a task forever.
- `aiohttp.ClientTimeout` supports `total` (the whole request), `connect`
  (DNS, TCP/TLS handshake and waiting for a pooled connection), `sock_connect`
  (the TCP handshake alone) and `sock_read` (the gap between received chunks).
- Generic tools: `asyncio.timeout(seconds)` (3.11+) and `asyncio.wait_for`.
  Since 3.11, `asyncio.TimeoutError` is an alias of the built-in `TimeoutError`.

## Exceptions in async code

- An exception inside a task is stored in the task and re-raised when the
  task is awaited. If nobody awaits it, you only get a "Task exception was
  never retrieved" warning. Errors can be lost silently.
- Order of `except` clauses matters with class hierarchies. In aiohttp,
  `ClientResponseError` is a subclass of `ClientError`, and
  `ServerTimeoutError` is both a `ClientError` and a `TimeoutError`.
- Wrap low-level errors in domain exceptions (`raise NewError(...) from exc`):
  callers stay independent from the HTTP library, and the original cause is
  kept in `__cause__` for debugging.
- `ExceptionGroup` and `except*` (3.11+) handle several errors raised at
  once, e.g. from a `TaskGroup`.

## Cancellation

- `task.cancel()` raises `CancelledError` at the task's current `await`.
- `CancelledError` derives from `BaseException`, so `except Exception` does not
  swallow it. If you catch it for cleanup, re-raise it.
- `async with` / `try...finally` guarantee resources such as sessions are
  released even when a task is cancelled.
