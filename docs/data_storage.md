# Data storage: files, databases, batching and failed writes

Short notes on saving what a crawler collects without
blocking the event loop, losing pages or writing them twice.

## File I/O and the event loop

- A regular file is **always "ready"** for the OS: `select`/`epoll` cannot
  wait for it, so there is no truly non-blocking `read`/`write` for files, as
  there is for sockets. A plain `file.write()` in a coroutine blocks the
  whole loop for as long as the disk takes.
- `aiofiles` does not change that: it runs the same blocking calls in a
  **thread pool** and gives them an `await`. The loop stays free; the disk is
  not faster. `asyncio.to_thread` does the same for any blocking function.
  (Linux `io_uring` offers real asynchronous file I/O; asyncio does not use it.)
- `aiosqlite` is the same idea for SQLite: one **dedicated thread** per
  connection, and a queue of calls to it. `asyncpg` is different: PostgreSQL
  is spoken to over a socket, so it is asynchronous for real, with its own
  binary protocol implementation.
- A thread hop costs tens of microseconds. For one small write it is more
  than the write itself, which is one more reason to write in batches.

## One interface, many backends

- The crawler knows one abstraction, `DataStorage`: `save`, `flush`, `read`,
  `close`. Which file format or database is behind it is decided where the
  crawler is put together (**dependency inversion**).
- **Template method**: the base class holds what all backends share (the
  buffer, the lock, the retries, closing), a subclass implements three hooks:
  write a batch, read, release the resource.
- **Driver (adapter)** for databases: the SQL logic lives once in
  `DatabaseStorage`; a driver supplies the connection and the dialect:
  placeholders (`?` vs `$1`), column types (`TEXT` vs `JSONB`, `TIMESTAMPTZ`),
  the auto-increment key. A new database is a new driver, not a new copy of
  the logic.
- **Registry + factory**: a URL scheme (`sqlite://`, `postgresql://`) maps to
  a builder, so the backend is chosen by configuration (twelve-factor: config
  in the environment) and a third party can register its own. Read the
  environment in one place, at the edge; classes take arguments.
- **Composite**: a storage that forwards to several storages lets one crawl
  write to a file and a database without the crawler knowing.

## Formats

| Format | Good at | Weak at |
|--------|---------|---------|
| JSON Lines | appending, streaming line by line, nested data, tools (`jq`, pandas, BigQuery) | no schema, repeats the keys in every record |
| JSON array | one valid document, easy on the eye | appending means rewriting the end; a naive reader loads it whole |
| CSV | spreadsheets, any tool reads it | flat: nested values need an encoding of their own; no types; quoting and encodings are a classic source of bugs |
| SQLite | queries, indexes, transactions, zero setup: one file | one writer at a time |
| PostgreSQL | concurrent writers, `JSONB` queries, real types, scale | a server to run |

- **Append without reading**: a JSON array can still be appended in O(1):
  write `\n]\n` after the records, remember where it starts, and overwrite it
  with `,\n<record>\n]\n` next time. The file is valid JSON after every write.
- **Read without loading**: iterate (an async generator) instead of returning
  a list. For JSON, `JSONDecoder.raw_decode` takes one value off the front of
  a buffer that is refilled in chunks.
- **CSV details**: RFC 4180 quoting (commas, quotes doubled, line breaks
  inside quotes, so one row may span lines); write nested values as JSON in a
  cell; the header comes from the first record and must not be written twice
  on append; `utf-8-sig` adds the BOM Excel needs; decide what happens to a
  character the encoding lacks.
- **Empty vs missing**: CSV cannot tell `None` from `""`. If all backends
  must return the same record, normalize before saving (here: empty strings).
- **Time**: store UTC, as ISO 8601 text or a `TIMESTAMPTZ`; ISO 8601 in one
  zone sorts as text in time order. Naive datetimes are a bug waiting.

## Batching and buffering

- One write per record pays the fixed cost every time: a thread hop, a
  syscall, and in a database a **transaction with an fsync**. A batch pays it
  once; inserts go 10-100x faster.
- `executemany` in one transaction is the simple form; multi-row `INSERT`,
  PostgreSQL `COPY` and `unnest` arrays are faster still.
- The trade-off is **durability**: records in the buffer are lost if the
  process dies. Bound the loss with the batch size, flush at the end of the
  work and on close; a time-based flush bounds it for a slow trickle.
- A batch in a transaction is **atomic**: all rows or none, which makes a
  retry of the whole batch safe.
- **Concurrency**: many workers call `save`. A lock around "append and maybe
  flush" keeps batches in order and a record out of two writes. Holding the
  lock during the write is also the **backpressure**: when the storage is
  slower than the crawl, workers wait instead of filling the memory.
- SQLite: one writer; WAL mode lets readers go on during a write;
  `busy_timeout` makes a writer wait for a lock instead of failing at once.
- PostgreSQL: a **connection pool**. A cursor lives inside a transaction and
  holds its connection, so a long read needs a connection apart from the writes.

## Failed writes

- Classify, as with requests: a full disk, a locked database, a dropped
  connection, a deadlock **may pass**, so retry with backoff. A value that
  cannot be serialized, a constraint violation **never pass**: a retry
  repeats the bug; let it surface.
- A retry must not write twice (**idempotency**):
  - files: remember the offset where the batch starts and seek there before
    writing; a retry overwrites a half-written batch instead of appending
    after it;
  - databases: a transaction per batch, plus an **upsert**
    (`INSERT ... ON CONFLICT (url) DO UPDATE`) on a natural key, so writing
    the same page again replaces the row. Upsert turns at-least-once
    delivery into exactly-once results.
- When the retries run out, **keep the records** and raise: the next write
  takes them along, and a short outage loses nothing. The cost is memory.
- Do not retry on every save while the outage lasts: the retries run under
  the lock, and every worker would wait for them. After a failed write
  **back off for a cooldown** and only buffer; an explicit flush still
  writes at once.
- Keep only what a retry can cure. A batch that fails with any other error
  (a value the database refuses, a record that cannot be serialized) is a
  **poison batch**: kept in the buffer, it fails every later write. Drop it,
  log it and raise.
- **Saving must not kill the crawl**: catch at the boundary of one page, log,
  count, go on. Fetched pages are expensive, a failed save is not a reason to
  throw away the rest.
- **Count honestly**. With batches, "save() did not raise" does not mean
  "written": the record may sit in the buffer, and one failed flush is many
  pages. Count what the storage has actually written out and derive the
  failures from that.
- **Close in `finally`**: flush, then release the file or the connection
  even if the flush failed. Make `close` idempotent. An async generator that
  holds a cursor must be closed too (`contextlib.aclosing`), or a reader that
  stops early leaves the cursor open past the connection.

## Schema and indexes

- A surrogate key (`id`) keeps insertion order; the **natural key** (`url`)
  is `UNIQUE`, which both forbids duplicates and gives the index that makes
  the upsert and lookups by URL fast.
- Index what is queried: `crawled_at` (what changed since), `status_code`
  (which pages failed). Every index slows inserts, so not "all columns".
- Check that an index is used: `EXPLAIN QUERY PLAN` (SQLite), `EXPLAIN
  ANALYZE` (PostgreSQL).
- Semi-structured fields (links, meta tags) fit a JSON column: `JSONB` in
  PostgreSQL is binary, queryable (`metadata ->> 'language'`) and indexable
  with GIN; in SQLite it is text with JSON functions.
- **Always bind parameters**; never format values into SQL. Page text is
  untrusted input, and it does contain quotes. Identifiers (table, column
  names) cannot be bound, so they must come from the code, not from data.
- `CREATE TABLE IF NOT EXISTS` makes initialization repeatable; real projects
  move on to migrations (Alembic) once the schema changes.

## Testing storage

- **Contract tests**: one set of tests, parametrized over the backends:
  whatever is saved is read back equal, with the same types. This is what
  keeps the backends interchangeable.
- Round-trip the nasty values: quotes, commas, line breaks, non-ASCII, empty
  strings, nested JSON, a text longer than a buffer, time zones.
- Files: `tmp_path`. SQLite: a file in `tmp_path` (an in-memory database
  disappears with its connection). PostgreSQL: a container, tests behind a
  marker so the default run needs no server.
- Failures: a fake backend that fails N times checks the retries, the kept
  buffer and the counters; a real lock (`BEGIN EXCLUSIVE` from a second
  connection) checks that the right driver error is the one retried.
- Test what must not happen: no duplicates after a retried write, no second
  header on append, nothing written when a batch fails.
