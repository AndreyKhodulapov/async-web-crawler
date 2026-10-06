"""Helpers shared by unit and integration tests."""

import asyncio
import base64
import importlib.util
import os
from collections.abc import AsyncIterator, Collection, Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

from crawler import CircuitBreaker, CrawlerConfig, DataStorage, PageRecord, RetryStrategy
from demo_site import free_port

BOT = "TestBot/1.0 (+https://example.com/bot)"

# The server of docker-compose.yml, on the port it was started with,
# unless CRAWLER_TEST_DATABASE_URL names another one. An empty port is no
# port, as in the compose file.
POSTGRES_DSN = os.environ.get(
    "CRAWLER_TEST_DATABASE_URL",
    f"postgresql://crawler:crawler@localhost:{os.environ.get('CRAWLER_POSTGRES_PORT') or 5432}/crawler",
)
SITEMAP_NAMESPACE = "http://www.sitemaps.org/schemas/sitemap/0.9"
COOKIES_FILE_HEADER = "# Netscape HTTP Cookie File\n"
EXAMPLES = Path(__file__).parents[1] / "examples"

# Crawler options for tests that check something other than politeness:
# without the rate limit, robots.txt, retries and the circuit breaker they
# run fast and see only the requests they make themselves.
UNTHROTTLED = {
    "requests_per_second": None,
    "respect_robots": False,
    "retry_strategy": RetryStrategy(max_retries=0),
    "circuit_breaker": CircuitBreaker(failure_threshold=None),
}
# The same as sections of a configuration file.
FAST_CONFIG = {
    "crawler": {"rate_limit": None, "respect_robots": False, "user_agent": BOT, "max_depth": 1},
    "retry": {"max_retries": 0},
    "circuit_breaker": {"failure_threshold": None},
}


# The password of the user `crawler` of a test proxy, and the header a proxy that asks for it expects.
PROXY_PASSWORD = "s3cr3t-pw"
PROXY_AUTHORIZATION = "Basic " + base64.b64encode(f"crawler:{PROXY_PASSWORD}".encode()).decode()


def make_config(**sections) -> CrawlerConfig:
    """`FAST_CONFIG` with `sections` over it; a section given as a mapping keeps the other keys of its own."""
    data = {name: dict(section) for name, section in FAST_CONFIG.items()}
    for name, section in sections.items():
        data[name] = {**data[name], **section} if isinstance(section, dict) and name in data else section
    return CrawlerConfig.from_dict(data)


def with_password(proxy_url: str, password: str = PROXY_PASSWORD) -> str:
    return proxy_url.replace("http://", f"http://crawler:{password}@")


def dead_proxy() -> str:
    """A proxy URL at a local port nothing listens on."""
    return f"http://127.0.0.1:{free_port()}"


def cookies_file(path: Path, *lines: str) -> str:
    """Writes a Netscape cookies.txt of `lines` to `path`; returns the path as a string."""
    path.write_text(COOKIES_FILE_HEADER + "".join(f"{line}\n" for line in lines), encoding="utf-8")
    return str(path)


def urlset(*locations: str) -> bytes:
    """A sitemap that lists pages."""
    entries = "".join(f"<url><loc>{location}</loc><lastmod>2026-01-01</lastmod></url>" for location in locations)
    return f'<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="{SITEMAP_NAMESPACE}">{entries}</urlset>'.encode()


def index(*locations: str) -> bytes:
    """A sitemap index that lists other sitemaps."""
    entries = "".join(f"<sitemap><loc>{location}</loc></sitemap>" for location in locations)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="{SITEMAP_NAMESPACE}">{entries}</sitemapindex>'
    ).encode()


class FakeClock:
    """A clock for the `clock` option that moves only when a test sets `now`."""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def make_record(url: str = "https://site/page", **fields: object) -> PageRecord:
    """A page record for storage tests; `fields` replace the defaults."""
    record: PageRecord = {
        "url": url,
        "title": "Page",
        "text": "Some text",
        "links": ["https://site/a", "https://site/b"],
        "metadata": {"description": "A page", "keywords": ["one", "two"], "language": "en", "depth": 1},
        "crawled_at": datetime(2025, 3, 14, 15, 9, 26, 535897, tzinfo=UTC),
        "status_code": 200,
        "content_type": "text/html",
    }
    return record | fields


class MemoryStorage(DataStorage):
    """Keeps the batches in a list; a write fails with the next of `failures`, if any.

    A write of a batch with a record whose URL is in `refused` fails with
    ValueError, every time: the record is one the storage cannot write.
    """

    def __init__(
        self,
        batch_size: int = 100,
        *,
        failures: Sequence[Exception] = (),
        refused: Collection[str] = (),
        **options,
    ) -> None:
        options.setdefault("retry_strategy", RetryStrategy(retry_on=(OSError,), base_delay=0.001, max_delay=0.001))
        # No pause after a failed write, unless a test asks for one.
        options.setdefault("cooldown", 0)
        super().__init__(batch_size, **options)
        self.batches: list[list[PageRecord]] = []
        self.failures = list(failures)
        self.refused = refused
        self.attempts = 0
        self.released = 0
        self._writing = False

    @property
    def urls(self) -> list[list[str]]:
        return [[record["url"] for record in batch] for batch in self.batches]

    async def _write_batch(self, records: Sequence[PageRecord]) -> None:
        assert not self._writing, "two writes at once"
        self._writing = True
        try:
            self.attempts += 1
            await asyncio.sleep(0)
            if self.failures:
                raise self.failures.pop(0)
            for record in records:
                if record["url"] in self.refused:
                    raise ValueError(f"cannot write {record['url']}")
            self.batches.append(list(records))
        finally:
            self._writing = False

    async def _read(self) -> AsyncIterator[PageRecord]:
        for batch in self.batches:
            for record in batch:
                yield record

    async def _close(self) -> None:
        self.released += 1


def load_example(name: str) -> ModuleType:
    """The script `name`.py of examples/, imported as a module."""
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
