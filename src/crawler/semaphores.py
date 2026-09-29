"""Concurrency limits: a global one and an optional one per domain."""

import asyncio
import contextlib
from collections import Counter
from collections.abc import AsyncIterator

from crawler.urls import get_host


class SemaphoreManager:
    """Limits concurrent requests overall and per domain, and counts active ones.

    Usage::

        async with manager.slot(url):
            ...  # at most `max_concurrent` requests in total and
                 # `max_per_domain` requests to the host of `url`

    With `max_per_domain=None` only the global limit applies.
    """

    def __init__(self, max_concurrent: int, max_per_domain: int | None = None) -> None:
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent must be >= 1, got {max_concurrent}")
        if max_per_domain is not None and max_per_domain < 1:
            raise ValueError(f"max_per_domain must be >= 1 or None, got {max_per_domain}")
        self.max_concurrent = max_concurrent
        self.max_per_domain = max_per_domain
        self._global = asyncio.Semaphore(max_concurrent)
        self._domains: dict[str, asyncio.Semaphore] = {}
        # Tasks inside slot() per domain, waiting or active: a domain's
        # semaphore is dropped when nobody uses it, so a long-lived manager
        # does not keep one per host it has ever seen.
        self._users: Counter[str] = Counter()
        self._active: Counter[str] = Counter()

    @property
    def active(self) -> int:
        """Number of tasks currently holding a slot."""
        return self._active.total()

    @contextlib.asynccontextmanager
    async def slot(self, url: str) -> AsyncIterator[None]:
        # An invalid URL still takes a global slot; it fails right after.
        domain = get_host(url) or ""
        self._users[domain] += 1
        try:
            # The domain slot is taken first: a task waiting for a busy
            # domain must not hold a global slot that another domain could use.
            async with self._domain_semaphore(domain), self._global:
                self._active[domain] += 1
                try:
                    yield
                finally:
                    self._active[domain] -= 1
                    if not self._active[domain]:
                        del self._active[domain]
        finally:
            self._users[domain] -= 1
            if not self._users[domain]:
                del self._users[domain]
                self._domains.pop(domain, None)

    def get_stats(self) -> dict[str, object]:
        return {
            "active": self.active,
            "active_by_domain": dict(self._active),
        }

    def _domain_semaphore(self, domain: str) -> contextlib.AbstractAsyncContextManager[object]:
        if self.max_per_domain is None:
            return contextlib.nullcontext()
        if domain not in self._domains:
            self._domains[domain] = asyncio.Semaphore(self.max_per_domain)
        return self._domains[domain]
