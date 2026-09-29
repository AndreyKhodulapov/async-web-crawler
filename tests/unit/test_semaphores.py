"""Unit tests for SemaphoreManager: global and per-domain limits, active counters."""

import asyncio
from collections import Counter

import pytest

from crawler import SemaphoreManager


class Load:
    """Runs tasks through a manager and records peak concurrency."""

    def __init__(self, manager: SemaphoreManager) -> None:
        self.manager = manager
        self.active: Counter[str] = Counter()
        self.peak: Counter[str] = Counter()
        self.peak_total = 0

    async def request(self, url: str, host: str) -> None:
        async with self.manager.slot(url):
            self.active[host] += 1
            self.peak[host] = max(self.peak[host], self.active[host])
            self.peak_total = max(self.peak_total, self.active.total())
            await asyncio.sleep(0.01)
            self.active[host] -= 1

    async def run(self, requests: list[tuple[str, str]]) -> None:
        await asyncio.gather(*(self.request(url, host) for url, host in requests))


def requests_to(host: str, count: int) -> list[tuple[str, str]]:
    return [(f"http://{host}/{i}", host) for i in range(count)]


@pytest.mark.parametrize(("max_concurrent", "max_per_domain"), [(0, None), (1, 0), (2, -1)])
def test_rejects_invalid_limits(max_concurrent, max_per_domain):
    with pytest.raises(ValueError):
        SemaphoreManager(max_concurrent, max_per_domain)


async def test_global_limit():
    load = Load(SemaphoreManager(max_concurrent=3))
    await load.run(requests_to("a", 5) + requests_to("b", 5))
    assert load.peak_total == 3


async def test_per_domain_limit():
    load = Load(SemaphoreManager(max_concurrent=10, max_per_domain=2))
    await load.run(requests_to("a", 5) + requests_to("b", 5))
    assert load.peak == {"a": 2, "b": 2}
    assert load.peak_total == 4


async def test_same_host_in_different_spellings_shares_a_limit():
    load = Load(SemaphoreManager(max_concurrent=10, max_per_domain=1))
    await load.run([("http://Site/a", "site"), ("http://site:80/b", "site"), ("http://site./c", "site")])
    assert load.peak == {"site": 1}


async def test_busy_domain_does_not_hold_global_slots():
    # Two global slots: while "slow" uses one, requests queued for it must not
    # take the other one away from "fast".
    manager = SemaphoreManager(max_concurrent=2, max_per_domain=1)
    release = asyncio.Event()
    order = []

    async def slow(i: int) -> None:
        async with manager.slot(f"http://slow/{i}"):
            await release.wait()

    async def fast() -> None:
        async with manager.slot("http://fast/"):
            order.append("fast")

    slow_tasks = [asyncio.create_task(slow(i)) for i in range(3)]
    await asyncio.sleep(0)
    await asyncio.wait_for(fast(), timeout=1)
    release.set()
    await asyncio.gather(*slow_tasks)
    assert order == ["fast"]


async def test_active_counters():
    manager = SemaphoreManager(max_concurrent=5)
    release = asyncio.Event()
    entered = asyncio.Event()

    async def hold(url: str) -> None:
        async with manager.slot(url):
            if manager.active == 3:
                entered.set()
            await release.wait()

    tasks = [asyncio.create_task(hold(url)) for url in ("http://a/1", "http://a/2", "http://b/")]
    await entered.wait()
    assert manager.get_stats() == {"active": 3, "active_by_domain": {"a": 2, "b": 1}}

    release.set()
    await asyncio.gather(*tasks)
    assert manager.get_stats() == {"active": 0, "active_by_domain": {}}


async def test_slot_is_released_on_error():
    manager = SemaphoreManager(max_concurrent=1, max_per_domain=1)
    with pytest.raises(RuntimeError):
        async with manager.slot("http://a/"):
            raise RuntimeError("boom")
    assert manager.active == 0
    async with asyncio.timeout(1), manager.slot("http://a/"):
        assert manager.active == 1


async def test_idle_domains_are_forgotten():
    manager = SemaphoreManager(max_concurrent=5, max_per_domain=1)
    await Load(manager).run(requests_to("a", 3) + requests_to("b", 3))
    assert manager._domains == {}
