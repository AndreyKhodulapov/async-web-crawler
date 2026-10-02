"""Unit tests for CrawlerStats: counting pages by outcome, status code and domain, speed and running time."""

from datetime import UTC, datetime

import pytest
from helpers import FakeClock

from crawler import CrawlerStats


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def stats(clock) -> CrawlerStats:
    stats = CrawlerStats(clock=clock)
    stats.start()
    return stats


def test_stats_before_start_are_empty():
    assert CrawlerStats().get_stats() == {
        "total_pages": 0,
        "successful": 0,
        "failed": 0,
        "skipped": 0,
        "elapsed_seconds": 0.0,
        "pages_per_second": 0.0,
        "avg_response_time": 0.0,
        "status_codes": {},
        "errors": {},
        "top_domains": {},
        "started_at": None,
        "finished_at": None,
    }


def test_known_scenario(stats, clock):
    stats.record_page("https://site/", status=200, elapsed=0.1)
    stats.record_page("https://site/a", status=200, elapsed=0.3)
    stats.record_page("https://site/gone", status=404, elapsed=0.2, error="PermanentHTTPError")
    stats.record_page("https://site/data.json", status=200, elapsed=0.2, error="ParseError")
    stats.record_page("https://site/sign-in", status=200, elapsed=0.2, skipped=True)
    stats.record_page("https://down/", elapsed=1.0, error="NetworkError")
    stats.record_page("https://down/a", elapsed=1.0, error="NetworkError")
    stats.record_page("https://blocked/", error="CircuitOpenError")  # no request was sent
    clock.now += 4
    result = stats.get_stats()

    assert (result["total_pages"], result["successful"], result["failed"], result["skipped"]) == (8, 2, 5, 1)
    assert result["elapsed_seconds"] == 4
    assert result["pages_per_second"] == 2
    assert result["avg_response_time"] == pytest.approx(3.0 / 7)
    assert list(result["status_codes"].items()) == [(200, 4), (404, 1)]
    # Most frequent first, equal counts by name.
    assert list(result["errors"].items()) == [
        ("NetworkError", 2),
        ("CircuitOpenError", 1),
        ("ParseError", 1),
        ("PermanentHTTPError", 1),
    ]
    assert list(result["top_domains"].items()) == [("site", 5), ("down", 2), ("blocked", 1)]


def test_top_domains_are_limited(clock):
    stats = CrawlerStats(top_domains=2, clock=clock)
    stats.start()
    for host, pages in (("small", 1), ("large", 3), ("medium", 2)):
        for page in range(pages):
            stats.record_page(f"https://{host}/{page}", status=200)

    assert list(stats.get_stats()["top_domains"].items()) == [("large", 3), ("medium", 2)]
    assert stats.get_stats()["total_pages"] == 6


def test_domain_is_the_host_without_port_in_lower_case(stats):
    stats.record_page("https://Example.COM:8443/a", status=200)
    stats.record_page("http://example.com/b", status=200)
    stats.record_page("not a url", error="InvalidURLError")

    assert stats.get_stats()["top_domains"] == {"example.com": 2, "unknown": 1}


def test_clock_runs_until_finish(stats, clock):
    stats.record_page("https://site/", status=200)
    clock.now += 2
    assert stats.get_stats()["elapsed_seconds"] == 2
    assert stats.get_stats()["finished_at"] is None

    stats.finish()
    clock.now += 10
    stats.finish()  # the second call changes nothing
    result = stats.get_stats()

    assert result["elapsed_seconds"] == 2
    assert result["pages_per_second"] == 0.5
    started, finished = datetime.fromisoformat(result["started_at"]), datetime.fromisoformat(result["finished_at"])
    assert started.tzinfo is UTC and started <= finished <= datetime.now(UTC)


def test_no_time_elapsed_gives_zero_speed(stats):
    stats.record_page("https://site/", status=200)
    assert stats.get_stats()["pages_per_second"] == 0.0


def test_finish_before_start_does_nothing():
    stats = CrawlerStats()
    stats.finish()
    assert stats.get_stats()["finished_at"] is None


def test_start_forgets_the_previous_crawl(stats, clock):
    stats.record_page("https://site/gone", status=404, elapsed=0.5, error="PermanentHTTPError")
    stats.finish()
    clock.now += 5
    stats.start()
    clock.now += 1
    result = stats.get_stats()

    assert (result["total_pages"], result["failed"], result["elapsed_seconds"]) == (0, 0, 1)
    assert result["status_codes"] == result["errors"] == result["top_domains"] == {}
    assert result["avg_response_time"] == 0.0
    assert result["finished_at"] is None


def test_snapshot_does_not_change_afterwards(stats):
    stats.record_page("https://site/", status=200)
    snapshot = stats.get_stats()
    stats.record_page("https://site/a", status=200)

    assert snapshot["status_codes"] == {200: 1}
    assert snapshot["top_domains"] == {"site": 1}


def test_invalid_top_domains_is_rejected():
    with pytest.raises(ValueError, match="top_domains must be >= 1"):
        CrawlerStats(top_domains=0)
