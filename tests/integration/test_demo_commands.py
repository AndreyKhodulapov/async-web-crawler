"""Integration tests: run the demo commands against a local aiohttp server."""

import json
from urllib.parse import urlsplit

from main import parse_args, run_crawl, run_errors


async def test_crawl_reports_errors_and_circuit_breaker(url, tmp_path, capsys):
    report = tmp_path / "crawl.json"
    options = ["--max-depth", "0", "--rps", "0", "--no-robots", "--retries", "1", "--json", str(report)]
    await run_crawl(parse_args(["crawl", url("/flaky/1"), url("/status/404"), *options]))

    # 503 once, then the page; 404 with no retry.
    output = capsys.readouterr().out
    assert "=== Errors (2 failed attempts) ===" in output
    assert "By kind:  TransientError 1, PermanentError 1, NetworkError 0, ParseError 0, other 0" in output
    assert "By class: TransientHTTPError 1, PermanentHTTPError 1" in output
    assert "Retries: 1, pages recovered by a retry: 1," in output
    assert f"  {url('/status/404')}  PermanentHTTPError: HTTP 404 Not Found" in output
    assert "=== Circuit breaker (1 hosts: 0 open, 0 half-open) ===" in output
    # The politeness report counts robots.txt too; here it is off.
    assert "retries (robots.txt included): 1," in output

    saved = json.loads(report.read_text(encoding="utf-8"))
    errors = saved["errors"]
    assert (errors["total"], errors["retries"], errors["successful_retries"]) == (2, 1, 1)
    assert errors["by_class"] == {"TransientHTTPError": 1, "PermanentHTTPError": 1}
    assert errors["permanent_errors"] == {url("/status/404"): "PermanentHTTPError: HTTP 404 Not Found"}
    assert saved["circuit_breaker"] == {
        "127.0.0.1": {"state": "closed", "requests": 3, "failures": 1, "times_opened": 0, "rejected": 0}
    }


async def test_crawl_report_says_when_the_breaker_is_off(url, capsys):
    options = ["--max-depth", "0", "--rps", "0", "--no-robots", "--no-breaker"]
    await run_crawl(parse_args(["crawl", url("/ok"), *options]))

    output = capsys.readouterr().out
    assert "=== Errors (0 failed attempts) ===" in output
    assert "By class: none" in output
    assert "Circuit breaker: off" in output


async def test_errors_demo_meets_every_kind_of_error(url, tmp_path, capsys):
    report = tmp_path / "errors.json"
    # A real URL given on the command line is fetched along with the demo pages.
    await run_errors(parse_args(["errors", url("/ok"), "--json", str(report)]))

    saved = json.loads(report.read_text(encoding="utf-8"))
    fetched = {urlsplit(page).path for page in saved["fetched"]}
    articles = {f"/articles/{number}" for number in range(1, 9)}
    # 503 twice, 429 once and a read timeout, each made good by a retry.
    assert fetched == {"/", *articles, "/flaky", "/rate-limited", "/slow", "/ok"}
    failed = {page["url"]: page["error"] for page in saved["failed"]}
    assert failed.pop("http://unreachable.invalid/").startswith("NetworkError: ClientConnectorDNSError")
    down = {page: error for page, error in failed.items() if urlsplit(page).hostname == "localhost"}
    assert len(down) == 4
    assert all(error.startswith(("NetworkError", "CircuitOpenError")) for error in down.values())
    failed = {urlsplit(page).path: error for page, error in failed.items() if page not in down}
    assert failed == {
        "/server-error": "TransientHTTPError: HTTP 500 Internal Server Error",
        "/missing": "PermanentHTTPError: HTTP 404 Not Found",
        "/private": "PermanentHTTPError: HTTP 403 Forbidden",
        "/data.json": "ParseError: unsupported content type: application/json",
    }

    errors = saved["errors"]
    # HTTP 500 is retried once by the default rules.
    assert {kind: errors["by_kind"][kind] for kind in ("TransientError", "PermanentError", "ParseError")} == {
        "TransientError": 6,
        "PermanentError": 2,
        "ParseError": 1,
    }
    # The server that is down fails 5 times before its breaker opens; the bad domain 4 times.
    assert errors["by_kind"]["NetworkError"] >= 9
    assert errors["successful_retries"] == 3
    assert set(errors["permanent_errors"].values()) == {
        "PermanentHTTPError: HTTP 404 Not Found",
        "PermanentHTTPError: HTTP 403 Forbidden",
    }

    breakers = saved["circuit_breaker"]
    # The site's own failures stay under the threshold.
    assert (breakers["127.0.0.1"]["state"], breakers["127.0.0.1"]["times_opened"]) == ("closed", 0)
    assert (breakers["localhost"]["state"], breakers["localhost"]["times_opened"]) == ("open", 1)

    output = capsys.readouterr().out
    assert "=== Circuit breaker (3 hosts: 1 open, 0 half-open) ===" in output
    assert f"Error report saved to {report}" in output
