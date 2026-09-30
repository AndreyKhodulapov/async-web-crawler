"""Integration tests: run the demo commands against a local aiohttp server."""

import json

from main import parse_args, run_crawl


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
    assert "=== Circuit breaker (0 of 1 hosts blocked) ===" in output

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
