"""Integration tests: run the demo commands against a local aiohttp server."""

import csv
import json
import os
import sqlite3
from urllib.parse import urlsplit

import asyncpg
import pytest

from demo_main import parse_args, run_crawl, run_errors, run_save

# Start page, 8 articles and the three pages a retry makes good.
SAVED_PATHS = {"/", *(f"/articles/{number}" for number in range(1, 9)), "/flaky", "/rate-limited", "/slow"}


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
    # The page made good by its retry counts once, as a success.
    assert saved["circuit_breaker"] == {
        "127.0.0.1": {"state": "closed", "requests": 2, "failures": 0, "times_opened": 0, "rejected": 0}
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
    assert failed.pop("http://unreachable.invalid/").startswith("DNSError: ClientConnectorDNSError")
    down = {page: error for page, error in failed.items() if urlsplit(page).hostname == "localhost"}
    # The pages refused by its breaker wait for the probes, then fail with the others.
    assert len(down) == 8
    assert all(error.startswith(("NetworkError", "CircuitOpenError")) for error in down.values())
    failed = {urlsplit(page).path: error for page, error in failed.items() if page not in down}
    assert failed == {
        "/server-error": "TransientHTTPError: HTTP 500 Internal Server Error",
        "/missing": "PermanentHTTPError: HTTP 404 Not Found",
        "/private": "PermanentHTTPError: HTTP 403 Forbidden",
        "/empty": "ParseError: empty document",
    }
    # JSON instead of HTML is not an error: the page is left out.
    assert [(urlsplit(page["url"]).path, page["reason"]) for page in saved["skipped"]] == [
        ("/data.json", "not HTML: application/json")
    ]

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
    # Opened by the first failures, then by two failed probes; half-open if
    # the last cooldown is over by the end of the crawl.
    assert breakers["localhost"]["times_opened"] == 3
    assert breakers["localhost"]["state"] in ("open", "half-open")

    output = capsys.readouterr().out
    assert "=== Circuit breaker (3 hosts: " in output
    assert f"Error report saved to {report}" in output


def save_options(tmp_path, *extra: str) -> list[str]:
    files = ["--json", str(tmp_path / "pages.jsonl"), "--csv", str(tmp_path / "pages.csv")]
    return ["save", *files, "--database-url", f"sqlite:///{tmp_path / 'crawler.db'}", *extra]


async def test_save_demo_writes_three_storages_and_reads_them_back(tmp_path, capsys):
    await run_save(parse_args(save_options(tmp_path, "--preview", "2", "--batch-size", "5")))

    lines = (tmp_path / "pages.jsonl").read_text(encoding="utf-8").splitlines()
    from_json = [json.loads(line) for line in lines]
    assert {urlsplit(record["url"]).path for record in from_json} == SAVED_PATHS
    with (tmp_path / "pages.csv").open(encoding="utf-8", newline="") as file:
        from_csv = list(csv.DictReader(file))
    with sqlite3.connect(tmp_path / "crawler.db") as connection:
        from_database = connection.execute("SELECT url, title, status_code FROM pages ORDER BY id").fetchall()
    connection.close()
    # The same pages in the same order, whatever the storage.
    in_json = [(record["url"], record["title"], record["status_code"]) for record in from_json]
    assert [(row["url"], row["title"], int(row["status_code"])) for row in from_csv] == in_json
    assert from_database == in_json
    start = from_json[0]
    assert (start["title"], start["status_code"], start["content_type"]) == ("Unreliable site", 200, "text/html")
    assert (start["metadata"]["depth"], len(start["links"])) == (0, 25)

    output = capsys.readouterr().out
    assert "=== Saved pages (this crawl: 12 saved, 0 not saved) ===" in output
    report = {line.split()[0]: line.split() for line in output.splitlines() if "Storage " in line}
    assert set(report) == {"JSONStorage", "CSVStorage", "SQLiteStorage"}
    for row in report.values():
        assert row[1] == "12"
        assert row[-3:-1] == ["200:", "12"]
    assert report["JSONStorage"][-1] == str(tmp_path / "pages.jsonl")
    assert "earlier runs" not in output
    database = f"sqlite:///{tmp_path / 'crawler.db'}"
    for title in (f"First records in {tmp_path / 'pages.jsonl'} (JSONStorage)", f"Pages found by URL in {database}"):
        preview = output.split(f"=== {title}")[1].split("\n\n")[0].splitlines()[2:]
        assert len(preview) == 2
        assert preview[0].endswith(f"{start['url']}  'Unreliable site'")


async def test_save_demo_replaces_the_files_unless_told_to_append(tmp_path, capsys):
    await run_save(parse_args(save_options(tmp_path)))
    await run_save(parse_args(save_options(tmp_path)))

    # The site gets another port every run, so its pages are new to the database.
    assert len((tmp_path / "pages.jsonl").read_text(encoding="utf-8").splitlines()) == 12
    output = capsys.readouterr().out
    assert "A storage with more records than this crawl had pages also keeps those of earlier runs." in output

    await run_save(parse_args(save_options(tmp_path, "--append")))

    assert len((tmp_path / "pages.jsonl").read_text(encoding="utf-8").splitlines()) == 24
    with sqlite3.connect(tmp_path / "crawler.db") as connection:
        assert connection.execute("SELECT COUNT(*) FROM pages").fetchone() == (36,)
    connection.close()


async def test_save_demo_goes_on_when_a_storage_cannot_be_written(tmp_path, capsys):
    options = save_options(tmp_path, "--batch-size", "100")
    options[options.index("--csv") + 1] = str(tmp_path / "missing" / "pages.csv")

    await run_save(parse_args([*options, "--log-level", "ERROR"]))

    assert len((tmp_path / "pages.jsonl").read_text(encoding="utf-8").splitlines()) == 12
    output = capsys.readouterr().out
    # A page counts as saved once every storage has it.
    assert "=== Saved pages (this crawl: 0 saved, 12 not saved) ===" in output
    report = {line.split()[0]: line.split()[1] for line in output.splitlines() if "Storage " in line}
    assert report == {"JSONStorage": "12", "CSVStorage": "0", "SQLiteStorage": "12"}
    assert "earlier runs" not in output


async def test_save_demo_writes_an_indented_array_in_another_encoding(tmp_path):
    await run_save(parse_args(save_options(tmp_path, "--indent", "2", "--csv-encoding", "utf-16")))

    pages = json.loads((tmp_path / "pages.jsonl").read_text(encoding="utf-8"))
    assert len(pages) == 12
    with (tmp_path / "pages.csv").open(encoding="utf-16", newline="") as file:
        assert len(list(csv.DictReader(file))) == 12


@pytest.mark.postgres
async def test_save_demo_with_postgres(tmp_path, capsys, monkeypatch):
    dsn = os.environ.get("CRAWLER_TEST_DATABASE_URL", "postgresql://crawler:crawler@localhost:5432/crawler")
    connection = await asyncpg.connect(dsn)
    await connection.execute("DROP TABLE IF EXISTS pages")
    monkeypatch.setenv("CRAWLER_DATABASE_URL", dsn)
    files = ["--json", str(tmp_path / "pages.jsonl"), "--csv", str(tmp_path / "pages.csv")]

    try:
        await run_save(parse_args(["save", *files]))
        saved = await connection.fetch("SELECT url FROM pages")
    finally:
        await connection.close()

    assert {urlsplit(url).path for (url,) in saved} == SAVED_PATHS
    output = capsys.readouterr().out
    assert "=== Saved pages (this crawl: 12 saved, 0 not saved) ===" in output
    row = next(line.split() for line in output.splitlines() if line.startswith("PostgresStorage "))
    assert row[1:3] == ["12", "-"]
    assert f":{urlsplit(dsn).password}@" not in row[-1]
    assert ":***@" in row[-1]
    assert "Pages found by URL in postgresql://" in output
