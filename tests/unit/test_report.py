"""Unit tests for the export of crawl statistics: the JSON file and the HTML report."""

import base64
import json
import re
from html import unescape

import pytest
from helpers import FakeClock

from crawler import CrawlerStats
from crawler.report import render_html, render_json

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
CHART = re.compile(r'<img src="data:image/png;base64,([A-Za-z0-9+/=]+)"')


@pytest.fixture
def stats() -> CrawlerStats:
    clock = FakeClock()
    stats = CrawlerStats(clock=clock)
    stats.start()
    stats.record_page("https://site/", status=200, elapsed=0.1)
    stats.record_page("https://site/a", status=200, elapsed=0.3)
    stats.record_page("https://site/gone", status=404, elapsed=0.2, error="PermanentHTTPError")
    stats.record_page("https://down/", elapsed=1.0, error="NetworkError")
    clock.now += 8
    stats.finish()
    return stats


def empty_stats() -> dict:
    return CrawlerStats().get_stats()


def cells(html: str) -> list[str]:
    """The text of the table cells, as a reader sees it."""
    return [unescape(cell) for cell in re.findall(r"<td[^>]*>(.*?)</td>", html)]


def test_json_file_reads_back(stats, tmp_path):
    path = tmp_path / "stats.json"
    stats.export_to_json(path)

    expected = stats.get_stats()
    expected["status_codes"] = {"200": 2, "404": 1}  # JSON keys are strings
    assert json.loads(path.read_text(encoding="utf-8")) == expected
    assert list(json.loads(path.read_text(encoding="utf-8"))["top_domains"]) == ["site", "down"]  # the order is kept


def test_json_file_is_replaced_and_takes_a_string_path(stats, tmp_path):
    path = tmp_path / "stats.json"
    path.write_text("an older report, much longer than the new one" * 100, encoding="utf-8")
    stats.export_to_json(str(path))

    assert json.loads(path.read_text(encoding="utf-8"))["total_pages"] == 4


def test_json_keeps_non_ascii_text_readable():
    rendered = render_json(empty_stats() | {"top_domains": {"пример.рф": 1}})

    assert "пример.рф" in rendered
    assert rendered.endswith("}\n")
    assert json.loads(rendered)["top_domains"] == {"пример.рф": 1}


def test_export_to_a_missing_directory_fails(stats, tmp_path):
    with pytest.raises(OSError):
        stats.export_to_json(tmp_path / "missing" / "stats.json")
    with pytest.raises(OSError):
        stats.export_to_html_report(tmp_path / "missing" / "report.html")


def test_html_report_has_the_summary_tables_and_charts(stats, tmp_path):
    path = tmp_path / "report.html"
    stats.export_to_html_report(str(path), title="Crawl of site")
    html = path.read_text(encoding="utf-8")

    assert html.startswith("<!DOCTYPE html>")
    assert "<title>Crawl of site</title>" in html and "<h1>Crawl of site</h1>" in html
    for name, value in (("Pages", "4"), ("Successful", "2"), ("Failed", "2"), ("Skipped", "0")):
        assert f"<dt>{name}</dt><dd>{value}</dd>" in html
    assert "<dt>Running time</dt><dd>8.0 s</dd>" in html
    assert "<dt>Pages per second</dt><dd>0.50</dd>" in html
    assert "<dt>Average response time</dt><dd>400 ms</dd>" in html
    assert cells(html) == [
        *("200 OK", "2", "50.0%"),
        *("404 Not Found", "1", "25.0%"),
        *("site", "3", "75.0%"),
        *("down", "1", "25.0%"),
        *("NetworkError", "1", "25.0%"),
        *("PermanentHTTPError", "1", "25.0%"),
    ]
    assert re.search(r"Started \d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC, finished \d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC\.", html)
    charts = CHART.findall(html)
    assert len(charts) == 3  # status codes, domains, errors
    assert all(base64.b64decode(chart).startswith(PNG_SIGNATURE) for chart in charts)


def test_html_report_needs_no_other_files(stats):
    html = render_html(stats.get_stats())

    assert "<script" not in html
    assert "<link" not in html
    assert not re.search(r'(src|href)="(?!data:)', html)


def test_html_report_escapes_what_comes_from_the_crawl():
    hostile = '<script>alert("x")</script>&'
    html = render_html(
        empty_stats() | {"total_pages": 1, "failed": 1, "top_domains": {hostile: 1}, "errors": {hostile: 1}},
        title="<b>Report</b>",
    )

    assert "<script>" not in html and "<b>" not in html
    assert "&lt;b&gt;Report&lt;/b&gt;" in html
    assert cells(html) == [hostile, "1", "100.0%"] * 2


def test_chart_labels_are_not_read_as_formulas():
    # Matplotlib takes text between dollar signs for a formula and fails on a broken one.
    html = render_html(empty_stats() | {"total_pages": 1, "top_domains": {"$\\frac{$ host_name": 1}})

    assert len(CHART.findall(html)) == 1


def test_long_chart_label_is_kept_whole_in_the_table():
    host = "a" * 200 + ".example"
    html = render_html(empty_stats() | {"total_pages": 1, "top_domains": {host: 1}})

    assert cells(html)[0] == host
    assert len(CHART.findall(html)) == 1


def test_html_report_of_an_empty_crawl_has_no_charts():
    html = render_html(empty_stats())

    assert "<img" not in html and "<table" not in html
    assert "The crawl has not started." in html
    assert "No page got a response." in html
    assert "No pages were crawled." in html
    assert "No page failed." in html


def test_html_report_of_a_running_crawl_says_so():
    stats = CrawlerStats()
    stats.start()
    stats.record_page("https://site/", status=299)
    html = render_html(stats.get_stats())

    assert "still running." in html
    assert cells(html)[0] == "299"  # a code without a standard name


@pytest.mark.parametrize(
    ("seconds", "text"),
    [(0, "0 ms"), (0.25, "250 ms"), (59.94, "59.9 s"), (75, "1 min 15 s"), (3725, "1 h 2 min 5 s")],
)
def test_running_time_is_readable(seconds, text):
    assert f"<dt>Running time</dt><dd>{text}</dd>" in render_html(empty_stats() | {"elapsed_seconds": seconds})
