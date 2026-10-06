"""Renders the statistics of a crawl as JSON and as an HTML report with tables and charts."""

import base64
import json
import textwrap
from collections.abc import Mapping
from datetime import UTC, datetime
from html import escape
from http import HTTPStatus
from io import BytesIO
from typing import Any

from crawler.progress import format_duration

# Colors of the charts and of the page around them.
_SURFACE = "#fcfcfb"
_TEXT = "#0b0b0b"
_MUTED = "#52514e"


def render_json(stats: Mapping[str, Any]) -> str:
    """The statistics as an indented JSON document.

    The keys of `status_codes` are numbers in `stats` and strings in JSON,
    which has no other keys.
    """
    return json.dumps(stats, indent=2, ensure_ascii=False) + "\n"


def render_html(stats: Mapping[str, Any], *, title: str = "Crawl report") -> str:
    """The statistics as an HTML page: a summary, then a chart and a table per breakdown.

    `stats` is what `CrawlerStats.get_stats()` returns; with a `proxies`
    key, as `AdvancedCrawler.get_stats()` has it, a table of the proxies
    follows, and with a `rendering` key, the numbers of the rendering. The page is one file that needs nothing else: the styles are
    inline, the charts are PNG images embedded as data URIs, and there are
    no scripts. Everything that comes from the crawl (hosts, error names,
    proxies) is escaped.
    """
    style = textwrap.dedent(f"""
    body {{ margin: 0; padding: 32px 16px; background: {_SURFACE}; color: {_TEXT};
           font: 15px/1.5 -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }}
    main {{ max-width: 860px; margin: 0 auto; }}
    h1 {{ margin: 0 0 4px; font-size: 26px; }}
    h2 {{ margin: 40px 0 12px; font-size: 19px; }}
    .period, .empty {{ color: {_MUTED}; }}
    .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: 24px 0 0; }}
    .summary div {{ padding: 12px 14px; border: 1px solid #e4e3df; border-radius: 8px; background: #fff; }}
    .summary dt {{ color: {_MUTED}; font-size: 13px; }}
    .summary dd {{ margin: 2px 0 0; font-size: 22px; font-weight: 600; }}
    img {{ display: block; max-width: 100%; height: auto; margin: 0 0 12px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 6px 10px; border-bottom: 1px solid #e4e3df; text-align: left; overflow-wrap: anywhere; }}
    th {{ color: {_MUTED}; font-size: 13px; font-weight: 600; }}
    th.number, td.number {{ text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }}
    """)
    total = stats["total_pages"]
    summary = {
        "Pages": _count(total),
        "Successful": _count(stats["successful"]),
        "Failed": _count(stats["failed"]),
        "Skipped": _count(stats["skipped"]),
        "Running time": _duration(stats["elapsed_seconds"]),
        "Pages per second": f"{stats['pages_per_second']:.2f}",
        "Average response time": _duration(stats["avg_response_time"]),
    }
    tiles = "".join(f"<div><dt>{name}</dt><dd>{value}</dd></div>" for name, value in summary.items())
    status_codes = {_status_name(code): pages for code, pages in stats["status_codes"].items()}
    sections = [
        _section("Status codes", "Status", status_codes, total, "No page got a response."),
        _section("Top domains", "Domain", stats["top_domains"], total, "No pages were crawled."),
        _section("Errors", "Error", stats["errors"], total, "No page failed."),
    ]
    if "proxies" in stats:
        sections.append(_proxy_section(stats["proxies"]))
    if "rendering" in stats:
        sections.append(_rendering_section(stats["rendering"]))
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{escape(title)}</title>\n<style>{style}</style>\n</head>\n<body>\n<main>\n"
        f"<h1>{escape(title)}</h1>\n"
        f'<p class="period">{escape(_period(stats["started_at"], stats["finished_at"]))}</p>\n'
        f'<dl class="summary">{tiles}</dl>\n' + "".join(sections) + "</main>\n</body>\n</html>\n"
    )


def _section(title: str, column: str, counts: Mapping[str, int], total: int, empty: str) -> str:
    if not counts:
        return f'<section>\n<h2>{title}</h2>\n<p class="empty">{empty}</p>\n</section>\n'
    rows = "".join(
        f'<tr><td>{escape(str(name))}</td><td class="number">{_count(pages)}</td>'
        f'<td class="number">{_share(pages, total)}</td></tr>\n'
        for name, pages in counts.items()
    )
    chart = base64.b64encode(_bar_chart(counts)).decode("ascii")
    return (
        f"<section>\n<h2>{title}</h2>\n"
        f'<img src="data:image/png;base64,{chart}" alt="{title}: pages as bars, the same numbers as in the table">\n'
        f'<table>\n<thead><tr><th>{column}</th><th class="number">Pages</th>'
        f'<th class="number">Share of pages</th></tr></thead>\n<tbody>\n{rows}</tbody>\n</table>\n</section>\n'
    )


def _proxy_section(proxies: Mapping[str, Mapping[str, Any]]) -> str:
    rows = "".join(
        f'<tr><td>{escape(label)}</td><td class="number">{_count(proxy["requests"])}</td>'
        f'<td class="number">{_count(proxy["failures"])}</td><td class="number">{_count(proxy["times_removed"])}</td>'
        f"<td>{'out of rotation' if proxy['state'] == 'out' else 'active'}</td></tr>\n"
        for label, proxy in proxies.items()
    )
    return (
        "<section>\n<h2>Proxies</h2>\n"
        '<table>\n<thead><tr><th>Proxy</th><th class="number">Requests</th><th class="number">Failures</th>'
        '<th class="number">Times out of rotation</th><th>State</th></tr></thead>\n'
        f"<tbody>\n{rows}</tbody>\n</table>\n</section>\n"
    )


def _rendering_section(rendering: Mapping[str, Any]) -> str:
    numbers = {
        "Pages rendered": _count(rendering["rendered"]),
        "Failed": _count(rendering["failed"]),
        "Average render time": _duration(rendering["avg_render_time"]),
    }
    tiles = "".join(f"<div><dt>{name}</dt><dd>{value}</dd></div>" for name, value in numbers.items())
    return f'<section>\n<h2>Rendering</h2>\n<dl class="summary">{tiles}</dl>\n</section>\n'


def _bar_chart(counts: Mapping[str, int]) -> bytes:
    """A PNG with a horizontal bar per item, in the order given, each labelled with its value."""
    # Imported here: matplotlib takes a while to load, and only a report needs it.
    # A figure of its own, not pyplot: no global state and no GUI backend.
    from matplotlib.figure import Figure

    labels = [_shorten(str(name)) for name in counts]
    values = list(counts.values())
    figure = Figure(figsize=(7.6, 0.2 + 0.36 * len(values)), dpi=120, facecolor=_SURFACE, layout="constrained")
    axes = figure.add_subplot()
    axes.set_facecolor(_SURFACE)
    bars = axes.barh(range(len(values)), values, height=0.62, color="#2a78d6")
    axes.bar_label(bars, labels=[_count(value) for value in values], padding=5, color=_TEXT, fontsize=10)
    # Labels come from the crawl: `$` in one must not start a formula.
    axes.set_yticks(range(len(values)), labels=labels, color=_TEXT, fontsize=10, parse_math=False)
    axes.set_ylim(len(values) - 0.5, -0.5)  # the first item on top; bars equally thick however many there are
    axes.set_xlim(0, max(values) * 1.12)  # room for the value after the longest bar
    axes.xaxis.set_visible(False)  # every bar carries its value
    axes.tick_params(axis="y", length=0)
    axes.spines[["top", "right", "bottom"]].set_visible(False)
    axes.spines["left"].set_color("#c9c8c2")
    image = BytesIO()
    figure.savefig(image, format="png")
    return image.getvalue()


def _shorten(label: str) -> str:
    # Longer chart labels are cut; the table next to the chart has them in full.
    longest = 48
    return label if len(label) <= longest else f"{label[: longest - 1]}…"


def _status_name(code: int) -> str:
    try:
        return f"{code} {HTTPStatus(code).phrase}"
    except ValueError:  # a code no standard defines
        return str(code)


def _count(value: int) -> str:
    return f"{value:,}"


def _share(pages: int, total: int) -> str:
    return f"{pages / total:.1%}" if total else "–"


def _duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.1f} s"
    return format_duration(seconds)


def _period(started_at: str | None, finished_at: str | None) -> str:
    if started_at is None:
        return "The crawl has not started."
    if finished_at is None:
        return f"Started {_time(started_at)}, still running."
    return f"Started {_time(started_at)}, finished {_time(finished_at)}."


def _time(iso: str) -> str:
    return datetime.fromisoformat(iso).astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
