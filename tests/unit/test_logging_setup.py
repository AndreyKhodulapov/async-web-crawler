"""Unit tests for configure_logging: the console, the JSON Lines file, its rotation, levels, repeated calls."""

import io
import json
import logging
import re
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from crawler import configure_logging
from crawler.logging_setup import JsonLinesFormatter, ProgressAwareHandler, reset_logging

logger = logging.getLogger("crawler.test")


pytestmark = pytest.mark.usefixtures("restore_logging")


def own_handlers() -> list[logging.Handler]:
    """The handlers configure_logging installed; pytest keeps its own on the root logger too."""
    return [handler for handler in logging.getLogger().handlers if getattr(handler, "_crawler_logging", False)]


def read_entries(path: Path) -> list[dict[str, str]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_console_only_by_default(capsys):
    configure_logging()
    logger.info("fetched %s", "https://site/")

    assert [type(handler) for handler in own_handlers()] == [ProgressAwareHandler]
    time, level, name, message = capsys.readouterr().err.rstrip("\n").split(" | ")
    assert re.fullmatch(r"\d\d:\d\d:\d\d", time)
    assert (level.strip(), name, message) == ("INFO", "crawler.test", "fetched https://site/")


def test_console_gets_json_lines_with_the_json_format(tmp_path, capsys):
    path = tmp_path / "crawler.log"
    configure_logging(file=path, console_format="json")
    logger.info("fetched %s", "https://site/")
    logger.warning("two\nlines")

    lines = capsys.readouterr().err.splitlines()
    assert [json.loads(line) for line in lines] == read_entries(path)
    assert [entry["message"] for entry in read_entries(path)] == ["fetched https://site/", "two\nlines"]


def test_every_line_of_the_file_is_json(tmp_path):
    path = tmp_path / "crawler.log"
    configure_logging("DEBUG", path)
    logger.debug("plain")
    logger.warning('two\nlines, "quotes" and %s', "café")

    assert read_entries(path) == [
        {"time": read_entries(path)[0]["time"], "level": "DEBUG", "logger": "crawler.test", "message": "plain"},
        {
            "time": read_entries(path)[1]["time"],
            "level": "WARNING",
            "logger": "crawler.test",
            "message": 'two\nlines, "quotes" and café',
        },
    ]
    assert "café" in path.read_text(encoding="utf-8")  # readable, not \u escapes


def test_time_in_the_file_is_utc_iso_8601(tmp_path):
    path = tmp_path / "crawler.log"
    configure_logging(file=path)
    logger.info("now")

    time = datetime.fromisoformat(read_entries(path)[0]["time"])
    assert time.utcoffset() == timedelta(0)
    assert abs(datetime.now(time.tzinfo) - time) < timedelta(minutes=1)


def test_exception_goes_into_the_record(tmp_path):
    path = tmp_path / "crawler.log"
    configure_logging(file=path)
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logger.exception("page failed")
    logger.info("no exception")

    failed, plain = read_entries(path)
    assert failed["message"] == "page failed"
    assert failed["exception"].startswith("Traceback (most recent call last):")
    assert failed["exception"].endswith("RuntimeError: boom")
    assert "exception" not in plain


def test_file_and_console_get_the_same_records(tmp_path, capsys):
    path = tmp_path / "crawler.log"
    configure_logging("WARNING", path)
    logger.info("hidden")
    logger.warning("shown")
    logger.error("shown too")

    assert [entry["message"] for entry in read_entries(path)] == ["shown", "shown too"]
    console = capsys.readouterr().err
    assert "hidden" not in console
    assert console.count("shown") == 2


def test_debug_records_of_matplotlib_are_left_out(tmp_path):
    log = tmp_path / "crawler.log"
    configure_logging("DEBUG", log)

    logging.getLogger("matplotlib.font_manager").debug("findfont: score(FontEntry(...)) = 10.05")
    logging.getLogger("matplotlib.font_manager").warning("findfont: font family not found")
    logger.debug("ours")

    assert [entry["message"] for entry in read_entries(log)] == ["findfont: font family not found", "ours"]


@pytest.mark.parametrize("level", ["debug", "DEBUG", logging.DEBUG])
def test_level_by_name_in_any_case_or_by_number(level):
    configure_logging(level)
    assert logging.getLogger().level == logging.DEBUG


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"level": "LOUD"}, "unknown logging level: 'LOUD'"),
        ({"max_bytes": -1}, "max_bytes must be >= 0, got -1"),
        ({"backup_count": -1}, "backup_count must be >= 0, got -1"),
        ({"console_format": "xml"}, "console_format must be one of text, json, got 'xml'"),
    ],
)
def test_rejects_invalid_options(options, message):
    with pytest.raises(ValueError, match=message):
        configure_logging(**options)
    assert own_handlers() == []


def test_file_is_rotated_by_size(tmp_path):
    path = tmp_path / "crawler.log"
    configure_logging(file=path, max_bytes=300, backup_count=2)
    for number in range(30):
        logger.info("record %d", number)

    assert sorted(file.name for file in tmp_path.iterdir()) == ["crawler.log", "crawler.log.1", "crawler.log.2"]
    # Oldest file first: the records are in order, the latest ones are kept.
    messages = [
        entry["message"]
        for file in (tmp_path / "crawler.log.2", tmp_path / "crawler.log.1", path)
        for entry in read_entries(file)
    ]
    assert messages == [f"record {number}" for number in range(30 - len(messages), 30)]
    assert all(file.stat().st_size <= 300 for file in tmp_path.iterdir())


@pytest.mark.parametrize(("max_bytes", "backup_count"), [(0, 2), (300, 0)])
def test_file_is_not_rotated_without_a_size_or_backups(tmp_path, max_bytes, backup_count):
    path = tmp_path / "crawler.log"
    configure_logging(file=path, max_bytes=max_bytes, backup_count=backup_count)
    (handler,) = [handler for handler in own_handlers() if isinstance(handler, RotatingFileHandler)]
    stream = handler.stream
    for number in range(30):
        logger.info("record %d", number)

    assert [file.name for file in tmp_path.iterdir()] == ["crawler.log"]
    assert len(read_entries(path)) == 30
    assert handler.stream is stream  # not closed and reopened on every record past the size


def test_file_of_an_earlier_run_is_appended_to(tmp_path):
    path = tmp_path / "crawler.log"
    configure_logging(file=path)
    logger.info("first run")
    configure_logging(file=path)
    logger.info("second run")

    assert [entry["message"] for entry in read_entries(path)] == ["first run", "second run"]


def test_second_call_replaces_the_handlers(tmp_path, capsys):
    first, second = tmp_path / "first.log", tmp_path / "second.log"
    configure_logging(file=first)
    old_file_handler = own_handlers()[1]
    configure_logging(file=second)
    logger.info("once")

    assert len(own_handlers()) == 2
    assert old_file_handler.stream is None  # closed
    assert first.read_text() == ""
    assert len(read_entries(second)) == 1
    assert capsys.readouterr().err.count("once") == 1


def test_handlers_of_other_code_are_kept():
    foreign = logging.NullHandler()
    logging.getLogger().addHandler(foreign)
    try:
        configure_logging()
        configure_logging()
        assert foreign in logging.getLogger().handlers
    finally:
        logging.getLogger().removeHandler(foreign)


def test_file_that_cannot_be_opened_leaves_logging_as_it_was(tmp_path):
    configure_logging("WARNING")
    before = own_handlers()

    with pytest.raises(OSError):
        configure_logging("DEBUG", tmp_path / "missing" / "crawler.log")

    assert own_handlers() == before
    assert logging.getLogger().level == logging.WARNING


def test_reset_removes_the_handlers(tmp_path):
    configure_logging(file=tmp_path / "crawler.log")
    reset_logging()
    assert own_handlers() == []


def test_formatter_keeps_a_record_on_one_line():
    record = logging.LogRecord("crawler", logging.ERROR, __file__, 1, "a\nb", None, None)
    line = JsonLinesFormatter().format(record)
    assert "\n" not in line
    assert json.loads(line)["message"] == "a\nb"


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


def test_progress_line_is_erased_in_a_terminal(monkeypatch):
    stream = Terminal()
    monkeypatch.setattr("sys.stderr", stream)
    configure_logging()
    logger.warning("page failed")

    assert stream.getvalue().startswith("\r\033[K")
    assert stream.getvalue().endswith("page failed\n")


def test_progress_line_is_not_erased_in_a_file_or_pipe(monkeypatch):
    stream = io.StringIO()
    monkeypatch.setattr("sys.stderr", stream)
    configure_logging()
    logger.warning("page failed")

    assert "\033" not in stream.getvalue()
