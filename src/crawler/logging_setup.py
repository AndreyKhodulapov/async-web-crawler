"""Sets up logging: text on the console and, optionally, JSON Lines in a rotated file."""

import json
import logging
import os
import sys
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

CONSOLE_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
CONSOLE_DATE_FORMAT = "%H:%M:%S"

# Marks the handlers `configure_logging` installed, so that a later call
# replaces them and leaves the handlers of the application alone.
_OWNED = "_crawler_logging"


class ProgressAwareHandler(logging.StreamHandler):
    """Writes log records to stderr, erasing the live progress line first.

    The progress line ends with "\r" instead of a newline, so a record would
    otherwise be glued to its end. The next progress update redraws it below.
    """

    def __init__(self) -> None:
        super().__init__(sys.stderr)
        self._live = sys.stderr.isatty()

    def emit(self, record: logging.LogRecord) -> None:
        if self._live:
            # Like StreamHandler.emit: a failed write (closed terminal, broken
            # pipe) must not raise into the code that logged the record.
            try:
                self.stream.write("\r\033[K")
            except (OSError, ValueError):
                self.handleError(record)
                return
        super().emit(record)


class JsonLinesFormatter(logging.Formatter):
    """A log record as one line of JSON.

    Keys: `time` (UTC, ISO 8601 with milliseconds), `level`, `logger`,
    `message` and, for a record logged with an exception, `exception`: its
    traceback. Line breaks are escaped, so a record never takes two lines.
    """

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def configure_logging(
    level: int | str = "INFO",
    file: str | os.PathLike[str] | None = None,
    *,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """Send the records of `level` and above to the console and, with `file`, to that file too.

    The console (stderr) gets text lines: time, level, logger, message. The
    file gets JSON Lines (see `JsonLinesFormatter`) in UTF-8 and is appended
    to. Once it reaches `max_bytes` it is renamed to `file.1` (the older
    ones to `file.2` and so on, `backup_count` of them are kept) and a new
    one is started; with `max_bytes` or `backup_count` of 0 the file is
    never rotated.

    The root logger is configured, so the records of other libraries are
    written as well; of matplotlib, which draws the charts of the HTML
    report, only warnings and errors are. Calling this again replaces the handlers of the
    previous call; handlers added by other code are left in place.

    Raises:
        ValueError: `level` is not a logging level, or a limit is negative.
        OSError: `file` cannot be opened; its directory is not created.
    """
    if max_bytes < 0:
        raise ValueError(f"max_bytes must be >= 0, got {max_bytes}")
    if backup_count < 0:
        raise ValueError(f"backup_count must be >= 0, got {backup_count}")
    if isinstance(level, str):
        level = level.upper()
        if level not in logging.getLevelNamesMapping():
            raise ValueError(f"unknown logging level: {level!r}")

    handlers: list[logging.Handler] = [ProgressAwareHandler()]
    handlers[0].setFormatter(logging.Formatter(CONSOLE_FORMAT, datefmt=CONSOLE_DATE_FORMAT))
    if file is not None:
        # Opened before the old handlers are removed: a file that cannot be
        # opened leaves the logging as it was.
        file_handler = RotatingFileHandler(
            Path(file).expanduser(), maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        file_handler.setFormatter(JsonLinesFormatter())
        handlers.append(file_handler)

    reset_logging()
    root = logging.getLogger()
    root.setLevel(level)
    # At DEBUG matplotlib logs every font it looks at: about a thousand
    # records for one report, among which those of the crawl are lost.
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    for handler in handlers:
        setattr(handler, _OWNED, True)
        root.addHandler(handler)


def reset_logging() -> None:
    """Remove and close the handlers `configure_logging` installed; the level stays."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _OWNED, False):
            root.removeHandler(handler)
            handler.close()
