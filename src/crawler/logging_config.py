"""Logging setup for applications built on top of the crawler.

Library modules only create loggers; configuring handlers is left to the
application entry point, which calls :func:`setup_logging` once.
"""

import logging

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"


def setup_logging(level: int | str = logging.INFO) -> None:
    logging.basicConfig(level=level, format=LOG_FORMAT, datefmt=DATE_FORMAT)
