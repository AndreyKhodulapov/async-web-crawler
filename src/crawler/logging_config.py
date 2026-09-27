"""Logging setup for applications built on top of the crawler.

Library modules only create loggers; configuring handlers is left to the
application entry point, which calls `setup_logging` once.
"""

import logging


def setup_logging(level: int | str = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
