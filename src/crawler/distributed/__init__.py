"""Distributed crawling: the frontier of a crawl job shared by workers through PostgreSQL."""

from crawler.distributed.frontier import PostgresFrontier

__all__ = ["PostgresFrontier"]
