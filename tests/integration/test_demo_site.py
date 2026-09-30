"""Integration tests for the local site of the `errors` demo."""

import asyncio
from urllib.parse import urlsplit

import pytest
from aiohttp import web

import demo_site
from demo_site import DemoSite


async def test_server_that_is_down_refuses_connections():
    async with DemoSite() as site:
        ports = {urlsplit(link).port for link in site.links() if urlsplit(link).hostname == "localhost"}
        assert len(ports) == 1
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("127.0.0.1", ports.pop())


async def test_server_that_is_down_gets_its_port_once_the_site_listens(monkeypatch):
    # A port picked earlier is free again when the site asks the system for
    # one, and the site could get it: the server would not be down then.
    site = DemoSite()
    site_ports = []

    def free_port():
        site_ports.append(urlsplit(site.url).port)
        return real_free_port()

    real_free_port = demo_site.free_port
    monkeypatch.setattr(demo_site, "free_port", free_port)
    async with site:
        pass
    assert len(site_ports) == 1
    assert site_ports[0] != 0


async def test_site_that_fails_to_start_stops_its_runner(monkeypatch):
    cleaned = []
    cleanup = web.AppRunner.cleanup

    async def track_cleanup(runner):
        cleaned.append(runner)
        await cleanup(runner)

    async def fail(site):
        raise OSError("address already in use")

    monkeypatch.setattr(web.AppRunner, "cleanup", track_cleanup)
    monkeypatch.setattr(web.TCPSite, "start", fail)
    with pytest.raises(OSError, match="address already in use"):
        async with DemoSite():
            pass
    assert len(cleaned) == 1
