"""A forward HTTP proxy for the tests: requests in absolute form, and CONNECT tunnels for https."""

import asyncio
import contextlib
from urllib.parse import urlsplit


class ProxyServer:
    """A local HTTP proxy that tells what went through it.

    An http:// request comes in absolute form ("GET http://site/page") and
    is passed to the site in origin form, with Connection: close both ways
    and without the Proxy-Authorization header. An https:// one comes as
    CONNECT, and the bytes of the tunnel are passed both ways as they are.

    `requests` lists the request lines as they came ("GET http://...",
    "CONNECT host:port"); `authorizations` their Proxy-Authorization
    headers, None when there was none. With `authorization` set, a request
    without that header gets HTTP 407. `connect_status` answers CONNECT with
    that status instead of opening the tunnel. A request whose target holds
    `drop` gets no answer: its connection is closed.
    """

    def __init__(self, *, authorization: str | None = None, connect_status: int = 200, drop: str | None = None) -> None:
        self.authorization = authorization
        self.connect_status = connect_status
        self.drop = drop
        self.requests: list[str] = []
        self.authorizations: list[str | None] = []
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def hosts(self) -> set[str]:
        """The hosts the requests went to."""
        found = set()
        for line in self.requests:
            method, target = line.split(" ", 1)
            found.add(target.rpartition(":")[0] if method == "CONNECT" else urlsplit(target).hostname)
        return found

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)

    async def close(self) -> None:
        assert self._server is not None
        self._server.close()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        try:
            await self._handle(reader, writer)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self._tasks.discard(task)
            writer.close()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
        line, *header_lines = head.rstrip("\r\n").split("\r\n")
        headers = [tuple(part.strip() for part in header.split(":", 1)) for header in header_lines]
        authorization = next((value for name, value in headers if name.lower() == "proxy-authorization"), None)
        self.requests.append(line.rpartition(" ")[0])
        self.authorizations.append(authorization)
        method, target, _ = line.split(" ")
        if self.authorization is not None and authorization != self.authorization:
            await _answer(writer, 407, "Proxy Authentication Required", 'Proxy-Authenticate: Basic realm="test"\r\n')
            return
        if self.drop is not None and self.drop in target:
            return
        if method == "CONNECT":
            await self._tunnel(target, reader, writer)
            return
        parts = urlsplit(target)
        site_reader, site_writer = await asyncio.open_connection(parts.hostname, parts.port or 80)
        try:
            path = parts.path + (f"?{parts.query}" if parts.query else "")
            kept = "".join(
                f"{name}: {value}\r\n"
                for name, value in headers
                if name.lower() not in ("proxy-authorization", "proxy-connection", "connection")
            )
            site_writer.write(f"{method} {path} HTTP/1.1\r\n{kept}Connection: close\r\n\r\n".encode("latin-1"))
            await site_writer.drain()
            response_head = (await site_reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            status_line, *response_lines = response_head.rstrip("\r\n").split("\r\n")
            kept = "".join(f"{line}\r\n" for line in response_lines if not line.lower().startswith("connection:"))
            writer.write(f"{status_line}\r\n{kept}Connection: close\r\n\r\n".encode("latin-1"))
            await _pipe(site_reader, writer)
        finally:
            site_writer.close()

    async def _tunnel(self, target: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.connect_status != 200:
            await _answer(writer, self.connect_status, "Bad Gateway" if self.connect_status == 502 else "Refused")
            return
        host, _, port = target.rpartition(":")
        site_reader, site_writer = await asyncio.open_connection(host, int(port))
        try:
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()
            await asyncio.gather(_pipe(reader, site_writer), _pipe(site_reader, writer))
        finally:
            site_writer.close()


async def _answer(writer: asyncio.StreamWriter, status: int, reason: str, headers: str = "") -> None:
    writer.write(f"HTTP/1.1 {status} {reason}\r\n{headers}Content-Length: 0\r\nConnection: close\r\n\r\n".encode())
    await writer.drain()


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Pass the bytes of `reader` to `writer` until the end of the stream."""
    with contextlib.suppress(ConnectionError):
        while chunk := await reader.read(64 * 1024):
            writer.write(chunk)
            await writer.drain()
        if writer.can_write_eof():
            writer.write_eof()
