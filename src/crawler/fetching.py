"""Request layer of the crawler: one URL fetched politely, with its redirects and retries."""

import asyncio
import contextlib
import dataclasses
import itertools
import logging
import time
from collections.abc import AsyncGenerator, Awaitable, Callable

import aiohttp

from crawler.circuit_breaker import BreakerCall, CircuitBreaker
from crawler.error_stats import ErrorTracker
from crawler.exceptions import (
    CircuitOpenError,
    CrawlerClosedError,
    FetchError,
    FetchTimeoutError,
    HostHeldBackError,
    HTTPStatusError,
    InvalidURLError,
    ProxyError,
    ProxyNetworkError,
    RenderTimeoutError,
    RobotsDisallowedError,
    RobotsUnreachableError,
    TooManyRedirectsError,
    UnexpectedError,
)
from crawler.models import FetchResult
from crawler.rate_limiter import HostPenalizedError, RateLimiter
from crawler.retry import RetryStrategy
from crawler.robots import RobotsParser, robots_tag_directives
from crawler.semaphores import SemaphoreManager
from crawler.sitemap import SitemapParser
from crawler.transport import Transport
from crawler.urls import get_host, resolve_url

logger = logging.getLogger(__name__)


class Fetcher:
    """Fetches one URL at a time as a polite client must, knowing nothing of a crawl.

    Every request goes through robots.txt of its site, the circuit breaker,
    the rate limit and the concurrency limits of its host, and is retried
    as `retry_strategy` says; redirects are followed one request at a
    time, each target checked the same way. `fetch()` reports every
    outcome in its `FetchResult`. The requests are sent by `transport` (see `Transport`).

    `robots` and `sitemaps` download through it. `errors` and `retries`
    count the attempts since the last `reset_stats()`. A host held back
    after a Retry-After, or for the pause before a retry that the whole
    host waits out, is told to `on_host_held`, if it is set: the crawl of
    several processes holds the host back in all of them.
    """

    MAX_TIMEOUT_GROWTH = 4.0
    MAX_REDIRECTS = 10
    # The constants the crawler sets on its fetcher.
    SETTINGS = ("MAX_TIMEOUT_GROWTH", "MAX_REDIRECTS")

    def __init__(
        self,
        transport: Transport,
        *,
        limits: SemaphoreManager,
        rate_limiter: RateLimiter,
        retry_strategy: RetryStrategy,
        circuit_breaker: CircuitBreaker,
        respect_robots: bool,
        timeout: aiohttp.ClientTimeout,
        timeout_growth: float,
        max_retry_after: float,
        user_agent: str,
    ) -> None:
        self._transport = transport
        self._limits = limits
        self.rate_limiter = rate_limiter
        self.retry_strategy = retry_strategy
        self.circuit_breaker = circuit_breaker
        self.robots = RobotsParser(self._download_robots) if respect_robots else None
        self.sitemaps = SitemapParser(self._download_sitemap)
        self._timeout = timeout
        self.timeout_growth = timeout_growth
        self.max_retry_after = max_retry_after
        self._user_agent = user_agent
        self.errors = ErrorTracker()
        self.retries = 0
        # Called with the host, the seconds it is held back for and why; see tell_host_held.
        self.on_host_held: Callable[[str, float, str | None], Awaitable[None]] | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def reset_stats(self) -> None:
        """Count errors and retries anew, and the requests of the transport."""
        self.errors = ErrorTracker()
        self.retries = 0
        self._transport.reset_stats()

    async def close(self) -> None:
        """Refuse further requests and close the transport. Safe to call more than once."""
        self._closed = True
        await self._transport.close()

    async def fetch(
        self,
        url: str,
        *,
        html_only: bool = False,
        raw: bool = False,
        truncate_at: int | None = None,
        check_robots: bool = True,
        check_redirect_robots: bool = True,
        follow: Callable[[str], Awaitable[bool]] | None = None,
        failure_level: int = logging.WARNING,
        track_errors: bool = True,
        retry: bool = True,
        robots_wait: float | None = None,
        max_wait: float | None = None,
    ) -> FetchResult:
        """Download a page, following its redirects one at a time.

        Every request of the chain goes through robots.txt, the circuit
        breaker, the rate limit and the retries of its own host, as a link
        to the target would (see `_fetch_hop`). robots.txt is checked for
        `url` with `check_robots`, for the targets of its redirects with
        `check_redirect_robots`. `follow`, if given, is asked about every
        target, normalized, before it is requested; when it says no, the
        result is that of the redirect itself: no content, `redirected`
        set and `final_url` the target. More than `MAX_REDIRECTS` redirects
        in a row fail with `TooManyRedirectsError`.

        With `raw`, which is how a sitemap is downloaded, the result has the
        `body` as it was sent instead of the decoded `content`, and a body
        over `sitemaps.MAX_SIZE` fails with `SitemapError`. With
        `truncate_at`, which is how robots.txt is downloaded, the body is
        cut to that many bytes instead of failing over `max_page_size`. With
        `track_errors`, the attempts count in `errors`. Without
        `retry`, every request of the chain is a single attempt at its
        site: one that failed in a proxy is still made again. With
        `robots_wait`, a download of robots.txt is waited for that many
        seconds at most (see `check_robots`). With `max_wait`, a request of
        the chain whose host is held back longer than that (see
        `RateLimiter.penalize`) is not sent and fails with
        `HostHeldBackError`, which is neither retried nor counted in
        `errors`; a crawl of several workers puts its page back and takes
        another one meanwhile. A retry waits out its pause however long:
        the page put back would start its retries anew.
        """
        target, elapsed, retried, attempts = url, 0.0, False, 0
        for redirects in itertools.count():
            result, attempts = await self._fetch_hop(
                target,
                html_only=html_only,
                raw=raw,
                truncate_at=truncate_at,
                check_robots=check_robots if redirects == 0 else check_redirect_robots,
                failure_level=failure_level,
                track_errors=track_errors,
                retry=retry,
                robots_wait=robots_wait,
                max_wait=max_wait,
            )
            elapsed += result.elapsed
            retried = retried or attempts > 1
            if not result.redirected:
                break
            if redirects == self.MAX_REDIRECTS:
                # Checked before the target is asked about: it is not reached.
                error = TooManyRedirectsError(url, f"too many redirects (more than {self.MAX_REDIRECTS})")
                result = FetchResult.failure(url, error, elapsed)
                break
            assert result.final_url is not None
            location = resolve_url(result.final_url, target)
            if location is None:
                error = InvalidURLError(target, f"redirects to an invalid URL: {result.final_url!r}")
                result = FetchResult.failure(url, error, elapsed)
                break
            if follow is not None and not await follow(location):
                result = dataclasses.replace(result, final_url=location)
                break
            logger.info("Redirect %s -> %s (%d)", target, location, result.status)
            target = location
        # A request refused before it was sent (robots.txt, the circuit
        # breaker) has no outcome to count.
        if track_errors and attempts:
            self.errors.record_outcome(url, result.error, retried=retried)
        if result.error is None and redirects:
            result = dataclasses.replace(result, redirected=True)
        return dataclasses.replace(result, url=url, elapsed=elapsed)

    async def _fetch_hop(
        self,
        url: str,
        *,
        html_only: bool,
        raw: bool,
        truncate_at: int | None,
        check_robots: bool,
        failure_level: int,
        track_errors: bool,
        retry: bool,
        robots_wait: float | None,
        max_wait: float | None,
    ) -> tuple[FetchResult, int]:
        """Make the request for one URL, checking robots.txt first and retrying transient failures.

        A redirect is not followed: it comes back as a result with
        `redirected` set and `final_url` its Location header as sent. Also
        returns the number of attempts made, 0 if the request was refused
        before it was sent. Without `retry`, the request is a single attempt
        at the site: a proxy that failed is passed over all the same. The
        first attempt gives up on a host held back longer than `max_wait`.
        """
        # Checked up front as well as by the transport: a closed crawler must
        # report itself even for a URL that robots.txt would block.
        if self._closed:
            return FetchResult.failure(url, CrawlerClosedError(url, "crawler is closed"), 0.0), 0
        if check_robots and (refusal := await self.check_robots(url, wait=robots_wait)) is not None:
            return FetchResult.failure(url, refusal, 0.0), 0
        # Refused at once, without waiting for the turn of the host.
        if (refusal := self.check_circuit(url)) is not None:
            logger.info("Refused %s: %s", url, refusal.message)
            return FetchResult.failure(url, refusal, 0.0), 0
        # The strategy needs an attempt that raises; the result of the last
        # one is kept to report a failure with its timing.
        last: FetchResult | None = None
        attempts = 0
        failed_at: float | None = None  # when the previous attempt failed
        # Shared by the attempts: the request counts once in the breaker's window.
        call = self.circuit_breaker.call(url)

        async def attempt() -> FetchResult:
            nonlocal last, attempts, failed_at
            timeout = self._timeout_for(retries=attempts)
            result = await self._fetch_once(
                url,
                html_only=html_only,
                raw=raw,
                truncate_at=truncate_at,
                timeout=timeout,
                call=call,
                max_wait=max_wait if attempts == 0 else None,
            )
            if isinstance(result.error, HostHeldBackError):
                # Not sent, and nothing for the strategy to retry or report.
                return result
            attempts += 1
            if isinstance(result.error, CircuitOpenError):
                # Not sent. A retry fails as the attempt before it did: the
                # strategy sees the circuit open and stops, and the failure
                # reported is that of a request that was sent.
                last = last or result
                assert last.error is not None
                raise last.error
            last = result
            if attempts > 1:
                self.retries += 1
            if track_errors:
                now = time.perf_counter()
                if failed_at is not None:
                    self.errors.record_retry(now - failed_at)
                if last.error is not None:
                    failed_at = now
                    self.errors.record_error(last.error)
            if last.error is not None:
                raise last.error
            return last

        def veto(error: Exception) -> str | None:
            # A proxy that failed says nothing of the site: the attempt is still owed.
            if not retry and not isinstance(error, ProxyNetworkError):
                return "a single attempt was asked for"
            # A retry the circuit breaker would refuse is not waited for.
            return self.circuit_breaker.refusal(url)

        try:
            result = await self.retry_strategy.run(
                attempt, wait=self._wait_before_retry, target=url, failure_level=failure_level, veto=veto
            )
        except FetchError as error:
            assert last is not None and last.error is error
            host = get_host(url)
            # The server asked to wait: the other requests to the host wait
            # as long as it asked, even when this one is not retried.
            if isinstance(error, HTTPStatusError) and error.retry_after and host is not None:
                if error.retry_after > self.max_retry_after:
                    logger.warning(
                        "%s asked to wait %gs (Retry-After), waiting %gs", host, error.retry_after, self.max_retry_after
                    )
                seconds = min(error.retry_after, self.max_retry_after)
                self.rate_limiter.penalize(host, seconds)
                await self.tell_host_held(host, seconds, f"{error.message}, Retry-After {error.retry_after:g}s")
            return last, attempts
        return result, attempts

    async def _wait_before_retry(self, error: Exception, delay: float) -> None:
        """Wait `delay` seconds before the retry; a failure that speaks for the whole host holds the host back.

        HTTP 429, a Retry-After header or a timeout usually means the whole
        site is overloaded: the pause is spent in the rate limiter, so that
        the retry and every other request to the host wait for it. Any
        other failure (HTTP 500, a reset connection, a page the browser
        took too long to render) is taken to be about the one page: only
        this request sleeps, and the host is asked for its other pages
        meanwhile.
        """
        assert isinstance(error, FetchError)  # _fetch_once() reports every failure as one
        if not _signals_overload(error):
            await asyncio.sleep(delay)
            return
        host = get_host(error.url)
        assert host is not None  # an invalid URL fails with an error that is not retried
        self.rate_limiter.penalize(host, delay)
        await self.tell_host_held(host, delay, f"{error.message}, pause before a retry")

    async def tell_host_held(self, host: str, seconds: float, reason: str | None) -> None:
        """Tell `on_host_held`, if it is set, that `host` is held back for `seconds` from now, and why.

        `reason` may be None when the caller does not know it. A failure to
        tell is logged: the host is held back in this process all the same,
        and the request goes on.
        """
        if self.on_host_held is None:
            return
        try:
            await self.on_host_held(host, seconds, reason)
        except Exception:
            logger.warning("Could not tell the other workers that %s is held back", host, exc_info=True)

    def _timeout_for(self, retries: int) -> aiohttp.ClientTimeout:
        """Timeouts of a request after `retries` failed attempts."""
        try:
            growth = min(self.timeout_growth**retries, self.MAX_TIMEOUT_GROWTH)
        except OverflowError:
            growth = self.MAX_TIMEOUT_GROWTH
        base = self._timeout
        assert base.total is not None and base.connect is not None and base.sock_read is not None
        return aiohttp.ClientTimeout(
            total=base.total * growth,
            connect=base.connect * growth,
            sock_read=base.sock_read * growth,
        )

    async def check_robots(self, url: str, *, wait: float | None = None) -> FetchError | None:
        """Return the error to fail `url` with if robots.txt does not allow it, None if it may be fetched.

        With `wait`, a download of robots.txt that takes longer than that
        many seconds is not waited for: the URL is refused with
        `RobotsUnreachableError` for now, and the download goes on.
        """
        host = get_host(url)
        if self.robots is None or host is None:
            return None  # an invalid URL fails in the transport with InvalidURLError
        try:
            allowed = await self.robots.is_allowed(url, self._user_agent, wait=wait)
        except (CrawlerClosedError, CircuitOpenError, ProxyError) as error:
            # Raised for the robots.txt URL; the page fails for the same reason under its own.
            return type(error)(url, error.message)
        except TimeoutError:
            return RobotsUnreachableError(url, "robots.txt is being downloaded")
        crawl_delay = self.robots.get_crawl_delay(url, self._user_agent)
        if crawl_delay:
            self.rate_limiter.set_delay(host, crawl_delay)
        if allowed:
            return None
        unreachable = self.robots.unreachable_reason(url)
        refusal: FetchError
        if unreachable is None:
            refusal = RobotsDisallowedError(url, "disallowed by robots.txt")
        else:
            refusal = RobotsUnreachableError(url, f"robots.txt is unreachable ({unreachable})")
        logger.info("Blocked %s: %s", url, refusal.message)
        return refusal

    def check_circuit(self, url: str) -> CircuitOpenError | None:
        """The error to fail `url` with if the circuit breaker of its host refuses it, None if it may be fetched."""
        try:
            self.circuit_breaker.check(url)
        except CircuitOpenError as error:
            return error
        return None

    async def _download_robots(self, url: str) -> tuple[int, str]:
        """Fetcher for RobotsParser: robots.txt goes through the same limits and retries as a page.

        A download after a failed one is a single attempt: the site is
        known to be unreachable, and the retries with their growing
        timeouts would hold the page that started it for minutes. An
        attempt that failed in a proxy is made again through another one
        all the same: the page would otherwise fail for it.
        """
        assert self.robots is not None  # it is asking
        # Many sites have no robots.txt; RobotsParser logs the outcomes that matter.
        result = await self.fetch(
            url,
            truncate_at=RobotsParser.MAX_SIZE,
            # Its redirects too: their robots.txt may be the one being downloaded.
            check_robots=False,
            check_redirect_robots=False,
            failure_level=logging.INFO,
            track_errors=False,
            retry=self.robots.failed_downloads(url) == 0,
        )
        if isinstance(result.error, HTTPStatusError):
            return result.error.status, ""
        if result.error is not None:
            raise result.error
        assert result.status is not None and result.content is not None
        return result.status, result.content

    async def _download_sitemap(self, url: str) -> bytes:
        """Fetcher for SitemapParser: a sitemap goes through robots.txt, the limits and retries as a page does."""
        # Not decoded: a sitemap may be gzipped. Whoever asked for the sitemap logs the failure.
        result = await self.fetch(url, raw=True, failure_level=logging.INFO, track_errors=False)
        if result.error is not None:
            raise result.error
        assert result.body is not None
        return result.body

    async def _fetch_once(
        self,
        url: str,
        *,
        html_only: bool,
        raw: bool,
        truncate_at: int | None,
        timeout: aiohttp.ClientTimeout,
        call: BreakerCall,
        max_wait: float | None,
    ) -> FetchResult:
        """Make one request under the breaker `call` of `url`, unless the circuit breaker of the host refuses it.

        Nor is it made when its host is held back longer than `max_wait`.
        A probe of a half-open circuit it took is let go then.
        """
        host = get_host(url)

        @contextlib.asynccontextmanager
        async def gate() -> AsyncGenerator[None, None]:
            # Asked again after the wait for the rate limit, as the circuit
            # may have opened meanwhile; a refused request does not wait for
            # a slot and does not count as sent.
            call.admit()
            async with self._limits.slot(url):
                # And once more with the slot: the request that held it
                # before may have opened the circuit.
                call.admit()
                yield

        try:
            # Admitted before the wait, so that of the requests to a
            # half-open circuit only the probe waits for its turn.
            with call:
                # The rate limit is waited for before taking a concurrency slot,
                # so a request waiting for its host does not hold a slot another
                # host could use; inside the slot the interval is checked once more.
                async with gate() if host is None else self.rate_limiter.slot(host, gate, max_wait=max_wait):
                    result = await self._send(
                        url, html_only=html_only, raw=raw, truncate_at=truncate_at, timeout=timeout
                    )
                    call.record(result.error)
                    return result
        except CircuitOpenError as error:
            return FetchResult.failure(url, error, 0.0)
        except HostPenalizedError as held:
            return FetchResult.failure(url, HostHeldBackError(url, str(held), seconds=held.seconds), 0.0)

    async def _send(
        self, url: str, *, html_only: bool, raw: bool, truncate_at: int | None, timeout: aiohttp.ClientTimeout
    ) -> FetchResult:
        """Send the request and report its outcome, whatever it is, as a FetchResult."""
        logger.info("Fetching %s", url)
        started = time.perf_counter()
        try:
            response = await self._transport.get(
                url,
                html_only=html_only,
                # Read on every request, as the parser reads it on every sitemap.
                raw_limit=self.sitemaps.MAX_SIZE if raw else None,
                truncate_at=truncate_at,
                timeout=timeout,
            )
        except FetchError as error:
            elapsed = time.perf_counter() - started
            # RetryStrategy logs the failure along with what comes next.
            logger.debug("Request to %s failed after %.2fs: %s: %s", url, elapsed, type(error).__name__, error.message)
            return FetchResult.failure(url, error, elapsed)
        except Exception as exc:
            # Last line of defense: a bug or an error type we did not
            # anticipate fails only this URL instead of cancelling the
            # whole batch. The traceback is logged so it stays visible.
            elapsed = time.perf_counter() - started
            logger.exception("Unexpected error for %s after %.2fs", url, elapsed)
            error = UnexpectedError(url, f"{type(exc).__name__}: {exc}")
            error.__cause__ = exc
            return FetchResult.failure(url, error, elapsed)

        elapsed = time.perf_counter() - started
        logger.info(
            "Fetched %s: status=%d size=%dB elapsed=%.2fs",
            url,
            response.status,
            response.size,
            elapsed,
        )
        return FetchResult(
            url=url,
            elapsed=elapsed,
            status=response.status,
            content=response.content,
            size=response.size,
            final_url=response.final_url,
            content_type=response.content_type,
            redirected=response.redirected,
            body=response.body,
            robots_tag=robots_tag_directives(response.robots_tag, self._user_agent),
        )


def _signals_overload(error: FetchError) -> bool:
    """Whether the failure says the whole host is overloaded, not one page: HTTP 429, a Retry-After header or a timeout.

    A timeout of rendering does not: the host answered in time.
    """
    if isinstance(error, HTTPStatusError):
        return error.status == 429 or bool(error.retry_after)
    return isinstance(error, FetchTimeoutError) and not isinstance(error, RenderTimeoutError)
