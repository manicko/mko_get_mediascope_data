"""Network handler — common network calls to the Mediascope API with retries and error handling."""

import asyncio
import logging
import socket
from collections.abc import Callable
from random import uniform
from typing import Any, TypeVar

from mediascope_api.core.errors import (
    AuthorizationError,
    BadRequestError,
    MediascopeApiError,
    ServerError,
    TooManyRequestsError,
)
from mediascope_api.core.net import MediascopeApiNetwork
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ConnectTimeout,
    RetryError,
    Timeout,
)
from urllib3.exceptions import NameResolutionError, ProtocolError, IncompleteRead

from mko_get_mediascope_data.core.errors import CallTimeoutError, MaxRetriesExceeded

logger = logging.getLogger(__name__)

T = TypeVar("T")

# All errors considered transient and worthy of retries
#
# Membership describes exception *type*, not remote write state. ``Timeout``
# also covers ``ReadTimeout``, which only means the response was not received;
# it does not establish that the remote write was unapplied. Retrying such an
# exception may therefore replay a request whose body was already committed
# (see ``SendTaskSettings`` and ``TVMediaReport.send_task``).
RETRIABLE_EXCEPTIONS = (
    ConnectTimeout,
    ConnectionError,
    Timeout,
    RetryError,
    ChunkedEncodingError,
    ConnectionResetError,
    socket.timeout,
    TimeoutError,  # CallTimeoutError (call_timeout deadline in _invoke) subclasses TimeoutError
    ProtocolError,
    IncompleteRead,  # connection reset mid-stream under server throttling
    NameResolutionError,  # ← added: DNS resolution failed
    socket.gaierror,  # ← added: basic name resolution error
)


def _req_to_json_no_dump(req: Any, endpoint: str, data: Any) -> Any:
    """Drop-in replacement for ``MediascopeApiNetwork._req_to_json``.

    The vendor's original ``@staticmethod`` writes a ``json-error-<timestamp>.txt``
    file to the *current working directory* on JSON decode failures.  That
    pollutes the project root and is not thread-safe under
    ``asyncio.to_thread`` (used by :meth:`NetworkClient._invoke`).

    Instead we log the response detail and re-raise ``ServerError(code=500)``
    so the existing 5xx-retry logic in :meth:`NetworkClient.call` continues to
    behave identically — no files are written.
    """
    try:
        return req.json()
    except Exception as exc:
        logger.warning(
            "Mediascope API returned a non-JSON response on endpoint %s "
            "(HTTP %s). Response body (truncated to 1000 chars): %s",
            endpoint,
            getattr(req, "status_code", None),
            (req.text or "")[:1000],
        )
        raise ServerError(
            f"JSON decode error on {endpoint}: {exc}", 500
        ) from exc


def _patch_vendor_json_error_dump() -> None:
    """Suppress the vendor's CWD ``json-error-*.txt`` file writes.

    Must wrap the replacement in ``staticmethod`` so it keeps the same calling
    convention as the original (``self._req_to_json(req, endpoint, data)``).
    """
    MediascopeApiNetwork._req_to_json = staticmethod(_req_to_json_no_dump)


# Apply once at import time so the patch is active for both CLI and library use.
_patch_vendor_json_error_dump()


class NetworkClient:
    """
    Centralized async network handler with:
    - retry logic
    - exponential backoff + jitter
    - concurrency control via Semaphore

    Ready for extension:
    TODO (Architect): can add refresh_token_func: Callable | None = None in the future
    """

    def __init__(
        self,
        max_concurrent_requests: int = 5,
        sleep_time: int = 2,
        max_attempts: int = 10,
        call_timeout: float | None = 300.0,
        retry_5xx: bool = True,
        log_errors_detail: bool = False,
    ):
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._sleep_time = sleep_time
        self._max_attempts = max_attempts
        self._call_timeout = call_timeout
        self._retry_5xx = retry_5xx
        self._log_errors_detail = log_errors_detail
        # Threads retained after ``call_timeout`` elapses (ASY-009).  They keep
        # running the synchronous vendor call and may still hold references to
        # the vendor sessions, so callers must not close those sessions while
        # this set is non-empty (EXT-010).
        self._deferred_threads: set[asyncio.Future[Any]] = set()

    async def _invoke(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute *func* in a worker thread, bounded by ``call_timeout``.

        The dispatched thread keeps running even if ``call_timeout`` elapses, so a
        timeout raises :class:`CallTimeoutError` retaining the inner task wrapping
        ``asyncio.to_thread``.  The deadline is enforced with ``asyncio.wait``
        rather than ``asyncio.wait_for(asyncio.shield(inner), ...)``: ``wait_for``
        cancels the awaited future on timeout, which forces a shield around the
        thread task, and Python 3.14's ``shield`` reports a late failure of that
        task to the loop exception handler ("<Exc> exception in shielded future")
        even though the retained thread's outcome is consumed (ASY-009).
        ``asyncio.wait`` never cancels the awaited task, so the thread task stays
        pending until the thread actually completes, allowing the permit to be
        held for the thread's real lifetime (ASY-009).
        """
        inner = asyncio.ensure_future(asyncio.to_thread(func, *args, **kwargs))
        if self._call_timeout is None:
            return await inner
        try:
            done, _ = await asyncio.wait({inner}, timeout=self._call_timeout)
        except asyncio.CancelledError:
            # Caller cancellation cannot stop the thread; retain it so the
            # deferred-work guard (EXT-010) sees it and consume its terminal
            # outcome instead of surfacing it as an unretrieved exception.
            self._track_deferred(inner)
            inner.add_done_callback(self._consume_thread_result)
            raise
        if not done:
            # Retain the still-running thread so its lifetime is observable
            # (EXT-010 deferred-work guard) and its terminal outcome is consumed.
            # The callback only discards tracking state; permit handling stays
            # with the caller's own callback.
            self._track_deferred(inner)
            raise CallTimeoutError(thread_task=inner) from None
        return await inner

    def _track_deferred(self, thread_task: asyncio.Future[Any]) -> None:
        """Record a retained thread future until it completes (EXT-010 guard)."""
        self._deferred_threads.add(thread_task)
        thread_task.add_done_callback(self._deferred_threads.discard)

    def has_outstanding_deferred(self) -> bool:
        """Return ``True`` while a timed-out thread is still running.

        ``call_timeout`` (ASY-009) lets the synchronous vendor call outlive the
        awaited call; such threads may still reference the vendor sessions.
        Callers releasing those sessions (EXT-010) must defer the close until
        this returns ``False`` so a session is never closed beneath in-flight
        work.
        """
        return bool(self._deferred_threads)

    def _consume_thread_result(self, thread_task: asyncio.Future[Any]) -> None:
        """Consume a retained thread future's outcome without raising."""
        try:
            thread_task.result()
        except BaseException:  # noqa: BLE001 — never raise from a done-callback
            pass

    def _drain_thread_and_release(self, thread_task: asyncio.Future[Any]) -> None:
        """Release one permit and consume the retained thread's terminal outcome."""
        self._semaphore.release()
        self._consume_thread_result(thread_task)

    async def _attempt_with_permit(
        self, func: Callable[..., T], *args: Any, **kwargs: Any
    ) -> T:
        """Run one attempt holding a permit until the thread actually completes.

        On :class:`CallTimeoutError` the permit release is deferred to a
        done-callback on the retained thread task, so the permit is held until the
        synchronous work finishes rather than at the moment the ``call_timeout`` deadline fires.
        """
        deferred_release = False
        await self._semaphore.acquire()
        try:
            return await self._invoke(func, *args, **kwargs)
        except CallTimeoutError as err:
            if err.thread_task is not None:
                err.thread_task.add_done_callback(self._drain_thread_and_release)
            deferred_release = True
            raise
        finally:
            if not deferred_release:
                self._semaphore.release()

    def _redact_args(self, value: Any) -> str:
        """Redact potentially sensitive arguments from error-log messages.

        When ``log_errors_detail`` is ``False`` (default), args and kwargs are
        replaced with ``"<redacted>"`` to avoid leaking PII (e.g. userName in
        task JSON).  Set ``log_errors_detail=True`` for debugging.
        """
        if self._log_errors_detail:
            return repr(value)
        return "<redacted>"

    async def call(
        self,
        func: Callable[..., T],
        *args: Any,
        sleep_time: int | float = 2,
        retryable: bool = True,
        api_call: bool = True,
        retry_on: tuple[type[BaseException], ...] | None = None,
        task_name: str | None = None,
        **kwargs: Any,
    ) -> T:
        """Execute *func* with retry logic and concurrency control.

        Parameters
        ----------
        func
            The callable to execute (typically a vendor API method).
        sleep_time
            Base backoff time in seconds; effective sleep is
            ``max(sleep_time, self._sleep_time)``.
        retryable
            If ``False``, the function is called exactly once (no retry loop).
            Equivalent to ``retry_on=()`` when ``retry_on`` is not given.
        api_call
            If ``False``, bypasses the initial jitter sleep, semaphore, and retry
            loop entirely — used for CPU-bound calls (e.g. ``build_task``).
        retry_on
            When provided, only exception types in this tuple are retried.
            All other exceptions are re-raised immediately.  This overrides
            ``retryable=False`` for the specified exception types, allowing
            selective retry on a type subset (e.g. transport-level errors)
            for POST calls where HTTP-level errors should not be retried.
            Note that retrying a type subset does not prove the remote write
            was unapplied — ``ReadTimeout`` may follow a committed request
            body — so ensuring unique task creation is best-effort, not a
            guarantee.  When ``None``, all ``RETRIABLE_EXCEPTIONS`` and
            429/5xx errors are retried.
        task_name
            Optional task name appended to log messages (``task=<name>``)
            for correlation when hundreds of concurrent tasks are running.
            When ``None`` (default), log messages remain unchanged.

        Returns
        -------
        T
            The return value of *func*.
        """
        if not api_call:
            try:
                return await self._invoke(func, *args, **kwargs)
            except CallTimeoutError as err:
                if err.thread_task is not None:
                    err.thread_task.add_done_callback(self._consume_thread_result)
                raise

        _sleep_time = max(sleep_time, self._sleep_time)
        _task_ctx = f" task={task_name}" if task_name is not None else ""

        # Small initial delay to smooth peak loads
        await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))

        if not retryable and retry_on is None:
            # Legacy behavior: single attempt, no retry loop at all.
            return await self._attempt_with_permit(func, *args, **kwargs)

        for attempt in range(1, self._max_attempts + 1):
            try:
                return await self._attempt_with_permit(func, *args, **kwargs)

            except BadRequestError as err:
                logger.error(f"Bad request. Check configuration. {err}{_task_ctx}")
                raise

            except AuthorizationError as err:
                # 401 — is a known client error, not "Unexpected"
                logger.error(
                    f'Authorization error (token invalid/expired) in "{func.__name__}" '
                    f'with args="{self._redact_args(args)}" and kwargs='
                    f'"{self._redact_args(kwargs)}": {err}{_task_ctx}'
                )
                raise

            except FileNotFoundError as err:
                logger.error(
                    f'File not found: "{err}" in function "{func.__name__}" '
                    f'with args="{self._redact_args(args)}" and kwargs='
                    f'"{self._redact_args(kwargs)}"{_task_ctx}'
                )
                raise

            except RETRIABLE_EXCEPTIONS as err:
                # When retry_on is specified, only retry the named exception types.
                # Others (even if technically retryable) are re-raised immediately.
                # This is used for POST calls (send_task) to limit replay to a
                # selected transport-error subset.  Retrying that subset does not
                # establish that a prior request body was unapplied (a lost-response
                # timeout may follow a committed write), so unique task creation
                # remains best-effort rather than guaranteed.
                if retry_on is not None and not isinstance(err, retry_on):
                    logger.error(
                        f'Rejected error type "{type(err).__name__}" '
                        f'for "{func.__name__}" (not in retry_on): {err}{_task_ctx}'
                    )
                    raise
                logger.warning(
                    f"Network error ({type(err).__name__}) "
                    f"attempt {attempt}/{self._max_attempts}: {err}{_task_ctx}"
                )

            except TooManyRequestsError as err:
                # 429 — vendor does not expose Retry-After header on the
                # exception (verified); falls back to exponential backoff below.
                # When retry_on is specified (e.g. send_task), 429 is an HTTP-level
                # response that is re-raised immediately rather than replayed by
                # the application.
                if retry_on is not None:
                    logger.error(
                        f"HTTP 429 on retry_on-limited call to "
                        f'"{func.__name__}" — re-raising (HTTP response, not retried){_task_ctx}'
                    )
                    raise
                logger.warning(
                    f"Rate limited (429) on attempt "
                    f"{attempt}/{self._max_attempts}: {err}{_task_ctx}"
                )

            except MediascopeApiError as err:
                # Unhandled HTTP status (403, 404, 204, 500, 502, etc.).
                # When retry_on is specified (e.g. send_task), ALL HTTP-level errors
                # are re-raised immediately so the application does not replay a
                # response it already received.
                if retry_on is not None:
                    code = getattr(err, "code", None)
                    logger.error(
                        f"HTTP error ({code}) on retry_on-limited call to "
                        f'"{func.__name__}" — re-raising (HTTP response, not retried){_task_ctx}'
                    )
                    raise
                # 5xx is retried when retry_5xx=True; all others re-raise.
                code = getattr(err, "code", None)
                if self._retry_5xx and code is not None and 500 <= code < 600:
                    logger.warning(
                        f"Server error ({code}) on attempt "
                        f"{attempt}/{self._max_attempts}: {err}{_task_ctx}"
                    )
                else:
                    logger.error(
                        f"HTTP error ({code}) for {func.__name__}: {err}{_task_ctx}"
                    )
                    raise

            except Exception as err:
                logger.exception(
                    f'Unexpected error in "{func.__name__}" '
                    f'with args="{self._redact_args(args)}" and kwargs='
                    f'"{self._redact_args(kwargs)}": {err}{_task_ctx}'
                )
                raise

            # backoff + jitter
            base = 1.2
            backoff = _sleep_time * (base ** (attempt - 1))
            jitter = uniform(0, 5)
            await asyncio.sleep(backoff + jitter)

        logger.error(
            f'Max retries exceeded for "{func.__name__}" '
            f'with args="{self._redact_args(args)}" and kwargs='
            f'"{self._redact_args(kwargs)}"{_task_ctx}'
        )
        raise MaxRetriesExceeded("Network retry limit exceeded")
