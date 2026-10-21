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

from mko_get_mediascope_data.core.errors import MaxRetriesExceeded

logger = logging.getLogger(__name__)

T = TypeVar("T")

# All errors considered transient and worthy of retries
RETRIABLE_EXCEPTIONS = (
    ConnectTimeout,
    ConnectionError,
    Timeout,
    RetryError,
    ChunkedEncodingError,
    ConnectionResetError,
    socket.timeout,
    TimeoutError,  # asyncio.wait_for cancellation raises TimeoutError (= asyncio.TimeoutError in 3.11+)
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

    async def _invoke(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute *func* in a worker thread, bounded by ``call_timeout``."""
        coro = asyncio.to_thread(func, *args, **kwargs)
        if self._call_timeout is not None:
            return await asyncio.wait_for(coro, timeout=self._call_timeout)
        return await coro

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
            selective retry on transport-level errors (SSL, timeout, connection
            reset) even for POST calls where HTTP-level errors should not be
            retried (to avoid duplicate task creation).  When ``None``, all
            ``RETRIABLE_EXCEPTIONS`` and 429/5xx errors are retried.
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
            return await self._invoke(func, *args, **kwargs)

        _sleep_time = max(sleep_time, self._sleep_time)
        _task_ctx = f" task={task_name}" if task_name is not None else ""

        # Small initial delay to smooth peak loads
        await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))

        if not retryable and retry_on is None:
            # Legacy behavior: single attempt, no retry loop at all.
            async with self._semaphore:
                return await self._invoke(func, *args, **kwargs)

        for attempt in range(1, self._max_attempts + 1):
            try:
                async with self._semaphore:
                    return await self._invoke(func, *args, **kwargs)

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
                # This is used for POST calls (send_task) where HTTP-level errors
                # indicate the request may have been committed and should not be
                # retried to avoid duplicate task creation.
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
                # response that may have been committed — re-raise immediately.
                if retry_on is not None:
                    logger.error(
                        f"HTTP 429 on retry_on-limited call to "
                        f'"{func.__name__}" — re-raising (may have been committed){_task_ctx}'
                    )
                    raise
                logger.warning(
                    f"Rate limited (429) on attempt "
                    f"{attempt}/{self._max_attempts}: {err}{_task_ctx}"
                )

            except MediascopeApiError as err:
                # Unhandled HTTP status (403, 404, 204, 500, 502, etc.).
                # When retry_on is specified (e.g. send_task), ALL HTTP-level errors
                # are re-raised immediately because the POST may have been committed
                # and retrying risks duplicate task creation.
                if retry_on is not None:
                    code = getattr(err, "code", None)
                    logger.error(
                        f"HTTP error ({code}) on retry_on-limited call to "
                        f'"{func.__name__}" — re-raising (may have been committed){_task_ctx}'
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
