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
    TooManyRequestsError,
)
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
        **kwargs: Any,
    ) -> T:
        if not api_call:
            return await self._invoke(func, *args, **kwargs)

        _sleep_time = max(sleep_time, self._sleep_time)

        # Small initial delay to smooth peak loads
        await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))

        if not retryable:
            async with self._semaphore:
                return await self._invoke(func, *args, **kwargs)

        for attempt in range(1, self._max_attempts + 1):
            try:
                async with self._semaphore:
                    return await self._invoke(func, *args, **kwargs)

            except BadRequestError as err:
                logger.error(f"Bad request. Check configuration. {err}")
                raise

            except AuthorizationError as err:
                # 401 — is a known client error, not "Unexpected"
                logger.error(
                    f'Authorization error (token invalid/expired) in "{func.__name__}" '
                    f'with args="{self._redact_args(args)}" and kwargs="{self._redact_args(kwargs)}": {err}'
                )
                raise

            except FileNotFoundError as err:
                logger.error(
                    f'File not found: "{err}" in function "{func.__name__}" '
                    f'with args="{self._redact_args(args)}" and kwargs="{self._redact_args(kwargs)}"'
                )
                raise

            except RETRIABLE_EXCEPTIONS as err:
                logger.warning(
                    f"Network error ({type(err).__name__}) "
                    f"attempt {attempt}/{self._max_attempts}: {err}"
                )

            except TooManyRequestsError as err:
                # 429 — vendor does not expose Retry-After header on the
                # exception (verified); falls back to exponential backoff below.
                logger.warning(
                    f"Rate limited (429) on attempt {attempt}/{self._max_attempts}: {err}"
                )

            except MediascopeApiError as err:
                # Unhandled HTTP status (403, 404, 204, 500, 502, etc.).
                # 5xx is retried when retry_5xx=True; all others re-raise.
                code = getattr(err, "code", None)
                if self._retry_5xx and code is not None and 500 <= code < 600:
                    logger.warning(
                        f"Server error ({code}) on attempt {attempt}/{self._max_attempts}: {err}"
                    )
                else:
                    logger.error(f"HTTP error ({code}) for {func.__name__}: {err}")
                    raise

            except Exception as err:
                logger.exception(
                    f'Unexpected error in "{func.__name__}" '
                    f'with args="{self._redact_args(args)}" and kwargs="{self._redact_args(kwargs)}": {err}'
                )
                raise

            # backoff + jitter
            base = 1.2
            backoff = _sleep_time * (base ** (attempt - 1))
            jitter = uniform(0, 5)
            await asyncio.sleep(backoff + jitter)

        logger.error(
            f'Max retries exceeded for "{func.__name__}" '
            f'with args="{self._redact_args(args)}" and kwargs="{self._redact_args(kwargs)}"'
        )
        raise MaxRetriesExceeded("Network retry limit exceeded")
