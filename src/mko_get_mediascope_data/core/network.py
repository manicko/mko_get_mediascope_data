"""Network handler — common network calls to the Mediascope API with retries and error handling."""

import asyncio
import logging
import socket
from collections.abc import Callable
from random import uniform
from typing import Any, TypeVar

from mediascope_api.core.errors import AuthorizationError, BadRequestError
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ConnectTimeout,
    HTTPError,
    RetryError,
    Timeout,
)
from urllib3.exceptions import NameResolutionError, ProtocolError

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
    ProtocolError,
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
    ):
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._sleep_time = sleep_time
        self._max_attempts = max_attempts

    async def call(
        self,
        func: Callable[..., T],
        *args: Any,
        sleep_time: int | float = 2,
        retryable: bool = True,
        **kwargs: Any,
    ) -> T:
        _sleep_time = max(sleep_time, self._sleep_time)

        # Small initial delay to smooth peak loads
        await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))

        if not retryable:
            async with self._semaphore:
                return await asyncio.to_thread(func, *args, **kwargs)

        for attempt in range(1, self._max_attempts + 1):
            try:
                async with self._semaphore:
                    return await asyncio.to_thread(func, *args, **kwargs)

            except BadRequestError as err:
                logger.error(f"Bad request. Check configuration. {err}")
                raise

            except AuthorizationError as err:
                # 401 — is a known client error, not "Unexpected"
                logger.error(
                    f'Authorization error (token invalid/expired) in "{func.__name__}" '
                    f'with args="{args}" and kwargs="{kwargs}": {err}'
                )
                raise

            except FileNotFoundError as err:
                logger.error(
                    f'File not found: "{err}" in function "{func.__name__}" '
                    f'with args="{args}" and kwargs="{kwargs}"'
                )
                raise

            except RETRIABLE_EXCEPTIONS as err:
                logger.warning(
                    f"Network error ({type(err).__name__}) "
                    f"attempt {attempt}/{self._max_attempts}: {err}"
                )

            except HTTPError as err:
                status = getattr(err.response, "status_code", None)

                if status and 400 <= status < 500:
                    logger.error(f"Client error {status}: {err}")
                    raise
                else:
                    logger.warning(
                        f"Server error {status} "
                        f"attempt {attempt}/{self._max_attempts}: {err}"
                    )

            except Exception as err:
                logger.exception(
                    f'Unexpected error in "{func.__name__}" '
                    f'with args="{args}" and kwargs="{kwargs}": {err}'
                )
                raise

            # backoff + jitter
            base = 1.2
            backoff = _sleep_time * (base ** (attempt - 1))
            jitter = uniform(0, 5)
            await asyncio.sleep(backoff + jitter)

        raise MaxRetriesExceeded("Network retry limit exceeded")
