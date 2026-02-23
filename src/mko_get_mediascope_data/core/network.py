"""Network handler — общие сетевые вызовы к Mediascope API с ретраями и обработкой ошибок."""

import asyncio
import logging
from typing import Callable, Any
from requests.exceptions import (
    ConnectTimeout,
    HTTPError,
    ConnectionError,
    Timeout,
    RetryError,
    ChunkedEncodingError
)
from mediascope_api.core.errors import BadRequestError

logger = logging.getLogger(__name__)

RETRIABLE_EXCEPTIONS = (
    ConnectTimeout,
    ConnectionError,
    Timeout,
    RetryError,
    ChunkedEncodingError,
)


class NetworkClient:
    """
    Centralized async network handler with:
    - retry logic
    - exponential backoff
    - concurrency control via Semaphore
    """

    def __init__(
            self,
            max_concurrent_requests: int = 10,
            sleep_time: int = 2,
            max_attempts: int = 10,
    ):
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._sleep_time = sleep_time
        self._max_attempts = max_attempts

    async def call(
            self,
            func: Callable[..., Any],
            *args,
            sleep_time: int = 2,
            **kwargs,
    ) -> Any:

        for attempt in range(1, self._max_attempts + 1):
            try:
                async with self._semaphore:
                    return await asyncio.to_thread(func, *args, **kwargs)

            except BadRequestError as err:
                logger.error(f"Bad request. Check configuration. {err}")
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
                    f'Unexpected error {err} in "{func.__name__}" '
                    f'with args="{args}" and kwargs="{kwargs}"'
                )
                raise

            backoff = self._sleep_time * (2 ** (attempt - 1))
            await asyncio.sleep(backoff)

        raise RuntimeError("Network retry limit exceeded")
