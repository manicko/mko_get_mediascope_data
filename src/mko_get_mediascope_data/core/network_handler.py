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


async def network_handler(
    func: Callable[..., Any],
    *args,
    sleep_time: int = 2,
    max_attempts: int = 10,
    **kwargs,
) -> Any:
    """
    Wrap network call with retry logic and error handling.

    Args:
        func: Function that sends request to Mediascope API.
        sleep_time: Base sleep time between retries.
        max_attempts: Maximum number of retry attempts.
        *args, **kwargs: Parameters passed to func.

    Returns:
        Result of func.

    Raises:
        RuntimeError: If retry limit exceeded.
        BadRequestError: If request configuration is invalid.
        FileNotFoundError: If file operation fails.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return await asyncio.to_thread(func, *args, **kwargs)

        except BadRequestError as err:
            logger.error(f"Bad request. Check configuration. {err}")
            raise

        except FileNotFoundError as err:
            logger.error(
                f'File not found: "{err}" in function "{func}" '
                f'with args="{args}" and kwargs="{kwargs}"'
            )
            raise

        except RETRIABLE_EXCEPTIONS as err:
            logger.warning(
                f"Network error ({type(err).__name__}) "
                f"attempt {attempt}/{max_attempts}: {err}"
            )

        except HTTPError as err:
            status = getattr(err.response, "status_code", None)

            if status and 400 <= status < 500:
                logger.error(f"Client error {status}: {err}")
                raise
            else:
                logger.warning(
                    f"Server error {status} attempt {attempt}/{max_attempts}: {err}"
                )

        except Exception as err:
            logger.exception(
                f'Unexpected error in "{func}" with args="{args}" and kwargs="{kwargs}"'
            )
            raise
        backoff = sleep_time * (2 ** (attempt - 1))
        await asyncio.sleep(backoff)
    raise RuntimeError("Network retry limit exceeded")
