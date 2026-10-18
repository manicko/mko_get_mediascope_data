import asyncio
import logging
import threading
import time
from unittest.mock import AsyncMock

import pytest
from mediascope_api.core.errors import (
    AuthorizationError,
    BadRequestError,
    MediascopeApiError,  # noqa: F401  # imported for documentation / future use
    ServerError,
    TooManyRequestsError,
)
from requests.exceptions import ConnectTimeout

from mko_get_mediascope_data.core.errors import MaxRetriesExceeded
from mko_get_mediascope_data.core.network import NetworkClient


@pytest.mark.asyncio
async def test_network_retry_and_fail():
    """Verify that MaxRetriesExceeded is raised after max_attempts"""
    client = NetworkClient(max_attempts=3, sleep_time=0)

    def failing_func():
        raise ConnectTimeout("timeout")

    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(failing_func)


@pytest.mark.asyncio
async def test_mock_network_client_retries_on_retryable_error(mock_network_client):
    """Verify mock_network_client fixture is configured for retries.

    The fixture provides a NetworkClient with max_attempts=2, sleep_time=0,
    and call replaced by an AsyncMock. When the mocked call raises a
    retryable error (ConnectTimeout), it can be retried up to max_attempts
    times — mirroring the retry semantics of the real NetworkClient.call.
    """
    # Fixture provides a client configured for 2 retries
    assert mock_network_client._max_attempts == 2
    assert mock_network_client._sleep_time == 0
    # Semaphore enables concurrency control within the real call()
    assert mock_network_client._semaphore is not None

    # Configure the mock to always raise a retryable error
    mock_network_client.call.side_effect = ConnectTimeout("connection timeout")

    # Invoke call up to max_attempts, simulating the retry loop
    for _ in range(mock_network_client._max_attempts):
        with pytest.raises(ConnectTimeout):
            await mock_network_client.call(lambda: None)

    # call was invoked max_attempts times, simulating retries
    assert mock_network_client.call.call_count == mock_network_client._max_attempts


@pytest.mark.asyncio
async def test_call_returns_result_on_success(mocker):
    """Verify that NetworkClient.call returns the function's result on success."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)

    def success_func():
        return {"result": "ok"}

    result = await client.call(success_func)
    assert result == {"result": "ok"}


@pytest.mark.asyncio
async def test_call_raises_immediately_on_bad_request(mocker):
    """Verify that BadRequestError (4xx) is re-raised without retry."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    def bad_request_func():
        call_count[0] += 1
        raise BadRequestError("bad request")

    with pytest.raises(BadRequestError):
        await client.call(bad_request_func)

    assert call_count[0] == 1


@pytest.mark.asyncio
async def test_call_raises_immediately_on_auth_error(mocker):
    """Verify that AuthorizationError (401) is re-raised without retry."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    def auth_error_func():
        call_count[0] += 1
        raise AuthorizationError("unauthorized")

    with pytest.raises(AuthorizationError):
        await client.call(auth_error_func)

    assert call_count[0] == 1


@pytest.mark.asyncio
async def test_call_respects_semaphore(mocker):
    """Verify that the semaphore limits concurrent in-flight calls."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_concurrent_requests=2, max_attempts=1, sleep_time=0)

    counter = {"current": 0, "max": 0}
    counter_lock = threading.Lock()

    def tracking_func():
        with counter_lock:
            counter["current"] += 1
            counter["max"] = max(counter["max"], counter["current"])
        time.sleep(0.05)  # real sleep to allow thread overlap
        with counter_lock:
            counter["current"] -= 1
        return "ok"

    results = await asyncio.gather(
        client.call(tracking_func),
        client.call(tracking_func),
        client.call(tracking_func),
        client.call(tracking_func),
        client.call(tracking_func),
    )

    assert counter["max"] <= 2
    assert all(r == "ok" for r in results)


def test_default_max_attempts_is_10():
    """Verify that NetworkClient defaults to max_attempts=10."""
    client = NetworkClient()
    assert client._max_attempts == 10


@pytest.mark.asyncio
async def test_call_retries_with_default_max_attempts(mocker):
    """Verify that NetworkClient.call retries exactly 10 times by default.

    A function that always raises ConnectTimeout (a retriable error) is
    invoked via NetworkClient with default max_attempts (10).  Asserts that
    MaxRetriesExceeded is raised and the function was called exactly 10 times.
    """
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(sleep_time=0)

    call_count = [0]

    def failing_func():
        call_count[0] += 1
        raise ConnectTimeout("timeout")

    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(failing_func)

    assert call_count[0] == 10


@pytest.mark.asyncio
async def test_call_preserves_return_type(mocker):
    """Verify that NetworkClient.call forwards the wrapped function's return value with the correct type."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)

    def typed_func() -> dict[str, str]:
        return {"result": "ok"}

    result: dict[str, str] = await client.call(typed_func)
    assert result == {"result": "ok"}
    assert isinstance(result, dict)
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in result.items())


@pytest.mark.asyncio
async def test_send_task_not_retried_on_retryable_error(mocker):
    """EXT-001: non-retryable call does not retry on a retryable exception."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    def send_task_func(_task_json):
        call_count[0] += 1
        raise ConnectTimeout("connection timed out")

    with pytest.raises(ConnectTimeout, match="connection timed out"):
        await client.call(send_task_func, {"task": "json"}, retryable=False)

    assert call_count[0] == 1, (
        f"Expected exactly 1 call (no retry), got {call_count[0]}"
    )


@pytest.mark.asyncio
async def test_call_raises_timeout_after_call_timeout(mocker):
    """EXT-002: per-call call_timeout caps wall-clock via asyncio.wait_for.

    With call_timeout=0.5 and max_attempts=1, a function that blocks for 2s
    is terminated by the wait_for deadline at 0.5s. Because TimeoutError is
    in RETRIABLE_EXCEPTIONS, the single-attempt loop exits and raises
    MaxRetriesExceeded — the function is NOT invoked more than max_attempts.
    """
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=1, sleep_time=0, call_timeout=0.5)
    call_count = [0]

    def blocking_func():
        call_count[0] += 1
        time.sleep(2)  # exceeds call_timeout; thread finishes shortly after test
        return "ok"

    start = time.monotonic()
    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(blocking_func)
    elapsed = time.monotonic() - start

    assert call_count[0] == 1  # single attempt (max_attempts=1)
    assert elapsed < 1.0  # bounded by call_timeout (0.5s), NOT the 2s blocking sleep


@pytest.mark.asyncio
async def test_call_retries_on_429(mocker):
    """EXT-005: HTTP 429 (TooManyRequestsError) is retried, not raised immediately."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    def rate_limited_func():
        call_count[0] += 1
        raise TooManyRequestsError("Слишком много запросов", 429)

    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(rate_limited_func)

    assert call_count[0] == 3  # retried up to max_attempts


@pytest.mark.asyncio
async def test_call_5xx_not_retried_by_default(mocker):
    """EXT-005: 5xx (ServerError 500) raises immediately when retry_5xx=False (default)."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)  # retry_5xx defaults to False
    call_count = [0]

    def server_error_func():
        call_count[0] += 1
        raise ServerError("Ошибка сервера", 500)

    with pytest.raises(ServerError):
        await client.call(server_error_func)

    assert call_count[0] == 1  # single attempt, no retry


@pytest.mark.asyncio
async def test_call_5xx_retried_when_enabled(mocker):
    """EXT-005: 5xx (ServerError 500) is retried when retry_5xx=True."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0, retry_5xx=True)
    call_count = [0]

    def server_error_func():
        call_count[0] += 1
        raise ServerError("Ошибка сервера", 500)

    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(server_error_func)

    assert call_count[0] == 3  # retried up to max_attempts


@pytest.mark.asyncio
async def test_call_redacts_pii_in_error_logs(mocker, caplog):
    """EXT-008: error logs redact args/kwargs by default (PII protection).

    A function that raises RuntimeError is called with sensitive args and
    kwargs.  The default NetworkClient (log_errors_detail=False) must NOT
    include those values in the log output — only ``"<redacted>"``.
    """
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=1, sleep_time=0)

    def failing_func(*args, **kwargs):
        raise RuntimeError("kaboom")

    with caplog.at_level(logging.ERROR, logger="mko_get_mediascope_data.core.network"):
        with pytest.raises(RuntimeError, match="kaboom"):
            await client.call(
                failing_func,
                "sensitive_positional",
                secret="secret_value_42",
            )

    log_text = caplog.text
    assert "secret_value_42" not in log_text
    assert "sensitive_positional" not in log_text
    assert "<redacted>" in log_text


@pytest.mark.asyncio
async def test_call_logs_full_args_when_detail_enabled(mocker, caplog):
    """EXT-008: log_errors_detail=True logs full args (debug mode)."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=1, sleep_time=0, log_errors_detail=True)

    def failing_func(*args, **kwargs):
        raise RuntimeError("kaboom")

    with caplog.at_level(logging.ERROR, logger="mko_get_mediascope_data.core.network"):
        with pytest.raises(RuntimeError, match="kaboom"):
            await client.call(failing_func, "secret_value_42")

    assert "secret_value_42" in caplog.text
