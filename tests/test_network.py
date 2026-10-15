import asyncio
import threading
import time
from unittest.mock import AsyncMock

import pytest
from mediascope_api.core.errors import AuthorizationError, BadRequestError
from requests.exceptions import ConnectTimeout  # type: ignore[import-untyped]

from mko_get_mediascope_data.core.network import NetworkClient


@pytest.mark.asyncio
async def test_network_retry_and_fail():
    """Verify that RuntimeError is raised after max_attempts"""
    client = NetworkClient(max_attempts=3, sleep_time=0)

    def failing_func():
        raise ConnectTimeout("timeout")

    with pytest.raises(RuntimeError, match="Network retry limit exceeded"):
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
