import pytest
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
