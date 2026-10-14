import pytest
from requests.exceptions import ConnectTimeout

from mko_get_mediascope_data.core.network import NetworkClient


@pytest.mark.asyncio
async def test_network_retry_and_fail():
    """Verify that RuntimeError is raised after max_attempts"""
    client = NetworkClient(max_attempts=3, sleep_time=0)

    def failing_func():
        raise ConnectTimeout("timeout")

    with pytest.raises(RuntimeError, match="Network retry limit exceeded"):
        await client.call(failing_func)
