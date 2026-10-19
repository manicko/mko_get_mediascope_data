import asyncio
import logging
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from mediascope_api.core.errors import (
    AuthorizationError,
    BadRequestError,
    ServerError,
    TooManyRequestsError,
)
from requests.exceptions import ConnectTimeout

from mko_get_mediascope_data.core.errors import MaxRetriesExceeded
from mko_get_mediascope_data.core.network import NetworkClient


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


async def test_call_respects_semaphore(mocker):
    """Verify that the semaphore limits concurrent in-flight calls.

    Mocks ``_invoke`` with a coroutine that yields via a real ``asyncio.sleep``
    so the event loop schedules all concurrent tasks, allowing contention on
    the semaphore.  This is async-native — no OS threads or ``threading``
    primitives.
    """
    client = NetworkClient(max_concurrent_requests=2, max_attempts=1, sleep_time=0)

    active = {"count": 0, "max": 0}

    async def tracking_invoke(*args, **kwargs):
        active["count"] += 1
        active["max"] = max(active["max"], active["count"])
        await asyncio.sleep(0.01)  # real yield — not mocked
        active["count"] -= 1
        return "ok"

    mocker.patch.object(client, "_invoke", new=tracking_invoke)

    results = await asyncio.gather(
        client.call(lambda: None, sleep_time=0),
        client.call(lambda: None, sleep_time=0),
        client.call(lambda: None, sleep_time=0),
        client.call(lambda: None, sleep_time=0),
        client.call(lambda: None, sleep_time=0),
    )

    assert active["max"] <= 2
    assert all(r == "ok" for r in results)


async def test_call_retries_with_default_max_attempts(mocker):
    """Verify that NetworkClient.call retries exactly 10 times by default.

    A function that always raises ConnectTimeout (a retriable error) is
    invoked via NetworkClient with default max_attempts (10).  Asserts that
    MaxRetriesExceeded is raised and the function was called exactly 10 times.
    """
    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(sleep_time=0)

    call_count = [0]

    def failing_func():
        call_count[0] += 1
        raise ConnectTimeout("timeout")

    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(failing_func)

    assert call_count[0] == 10
    # 10 attempts → 10 backoff sleeps (after each failed attempt) + 1 initial jitter
    assert mock_sleep.call_count >= 10


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


async def test_call_raises_timeout_after_call_timeout(mocker):
    """EXT-002: a TimeoutError from _invoke is retried and exhausts max_attempts.

    Mocks ``_invoke`` (instead of delegating to ``asyncio.to_thread`` with a
    blocking ``time.sleep``) so no OS thread outlives the test.  A
    ``TimeoutError`` raised by ``_invoke`` is in ``RETRIABLE_EXCEPTIONS``
    (network.py:40) and is retried; with ``max_attempts=1`` the loop exits
    and raises ``MaxRetriesExceeded``.  No real 2-second wait.
    """
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=1, sleep_time=0, call_timeout=0.5)
    call_count = [0]

    async def timeout_invoke(*args, **kwargs):
        call_count[0] += 1
        raise TimeoutError()

    mocker.patch.object(client, "_invoke", new=timeout_invoke)

    start = time.monotonic()
    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(lambda: "ok", sleep_time=0)
    elapsed = time.monotonic() - start

    assert call_count[0] == 1  # single attempt (max_attempts=1)
    assert elapsed < 0.1       # fast — no real 2s wait


async def test_call_retries_on_429(mocker):
    """EXT-005: HTTP 429 (TooManyRequestsError) is retried, not raised immediately."""
    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    def rate_limited_func():
        call_count[0] += 1
        raise TooManyRequestsError("Too many requests", 429)

    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(rate_limited_func)

    assert call_count[0] == 3  # retried up to max_attempts
    assert mock_sleep.call_count >= 2  # 3 attempts → 2 backoff sleeps + 1 initial


async def test_call_5xx_not_retried_by_default(mocker):
    """EXT-005: 5xx (ServerError 500) raises immediately when retry_5xx=False (default)."""
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)  # retry_5xx defaults to False
    call_count = [0]

    def server_error_func():
        call_count[0] += 1
        raise ServerError("Server error", 500)

    with pytest.raises(ServerError):
        await client.call(server_error_func)

    assert call_count[0] == 1  # single attempt, no retry


async def test_call_5xx_retried_when_enabled(mocker):
    """EXT-005: 5xx (ServerError 500) is retried when retry_5xx=True."""
    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0, retry_5xx=True)
    call_count = [0]

    def server_error_func():
        call_count[0] += 1
        raise ServerError("Server error", 500)

    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await client.call(server_error_func)

    assert call_count[0] == 3  # retried up to max_attempts
    assert mock_sleep.call_count >= 2  # 3 attempts → 2 backoff sleeps + 1 initial


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


async def test_backoff_grows_exponentially(mocker):
    """Backoff sleeps grow exponentially between retry attempts.

    Verifies the formula at network.py:166-169:
    ``backoff = _sleep_time * 1.2**(attempt-1)``.  With ``uniform`` mocked
    to return 0 (no jitter), the 2nd backoff sleep must exceed the 1st.
    """
    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    # Eliminate jitter from uniform() so backoff growth is deterministic.
    mocker.patch("mko_get_mediascope_data.core.network.uniform", return_value=0)

    # sleep_time=10 → _sleep_time = max(10, 10) = 10; backoff grows as 10, 12, 14.4.
    client = NetworkClient(max_attempts=3, sleep_time=10)
    call_count = [0]

    def failing_func():
        call_count[0] += 1
        raise ConnectTimeout("timeout")

    with pytest.raises(MaxRetriesExceeded):
        await client.call(failing_func, sleep_time=10)

    sleep_durations = [c.args[0] for c in mock_sleep.call_args_list]
    # sleep[0] = initial jitter (0 when uniform→0)
    # sleep[1] = backoff after attempt 1 = 10 * 1.2^0 = 10.0
    # sleep[2] = backoff after attempt 2 = 10 * 1.2^1 = 12.0
    assert len(sleep_durations) >= 3
    backoff_1 = sleep_durations[1]
    backoff_2 = sleep_durations[2]
    assert backoff_2 > backoff_1  # 12.0 > 10.0 — exponential growth confirmed


async def test_initial_jitter_in_range(mocker):
    """The initial pre-loop sleep is jittered within [sleep_time*0.5, sleep_time*2].

    network.py:103: ``await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))``.
    """
    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    # Mock uniform to return a specific value so we can assert the range.
    mocker.patch("mko_get_mediascope_data.core.network.uniform", return_value=1.5)

    client = NetworkClient(max_attempts=1, sleep_time=2)
    call_count = [0]

    def failing_func():
        call_count[0] += 1
        raise ConnectTimeout("timeout")

    with pytest.raises(MaxRetriesExceeded):
        await client.call(failing_func, sleep_time=2)

    # _sleep_time = max(2, 2) = 2; initial jitter range = [2*0.5, 2*2] = [1.0, 4.0].
    # With uniform mocked to 1.5, the first sleep call should be 1.5.
    first_sleep = mock_sleep.call_args_list[0].args[0]
    assert 1.0 <= first_sleep <= 4.0


async def test_file_not_found_error_not_retried(mocker):
    """FileNotFoundError is re-raised immediately (not in RETRIABLE_EXCEPTIONS).

    network.py:126-131 — caught separately from RETRIABLE_EXCEPTIONS.
    """
    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    def fnf_func():
        call_count[0] += 1
        raise FileNotFoundError("settings file missing")

    with pytest.raises(FileNotFoundError, match="settings file missing"):
        await client.call(fnf_func)

    assert call_count[0] == 1  # no retry
    assert mock_sleep.call_count == 1  # only the initial jitter sleep; no backoff


async def test_semaphore_released_after_exhausted_retries(mocker):
    """The semaphore is released even when all retry attempts fail.

    network.py:106-171 — the ``async with self._semaphore`` context must
    release on exit, even if the last attempt raises and MaxRetriesExceeded follows.
    """
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_concurrent_requests=2, max_attempts=1, sleep_time=0)

    # After a failed call, the semaphore should be acquirable again.
    def failing_func():
        raise ConnectTimeout("timeout")

    with pytest.raises(MaxRetriesExceeded):
        await client.call(failing_func)

    # Try to acquire the semaphore non-blocking — proves it was released.
    try:
        async with asyncio.timeout(0.1):
            await client._semaphore.acquire()
            client._semaphore.release()
    except TimeoutError:
        pytest.fail("Semaphore was not released after exhausted retries")


async def test_call_timeout_none_invokes_raw(mocker):
    """With call_timeout=None, _invoke returns the raw coroutine (no wait_for).

    network.py:77-79 — when ``_call_timeout is None``, no ``asyncio.wait_for``
    wraps the call.
    """
    client = NetworkClient(max_attempts=1, sleep_time=0, call_timeout=None)
    call_count = [0]

    def fast_func():
        call_count[0] += 1
        return "ok"

    result = await client.call(fast_func)
    assert result == "ok"
    assert call_count[0] == 1


async def test_invoke_applies_timeout_and_thread(mocker):
    """_invoke wraps func in asyncio.to_thread + asyncio.wait_for.

    network.py:74-79 — a func that exceeds call_timeout is cancelled,
    raising TimeoutError (which is in RETRIABLE_EXCEPTIONS).
    """
    client = NetworkClient(max_attempts=1, sleep_time=0, call_timeout=0.1)

    def blocking_func():
        time.sleep(1)
        return "should not reach"

    with pytest.raises(MaxRetriesExceeded):
        await client.call(blocking_func)


async def test_non_5xx_mediascope_api_error_raises(mocker):
    """Non-retryable MediascopeApiError (e.g. 403) is re-raised immediately.

    network.py:146-156 — the ``else: raise`` branch for non-5xx codes.
    """
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    from mediascope_api.core.errors import MediascopeApiError

    def api_error_func():
        call_count[0] += 1
        raise MediascopeApiError("Forbidden", 403)

    with pytest.raises(MediascopeApiError, match="Forbidden"):
        await client.call(api_error_func)

    assert call_count[0] == 1  # no retry


async def test_success_after_transient_retry(mocker):
    """Success on a later attempt returns the value (partial recovery path).

    network.py:109-171 — ConnectTimeout raised on attempt 1, then the func
    succeeds. MaxRetriesExceeded must NOT be raised.
    """
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    client = NetworkClient(max_attempts=3, sleep_time=0)
    call_count = [0]

    def flaky_func():
        call_count[0] += 1
        if call_count[0] < 3:
            raise ConnectTimeout("transient")
        return {"status": "ok"}

    result = await client.call(flaky_func)
    assert result == {"status": "ok"}
    assert call_count[0] == 3


async def test_cpu_local_calls_bypass_semaphore_and_jitter(mocker):
    """PERF-001: when ``api_call=False``, ``NetworkClient.call`` skips the initial
    jitter, bypasses the ``Semaphore``, runs the wrapped function exactly once
    (no retry loop), and forwards the return value.

    network.py:101-102 — the fast path returns directly via ``_invoke``
    before any ``asyncio.sleep`` / ``Semaphore`` / retry-loop logic.
    """
    client = NetworkClient(max_concurrent_requests=2, max_attempts=3, sleep_time=2)

    # asyncio.sleep should never be called (initial jitter is skipped)
    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # _invoke should be called exactly once (no retry loop)
    sentinel = "cpu_result"
    mock_invoke = mocker.patch.object(
        client, "_invoke", new_callable=AsyncMock, return_value=sentinel
    )

    # Replace the Semaphore with a mock to verify it is never acquired.
    mock_semaphore = MagicMock()
    mock_semaphore.acquire = AsyncMock()
    client._semaphore = mock_semaphore

    result = await client.call(lambda: None, api_call=False)

    assert result == sentinel
    assert mock_sleep.call_count == 0, (
        "Initial jitter should be skipped for api_call=False"
    )
    assert mock_invoke.call_count == 1, (
        "Wrapped function should execute exactly once (no retry)"
    )
    assert mock_semaphore.acquire.call_count == 0, (
        "Semaphore acquire should not be called for api_call=False"
    )
