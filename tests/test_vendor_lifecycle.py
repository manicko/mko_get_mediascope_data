"""EXT-010: deterministic report-owned release of vendor sessions.

These tests instrument the *real* vendor construction seam rather than the
``func.__name__ == "get_connection"`` MagicMock pattern used by the pipeline
tests.  ``mediascope_api.core.net.Session`` is replaced with a recording
subclass, while ``MediaVortexTask``/``MediaVortexCats`` singleton behaviour and
``MediascopeApiNetwork.__init__`` session allocation are left intact.  Only the
catalog I/O (which would otherwise hit the network) is stubbed.
"""

import asyncio
import json
import time
from collections.abc import Generator
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock

import pytest
import requests
from mediascope_api.core.errors import AuthorizationError
from mediascope_api.mediavortex.catalogs import MediaVortexCats
from mediascope_api.mediavortex.tasks import MediaVortexTask

from mko_get_mediascope_data.core import reports as reports_module
from mko_get_mediascope_data.core import services as services_module
from mko_get_mediascope_data.core.errors import CallTimeoutError, MaxRetriesExceeded
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.paths import AppPaths
from mko_get_mediascope_data.core.services import AppService
from mko_get_mediascope_data.core.vendor_lifecycle import (
    capture_sessions,
    release_sessions,
)

_CREDENTIALS = {
    "username": "user",
    "passw": "pass",
    "client_id": "client",
    "client_secret": "00000000-0000-0000-0000-000000000000",
    "auth_server": "https://auth.example.invalid/token",
    "root_url": "https://api.example.invalid/api",
}

_REPORT_ENTRY = {
    "media": "TV",
    "report_type": "crosstab",
    "report_subtype": "TOP_NAT_TV_ADVERTISERS",
    "relative_path": "tv/release_probe",
    "data_lang": "ru",
    "period": {"last_time": {"unit_count": 1}, "time_unit": "m"},
    "target_audiences": {"all 18+": "age >= 18"},
}


class InstrumentedSession(requests.Session):
    """A ``requests.Session`` that records every ``close()`` call.

    Instances are kept reachable through ``instances`` so the test can assert
    on close counts after the vendor object graph has moved on.
    """

    instances: list["InstrumentedSession"] = []

    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0
        InstrumentedSession.instances.append(self)

    def close(self) -> None:
        self.close_calls += 1
        super().close()


@pytest.fixture
def reset_vendor_singletons() -> Generator[None]:
    """Clear the process-wide vendor singletons before and after a test."""
    _clear_vendor_singletons()
    yield
    _clear_vendor_singletons()


def _clear_vendor_singletons() -> None:
    for cls in (MediaVortexTask, MediaVortexCats):
        if "instance" in cls.__dict__:
            del cls.instance


@pytest.fixture
def vendor_seam(mocker) -> None:
    """Instrument the real vendor construction seam with no network I/O.

    Patches the ``Session`` name used by the vendor's network module and stubs
    only the catalog I/O so construction allocates instrumented sessions without
    touching the network.
    """
    InstrumentedSession.instances.clear()
    mocker.patch("mediascope_api.core.net.Session", InstrumentedSession)
    mocker.patch.object(MediaVortexCats, "load_tv_property", return_value=None)
    mocker.patch.object(MediaVortexCats, "get_units", return_value={})
    mocker.patch.object(MediaVortexCats, "get_tv_demo_attribute", return_value=None)


def _write_credentials(app_paths: AppPaths) -> Path:
    connections_dir = app_paths.connection_config.parent
    connections_dir.mkdir(parents=True, exist_ok=True)
    app_paths.connection_config.write_text(json.dumps(_CREDENTIALS), encoding="utf-8")
    return app_paths.connection_config


def _write_report_config(tmp_path: Path) -> Path:
    config_file = tmp_path / "release_probe.json5"
    config_file.write_text(json.dumps([_REPORT_ENTRY]), encoding="utf-8")
    return config_file


def _close_calls(session: requests.Session) -> int:
    """Return the recorded ``close()`` count of an instrumented session."""
    return cast(InstrumentedSession, session).close_calls


# ---------------------------------------------------------------------------
# Unit-level: capture / release guards
# ---------------------------------------------------------------------------


def test_release_sessions_is_identity_guarded_and_idempotent(
    vendor_seam: None, hermetic_app_service: AppService
) -> None:
    """A session the singleton no longer holds is never closed (EXT-010 C1)."""
    report_service = MediaVortexTask(
        settings_filename=str(_write_credentials(hermetic_app_service.app_paths)),
        check_version=False,
    )
    captured = capture_sessions(report_service)
    assert captured is not None

    # A later entry replaces the singleton's network module; the captured task
    # session is now orphaned and must not be closed through the new owner.
    replacement = MediaVortexTask(
        settings_filename=str(_write_credentials(hermetic_app_service.app_paths)),
        check_version=False,
    )
    assert replacement is report_service  # singleton: same object, new attrs

    release_sessions(captured)
    # The captured sessions are no longer the owners' current sessions, so the
    # identity guard prevents a close that would target a live replacement.
    assert _close_calls(captured.task_session) == 0
    assert _close_calls(captured.cats_session) == 0

    # Closing the *current* sessions is safe; repeating it must not raise
    # (``requests.Session.close()`` is idempotent at the transport layer).
    current = capture_sessions(report_service)
    assert current is not None
    release_sessions(current)
    release_sessions(current)
    assert _close_calls(current.task_session) >= 1
    assert _close_calls(current.cats_session) >= 1


def test_release_sessions_skips_while_work_outstanding(
    vendor_seam: None, hermetic_app_service: AppService
) -> None:
    """Release is skipped while a retained operation is outstanding (EXT-010 C2)."""
    report_service = MediaVortexTask(
        settings_filename=str(_write_credentials(hermetic_app_service.app_paths)),
        check_version=False,
    )
    captured = capture_sessions(report_service)
    assert captured is not None

    release_sessions(captured, has_outstanding_work=True)
    assert _close_calls(captured.task_session) == 0
    assert _close_calls(captured.cats_session) == 0

    release_sessions(captured, has_outstanding_work=False)
    assert _close_calls(captured.task_session) == 1
    assert _close_calls(captured.cats_session) == 1


# ---------------------------------------------------------------------------
# Integration: AppService.run_report releases on every exit path
# ---------------------------------------------------------------------------


async def test_report_connections_have_explicit_success_and_failure_release(
    tmp_path: Path,
    mocker,
    vendor_seam: None,
    hermetic_app_service: AppService,
) -> None:
    """Both vendor sessions are explicitly released on success and failure.

    Scenario (i): a successful report closes both sessions exactly once.
    Scenario (ii): a constructor catalog failure closes both already-allocated
    sessions.  Scenario (iii): ``create_report`` raising closes both sessions.
    Scenario (iv) — no premature close during retained work — is covered by
    :func:`test_release_defers_while_outstanding_operation_present`.
    """
    _write_credentials(hermetic_app_service.app_paths)
    config_file = _write_report_config(tmp_path)

    # --- (i) success -------------------------------------------------------
    mocker.patch.object(reports_module.TVMediaReport, "create_report", return_value=None)
    export_paths = await hermetic_app_service.run_report(config_file)
    assert export_paths == [hermetic_app_service.export_folder / "tv" / "release_probe"]
    assert len(InstrumentedSession.instances) == 2
    _assert_all_closed_once(InstrumentedSession.instances, expected=1)

    # --- (ii) constructor partial failure ----------------------------------
    InstrumentedSession.instances.clear()
    _clear_vendor_singletons()
    mocker.patch.object(
        MediaVortexCats, "get_units", side_effect=RuntimeError("catalog I/O failed")
    )
    with pytest.raises(RuntimeError, match="catalog I/O failed"):
        await hermetic_app_service.run_report(config_file)
    assert len(InstrumentedSession.instances) == 2
    _assert_all_closed_once(InstrumentedSession.instances, expected=1)

    # --- (iii) execution failure -------------------------------------------
    InstrumentedSession.instances.clear()
    _clear_vendor_singletons()
    mocker.patch.object(MediaVortexCats, "get_units", return_value={})
    mocker.patch.object(
        reports_module.TVMediaReport,
        "create_report",
        side_effect=RuntimeError("execution failed"),
    )
    with pytest.raises(RuntimeError, match="execution failed"):
        await hermetic_app_service.run_report(config_file)
    assert len(InstrumentedSession.instances) == 2
    _assert_all_closed_once(InstrumentedSession.instances, expected=1)


async def test_release_defers_while_outstanding_operation_present(
    tmp_path: Path,
    mocker,
    vendor_seam: None,
    hermetic_app_service: AppService,
) -> None:
    """A retained synchronous operation defers the close (scenario iv).

    The real report boundary is exercised with a real ``NetworkClient`` whose
    ``call_timeout`` elapses while the vendor constructor is still running, so
    ``get_connection`` raises ``CallTimeoutError`` retaining the worker thread
    (ASY-009).  The thread keeps referencing the entry's sessions, so the
    boundary must NOT close them prematurely.
    """
    _write_credentials(hermetic_app_service.app_paths)
    config_file = _write_report_config(tmp_path)
    mocker.patch.object(reports_module.TVMediaReport, "create_report", return_value=None)

    # The seam fixture already patched get_units; re-patch it to block long
    # enough that the 0.05s call_timeout elapses while the constructor thread
    # is still running, so get_connection retains a worker thread (ASY-009).
    def slow_get_units() -> dict[str, object]:
        time.sleep(0.4)
        return {}

    mocker.patch.object(MediaVortexCats, "get_units", side_effect=slow_get_units)

    def short_timeout_client(**kwargs: object) -> NetworkClient:
        return NetworkClient(**{**kwargs, "call_timeout": 0.05})  # type: ignore[arg-type]

    mocker.patch.object(services_module, "NetworkClient", side_effect=short_timeout_client)

    with pytest.raises(CallTimeoutError):
        await hermetic_app_service.run_report(config_file)

    assert len(InstrumentedSession.instances) == 2
    # Outstanding work → the sessions were left open, not closed beneath it.
    for session in InstrumentedSession.instances:
        assert _close_calls(session) == 0

    # Let the retained thread finish so it does not leak into later tests.
    await asyncio.sleep(0.6)


def _assert_all_closed_once(
    sessions: list[InstrumentedSession], expected: int
) -> None:
    for session in sessions:
        calls = _close_calls(session)
        assert calls == expected, (
            f"session {session!r} closed {calls} times, expected {expected}"
        )


def _assert_attempt_pairs(
    sessions: list[InstrumentedSession], *, open_pairs: int
) -> None:
    """Assert each constructor attempt's session pair has the expected close state.

    ``MediaVortexTask.__init__`` allocates the task session and then the catalog
    session per attempt, so ``sessions`` is ordered in consecutive pairs.  Every
    pair except the last ``open_pairs`` must have been closed exactly once by the
    per-attempt cleanup; the trailing pairs stay open (owned by the singleton).
    """
    assert len(sessions) % 2 == 0, "sessions must form whole attempt pairs"
    pairs = [sessions[i : i + 2] for i in range(0, len(sessions), 2)]
    closed_pairs = pairs[: len(pairs) - open_pairs]
    open_pair_list = pairs[len(pairs) - open_pairs :]
    for pair in closed_pairs:
        _assert_all_closed_once(pair, expected=1)
    for pair in open_pair_list:
        _assert_all_closed_once(pair, expected=0)


# ---------------------------------------------------------------------------
# EXT-011: the constructor dependency call honors the declared retry contract
# ---------------------------------------------------------------------------


def _inject_retry_client(mocker, *, max_attempts: int = 3) -> NetworkClient:
    """Replace the real ``NetworkClient`` in ``run_report`` with a fast one.

    Keeps the *real* ``NetworkClient`` retry loop (only the timing is made
    deterministic: ``call_timeout=None`` disables the thread timeout so the
    constructor thread cannot outlive the awaited call) while removing config
    coupling to ``max_attempts``/``sleep_time``.
    """
    client = NetworkClient(max_attempts=max_attempts, sleep_time=0, call_timeout=None)
    mocker.patch.object(services_module, "NetworkClient", return_value=client)
    return client


async def test_constructor_dependency_calls_honor_declared_retry_contract(
    tmp_path: Path,
    mocker,
    vendor_seam: None,
    hermetic_app_service: AppService,
) -> None:
    """The ``get_connection`` constructor call uses the normal retry contract.

    Exercises the *real* vendor constructor seam via ``AppService.run_report``
    (``vendor_seam`` instruments ``Session`` allocation and stubs only the
    catalog I/O), so the singleton ``__init__`` re-run, session allocation, the
    per-attempt cleanup inside ``run_report`` and the entry-level release are
    all real.  ``create_report`` is stubbed so the entry isolates the
    constructor path.  Four scenarios:

    1. transient catalog failure → retried through the normal path; on
       exhaustion ``MaxRetriesExceeded``; intermediate-attempt sessions are
       closed (per-attempt cleanup), leaving only the last attempt's pair.
    2. permanent ``AuthorizationError`` → re-raised after a single attempt.
    3. partial-init cleanup between attempts → session count stays bounded.
    4. ordinary-call parity → retry/backoff matches ``NetworkClient.call``.
    """
    _write_credentials(hermetic_app_service.app_paths)
    config_file = _write_report_config(tmp_path)
    mocker.patch.object(reports_module.TVMediaReport, "create_report", return_value=None)
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)  # no real backoff wait

    # --- (1) transient failure → retried, exhausted, cleaned up ------------
    _inject_retry_client(mocker, max_attempts=3)
    attempts = [0]

    def flaky_get_units() -> dict[str, object]:
        attempts[0] += 1
        raise requests.exceptions.ConnectTimeout("transient catalog outage")

    mocker.patch.object(MediaVortexCats, "get_units", side_effect=flaky_get_units)
    with pytest.raises(MaxRetriesExceeded, match="Network retry limit exceeded"):
        await hermetic_app_service.run_report(config_file)
    assert attempts[0] == 3  # retried up to max_attempts
    # Two sessions are allocated per attempt (task + catalog).  Each failed
    # attempt's pair is released between attempts (per-attempt cleanup), so the
    # entry-level release sees only the last attempt's open pair.  As the
    # constructor never returned, that final pair is released from the
    # partially-initialized singletons, so every pair is closed exactly once.
    sessions = InstrumentedSession.instances
    assert len(sessions) == 6  # 3 attempts × 2 sessions — bounded, not unbounded
    _assert_attempt_pairs(sessions, open_pairs=0)
    _clear_vendor_singletons()

    # --- (2) permanent auth error → single attempt, no retry ---------------
    InstrumentedSession.instances.clear()
    _inject_retry_client(mocker, max_attempts=3)
    auth_attempts = [0]

    def auth_failing_get_units() -> dict[str, object]:
        auth_attempts[0] += 1
        raise AuthorizationError("token invalid")

    mocker.patch.object(
        MediaVortexCats, "get_units", side_effect=auth_failing_get_units
    )
    with pytest.raises(AuthorizationError, match="token invalid"):
        await hermetic_app_service.run_report(config_file)
    assert auth_attempts[0] == 1  # permanent error: no retry
    # One attempt allocates one pair; it is closed, never retried.
    assert len(InstrumentedSession.instances) == 2
    _assert_attempt_pairs(InstrumentedSession.instances, open_pairs=0)
    _clear_vendor_singletons()

    # --- (3)/(4) partial-init cleanup + ordinary-call parity ---------------
    InstrumentedSession.instances.clear()
    _inject_retry_client(mocker, max_attempts=3)
    parity_attempts = [0]

    def fail_twice_then_succeed() -> dict[str, object]:
        parity_attempts[0] += 1
        if parity_attempts[0] < 3:
            raise requests.exceptions.ConnectTimeout("transient")
        return {}

    mocker.patch.object(
        MediaVortexCats, "get_units", side_effect=fail_twice_then_succeed
    )
    export_paths = await hermetic_app_service.run_report(config_file)

    # Parity with NetworkClient.call: success on the third attempt (two
    # transient retries), exactly like test_success_after_transient_retry.
    assert parity_attempts[0] == 3
    assert export_paths == [hermetic_app_service.export_folder / "tv" / "release_probe"]

    # Partial-init cleanup between attempts: the two failed attempts' pairs are
    # closed, and the successful attempt's pair is closed by the entry-level
    # release — every pair closed exactly once, none left dangling.  Session
    # count is bounded by the attempt count.
    sessions = InstrumentedSession.instances
    assert len(sessions) == 6
    _assert_attempt_pairs(sessions, open_pairs=0)
    _clear_vendor_singletons()

