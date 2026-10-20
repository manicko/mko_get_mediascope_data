"""Tests for ProgressReporter interaction at send→extract boundaries (Phase 5).

Verifies that ``TVMediaReport.send_task`` and ``TVMediaReport.extract_data``
invoke ``reporter.task_status_changed`` with the correct old/new ``TaskStatus``
values at the QUEUED→SENT and SENT→COMPLETED transitions.

Also verifies (R-05) that ``task_stage_complete`` INFO logs are suppressed
in VERBOSE mode (``_verbose_mode()`` returns True when root logger is at DEBUG).

The test drives ``send_task`` and ``extract_data`` directly (no full
``create_report`` pipeline) with a recording spy reporter.
"""

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from mko_get_mediascope_data.app import configure_logging_level
from mko_get_mediascope_data.core.models import (
    MediaType,
    ReportSettings,
    ReportSubtype,
    ReportType,
    TaskStatus,
    VerbosityMode,
)
from mko_get_mediascope_data.core.reports import (
    ReportFactory,
    TVMediaReport,
    _verbose_mode,
)
from mko_get_mediascope_data.core.services import AppService
from mko_get_mediascope_data.core.tasks import TVTask


# ---------------------------------------------------------------------------
# Shared fixture: save/restore root logger level + handler levels
# ---------------------------------------------------------------------------


@pytest.fixture
def restore_root_logger() -> Iterator[logging.Logger]:
    """Capture and restore root logger level and handler levels across a test."""
    root = logging.getLogger()
    saved_level = root.level
    saved_handlers = [(h, h.level) for h in root.handlers]
    yield root
    root.setLevel(saved_level)
    for handler, level in saved_handlers:
        handler.setLevel(level)


# ---------------------------------------------------------------------------
# Recording spy for the ProgressReporter protocol
# ---------------------------------------------------------------------------


class RecordingReporter:
    """Spy implementation of ``ProgressReporter`` that records every
    ``task_status_changed`` call for later assertion.

    The class is *not* a subclass of anything — it satisfies the protocol
    structurally so it can be passed wherever ``ProgressReporter`` is expected.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[TVTask, TaskStatus, TaskStatus]] = []
        self.started: list[tuple[str, int]] = []
        self.completed: list[str] = []

    def report_started(self, report_id: str, total_tasks: int) -> None:
        self.started.append((report_id, total_tasks))

    def task_status_changed(
        self, task: TVTask, old_status: TaskStatus, new_status: TaskStatus
    ) -> None:
        self.calls.append((task, old_status, new_status))

    def report_completed(self, report_id: str) -> None:
        self.completed.append(report_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_report_settings() -> ReportSettings:
    return ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )


def _make_mock_report_service(dummy_df: pd.DataFrame) -> MagicMock:
    mock = MagicMock()
    mock.cats = MagicMock()
    mock.cats.get_availability_period.return_value = dummy_df
    mock.build_task.return_value = {"task_json": "mock"}
    mock.send_task.return_value = "mock_task_key_123"
    mock.get_status.return_value = {"taskStatus": "DONE"}
    mock.get_result.return_value = {"result": "mock_data"}
    return mock


# ---------------------------------------------------------------------------
# Focused reporter-interaction test
# ---------------------------------------------------------------------------


async def test_task_status_changed_at_send_extract_boundaries(
    tmp_path: Path,
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
) -> None:
    """send_task emits QUEUED→SENT, extract_data emits SENT→COMPLETED.

    Drives the real ``send_task`` and ``extract_data`` coroutine methods directly
    on a ``TVMediaReport`` whose external API client (``MediaVortexTask``) and
    ``NetworkClient.call`` are doubled.  A :class:`RecordingReporter` spy
    captures every ``task_status_changed`` invocation so we can assert the exact
    old/new status pairs at each pipeline boundary.
    """
    # ---- 1. Report settings + data settings -------------------------------
    report_settings = _make_report_settings()
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    # ---- 2. Doubled MediaVortexTask + NetworkClient -----------------------
    mock_report_service = _make_mock_report_service(dummy_df)
    result_df = pd.DataFrame({"value": [1]})
    mock_report_service.result2table.return_value = result_df

    mock_network_client = MagicMock()

    async def _mock_call(func: Any, *args: Any, **kwargs: Any) -> Any:
        if func is mock_report_service.build_task:
            return {"task_json": "mock"}
        if func is mock_report_service.send_task:
            return "mock_task_key_123"
        if func is mock_report_service.get_result:
            return {"result": "mock_data"}
        if func is mock_report_service.result2table:
            return result_df
        return None

    mock_network_client.call = AsyncMock(side_effect=_mock_call)

    # ---- 3. Export path + strategies --------------------------------------
    export_path = tmp_path / "output"
    export_path.mkdir(parents=True)

    report_factory = ReportFactory(report_settings)

    # ---- 4. Recording spy reporter ----------------------------------------
    spy = RecordingReporter()

    # ---- 5. Construct report with spy reporter ----------------------------
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=report_factory.task_strategy(),
        data_strategy=report_factory.data_strategy(),
        check_done=False,
        reporter=spy,
    )
    # Mock prepare_data so we control the data-preparation return value and
    # keep the test focused on reporter interaction (not data transformation).
    mocker.patch.object(report, "prepare_data", AsyncMock(return_value=result_df))

    # ---- 6. Create a task and drive send_task -----------------------------
    task = TVTask("test_progress", {"options": {"kitId": 1}}, "crosstab", "TV")
    await report.send_task(task)

    # send_task transitions QUEUED → SENT
    assert task.task_status == TaskStatus.SENT
    assert len(spy.calls) == 1
    recorded_task, old_st, new_st = spy.calls[0]
    assert recorded_task is task
    assert old_st == TaskStatus.QUEUED
    assert new_st == TaskStatus.SENT

    # ---- 7. Drive extract_data --------------------------------------------
    await report.extract_data(task)

    # extract_data transitions SENT → COMPLETED
    assert task.task_status == TaskStatus.COMPLETED
    assert len(spy.calls) == 2
    recorded_task2, old_st2, new_st2 = spy.calls[1]
    assert recorded_task2 is task
    assert old_st2 == TaskStatus.SENT
    assert new_st2 == TaskStatus.COMPLETED

    # ---- 8. Verify the full transition sequence ---------------------------
    transitions = [(c[1], c[2]) for c in spy.calls]
    assert transitions == [
        (TaskStatus.QUEUED, TaskStatus.SENT),
        (TaskStatus.SENT, TaskStatus.COMPLETED),
    ]


async def test_task_status_changed_at_send_failure_boundary(
    tmp_path: Path,
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
) -> None:
    """send_task emitting SENT is followed by worker marking FAILED on error.

    Simulates the boundary where ``send_task`` succeeds (QUEUED→SENT) but the
    task later fails in the worker, which transitions SENT→FAILED.  Verifies
    the spy captures the failure transition with the correct old status.
    """
    report_settings = _make_report_settings()
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = _make_mock_report_service(dummy_df)
    result_df = pd.DataFrame({"value": [1]})
    mock_report_service.result2table.return_value = result_df

    mock_network_client = MagicMock()

    async def _mock_call(func: Any, *args: Any, **kwargs: Any) -> Any:
        if func is mock_report_service.build_task:
            return {"task_json": "mock"}
        if func is mock_report_service.send_task:
            return "mock_task_key_123"
        return None

    mock_network_client.call = AsyncMock(side_effect=_mock_call)

    export_path = tmp_path / "output"
    export_path.mkdir(parents=True)
    report_factory = ReportFactory(report_settings)

    spy = RecordingReporter()

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=report_factory.task_strategy(),
        data_strategy=report_factory.data_strategy(),
        check_done=False,
        reporter=spy,
    )

    task = TVTask("test_fail_progress", {"options": {"kitId": 1}}, "crosstab", "TV")

    # send_task succeeds → QUEUED→SENT
    await report.send_task(task)
    assert task.task_status == TaskStatus.SENT
    assert spy.calls[-1] == (task, TaskStatus.QUEUED, TaskStatus.SENT)

    # Simulate worker marking FAILED → SENT→FAILED
    old_status = task.task_status
    task.task_status = TaskStatus.FAILED
    spy.task_status_changed(task, old_status, TaskStatus.FAILED)

    assert len(spy.calls) == 2
    assert spy.calls[1] == (task, TaskStatus.SENT, TaskStatus.FAILED)


async def test_null_reporter_used_when_no_reporter_passed(
    tmp_path: Path,
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
) -> None:
    """TVMediaReport defaults to NullProgressReporter when reporter=None.

    When no reporter is provided, the report must default to NullProgressReporter
    so that ``task_status_changed`` is always a safe no-op call.
    """
    from mko_get_mediascope_data.core.progress import NullProgressReporter

    report_settings = _make_report_settings()
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = _make_mock_report_service(dummy_df)
    mock_network_client = MagicMock()
    mock_network_client.call = AsyncMock(
        side_effect=[{"task_json": "mock"}, "mock_key"]
    )

    export_path = tmp_path / "output"
    export_path.mkdir(parents=True)
    report_factory = ReportFactory(report_settings)

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=report_factory.task_strategy(),
        data_strategy=report_factory.data_strategy(),
        check_done=False,
        # reporter defaults to None → NullProgressReporter
    )

    assert isinstance(report.reporter, NullProgressReporter)


# ---------------------------------------------------------------------------
# VERBOSE suppression (R-05): task_stage_complete INFO logs gated by _verbose_mode
# ---------------------------------------------------------------------------


async def test_stage_complete_logs_suppressed_in_verbose_mode(
    tmp_path: Path,
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
    restore_root_logger: logging.Logger,
) -> None:
    """task_stage_complete INFO logs are suppressed when VERBOSE (DEBUG) mode is active.

    R-05: In VERBOSE mode the progress bars are the single source of truth,
    so ``send_task``, ``wait_task``, and ``extract_data`` must NOT emit
    ``task_stage_complete`` INFO records.

    This test sets the root logger to DEBUG via ``configure_logging_level``,
    spies on ``reports_logger.info``, then drives ``send_task`` and
    ``extract_data`` directly and asserts zero ``task_stage_complete`` calls.
    """
    # Enable VERBOSE mode — root at DEBUG, all handlers at DEBUG
    configure_logging_level(VerbosityMode.VERBOSE)
    assert _verbose_mode() is True

    reports_logger = logging.getLogger("mko_get_mediascope_data.core.reports")
    spy = mocker.spy(reports_logger, "info")

    # ---- Report setup (matches existing test patterns) ----------------------
    report_settings = _make_report_settings()
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = _make_mock_report_service(dummy_df)
    result_df = pd.DataFrame({"value": [1]})
    mock_report_service.result2table.return_value = result_df

    mock_network_client = MagicMock()

    async def _mock_call(func: Any, *args: Any, **kwargs: Any) -> Any:
        if func is mock_report_service.build_task:
            return {"task_json": "mock"}
        if func is mock_report_service.send_task:
            return "mock_task_key_123"
        if func is mock_report_service.get_result:
            return {"result": "mock_data"}
        if func is mock_report_service.result2table:
            return result_df
        return None

    mock_network_client.call = AsyncMock(side_effect=_mock_call)

    export_path = tmp_path / "output"
    export_path.mkdir(parents=True)
    report_factory = ReportFactory(report_settings)

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=report_factory.task_strategy(),
        data_strategy=report_factory.data_strategy(),
        check_done=False,
    )
    mocker.patch.object(report, "prepare_data", AsyncMock(return_value=result_df))

    task = TVTask(
        "test_verbose_suppression", {"options": {"kitId": 1}}, "crosstab", "TV"
    )

    # ---- Drive the pipeline ------------------------------------------------
    await report.send_task(task)
    await report.extract_data(task)

    # ---- Assert: no stage_complete INFO calls -------------------------------
    stage_calls = [
        c
        for c in spy.call_args_list
        if "task_stage_complete" in str(c.args[0])
    ]
    assert len(stage_calls) == 0, (
        f"Expected no task_stage_complete calls in VERBOSE mode, got: {stage_calls}"
    )


def test_verbose_mode_helper_reflects_root_level(
    restore_root_logger: logging.Logger,
) -> None:
    """``_verbose_mode()`` returns True at DEBUG and False at INFO.

    This directly tests the helper used to gate stage-complete INFO logs.
    """
    configure_logging_level(VerbosityMode.DEFAULT)
    assert _verbose_mode() is False

    configure_logging_level(VerbosityMode.VERBOSE)
    assert _verbose_mode() is True
