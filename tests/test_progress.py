"""Tests for the ProgressReporter protocol, NullProgressReporter, TVTask defaults,
and RichProgressReporter log-handler redirect (Phase 4).

Covers the no-op semantics of :class:`NullProgressReporter`, the runtime
``isinstance`` check against the ``runtime_checkable`` :class:`ProgressReporter`
protocol, the ``TVTask.task_status`` default of ``QUEUED``, and the
``RichProgressReporter`` handler-stream redirect through the ``Live`` region.
"""

import io
import logging
import sys
from pathlib import Path

from mko_get_mediascope_data.core.models import TaskStatus
from mko_get_mediascope_data.core.progress import NullProgressReporter, ProgressReporter
from mko_get_mediascope_data.core.tasks import TVTask


# ---------------------------------------------------------------------------
# NullProgressReporter is a no-op
# ---------------------------------------------------------------------------


class TestNullProgressReporterNoop:
    """Every method on NullProgressReporter must complete without raising."""

    def test_report_started_does_not_raise(self) -> None:
        reporter = NullProgressReporter()
        reporter.report_started("my_report", total_tasks=5)

    def test_task_status_changed_does_not_raise(self) -> None:
        reporter = NullProgressReporter()
        task = TVTask("test_task", {}, "crosstab", "TV")
        reporter.task_status_changed(task, TaskStatus.QUEUED, TaskStatus.SENT)

    def test_report_completed_does_not_raise(self) -> None:
        reporter = NullProgressReporter()
        reporter.report_completed("my_report")

    def test_all_methods_in_sequence_do_not_raise(self) -> None:
        """Simulate a full report lifecycle through the no-op reporter."""
        reporter = NullProgressReporter()
        task = TVTask("test_task", {}, "crosstab", "TV")

        reporter.report_started("my_report", total_tasks=1)
        reporter.task_status_changed(task, TaskStatus.QUEUED, TaskStatus.SENT)
        reporter.task_status_changed(task, TaskStatus.SENT, TaskStatus.COMPLETED)
        reporter.report_completed("my_report")


# ---------------------------------------------------------------------------
# ProgressReporter protocol runtime check
# ---------------------------------------------------------------------------


class TestProgressReporterProtocol:
    """NullProgressReporter satisfies the runtime_checkable ProgressReporter protocol."""

    def test_null_reporter_is_instance_of_protocol(self) -> None:
        assert isinstance(NullProgressReporter(), ProgressReporter)

    def test_null_reporter_is_protocol_instance_after_construction(
        self,
    ) -> None:
        """Re-verify with a freshly constructed instance to guard against
        accidental attribute removal in NullProgressReporter."""
        reporter = NullProgressReporter()
        assert isinstance(reporter, ProgressReporter)


# ---------------------------------------------------------------------------
# TVTask.task_status default
# ---------------------------------------------------------------------------


class TestTVTaskDefaultStatus:
    """TVTask instances must start in the QUEUED state."""

    def test_default_task_status_is_queued(self) -> None:
        task = TVTask("test_task", {}, "crosstab", "TV")
        assert task.task_status == TaskStatus.QUEUED

    def test_default_task_status_is_queued_with_settings(self) -> None:
        task = TVTask(
            "test_task", {"options": {"kitId": 1}}, "DYNAMICS_BY_SPOTS", "crosstab"
        )
        assert task.task_status == TaskStatus.QUEUED


# ---------------------------------------------------------------------------
# RichProgressReporter handler-stream redirect through Live region
# ---------------------------------------------------------------------------


class TestRichProgressReporterHandlerRedirect:
    """Console StreamHandler streams are redirected through Live and restored after."""

    def test_console_handler_stream_redirected_through_live(
        self, tmp_path: Path
    ) -> None:
        """Non-file StreamHandlers are re-pointed to sys.stderr during Live.

        File handlers (FileHandler / RotatingFileHandler) are left untouched.
        After report_completed, the original console handler streams are restored.
        """
        from mko_get_mediascope_data.app import RichProgressReporter

        root = logging.getLogger()
        saved_handlers = root.handlers[:]
        original_levels = [(h, h.level) for h in root.handlers]

        console_stream = io.StringIO()
        console_handler = logging.StreamHandler(console_stream)
        console_handler.setLevel(logging.INFO)

        log_file = tmp_path / "test.log"
        file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        original_file_stream = file_handler.stream

        root.addHandler(console_handler)
        root.addHandler(file_handler)

        try:
            reporter = RichProgressReporter()
            reporter.report_started("test_report", total_tasks=5)

            # Console handler stream redirected to Live's sys.stderr
            assert console_handler.stream is not console_stream
            assert console_handler.stream is sys.stderr

            # File handler untouched
            assert file_handler.stream is original_file_stream

            reporter.report_completed("test_report")

            # Both handlers restored
            assert console_handler.stream is console_stream
            assert file_handler.stream is original_file_stream
        finally:
            root.removeHandler(console_handler)
            root.removeHandler(file_handler)
            for handler, level in original_levels:
                handler.setLevel(level)
            root.handlers.clear()
            root.handlers.extend(saved_handlers)

    def test_no_handlers_does_not_raise(self) -> None:
        """RichProgressReporter with zero console handlers must not crash."""
        from mko_get_mediascope_data.app import RichProgressReporter

        root = logging.getLogger()
        saved_handlers = root.handlers[:]

        root.handlers.clear()
        try:
            reporter = RichProgressReporter()
            reporter.report_started("empty", total_tasks=0)
            reporter.report_completed("empty")
        finally:
            root.handlers.clear()
            root.handlers.extend(saved_handlers)
