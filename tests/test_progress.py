"""Tests for the ProgressReporter protocol, NullProgressReporter, TVTask defaults,
and RichProgressReporter log-handler redirect and enqueued-count rendering.

Covers the no-op semantics of :class:`NullProgressReporter`, the runtime
``isinstance`` check against the ``runtime_checkable`` :class:`ProgressReporter`
protocol, the ``TVTask.task_status`` default of ``QUEUED``, the
``RichProgressReporter`` handler-stream redirect through the ``Live`` region,
and NUM-005: the initial enqueued-pending count must render correctly.
"""

import io
import logging
import sys
from pathlib import Path
from typing import Any

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


# ---------------------------------------------------------------------------
# NUM-005: initial display must show correct enqueued pending count
# ---------------------------------------------------------------------------


class TestRichProgressReporterEnqueuedCounts:
    """The initial render after report_started must show the real pending count.

    Before the fix, all bars showed 0/total because _refresh_bars only ran
    inside task_status_changed (i.e. after a transition).  Now _refresh_bars
    runs at startup, so the derived QUEUED bar correctly shows total/total.
    """

    @staticmethod
    def _bar_completed(reporter: Any, status: TaskStatus) -> int:
        """Read the actual completed value from the rendered Rich bar."""
        bar_id = reporter._bars[status]
        return int(reporter._progress._tasks[bar_id].completed)

    @staticmethod
    def _rendered_counts(reporter: Any) -> dict[TaskStatus, int]:
        """Aggregate the actual rendered bar values for every status."""
        return {s: TestRichProgressReporterEnqueuedCounts._bar_completed(reporter, s)
                for s in TaskStatus}

    def test_zero_total_shows_zero_queued(self) -> None:
        """With no tasks, the QUEUED bar must render 0/0 (not crash)."""
        from mko_get_mediascope_data.app import RichProgressReporter

        reporter = RichProgressReporter()
        try:
            reporter.report_started("empty", total_tasks=0)
            assert self._bar_completed(reporter, TaskStatus.QUEUED) == 0
            assert sum(self._rendered_counts(reporter).values()) == 0
        finally:
            reporter.report_completed("empty")

    def test_enqueued_pending_tasks_render_correct_count_before_first_transition(
        self,
    ) -> None:
        """After report_started(total=6), the QUEUED bar must show 6/6.

        Six tasks are all in the QUEUED lifecycle state at the report_started
        boundary.  The derived QUEUED bar (total - sent - completed - failed)
        must therefore show the full count immediately — not zero, which was
        the bug (NUM-005): the display stayed at 0/6 until the first transition.
        """
        from mko_get_mediascope_data.app import RichProgressReporter

        reporter = RichProgressReporter()
        try:
            reporter.report_started("test_report", total_tasks=6)

            # Rendered QUEUED bar must reflect 6 enqueued tasks, not 0.
            assert self._bar_completed(reporter, TaskStatus.QUEUED) == 6
            counts = self._rendered_counts(reporter)
            assert counts == {
                TaskStatus.QUEUED: 6,
                TaskStatus.SENT: 0,
                TaskStatus.COMPLETED: 0,
                TaskStatus.FAILED: 0,
            }
            assert sum(counts.values()) == 6

            # ---- Two skipped completions (QUEUED → COMPLETED) ---------------
            skip1 = TVTask("skip1", {}, "crosstab", "TV")
            skip2 = TVTask("skip2", {}, "crosstab", "TV")
            reporter.task_status_changed(skip1, TaskStatus.QUEUED, TaskStatus.COMPLETED)
            reporter.task_status_changed(skip2, TaskStatus.QUEUED, TaskStatus.COMPLETED)

            counts = self._rendered_counts(reporter)
            assert counts[TaskStatus.QUEUED] == 4  # 6 - 2 completed
            assert counts[TaskStatus.COMPLETED] == 2
            assert sum(counts.values()) == 6  # total conserved
            assert all(v >= 0 for v in counts.values())

            # ---- Send transition (QUEUED → SENT) -------------------------------
            send1 = TVTask("send1", {}, "crosstab", "TV")
            reporter.task_status_changed(send1, TaskStatus.QUEUED, TaskStatus.SENT)

            counts = self._rendered_counts(reporter)
            assert counts[TaskStatus.QUEUED] == 3  # 6 - 2 completed - 1 sent
            assert counts[TaskStatus.SENT] == 1
            assert sum(counts.values()) == 6
            assert all(v >= 0 for v in counts.values())

            # ---- Failure transition (SENT → FAILED) ----------------------------
            reporter.task_status_changed(send1, TaskStatus.SENT, TaskStatus.FAILED)

            counts = self._rendered_counts(reporter)
            assert counts[TaskStatus.QUEUED] == 3
            assert counts[TaskStatus.SENT] == 0
            assert counts[TaskStatus.FAILED] == 1
            assert counts[TaskStatus.COMPLETED] == 2
            assert sum(counts.values()) == 6  # total still conserved
            assert all(v >= 0 for v in counts.values())  # no negative counts
        finally:
            reporter.report_completed("test_report")
