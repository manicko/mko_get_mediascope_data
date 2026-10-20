"""Tests for the ProgressReporter protocol, NullProgressReporter, and TVTask defaults (Phase 4).

Covers the no-op semantics of :class:`NullProgressReporter`, the runtime
``isinstance`` check against the ``runtime_checkable`` :class:`ProgressReporter`
protocol, and the ``TVTask.task_status`` default of ``QUEUED``.
"""

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
