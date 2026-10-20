"""Progress reporting protocol and null implementation.

This module is intentionally Rich-free so that ``core`` modules remain
free of any presentation-layer dependency (AGENTS.md rule 3).
Concrete Rich-based implementations live in the CLI layer (``app.py``).
"""

from typing import Protocol, runtime_checkable

from .models import TaskStatus
from .tasks import TVTask


@runtime_checkable
class ProgressReporter(Protocol):
    """Protocol for reporting progress during report generation.

    Implementations may display live progress bars, status updates, or
    simply log to stderr.  The protocol is ``runtime_checkable`` so callers
    can use ``isinstance(reporter, ProgressReporter)``.
    """

    def report_started(self, report_id: str, total_tasks: int) -> None:
        """Called when a report's task list begins processing.

        :param report_id: Identifier of the report (typically the relative
            path to the report JSON5 file).
        :param total_tasks: Number of TVTask objects that will be processed.
        """
        ...

    def task_status_changed(
        self, task: TVTask, old_status: TaskStatus, new_status: TaskStatus
    ) -> None:
        """Called when a TVTask transitions between statuses.

        :param task: The task whose status changed.
        :param old_status: The previous task status.
        :param new_status: The new task status.
        """
        ...

    def report_completed(self, report_id: str) -> None:
        """Called when all tasks for a report have finished.

        :param report_id: Identifier of the report that completed.
        """
        ...


class NullProgressReporter:
    """No-op progress reporter for library mode and tests.

    Satisfies :class:`ProgressReporter` without producing any output,
    making it the safe default when no progress display is desired.
    """

    def report_started(self, report_id: str, total_tasks: int) -> None:
        """Do nothing."""
        pass

    def task_status_changed(
        self, task: TVTask, old_status: TaskStatus, new_status: TaskStatus
    ) -> None:
        """Do nothing."""
        pass

    def report_completed(self, report_id: str) -> None:
        """Do nothing."""
        pass
