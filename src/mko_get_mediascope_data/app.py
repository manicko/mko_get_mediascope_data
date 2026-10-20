"""
mko_get_mediascope_data — main public interface
"""

import asyncio
import io
import logging
import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.live import Live
from rich.progress import BarColumn, Progress, TaskID, TextColumn

from .core.init_service import init_project
from .core.models import TaskStatus, VerbosityMode
from .core.progress import NullProgressReporter, ProgressReporter
from .core.services import get_app_service
from .core.tasks import TVTask
from .core.utils import list_files_in_directory

console = Console()
app = typer.Typer(
    name="mko_get_mediascope_data",
    help="Tool for retrieving data from Mediascope (TV / TV_REG)",
    add_completion=True,
)


@app.command()
def init(
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite existing files",
    ),
):
    """Initialize user settings"""
    path = init_project(force=force)
    console.print(f"[green]✅ Settings initialized:[/green] {path}")


def configure_logging_level(mode: VerbosityMode) -> None:
    """Set the root logger and all handlers to the level mapped from *mode*.

    Per RES-02, handler levels are also lowered so that VERBOSE mode
    (DEBUG) actually surfaces records through handlers whose level was
    previously higher (e.g. INFO).
    """
    level = mode.to_logging_level()
    root = logging.getLogger()
    root.setLevel(level)
    for handler in root.handlers:
        handler.setLevel(level)


class RichProgressReporter(ProgressReporter):
    """Rich-based progress reporter with 4 fixed-position bars per status.

    Implements the :class:`ProgressReporter` protocol using
    ``rich.progress.Progress`` and ``rich.live.Live``.  A ``Live`` region
    with ``redirect_stdout=True`` is started lazily on the first
    :meth:`report_started` call and stopped in :meth:`report_completed`,
    so that startup logging from ``services.py`` remains visible above
    the bars.

    The class also supports use as a context manager from the ``run``
    CLI command to guarantee cleanup even when an exception propagates
    from ``asyncio.run``.
    """

    def __init__(self) -> None:
        self._live: Live | None = None
        self._progress: Progress | None = None
        self._bars: dict[TaskStatus, TaskID] = {}
        self._counts: dict[TaskStatus, int] = {s: 0 for s in TaskStatus}
        self._total: int = 0

    # -- Context-manager hooks ----------------------------------------------

    def __enter__(self) -> "RichProgressReporter":
        """Enter context — returns self.  ``Live`` is started lazily."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Ensure the ``Live`` region is stopped even on exception."""
        self._stop_live()

    # -- ProgressReporter protocol ------------------------------------------

    def report_started(self, report_id: str, total_tasks: int) -> None:
        """Create 4 status bars (QUEUED / SENT / COMPLETED / FAILED) and
        start the ``Live`` region.

        Each bar is displayed as ``STATUS [██░░░░░] XX% (N/total)``
        using :class:`TextColumn` + :class:`BarColumn`.
        """
        self._total = total_tasks
        self._counts = {s: 0 for s in TaskStatus}
        self._progress = Progress(
            TextColumn("{task.description}", style="bold"),
            BarColumn(bar_width=12, style="bright_blue",
                      complete_style="bright_green",
                      finished_style="bright_green"),
            TextColumn("{task.percentage:>3.0f}% ({task.completed}/{task.total})"),
        )
        self._bars = {
            status: self._progress.add_task(status.value, total=total_tasks)
            for status in TaskStatus
        }
        self._start_live()

    def task_status_changed(
        self, task: TVTask, old_status: TaskStatus, new_status: TaskStatus
    ) -> None:
        """Move one task from *old_status* to *new_status* and re-render.

        ``QUEUED`` is not reported via ``task_status_changed`` (tasks
        start in the QUEUED state at construction / enqueue).  It is
        therefore derived as ``total - SENT - COMPLETED - FAILED`` in
        :meth:`_refresh_bars`.
        """
        if self._progress is None:
            return
        if old_status != new_status:
            if old_status != TaskStatus.QUEUED:
                self._counts[old_status] -= 1
            if new_status != TaskStatus.QUEUED:
                self._counts[new_status] += 1
            self._refresh_bars()

    def report_completed(self, report_id: str) -> None:
        """Clear the bar set and stop the ``Live`` region."""
        self._progress = None
        self._bars = {}
        self._stop_live()

    # -- Internals ----------------------------------------------------------

    def _start_live(self) -> None:
        """Start the ``Live`` region, plugging in the current ``Progress``."""
        if self._live is not None:
            if self._progress is not None:
                self._live.update(self._progress)
            return
        if self._progress is not None:
            self._live = Live(
                self._progress,
                refresh_per_second=10,
                redirect_stdout=True,
                redirect_stderr=True,
                transient=True,
            )
            self._live.__enter__()

    def _stop_live(self) -> None:
        """Stop the ``Live`` region if it is currently running."""
        if self._live is not None:
            self._live.__exit__(None, None, None)
            self._live = None

    def _refresh_bars(self) -> None:
        """Refresh all four bars after a status transition."""
        tracked = (
            self._counts[TaskStatus.SENT]
            + self._counts[TaskStatus.COMPLETED]
            + self._counts[TaskStatus.FAILED]
        )
        self._counts[TaskStatus.QUEUED] = max(0, self._total - tracked)
        if self._progress is None:
            return
        for status in TaskStatus:
            bar_id = self._bars.get(status)
            if bar_id is not None:
                self._progress.update(
                    bar_id,
                    completed=self._counts[status],
                    total=self._total,
                )


@app.command()
def run(
    report: Annotated[
        Path,
        typer.Argument(
            exists=True, dir_okay=False, help="Path to the report JSON5 file"
        ),
    ],
    verbosity: Annotated[
        VerbosityMode,
        typer.Option("--verbosity", "-v", help="Output verbosity level"),
    ] = VerbosityMode.DEFAULT,
):
    """Run report export"""
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
    configure_logging_level(verbosity)
    if verbosity != VerbosityMode.QUIET:
        console.print(f"[blue]▶ Running report:[/blue] {report.name}")

    try:
        if verbosity != VerbosityMode.QUIET:
            reporter = RichProgressReporter()
            with reporter:
                asyncio.run(run_report_async(report, progress=reporter))
        else:
            asyncio.run(run_report_async(report))
        console.print("[green]✅ Report completed successfully[/green]")

    except Exception as e:
        console.print(f"[red]❌ Error:[/red] {e}")
        raise typer.Exit(1) from e


@app.command()
def list_reports():
    """Show all available report files"""
    reports_dir = get_app_service().app_paths.reports
    files = list_files_in_directory(
        reports_dir, extensions=("json5",), include_subfolders=True
    )
    if not files:
        console.print("[yellow]No report files found in reports/[/yellow]")
        return
    for f in sorted(files):
        console.print(f"• {f}")


# Public API
async def run_report_async(
    report_path: str | Path,
    progress: ProgressReporter | None = None,
) -> None:
    """
    Async version for async environments (e.g., FastAPI).

    :param progress: Optional progress reporter. When ``None``,
        a :class:`NullProgressReporter` is used.
    """
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    reporter = progress if progress is not None else NullProgressReporter()
    await get_app_service().run_report(path, progress=reporter)


def run_report(
    report_path: str | Path,
    progress: ProgressReporter | None = None,
) -> None:
    """
    Synchronous wrapper for Jupyter / Airflow.

    :param progress: Optional progress reporter. When ``None``,
        a :class:`NullProgressReporter` is used.

    Example in Jupyter:
        from mko_get_mediascope_data import run_report
        run_report("settings/reports/nat_tv_brands_last.json5")
    """
    asyncio.run(run_report_async(report_path, progress=progress))


def initialize_settings(force: bool = False) -> Path:
    """Wrapper for Jupyter/Airflow"""
    return init_project(force=force)


if __name__ == "__main__":
    app()


__all__ = [
    "run_report",
    "run_report_async",
    "initialize_settings",
    "init",
    "run",
    "list_reports",
    "app",
]
