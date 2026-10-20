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

from .core.init_service import init_project
from .core.models import VerbosityMode
from .core.progress import NullProgressReporter, ProgressReporter
from .core.services import get_app_service
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
        asyncio.run(get_app_service().run_report(report))
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
