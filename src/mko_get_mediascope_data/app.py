"""
mko_get_mediascope_data — main public interface
"""

import asyncio
import io
import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from .core.init_service import init_project
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


@app.command()
def run(
    report: Annotated[
        Path,
        typer.Argument(
            exists=True, dir_okay=False, help="Path to the report JSON5 file"
        ),
    ],
    verbose: bool = typer.Option(True, "--verbose", "-v"),
):
    """Run report export"""
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
    if verbose:
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
async def run_report_async(report_path: str | Path) -> None:
    """
    Async version for async environments (e.g., FastAPI).
    """
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    await get_app_service().run_report(path)


def run_report(report_path: str | Path) -> None:
    """
    Synchronous wrapper for Jupyter / Airflow.

    Example in Jupyter:
        from mko_get_mediascope_data import run_report
        run_report("settings/reports/nat_tv_brands_last.json5")
    """
    asyncio.run(run_report_async(report_path))


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
