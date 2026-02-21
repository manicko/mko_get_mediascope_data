"""
mko_get_mediascope_data — основной публичный интерфейс

Поддерживает:
- CLI (uv run -m mko_get_mediascope_data run ...)
- Jupyter Notebook
- Airflow (PythonOperator)
"""
import sys

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from .core.init_service import init_project
from .core.services import app_service

sys.stdout.reconfigure(line_buffering=True)
console = Console()
app = typer.Typer(
    name="mko_get_mediascope_data",
    help="Инструмент выгрузки данных из Mediascope (TV / TV_REG)",
    add_completion=True,
)


@app.command()
def init(
        force: bool = typer.Option(False, "--force", "-f", help="Перезаписать существующие файлы")
):
    """Инициализировать пользовательские настройки (выгрузить шаблоны в ~/.../settings)"""
    path = init_project(force=force)
    console.print(f"[green]✅ Настройки инициализированы:[/green] {path}")


@app.command()
def run(
        report: Path = typer.Argument(..., exists=True, dir_okay=False, help="Путь к yaml-файлу задания (reports/...)"),
        verbose: bool = typer.Option(True, "--verbose", "-v"),
):
    """Запустить выгрузку отчёта"""
    if verbose:
        console.print(f"[blue]▶ Запуск отчёта:[/blue] {report.name}")

    try:
        app_service.run_report(report)
        console.print("[green]✅ Отчёт успешно завершён[/green]")
    except Exception as e:
        console.print(f"[red]❌ Ошибка:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def list_reports():
    """Показать все доступные файлы заданий"""
    reports_dir = app_service.app_paths.reports
    files = list(reports_dir.glob("*.yaml"))
    if not files:
        console.print("[yellow]Нет файлов в reports/[/yellow]")
        return
    for f in sorted(files):
        console.print(f"• {f.name}")


# Публичные функции для Jupyter и Airflow

def run_report(report_path: str | Path) -> None:
    """
    Основная функция для Jupyter Notebook и Airflow.
    Пример в Jupyter:
        from mko_get_mediascope_data.app import run_report
        run_report("settings/reports/nat_tv_brands_last.yaml")
    """
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")
    app_service.run_report(path)


def initialize_settings(force: bool = False) -> Path:
    """Удобная обёртка для Jupyter/Airflow"""
    return init_project(force=force)


# Для прямого запуска как модуль (python -m mko_get_mediascope_data)
if __name__ == "__main__":
    app()

# Для удобства импорта
__all__ = [
    "run_report",
    "initialize_settings",
    "init",
    "run",
    "list_reports",
    "app",
]
