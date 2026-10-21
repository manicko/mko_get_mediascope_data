import asyncio
import importlib
import sys

import pytest
from typer.testing import CliRunner
from unittest.mock import AsyncMock

from mko_get_mediascope_data.app import app
from mko_get_mediascope_data.core.models import VerbosityMode


def test_cli_init_help():
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--help"])
    assert result.exit_code == 0
    assert "Initialize" in result.stdout


def test_cli_init_invokes_init_project(mocker, tmp_path):
    """`init` command calls init_project and prints the success message."""
    mock_init = mocker.patch("mko_get_mediascope_data.app.init_project", return_value=tmp_path / "settings")
    runner = CliRunner()
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 0
    mock_init.assert_called_once_with(force=False)
    assert "Settings initialized:" in result.stdout


def test_cli_init_force_flag(mocker, tmp_path):
    """`init --force` passes force=True to init_project."""
    mock_init = mocker.patch("mko_get_mediascope_data.app.init_project", return_value=tmp_path / "settings")
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--force"])
    assert result.exit_code == 0
    mock_init.assert_called_once_with(force=True)


def test_cli_run_invokes_report(mocker, tmp_path):
    """`run` command calls get_app_service().run_report with the given path."""
    report_file = tmp_path / "report.json5"
    report_file.write_text("{}", encoding="utf-8")
    mock_service = mocker.MagicMock()
    mock_service.run_report = AsyncMock(return_value=[tmp_path / "output.csv"])
    mock_service.app_config.app_settings.verbosity_mode = VerbosityMode.DEFAULT
    mocker.patch("mko_get_mediascope_data.app.get_app_service", return_value=mock_service)
    runner = CliRunner()
    result = runner.invoke(app, ["run", str(report_file)])
    assert result.exit_code == 0
    mock_service.run_report.assert_awaited_once()
    assert "Running report" in result.stdout
    assert "Report completed" in result.stdout


def test_cli_run_missing_file():
    """`run` with a nonexistent file exits with non-zero status."""
    runner = CliRunner()
    result = runner.invoke(app, ["run", "nonexistent_report.json5"])
    assert result.exit_code != 0


def test_cli_list_reports(mocker, tmp_path):
    """`list-reports` shows available report files."""
    mock_service = mocker.MagicMock()
    mock_service.app_paths.reports = tmp_path / "reports"
    mocker.patch("mko_get_mediascope_data.app.get_app_service", return_value=mock_service)
    mocker.patch(
        "mko_get_mediascope_data.app.list_files_in_directory",
        return_value=["nat_tv_brands.json5", "nat_tv_tvreport.json5"],
    )
    runner = CliRunner()
    result = runner.invoke(app, ["list-reports"])
    assert result.exit_code == 0
    assert "nat_tv_brands.json5" in result.stdout
    assert "nat_tv_tvreport.json5" in result.stdout


def test_cli_list_reports_empty(mocker, tmp_path):
    """`list-reports` shows a message when no reports exist."""
    mock_service = mocker.MagicMock()
    mock_service.app_paths.reports = tmp_path / "reports"
    mocker.patch("mko_get_mediascope_data.app.get_app_service", return_value=mock_service)
    mocker.patch("mko_get_mediascope_data.app.list_files_in_directory", return_value=[])
    runner = CliRunner()
    result = runner.invoke(app, ["list-reports"])
    assert result.exit_code == 0
    assert "No report files found" in result.stdout


def test_run_report_async_missing_file():
    """run_report_async raises FileNotFoundError for a nonexistent path."""
    from mko_get_mediascope_data.app import run_report_async

    with pytest.raises(FileNotFoundError, match="File not found"):
        asyncio.run(run_report_async("nonexistent_report.json5"))


def test_run_report_sync_wrapper(mocker, tmp_path):
    """run_report delegates to asyncio.run(run_report_async(path))."""
    from mko_get_mediascope_data.app import run_report

    report_file = tmp_path / "report.json5"
    report_file.write_text("{}", encoding="utf-8")
    mock_async = mocker.patch("mko_get_mediascope_data.app.run_report_async")
    mock_run = mocker.patch("asyncio.run")
    run_report(str(report_file))
    mock_async.assert_called_once_with(str(report_file), progress=None)
    mock_run.assert_called_once()


def test_sync_wrapper_drives_requested_async_report_work(mocker, tmp_path):
    """The sync wrapper's real ``asyncio.run`` must drive the requested async work.

    TST-014: separate assertions prove the async API was called and the
    scheduler was called, but not that the scheduler *actually drives* the
    requested async report operation.  Here the real ``asyncio.run`` is kept
    and only ``run_report_async`` is replaced with an async recording stub.
    The stub body must actually execute, with the requested path and progress,
    so a wrong or non-awaitable scheduled object fails the test.
    """
    from mko_get_mediascope_data.app import run_report

    report_file = tmp_path / "report.json5"
    report_file.write_text("{}", encoding="utf-8")

    progress = mocker.MagicMock()
    recorded: list[tuple[str, object]] = []

    async def recording_stub(report_path, *, progress=None):
        recorded.append((report_path, progress))

    mocker.patch(
        "mko_get_mediascope_data.app.run_report_async", new=recording_stub
    )

    run_report(str(report_file), progress=progress)

    # The async stub body must have actually run (work was delivered).
    assert recorded == [(str(report_file), progress)]


def test_initialize_settings_wrapper(mocker, tmp_path):
    """initialize_settings returns the path from init_project(force=...)."""
    from mko_get_mediascope_data.app import initialize_settings

    expected = tmp_path / "settings"
    mock_init = mocker.patch("mko_get_mediascope_data.app.init_project", return_value=expected)
    result = initialize_settings(force=True)
    assert result == expected
    mock_init.assert_called_once_with(force=True)


def test_sys_stdout_reconfigure_on_run(mocker, tmp_path):
    """`run` calls sys.stdout.reconfigure when stdout is a TextIOWrapper."""
    import io

    from mko_get_mediascope_data.app import run

    report_file = tmp_path / "report.json5"
    report_file.write_text("{}", encoding="utf-8")
    mock_service = mocker.MagicMock()
    mock_service.run_report = AsyncMock(return_value=[tmp_path / "output.csv"])
    mocker.patch("mko_get_mediascope_data.app.get_app_service", return_value=mock_service)

    # Make isinstance(sys.stdout, io.TextIOWrapper) return True by spoofing __class__
    mock_stdout = mocker.MagicMock()
    mock_stdout.__class__ = io.TextIOWrapper
    mock_stdout.reconfigure = mocker.MagicMock()
    mocker.patch("sys.stdout", mock_stdout)

    run(report_file, verbosity=VerbosityMode.DEFAULT)

    mock_stdout.reconfigure.assert_called_once_with(
        line_buffering=True, encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Lazy re-export via PEP 562 __getattr__ (ENT-004)
# ---------------------------------------------------------------------------


def test_lazy_import_run_report():
    """Importing run_report from the package root must succeed."""
    from mko_get_mediascope_data import run_report

    assert callable(run_report)


def test_lazy_import_raises_attribute_error():
    """Accessing a non-existent attribute must raise AttributeError."""
    import mko_get_mediascope_data

    with pytest.raises(AttributeError):
        _ = mko_get_mediascope_data.nonexistent_attr  # noqa: F841


def test_package_import_does_not_bootstrap():
    """Importing the package must NOT trigger ``app.py`` bootstrap.

    The package ``__init__`` uses PEP 562 ``__getattr__`` for lazy
    re-export, so importing the package must not eagerly import
    ``mko_get_mediascope_data.app``.
    """
    import mko_get_mediascope_data

    # Purge any pre-existing app submodule so we can verify that
    # reloading the package does not re-import it.
    saved_app = sys.modules.pop("mko_get_mediascope_data.app", None)
    try:
        importlib.reload(mko_get_mediascope_data)
        assert "mko_get_mediascope_data.app" not in sys.modules
    finally:
        if saved_app is not None:
            sys.modules["mko_get_mediascope_data.app"] = saved_app


def test_lazy_import_same_object_as_app_module():
    """The package-level run_report must be the same function object as in app.py."""
    from mko_get_mediascope_data import run_report as pkg_run_report
    from mko_get_mediascope_data.app import run_report as app_run_report

    assert pkg_run_report is app_run_report
