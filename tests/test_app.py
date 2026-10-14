import importlib

import pytest
from typer.testing import CliRunner

from mko_get_mediascope_data.app import app


def test_cli_init():
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--help"])
    assert result.exit_code == 0
    assert "Initialize" in result.stdout


# ---------------------------------------------------------------------------
# Lazy re-export via PEP 562 __getattr__ (ENT-004)
# ---------------------------------------------------------------------------


def test_lazy_import_run_report():
    """Importing run_report from the package root must succeed."""
    from mko_get_mediascope_data import run_report

    assert callable(run_report)


def test_lazy_import_run_report_async():
    """Importing run_report_async from the package root must succeed."""
    from mko_get_mediascope_data import run_report_async

    assert callable(run_report_async)


def test_lazy_import_initialize_settings():
    """Importing initialize_settings from the package root must succeed."""
    from mko_get_mediascope_data import initialize_settings

    assert callable(initialize_settings)


def test_lazy_import_raises_attribute_error():
    """Accessing a non-existent attribute must raise AttributeError."""
    import mko_get_mediascope_data

    with pytest.raises(AttributeError):
        _ = mko_get_mediascope_data.nonexistent_attr  # noqa: F841


def test_package_import_does_not_bootstrap(mocker):
    """Importing the package must NOT trigger AppService bootstrap.

    Reloads ``__init__.py`` while mocking ``AppService.__init__``.
    If no ``AppService`` is created during the reload, the mock is
    never called — proving the package body does not eagerly import
    ``app.py`` or invoke ``get_app_service()``.
    """
    import mko_get_mediascope_data

    mock_init = mocker.patch(
        "mko_get_mediascope_data.core.services.AppService.__init__"
    )

    importlib.reload(mko_get_mediascope_data)

    mock_init.assert_not_called()


def test_lazy_import_same_object_as_app_module():
    """The package-level run_report must be the same function object as in app.py."""
    from mko_get_mediascope_data import run_report as pkg_run_report
    from mko_get_mediascope_data.app import run_report as app_run_report

    assert pkg_run_report is app_run_report
