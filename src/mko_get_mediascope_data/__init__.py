"""mko_get_mediascope_data — tools for loading data from the Mediascope API."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mko_get_mediascope_data")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

# pyright: reportUnsupportedDunderAll=false
from typing import TYPE_CHECKING, Any

__all__ = [
    "run_report",
    "run_report_async",
    "initialize_settings",
    "init",
    "run",
    "list_reports",
    "app",
]

if TYPE_CHECKING:
    from .app import (
        app as app,
        init as init,
        list_reports as list_reports,
        run as run,
        run_report as run_report,
        run_report_async as run_report_async,
        initialize_settings as initialize_settings,
    )


def __getattr__(name: str) -> Any:
    """Lazily re-export public API names from the ``app`` submodule.

    Uses PEP 562 so that importing the package or accessing any single
    name does not eagerly execute ``app.py``'s module body.
    """
    if name in __all__:
        import importlib

        module = importlib.import_module("mko_get_mediascope_data.app")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__dir__ = lambda: sorted(set(globals()) | set(__all__))  # noqa: E731
