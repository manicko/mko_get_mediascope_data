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
    """Lazily re-export public API names from the ``app`` submodule (PEP 562).

    Bare package import does **not** import ``mko_get_mediascope_data.app``: the
    package module body only defines ``__version__``, ``__all__``, ``__getattr__``
    and ``__dir__``. The first access of a supported public name (any entry in
    ``__all__``) triggers ``importlib.import_module("mko_get_mediascope_data.app")``,
    which executes ``app.py``'s module body (importing ``.core.*`` and constructing
    the Typer app); the resolved object is cached in this package's ``globals()`` so
    subsequent accesses bypass ``__getattr__``. Accessing a name not in ``__all__``
    raises ``AttributeError``.
    """
    if name in __all__:
        import importlib

        module = importlib.import_module("mko_get_mediascope_data.app")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__dir__ = lambda: sorted(set(globals()) | set(__all__))  # noqa: E731
