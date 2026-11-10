"""Tests for VerbosityMode mapping, AppSettings defaults, configure_logging_level,
and the CLI --verbosity flag (Phase 5).

Covers:
- ``VerbosityMode.to_logging_level()`` enum→level mapping.
- ``AppSettings.verbosity_mode`` default and Pydantic string/enum coercion.
- ``configure_logging_level()`` setting both the root logger and all handlers.
- Configured verbosity applied in the service lifecycle after ``dictConfig``
  (service init/library access and ``reload_config``), with explicit CLI-style
  overrides still effective.
- ``run_report_async`` library usage with configured verbosity and mocked pipeline.
- CLI ``--verbosity`` flag integration via ``CliRunner``; config is consulted
  when the flag is omitted.
"""

import logging
import shutil
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

import mko_get_mediascope_data.app as app_module
import mko_get_mediascope_data.core.services as services_module
from mko_get_mediascope_data.app import app, configure_logging_level
from mko_get_mediascope_data.core.models import AppSettings, VerbosityMode
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.services import AppService


# ---------------------------------------------------------------------------
# Shared fixture: save/restore root logger level + handler levels
# ---------------------------------------------------------------------------


@pytest.fixture
def root_logger_state() -> Iterator[logging.Logger]:
    """Capture and restore root logger level and handler levels across a test."""
    root = logging.getLogger()
    saved_level = root.level
    saved_handlers = [(h, h.level) for h in root.handlers]
    yield root
    root.setLevel(saved_level)
    for handler, level in saved_handlers:
        handler.setLevel(level)


# ---------------------------------------------------------------------------
# VerbosityMode.to_logging_level()
# ---------------------------------------------------------------------------


class TestVerbosityModeToLoggingLevel:
    """Each VerbosityMode must map to the correct stdlib logging level."""

    def test_quiet_maps_to_warning(self) -> None:
        assert VerbosityMode.QUIET.to_logging_level() == logging.WARNING

    def test_default_maps_to_warning(self) -> None:
        assert VerbosityMode.DEFAULT.to_logging_level() == logging.WARNING

    def test_verbose_maps_to_info(self) -> None:
        assert VerbosityMode.VERBOSE.to_logging_level() == logging.INFO

    def test_debug_maps_to_debug(self) -> None:
        assert VerbosityMode.DEBUG.to_logging_level() == logging.DEBUG

    def test_silent_maps_to_critical(self) -> None:
        assert VerbosityMode.SILENT.to_logging_level() == logging.CRITICAL


# ---------------------------------------------------------------------------
# AppSettings.verbosity_mode
# ---------------------------------------------------------------------------


class TestAppSettingsVerbosityMode:
    """AppSettings.verbosity_mode defaults to DEFAULT and accepts enum/string values."""

    def test_default_is_default_mode(self) -> None:
        settings = AppSettings(export_folder=Path("output"))
        assert settings.verbosity_mode == VerbosityMode.DEFAULT

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("QUIET", VerbosityMode.QUIET),
            ("DEFAULT", VerbosityMode.DEFAULT),
            ("VERBOSE", VerbosityMode.VERBOSE),
            ("DEBUG", VerbosityMode.DEBUG),
            ("SILENT", VerbosityMode.SILENT),
        ],
    )
    def test_accepts_string_value(self, raw: str, expected: VerbosityMode) -> None:
        settings = AppSettings.model_validate(
            {"export_folder": Path("output"), "verbosity_mode": raw}
        )
        assert settings.verbosity_mode == expected

    def test_accepts_enum_member(self) -> None:
        settings = AppSettings(
            export_folder=Path("output"), verbosity_mode=VerbosityMode.VERBOSE
        )
        assert settings.verbosity_mode == VerbosityMode.VERBOSE


# ---------------------------------------------------------------------------
# configure_logging_level()
# ---------------------------------------------------------------------------


class TestConfigureLoggingLevel:
    """configure_logging_level sets the root logger *and* all handlers."""

    def test_quiet_sets_root_and_handlers_to_warning(
        self, root_logger_state: logging.Logger
    ) -> None:
        configure_logging_level(VerbosityMode.QUIET)
        assert root_logger_state.level == logging.WARNING
        for handler in root_logger_state.handlers:
            assert handler.level == logging.WARNING

    def test_default_sets_root_and_handlers_to_warning(
        self, root_logger_state: logging.Logger
    ) -> None:
        configure_logging_level(VerbosityMode.DEFAULT)
        assert root_logger_state.level == logging.WARNING
        for handler in root_logger_state.handlers:
            assert handler.level == logging.WARNING

    def test_verbose_sets_root_and_handlers_to_info(
        self, root_logger_state: logging.Logger
    ) -> None:
        configure_logging_level(VerbosityMode.VERBOSE)
        assert root_logger_state.level == logging.INFO
        for handler in root_logger_state.handlers:
            assert handler.level == logging.INFO

    def test_debug_sets_root_and_handlers_to_debug(
        self, root_logger_state: logging.Logger
    ) -> None:
        configure_logging_level(VerbosityMode.DEBUG)
        assert root_logger_state.level == logging.DEBUG
        for handler in root_logger_state.handlers:
            assert handler.level == logging.DEBUG

    def test_silent_sets_root_and_handlers_to_critical(
        self, root_logger_state: logging.Logger
    ) -> None:
        configure_logging_level(VerbosityMode.SILENT)
        assert root_logger_state.level == logging.CRITICAL
        for handler in root_logger_state.handlers:
            assert handler.level == logging.CRITICAL

    def test_lowers_handler_level_that_was_higher(
        self, root_logger_state: logging.Logger
    ) -> None:
        """A handler at ERROR level must be lowered to DEBUG in DEBUG mode."""
        test_handler = logging.StreamHandler()
        test_handler.setLevel(logging.ERROR)
        root_logger_state.addHandler(test_handler)
        try:
            configure_logging_level(VerbosityMode.DEBUG)
            assert test_handler.level == logging.DEBUG
        finally:
            root_logger_state.removeHandler(test_handler)

    def test_no_handlers_does_not_raise(
        self, root_logger_state: logging.Logger
    ) -> None:
        """Calling with zero handlers must not raise (just sets root level)."""
        original_handlers = root_logger_state.handlers[:]
        root_logger_state.handlers.clear()
        try:
            configure_logging_level(VerbosityMode.QUIET)
            assert root_logger_state.level == logging.WARNING
        finally:
            root_logger_state.handlers.extend(original_handlers)

    def test_configured_verbosity_applied_after_dict_config_and_override_effective(
        self,
        hermetic_app_service: AppService,
        root_logger_state: logging.Logger,
    ) -> None:
        """Service init applies configured verbosity after ``dictConfig``.

        Hermetic defaults: app config omits ``verbosity_mode`` => DEFAULT (WARNING)
        while ``log_config.json5`` sets root/handlers to INFO. ``AppService``
        ``prepare_app()`` runs ``dictConfig`` then applies configured verbosity, so
        root and all root handlers end at WARNING after initialization. An explicit
        ``configure_logging_level`` call (CLI-style precedence) still overrides.
        """
        assert root_logger_state.level == logging.WARNING
        for handler in root_logger_state.handlers:
            if handler.__class__.__name__ == "LogCaptureHandler":
                continue
            assert handler.level == logging.WARNING

        configure_logging_level(VerbosityMode.DEBUG)
        assert root_logger_state.level == logging.DEBUG
        for handler in root_logger_state.handlers:
            assert handler.level == logging.DEBUG


# ---------------------------------------------------------------------------
# CLI --verbosity flag
# ---------------------------------------------------------------------------


class TestCliVerbosityFlag:
    """The CLI ``--verbosity`` flag must invoke configure_logging_level with the
    correct mode and adjust the root logger level.
    """

    @pytest.fixture
    def report_file(self, tmp_path: Path) -> Path:
        report = tmp_path / "report.json5"
        report.write_text("{}", encoding="utf-8")
        return report

    @pytest.fixture
    def mock_run_report_async(self, mocker) -> AsyncMock:
        """Prevent the real pipeline from executing during CLI tests."""
        return mocker.patch(
            "mko_get_mediascope_data.app.run_report_async",
            new_callable=AsyncMock,
        )

    @pytest.fixture
    def mock_app_service(self, mocker) -> MagicMock:
        """Mock ``get_app_service`` so the CLI ``run`` command does not load
        real config files.

        Returns a ``MagicMock`` whose ``app_config.app_settings.verbosity_mode``
        is set to ``VerbosityMode.DEFAULT`` — the config default consulted when
        ``--verbosity`` is omitted.
        """
        mock_service = mocker.MagicMock()
        mock_service.app_config.app_settings.verbosity_mode = VerbosityMode.DEFAULT
        mocker.patch(
            "mko_get_mediascope_data.app.get_app_service", return_value=mock_service
        )
        return mock_service

    def test_quiet_sets_root_to_warning(
        self,
        mocker,
        mock_app_service: MagicMock,
        mock_run_report_async: AsyncMock,
        report_file: Path,
        root_logger_state: logging.Logger,
    ) -> None:
        spy = mocker.spy(app_module, "configure_logging_level")

        runner = CliRunner()
        result = runner.invoke(
            app, ["run", str(report_file), "--verbosity", "QUIET"]
        )
        assert result.exit_code == 0, result.stdout
        spy.assert_called_once_with(VerbosityMode.QUIET)
        assert logging.getLogger().level == logging.WARNING

    def test_default_sets_root_to_warning(
        self,
        mocker,
        mock_app_service: MagicMock,
        mock_run_report_async: AsyncMock,
        report_file: Path,
        root_logger_state: logging.Logger,
    ) -> None:
        spy = mocker.spy(app_module, "configure_logging_level")

        runner = CliRunner()
        result = runner.invoke(
            app, ["run", str(report_file), "--verbosity", "DEFAULT"]
        )
        assert result.exit_code == 0, result.stdout
        spy.assert_called_once_with(VerbosityMode.DEFAULT)
        assert logging.getLogger().level == logging.WARNING

    def test_verbose_sets_root_to_info(
        self,
        mocker,
        mock_app_service: MagicMock,
        mock_run_report_async: AsyncMock,
        report_file: Path,
        root_logger_state: logging.Logger,
    ) -> None:
        spy = mocker.spy(app_module, "configure_logging_level")

        runner = CliRunner()
        result = runner.invoke(
            app, ["run", str(report_file), "--verbosity", "VERBOSE"]
        )
        assert result.exit_code == 0, result.stdout
        spy.assert_called_once_with(VerbosityMode.VERBOSE)
        assert logging.getLogger().level == logging.INFO

    def test_debug_sets_root_to_debug(
        self,
        mocker,
        mock_app_service: MagicMock,
        mock_run_report_async: AsyncMock,
        report_file: Path,
        root_logger_state: logging.Logger,
    ) -> None:
        spy = mocker.spy(app_module, "configure_logging_level")

        runner = CliRunner()
        result = runner.invoke(
            app, ["run", str(report_file), "--verbosity", "DEBUG"]
        )
        assert result.exit_code == 0, result.stdout
        spy.assert_called_once_with(VerbosityMode.DEBUG)
        assert logging.getLogger().level == logging.DEBUG

    def test_silent_sets_root_to_critical(
        self,
        mocker,
        mock_app_service: MagicMock,
        mock_run_report_async: AsyncMock,
        report_file: Path,
        root_logger_state: logging.Logger,
    ) -> None:
        spy = mocker.spy(app_module, "configure_logging_level")

        runner = CliRunner()
        result = runner.invoke(
            app, ["run", str(report_file), "--verbosity", "SILENT"]
        )
        assert result.exit_code == 0, result.stdout
        spy.assert_called_once_with(VerbosityMode.SILENT)
        assert logging.getLogger().level == logging.CRITICAL

    def test_silent_suppresses_console_output(
        self,
        mocker,
        mock_app_service: MagicMock,
        mock_run_report_async: AsyncMock,
        report_file: Path,
        root_logger_state: logging.Logger,
    ) -> None:
        """SILENT mode must suppress 'Running report' banner, progress bars,
        and the 'Report completed' success message."""
        runner = CliRunner()
        result = runner.invoke(
            app, ["run", str(report_file), "--verbosity", "SILENT"]
        )
        assert result.exit_code == 0
        assert "Running report" not in result.stdout
        assert "Report completed" not in result.stdout

    def test_default_verbosity_when_flag_omitted(
        self,
        mocker,
        mock_app_service: MagicMock,
        mock_run_report_async: AsyncMock,
        report_file: Path,
        root_logger_state: logging.Logger,
    ) -> None:
        """Omitting --verbosity consults config for the default mode.

        The mock's ``app_config.app_settings.verbosity_mode`` is set to
        ``VerbosityMode.DEBUG`` (not the default) to prove the config value
        is consulted when the flag is absent.
        """
        mock_app_service.app_config.app_settings.verbosity_mode = VerbosityMode.DEBUG
        spy = mocker.spy(app_module, "configure_logging_level")

        runner = CliRunner()
        result = runner.invoke(app, ["run", str(report_file)])
        assert result.exit_code == 0, result.stdout
        spy.assert_called_once_with(VerbosityMode.DEBUG)
        assert logging.getLogger().level == logging.DEBUG
# ---------------------------------------------------------------------------
# Library/init configured verbosity (service lifecycle)
# ---------------------------------------------------------------------------


_LOG_CONFIG_JSON5 = """
{
  "version": 1,
  "disable_existing_loggers": false,
  "formatters": {
    "basic": {
      "class": "logging.Formatter",
      "format": "%(levelname)s %(message)s",
    },
  },
  "handlers": {
    "console": {
      "class": "logging.StreamHandler",
      "formatter": "basic",
      "level": "INFO",
      "stream": "ext://sys.stdout",
    },
  },
  "root": {
    "level": "INFO",
    "handlers": ["console"],
  },
  "loggers": {},
}
"""

_APP_CONFIG_WITH_VERBOSITY_TEMPLATE = """
{{
  "export_folder": "output",
  "max_concurrent_requests": 5,
  "max_tasks": 100,
  "call_timeout": 300.0,
  "max_attempts": 10,
  "sleep_time": 2,
  "retry_5xx": true,
  "log_errors_detail": false,
  "verbosity_mode": "{mode}",
  "poll": {{
    "sleep_time": 30,
    "initial_interval": 30.0,
    "max_interval": 60.0,
    "max_polls": 60,
    "deadline": 3600.0,
  }},
  "send_task": {{
    "retryable": true,
    "retry_on_connection_error": true,
  }},
}}
"""


def _write_app_service_config(
    tmp_path: Path, verbosity_mode: VerbosityMode
) -> tuple[AppPaths, PathResolver]:
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir(parents=True, exist_ok=True)
    (settings_dir / "log_config.json5").write_text(_LOG_CONFIG_JSON5, encoding="utf-8")
    (settings_dir / "app_config.json5").write_text(
        _APP_CONFIG_WITH_VERBOSITY_TEMPLATE.format(mode=verbosity_mode.value),
        encoding="utf-8",
    )
    shutil.copytree(APP_PATHS.app_settings_dir / "defaults", settings_dir / "defaults")
    app_paths = AppPaths(app_dir=tmp_path, app_name="test", user_dir=tmp_path)
    return app_paths, PathResolver(tmp_path)


class TestLibraryConfiguredVerbosity:
    """Configured verbosity is applied during service init/library access."""

    @pytest.mark.parametrize(
        "mode",
        [VerbosityMode.DEBUG, VerbosityMode.VERBOSE, VerbosityMode.SILENT],
    )
    def test_service_init_applies_explicit_verbosity(
        self,
        tmp_path: Path,
        root_logger_state: logging.Logger,
        mode: VerbosityMode,
    ) -> None:
        app_paths, resolver = _write_app_service_config(tmp_path, mode)
        AppService(app_paths=app_paths, resolver=resolver)
        expected = mode.to_logging_level()
        assert root_logger_state.level == expected
        for handler in root_logger_state.handlers:
            if handler.__class__.__name__ == "LogCaptureHandler":
                continue
            assert handler.level == expected

    @pytest.mark.parametrize(
        "mode",
        [VerbosityMode.DEBUG, VerbosityMode.VERBOSE],
    )
    def test_get_app_service_singleton_applies_explicit_verbosity(
        self,
        tmp_path: Path,
        root_logger_state: logging.Logger,
        monkeypatch: pytest.MonkeyPatch,
        mode: VerbosityMode,
    ) -> None:
        app_paths, resolver = _write_app_service_config(tmp_path, mode)
        monkeypatch.setattr(services_module, "APP_PATHS", app_paths)
        monkeypatch.setattr(
            services_module, "PathResolver", lambda _user_dir: resolver
        )
        services_module._app_service = None
        try:
            services_module.get_app_service()
            expected = mode.to_logging_level()
            assert root_logger_state.level == expected
            for handler in root_logger_state.handlers:
                if handler.__class__.__name__ == "LogCaptureHandler":
                    continue
                assert handler.level == expected
        finally:
            services_module._app_service = None


@pytest.mark.asyncio
async def test_run_report_async_applies_configured_verbosity(
    tmp_path: Path,
    root_logger_state: logging.Logger,
    mocker,
) -> None:
    """Library run_report_async uses service-configured verbosity (mocked pipeline)."""
    report_path = tmp_path / "report.json5"
    report_path.write_text("{}", encoding="utf-8")
    app_paths, resolver = _write_app_service_config(tmp_path, VerbosityMode.DEBUG)
    service = AppService(app_paths=app_paths, resolver=resolver)
    mocker.patch("mko_get_mediascope_data.app.get_app_service", return_value=service)
    run_report_mock = mocker.patch.object(
        AppService, "run_report", new_callable=AsyncMock, return_value=[]
    )

    await app_module.run_report_async(report_path)

    run_report_mock.assert_awaited_once()
    assert root_logger_state.level == logging.DEBUG
    for handler in root_logger_state.handlers:
        if handler.__class__.__name__ == "LogCaptureHandler":
            continue
        assert handler.level == logging.DEBUG


def test_reload_config_reapplies_configured_verbosity(
    tmp_path: Path,
    root_logger_state: logging.Logger,
) -> None:
    """reload_config restores configured verbosity after external mutation."""
    app_paths, resolver = _write_app_service_config(tmp_path, VerbosityMode.DEBUG)
    service = AppService(app_paths=app_paths, resolver=resolver)
    assert root_logger_state.level == logging.DEBUG

    root_logger_state.setLevel(logging.ERROR)
    for handler in root_logger_state.handlers:
        handler.setLevel(logging.ERROR)

    service.reload_config()

    assert root_logger_state.level == logging.DEBUG
    for handler in root_logger_state.handlers:
        if handler.__class__.__name__ == "LogCaptureHandler":
            continue
        assert handler.level == logging.DEBUG
