"""Tests for VerbosityMode mapping, AppSettings defaults, configure_logging_level,
and the CLI --verbosity flag (Phase 5).

Covers:
- ``VerbosityMode.to_logging_level()`` enum→level mapping.
- ``AppSettings.verbosity_mode`` default and Pydantic string/enum coercion.
- ``configure_logging_level()`` setting both the root logger and all handlers.
- ``configure_logging_level`` correctly overriding ``dictConfig``-set levels
  (ordering fix: ``get_app_service`` before ``configure_logging_level``).
- CLI ``--verbosity`` flag integration via ``CliRunner``; config is consulted
  when the flag is omitted.
"""

import logging
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

import mko_get_mediascope_data.app as app_module
from mko_get_mediascope_data.app import app, configure_logging_level
from mko_get_mediascope_data.core.models import AppSettings, VerbosityMode
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

    def test_default_maps_to_info(self) -> None:
        assert VerbosityMode.DEFAULT.to_logging_level() == logging.INFO

    def test_verbose_maps_to_debug(self) -> None:
        assert VerbosityMode.VERBOSE.to_logging_level() == logging.DEBUG


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

    def test_default_sets_root_and_handlers_to_info(
        self, root_logger_state: logging.Logger
    ) -> None:
        configure_logging_level(VerbosityMode.DEFAULT)
        assert root_logger_state.level == logging.INFO
        for handler in root_logger_state.handlers:
            assert handler.level == logging.INFO

    def test_verbose_sets_root_and_handlers_to_debug(
        self, root_logger_state: logging.Logger
    ) -> None:
        configure_logging_level(VerbosityMode.VERBOSE)
        assert root_logger_state.level == logging.DEBUG
        for handler in root_logger_state.handlers:
            assert handler.level == logging.DEBUG

    def test_lowers_handler_level_that_was_higher(
        self, root_logger_state: logging.Logger
    ) -> None:
        """A handler at ERROR level must be lowered to DEBUG in VERBOSE mode."""
        test_handler = logging.StreamHandler()
        test_handler.setLevel(logging.ERROR)
        root_logger_state.addHandler(test_handler)
        try:
            configure_logging_level(VerbosityMode.VERBOSE)
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

    def test_level_survives_dict_config(
        self,
        hermetic_app_service: AppService,
        root_logger_state: logging.Logger,
    ) -> None:
        """configure_logging_level overrides dictConfig-set levels (ordering fix).

        ``hermetic_app_service.__init__`` calls ``prepare_app()`` which runs
        ``dictConfig`` — setting the root logger to INFO.  ``configure_logging_level``
        must then successfully lower the level to DEBUG, proving the CLI
        calls ``get_app_service()`` (dictConfig) *before* ``configure_logging_level()``
        (spec §7: override must happen after dictConfig).
        """
        # dictConfig (triggered by hermetic_app_service) sets root to INFO
        assert root_logger_state.level == logging.INFO

        configure_logging_level(VerbosityMode.VERBOSE)
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

    def test_default_sets_root_to_info(
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
        assert logging.getLogger().level == logging.INFO

    def test_verbose_sets_root_to_debug(
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
        assert logging.getLogger().level == logging.DEBUG

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
        ``VerbosityMode.VERBOSE`` (not the default) to prove the config value
        is consulted when the flag is absent.
        """
        mock_app_service.app_config.app_settings.verbosity_mode = VerbosityMode.VERBOSE
        spy = mocker.spy(app_module, "configure_logging_level")

        runner = CliRunner()
        result = runner.invoke(app, ["run", str(report_file)])
        assert result.exit_code == 0, result.stdout
        spy.assert_called_once_with(VerbosityMode.VERBOSE)
        assert logging.getLogger().level == logging.DEBUG
