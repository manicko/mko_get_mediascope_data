import logging.config
from collections.abc import Generator
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Any

from mediascope_api.mediavortex.tasks import MediaVortexTask

import mko_get_mediascope_data.core.utils as utils
from mko_get_mediascope_data.core.models import (
    AppConfig,
    AppSettings,
    Data,
    DataDefaults,
    LoggingSettings,
    MediaType,
    ReportSettings,
)
from mko_get_mediascope_data.core.errors import WrongDataSettings
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.progress import NullProgressReporter, ProgressReporter
from mko_get_mediascope_data.core.reports import ReportFactory, TVMediaReport

logger = logging.getLogger(__name__)

_CONNECTION_MAP: dict[MediaType, type] = {
    MediaType.TV: MediaVortexTask,
    MediaType.TV_REG: MediaVortexTask,
}


class AppService:
    def __init__(self, app_paths: AppPaths, resolver: PathResolver):
        self.app_paths = app_paths
        self.resolver = resolver
        self.prepare_app()

    @cached_property
    def app_config(self) -> AppConfig:
        log_config_dict = utils.json5_to_dict(self.app_paths.log_config)
        if not isinstance(log_config_dict, dict):
            raise TypeError(
                f"Expected dict in {self.app_paths.log_config}, "
                f"got {type(log_config_dict).__name__}"
            )
        logging_settings = LoggingSettings(**log_config_dict)

        app_config_dict = utils.json5_to_dict(self.app_paths.app_config)
        if not isinstance(app_config_dict, dict):
            raise TypeError(
                f"Expected dict in {self.app_paths.app_config}, "
                f"got {type(app_config_dict).__name__}"
            )
        app_settings = AppSettings(**app_config_dict)

        return AppConfig(logging_settings=logging_settings, app_settings=app_settings)

    def prepare_app(self):
        for handler in self.app_config.logging_settings.handlers.values():
            if isinstance(handler, dict) and "filename" in handler:
                file_path = self.resolver.resolve(handler["filename"])
                self.resolver.ensure_file_parent(file_path)
                handler["filename"] = file_path

        logging.config.dictConfig(self.app_config.logging_settings.model_dump())

    def reload_config(self) -> None:
        """Invalidate cached configuration and re-apply from JSON5 files.

        Invalidates the ``cached_property`` caches for ``app_config`` and
        ``export_folder`` so the next access re-reads the JSON5 config files.
        Also re-runs ``prepare_app()`` to re-resolve handler paths and
        re-apply ``dictConfig``.
        """
        self.__dict__.pop("app_config", None)
        self.__dict__.pop("export_folder", None)
        self.prepare_app()

    @cached_property
    def export_folder(self) -> Path:
        _path = self.resolver.resolve(self.app_config.app_settings.export_folder)
        self.resolver.ensure_dir(_path)
        return _path

    @staticmethod
    def get_report_settings(report_settings_path: Path) -> Generator[ReportSettings]:
        try:
            report_configs = utils.json5_to_dict(report_settings_path)
            # json5_to_dict returns dict[str, Any] | list[Any]; normalize to a list
            # of report-config dicts so we never iterate dict keys (str) by mistake.
            if isinstance(report_configs, dict):
                report_configs = list(report_configs.values())
            for rep in report_configs:
                if isinstance(rep, dict):
                    yield ReportSettings(**rep)
        except Exception as e:
            logging.exception(e)
            raise e

    def get_default_data_settings(
        self, report_settings: ReportSettings
    ) -> DataDefaults:
        data_default_settings_path = Path(
            self.app_paths.defaults_dir,
            report_settings.media.value.lower(),
            report_settings.report_type.value.lower(),
        ).with_suffix(".json5")
        if not data_default_settings_path.exists():
            raise WrongDataSettings(
                f"Default data settings file not found for "
                f"media={report_settings.media.value}, "
                f"report_type={report_settings.report_type.value}: "
                f"{data_default_settings_path}"
            )
        data_defaults_dict = utils.json5_to_dict(data_default_settings_path)
        if not isinstance(data_defaults_dict, dict):
            raise TypeError(
                f"Expected dict in {data_default_settings_path}, "
                f"got {type(data_defaults_dict).__name__}"
            )
        return DataDefaults(**data_defaults_dict)

    @staticmethod
    def merge_data_settings(
        defaults: DataDefaults, report_settings: ReportSettings
    ) -> dict[str, Any]:
        data_settings = defaults.DATA_DEFAULTS.copy()

        subtype = report_settings.report_subtype
        if subtype.value and hasattr(defaults, subtype.value):
            data_settings.update(getattr(defaults, subtype.value))

        for k, v in report_settings.model_dump().items():
            if k in data_settings:
                data_settings[k] = v

        if report_settings.data_lang == "ru":
            slices = data_settings.get("slices")
            if slices is not None:
                data_settings["slices"] = utils.en_to_ru(slices)

        return data_settings

    def get_data_settings(self, report_settings: ReportSettings) -> Data:
        defaults: DataDefaults = self.get_default_data_settings(report_settings)
        data_settings: dict[str, Any] = self.merge_data_settings(defaults, report_settings)
        return Data(**data_settings)

    def get_connection(self, media_type: MediaType) -> MediaVortexTask:
        try:
            connection_service = _CONNECTION_MAP[media_type]
        except KeyError as err:
            raise ValueError(f"Unsupported media type: {media_type}") from err
        cred_path = self.app_paths.connection_config
        if not cred_path.exists():
            raise FileNotFoundError(
                f"Mediascope credentials not found at '{cred_path}'. "
                f"Run 'mko-get-mediascope init' to create the settings directory."
            )
        return connection_service(
            settings_filename=cred_path,
            check_version=False,
        )

    async def run_report(
        self,
        report_settings_path: Path,
        progress: ProgressReporter | None = None,
    ) -> list[Path]:
        reporter = progress if progress is not None else NullProgressReporter()
        report_settings_sequence: Generator[ReportSettings] = self.get_report_settings(
            report_settings_path
        )
        export_paths: list[Path] = []

        start_time = datetime.now().replace(microsecond=0)
        logger.info(
            f"{'-' * 10}  Processing started: {start_time} {'-' * 10}"
        )

        idx = 1
        for report_settings in report_settings_sequence:
            logger.info(f"Report {idx}: {report_settings.report_subtype}")
            export_path: Path = Path(self.export_folder / report_settings.relative_path)
            self.resolver.ensure_dir(export_path)

            data_settings: Data = self.get_data_settings(report_settings)

            network_client: NetworkClient = NetworkClient(
                max_concurrent_requests=self.app_config.app_settings.max_concurrent_requests,
                sleep_time=self.app_config.app_settings.sleep_time,
                max_attempts=self.app_config.app_settings.max_attempts,
                call_timeout=self.app_config.app_settings.call_timeout,
                retry_5xx=self.app_config.app_settings.retry_5xx,
                log_errors_detail=self.app_config.app_settings.log_errors_detail,
            )
            report_service: MediaVortexTask = await network_client.call(
                self.get_connection,
                report_settings.media,
                api_call=False,
            )

            report_strategies = ReportFactory(report_settings)

            rep = TVMediaReport(
                report_settings=report_settings,
                data_settings=data_settings,
                export_path=export_path,
                report_service=report_service,
                network_client=network_client,
                task_strategy=report_strategies.task_strategy(),
                data_strategy=report_strategies.data_strategy(),
                max_tasks=self.app_config.app_settings.max_tasks,
                poll=self.app_config.app_settings.poll,
                check_done=report_settings.check_done,
                reporter=reporter,
            )

            reporter.report_started(str(report_settings_path), 0)
            await rep.create_report()
            reporter.report_completed(str(report_settings_path))
            export_paths.append(export_path)
            idx += 1

        end_time = datetime.now().replace(microsecond=0)
        logger.info(
            f"{'-' * 10}  Processing finished: {end_time}. "
            f"Total duration: {end_time - start_time} {'-' * 10}"
        )

        return export_paths


_app_service: AppService | None = None


def get_app_service() -> AppService:
    """Return the process-wide AppService singleton, creating it on first call."""
    global _app_service
    if _app_service is None:
        _app_service = AppService(
            app_paths=APP_PATHS,
            resolver=PathResolver(APP_PATHS.user_dir),
        )
    return _app_service
