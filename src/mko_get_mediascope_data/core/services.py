import logging.config
from collections.abc import Generator
from datetime import datetime
from functools import cached_property
from pathlib import Path

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
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.reports import ReportFactory, TVMediaReport

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
        logging_settings = LoggingSettings(
            **utils.yaml_to_dict(self.app_paths.log_config)
        )
        app_settings = AppSettings(**utils.yaml_to_dict(self.app_paths.app_config))

        return AppConfig(logging_settings=logging_settings, app_settings=app_settings)

    def prepare_app(self):
        for handler in self.app_config.logging_settings.handlers.values():
            if isinstance(handler, dict) and "filename" in handler:
                file_path = self.resolver.resolve(handler["filename"])
                self.resolver.ensure_file_parent(file_path)
                handler["filename"] = file_path

    @cached_property
    def export_folder(self) -> Path:
        _path = self.resolver.resolve(self.app_config.app_settings.export_folder)
        self.resolver.ensure_dir(_path)
        return _path

    @staticmethod
    def get_report_settings(report_settings_path: Path) -> Generator[ReportSettings]:
        try:
            for rep in utils.yaml_to_dict(report_settings_path):
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
        ).with_suffix(".yaml")
        return DataDefaults(**utils.yaml_to_dict(data_default_settings_path))

    @staticmethod
    def merge_data_settings(
        defaults: DataDefaults, report_settings: ReportSettings
    ) -> dict:
        data_settings = defaults.DATA_DEFAULTS.copy()

        subtype = report_settings.report_subtype
        if subtype.value and hasattr(defaults, subtype.value):
            data_settings.update(getattr(defaults, subtype.value))

        for k, v in report_settings.model_dump().items():
            if k in data_settings:
                data_settings[k] = v

        if report_settings.data_lang == "ru":
            data_settings["slices"] = utils.en_to_ru(data_settings["slices"])

        return data_settings

    def get_data_settings(self, report_settings: ReportSettings) -> Data:
        defaults: DataDefaults = self.get_default_data_settings(report_settings)
        data_settings: dict = self.merge_data_settings(defaults, report_settings)
        return Data(**data_settings)

    def get_connection(self, media_type: MediaType) -> MediaVortexTask:
        try:
            connection_service = _CONNECTION_MAP[media_type]
        except KeyError as err:
            raise ValueError(f"Unsupported media type: {media_type}") from err
        return connection_service(
            settings_filename=self.app_paths.connection_config,
            check_version=False,
        )

    async def run_report(self, report_settings_path: Path) -> list[Path]:
        report_settings_sequence: Generator[ReportSettings] = self.get_report_settings(
            report_settings_path
        )
        export_paths: list[Path] = []

        start_time = datetime.now().replace(microsecond=0)
        print(
            f"\n{'-' * 10}  Обработка стартовала: {start_time} {'-' * 10}\n",
            flush=True,
        )

        idx = 1
        for report_settings in report_settings_sequence:
            print(f"\nОтчёт {idx} → {report_settings.report_subtype}\n")
            export_path: Path = Path(self.export_folder / report_settings.relative_path)
            self.resolver.ensure_dir(export_path)

            data_settings: Data = self.get_data_settings(report_settings)

            network_client: NetworkClient = NetworkClient()
            report_service: MediaVortexTask = await network_client.call(
                self.get_connection,
                report_settings.media,
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
                check_done=report_settings.check_done,
            )

            await rep.create_report()
            export_paths.append(export_path)
            idx += 1

        end_time = datetime.now().replace(microsecond=0)
        print(
            f"\n{'-' * 10}  Обработка завершена: {end_time}. "
            f"Общее время: {end_time - start_time} {'-' * 10}\n",
            flush=True,
        )

        return export_paths


app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(APP_PATHS.user_dir))
log_config = app_service.app_config.logging_settings

logging.config.dictConfig(log_config.model_dump())
