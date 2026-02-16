from pathlib import Path
from typing import Any, Literal
from datetime import date
from enum import Enum
from pydantic import HttpUrl, BaseModel, field_validator, Field, ConfigDict, model_validator
from pydantic_settings import BaseSettings

import mko_get_mediascope_data.core.utils as utils
from mko_get_mediascope_data.core.path_service import PathResolver
from mko_get_mediascope_data.core.path_service import PATHS


# Logging settings
class LoggingSettings(BaseModel):
    """
    Logging configuration.
    """

    version: int = 1
    disable_existing_loggers: bool = False
    formatters: dict[str, Any]
    handlers: dict[str, Any]
    loggers: dict[str, Any]
    root: dict[str, Any]


class MediascopeCredentials(BaseModel):
    username: str
    passw: str
    client_id: str = "dds-external"
    auth_server: HttpUrl = "https://auth.mediascope.net/auth/realms/mediascope/protocol/openid-connect/token"
    root_url: HttpUrl = "https://api.mediascope.net/tvindex/api/v1"


class AppSettings(BaseModel):
    export_folder: Path

    @field_validator('export_folder', mode="before")
    @classmethod
    def normalize_path(cls, folder: Path | str) -> Path:
        return Path(folder).expanduser()


class ReportType(str, Enum):
    TV_CROSSTAB = "TV_CROSSTAB"
    TV_TIMEBAND = "TV_TIMEBAND"
    TV_SIMPLE = "TV_SIMPLE"
    TV_DICT_CROSSTAB = "TV_DICT_CROSSTAB"
    REG_TV_CROSSTAB = "REG_TV_CROSSTAB"
    REG_TV_DICT_CROSSTAB = "REG_TV_DICT_CROSSTAB"


class MediaType(str, Enum):
    TV = "TV"
    REG_TV = "REG_TV"


class TVCrossTabSubtype(str, Enum):
    DYNAMICS_BY_SPOTS = "DYNAMICS_BY_SPOTS"


class LastTimeModel(BaseModel):
    period_num: int
    include_current: bool = False


class PeriodModel(BaseModel):
    date_filter: list[date] | None = None
    period_type: Literal['d', 'm', 'y'] = None
    last_time: LastTimeModel | None = None

    @model_validator(mode="after")
    def validate_exclusive(self):
        if not self.date_filter and not self.last_time:
            raise ValueError("Нужно указать либо date_filter, либо last_time")

        if self.date_filter and len(self.date_filter) != 2:
            raise ValueError("date_filter должен содержать 2 даты")

        return self


class ReportSettings(BaseModel):
    model_config = ConfigDict(extra="allow")
    report_type: ReportType
    report_subtype: TVCrossTabSubtype | None = None
    media: MediaType
    category_name: str
    compression: dict[str, str] | None = Field(default={'method': 'gzip'})
    data_lang: Literal['ru', 'en'] = 'ru'
    folder_name: Path
    multiple_files: bool = True  # True, если период больше 1 года, разбиваем выгрузки по месяцам/неделям
    ad_filter: str | None = None
    target_audiences: dict | None = None
    period: PeriodModel

    @field_validator('folder_name', mode="before")
    @classmethod
    def normalize_path(cls, folder: Path) -> Path:
        return Path(folder).expanduser()


class DataDefaultSettings(BaseModel):
    model_config = ConfigDict(extra="allow")
    DATA_DEFAULTS: dict


# Main configuration class
class Config(BaseSettings):
    """
    Main configuration class that loads and merges all configurations.
    """
    logging_settings: LoggingSettings
    mediascope_credentials: MediascopeCredentials
    module_settings: AppSettings
    report_settings: ReportSettings
    data_default_settings: DataDefaultSettings
    data_settings: dict | None = None

    def merge_settings(self):
        """ reads default settings yaml file and returns dictionary with
        default settings basing on the self report subtype

        :param defaults_file: str or path, absolute path to yaml file
        :return: dict, dictionary with default settings
        """
        # getting path corresponding to the current report subtype and combine settings

        self.data_settings = self.data_default_settings.DATA_DEFAULTS.copy()
        if self.report_settings.report_subtype and getattr(self.data_default_settings,
                                                           self.report_settings.report_subtype, None):
            self.data_settings.update(getattr(self.data_default_settings, self.report_settings.report_subtype))

        for k, v in self.report_settings.model_dump().items():
            if k in self.data_settings:
                self.data_settings[k] = v

        if self.report_settings.data_lang == 'ru':
            self.data_settings['slices'] = utils.en_to_ru(self.data_settings['slices'])


class RuntimeService:
    def __init__(self, config: Config, resolver: PathResolver):
        self.config = config
        self.resolver = resolver

    def prepare(self):
        export = self.resolver.resolve(self.config.module_settings.export_folder)
        self.resolver.ensure_dir(export)

        report_folder = export / self.config.report_settings.folder_name
        self.resolver.ensure_dir(report_folder)

        for handler in self.config.logging_settings.handlers.values():
            if isinstance(handler, dict) and "filename" in handler:
                file_path = self.resolver.resolve(handler["filename"])
                self.resolver.ensure_file_parent(file_path)
                handler["filename"] = file_path

        self.config.merge_settings()


def load_config(secret_path: Path, logging_config_path: Path, module_config_path: Path,
                report_settings_path: Path) -> Config:
    logging_settings = LoggingSettings(**utils.yaml_to_dict(logging_config_path))
    credentials = MediascopeCredentials(**utils.yaml_to_dict(secret_path))
    app_settings = AppSettings(**utils.yaml_to_dict(module_config_path))
    report_settings = ReportSettings(**utils.yaml_to_dict(report_settings_path)[0])

    data_default_settings_path = Path(
        PATHS.defaults_dir,
        report_settings.media.lower(),
        report_settings.report_type.lower()
    ).with_suffix('.yaml')
    data_default_settings = DataDefaultSettings(**utils.yaml_to_dict(data_default_settings_path))

    return Config(
        logging_settings=logging_settings,
        mediascope_credentials=credentials,
        module_settings=app_settings,
        report_settings=report_settings,
        data_default_settings=data_default_settings
    )


CONFIG = load_config(
    secret_path=PATHS.connection,
    logging_config_path=PATHS.log_config,
    module_config_path=PATHS.app_config,
    report_settings_path=Path(PATHS.reports, 'nat_tv_brands_last.yaml'),
)

path_resolver = PathResolver(PATHS.user_dir)
runtime_service = RuntimeService(CONFIG, path_resolver)
runtime_service.prepare()

if __name__ == '__main__':
    print(CONFIG.model_dump())
    print(PATHS.app_name)
