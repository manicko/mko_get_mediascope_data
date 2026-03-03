from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
    model_validator,
)


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


class AppSettings(BaseModel):
    export_folder: Path

    @field_validator("export_folder", mode="before")
    @classmethod
    def normalize_path(cls, folder: Path | str) -> Path:
        return Path(folder).expanduser()


class MediaType(Enum):
    TV = "TV"
    TV_REG = "TV_REG"


class ReportType(Enum):
    crosstab = "crosstab"
    timeband = "timeband"
    simple = "simple"


class ReportSubtype(Enum):
    default_value = ""
    DYNAMICS_BY_SPOTS = "DYNAMICS_BY_SPOTS"
    TOP_NAT_TV_ADVERTISERS = "TOP_NAT_TV_ADVERTISERS"
    TOP_NAT_TV_PROGRAMS = "TOP_NAT_TV_PROGRAMS"
    NAT_TV_CHANNELS_ATV = "NAT_TV_CHANNELS_ATV"
    NAT_TV_CHANNELS_BA = "NAT_TV_CHANNELS_BA"
    NAT_TV_CHANNELS_SOC_DEM = "NAT_TV_CHANNELS_SOC_DEM"
    DYNAMICS_BY_SPOTS_DICT = "DYNAMICS_BY_SPOTS_DICT"


class LastTimeModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    include_current: bool = False
    time_unit: Literal["d", "w", "m", "y"] = "w"
    unit_count: PositiveInt


class PeriodModel(BaseModel):
    date_filter: list[date] | None = None
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
    check_done: bool | None = True
    report_type: ReportType
    report_subtype: ReportSubtype = ReportSubtype.default_value
    media: MediaType
    compression: dict[str, str] | None = Field(
        default_factory=lambda: {"method": "gzip"}
    )
    data_lang: Literal["ru", "en"] = "ru"
    relative_path: Path
    multiple_files: bool = (
        True  # True, если период больше 1 года, разбиваем выгрузки по месяцам/неделям
    )
    ad_filter: str | None = None
    target_audiences: dict[str, Any] | None = Field(
        default_factory=lambda: {"not_set": None}
    )
    period: PeriodModel

    @field_validator("relative_path", mode="before")
    @classmethod
    def normalize_path(cls, folder: Path) -> Path:
        return Path(folder).expanduser()


# Main configuration class
class AppConfig(BaseModel):
    """
    Main configuration class that loads and merges all configurations.
    """

    logging_settings: LoggingSettings
    app_settings: AppSettings


class DataDefaults(BaseModel):
    model_config = ConfigDict(extra="allow")
    DATA_DEFAULTS: dict


class Data(BaseModel):
    model_config = ConfigDict(extra="allow")
    options: dict[str, Any] | list[Any] = Field(default_factory=list)
    statistics: dict[str, Any] | list[Any] = Field(default_factory=list)
    slices: dict[str, Any] | list[Any] = Field(default_factory=list)
