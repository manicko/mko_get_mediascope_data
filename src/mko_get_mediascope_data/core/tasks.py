import logging
from datetime import date
from os import PathLike
from pathlib import Path
from typing import Any, cast

from yaml import safe_dump

logger = logging.getLogger(__name__)


class TVTask:
    def __init__(
        self, name: str, settings: dict[str, Any], report_subtype: str, report_type: str
    ):
        self.name = name
        self.settings = settings
        self.key: dict[str, Any] | None = None
        self.status = True
        self.log_error = False
        self.error = ""
        self.interval: tuple[str, str] | tuple[date, date] | None = None
        self.target: str | None = None
        self.report_type = report_type
        self.report_subtype = report_subtype

    def to_yaml(self, folder: str | PathLike[str] | None = None):
        if folder is None:
            raise TypeError(
                "folder must be provided — to_yaml requires an output directory path"
            )
        folder = Path(folder) / "errors"
        folder.mkdir(parents=True, exist_ok=True)
        export_file = folder / f"{self.name}.yaml"

        interval: list[str] | list[date] | None = None
        if self.interval is not None:
            interval = cast("list[str] | list[date]", list(self.interval))

        dump_settings = {
            "name": self.name,
            "report_type": self.report_type,
            "report_subtype": self.report_subtype,
            "target": self.target,
            "error": self.error,
            "log_error": self.log_error,
            "interval": interval,
            "settings": self.settings,
        }
        logger.info(f"Saving task settings for {self.name}")
        with open(export_file, "w", encoding="utf-8") as outfile:
            safe_dump(
                dump_settings,
                outfile,
                sort_keys=False,
                allow_unicode=True,
            )
