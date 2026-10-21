import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from mko_get_mediascope_data.core.models import (
    Data,
    MediaType,
    ReportSettings,
    ReportSubtype,
    ReportType,
)
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.services import AppService

# Matches the default kitId in settings/defaults/tv/crosstab.json5
DEFAULT_KIT_ID = "7"

# Representative positive submission mapping returned by a doubled ``send_task``.
# Mirrors the vendor contract: ``MediaVortexTask.get_status`` / ``get_result``
# read the ``taskId`` key and route it to ``/task/state/<tid>`` /
# ``/task/result/<tid>``.  Non-empty dict → truthy, so ``not task.key`` falsy
# semantics are preserved.  Individual negative tests intentionally keep their
# own falsy values (``""``, ``False``) and must not use this constant.
MOCK_TASK_KEY = {"taskId": "mock_task_key_123"}

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

_APP_CONFIG_JSON5 = """
{
  "export_folder": "output",
  "max_concurrent_requests": 5,
  "max_tasks": 100,
  "call_timeout": 300.0,
  "max_attempts": 10,
  "sleep_time": 2,
  "retry_5xx": true,
  "log_errors_detail": false,
  "poll": {
    "sleep_time": 30,
    "initial_interval": 30.0,
    "max_interval": 60.0,
    "max_polls": 60,
    "deadline": 3600.0,
  },
  "send_task": {
    "retryable": true,
    "retry_on_connection_error": true,
  },
}
"""


def expected_output_columns(
    data_settings: Data, report_settings: ReportSettings
) -> dict[str, list[str]]:
    """Mirror ``TVMediaReport.set_output_columns`` (reports.py:282-296).

    Used by lifecycle + services tests to build a result DataFrame whose
    columns match what ``DefaultDataStrategy.prepare`` expects. Includes the
    ``frequencyDistInterval`` branch (reports.py:293-294) that the old
    ``_build_result_df`` / ``_compute_result_df`` helpers omitted.
    """
    output_columns = {
        "slices": list(data_settings.slices),
        "statistics": list(data_settings.statistics),
    }
    if report_settings.target_audiences:
        output_columns["slices"].insert(0, "targetAudience")

    if getattr(data_settings, "frequency_dist_conditions", None):
        output_columns["statistics"].append("frequencyDistInterval")

    return output_columns


@pytest.fixture
def dummy_df():
    return pd.DataFrame(
        {"id": [DEFAULT_KIT_ID], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]}
    )


@pytest.fixture
def tmp_report_dir(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    return reports


@pytest.fixture
def tv_json5_content() -> str:
    """Report definition built with enum ``.value`` references for type safety."""
    content = [
        {
            "report_type": ReportType.crosstab.value,
            "report_subtype": ReportSubtype.DYNAMICS_BY_SPOTS.value,
            "media": MediaType.TV.value,
            "compression": {"method": "gzip"},
            "data_lang": "ru",
            "relative_path": "finance/raw_data",
            "multiple_files": True,
            "ad_filter": "articleLevel2Id = 2272 and adIssueStatusId = R",
            "target_audiences": {
                "all 18+": "age >= 18",
            },
            "period": {"date_filter": ["2025-01-01", "2025-01-07"]},
        }
    ]
    return json.dumps(content)


@pytest.fixture
def hermetic_app_service(tmp_path: Path) -> AppService:
    """Shared AppService backed by isolated temp config files.

    Promoted from ``_build_hermetic_app_service`` (test_reports_lifecycle.py)
    and ``local_app_service`` (test_services.py) — both were functionally
    identical. Consumers across Block 7 and Block 8 (and services tests)
    now share this single fixture.
    """
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    (settings_dir / "log_config.json5").write_text(_LOG_CONFIG_JSON5, encoding="utf-8")
    (settings_dir / "app_config.json5").write_text(_APP_CONFIG_JSON5, encoding="utf-8")
    shutil.copytree(APP_PATHS.app_settings_dir / "defaults", settings_dir / "defaults")
    app_paths = AppPaths(app_dir=tmp_path, app_name="test", user_dir=tmp_path)
    return AppService(app_paths=app_paths, resolver=PathResolver(tmp_path))
