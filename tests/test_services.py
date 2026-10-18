import json
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import json5

from mko_get_mediascope_data.core.models import (
    DataDefaults,
    Data,
    MediaType,
    ReportSettings,
    ReportType,
)
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.services import AppService


def _compute_result_df(
    app_service: AppService, report_settings: ReportSettings
) -> pd.DataFrame:
    """Build a DataFrame whose columns match ``output_columns`` for the report.

    Loads default data settings, merges with report settings (applying
    ``en_to_ru`` for ``data_lang="ru"``), constructs ``Data``, then derives
    the column list the same way ``TVMediaReport.set_output_columns`` does.
    """
    data_defaults = app_service.get_default_data_settings(report_settings)
    data_dict = AppService.merge_data_settings(data_defaults, report_settings)
    data_settings = Data(**data_dict)

    columns = list(data_settings.slices) + list(data_settings.statistics)
    if report_settings.target_audiences:
        columns.insert(0, "targetAudience")

    df = pd.DataFrame(columns=columns)
    df.loc[0] = 0
    return df


@pytest.mark.slow
@pytest.mark.asyncio
async def test_run_report_with_real_models(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    local_app_service: AppService,
):
    """Exercise the full pipeline through ``local_app_service.run_report``.

    Only the external ``MediaVortexTask`` is doubled; ``create_report`` runs
    for real and we assert on actual CSV output.
    """

    # ==================== 1. Write real JSON5 file ====================
    test_json5 = tmp_path / "nat_tv_brands_last.json5"
    test_json5.write_text(tv_json5_content, encoding="utf-8")

    # ==================== 2. Parse ReportSettings from fixture ======
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ==================== 3. Doubled MediaVortexTask ==============
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = _compute_result_df(
        local_app_service, report_settings
    )

    # ==================== 4. Mock NetworkClient.call ============
    # Dispatch: get_connection → doubled MediaVortexTask;
    # everything else → invoke the doubled method's return_value.
    async def mock_call_side_effect(func, *args, **kwargs):
        if getattr(func, "__name__", None) == "get_connection":
            return mock_report_service
        return func(*args, **kwargs)

    mocker.patch(
        "mko_get_mediascope_data.core.network.NetworkClient.call",
        side_effect=mock_call_side_effect,
    )

    # ==================== 5. Mock asyncio.sleep ==================
    # Avoid 2s poll delay in wait_task (reports.py:278).
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ==================== 6. Run real pipeline ===================
    export_paths = await local_app_service.run_report(test_json5)

    # ==================== 7. Assertions ===========================
    assert len(export_paths) == 1
    assert export_paths[0].name == "raw_data"
    assert export_paths[0].parts[-2] == "finance"

    csv_files = list(
        (local_app_service.export_folder / "finance" / "raw_data").glob("*.csv.gz")
    )
    assert len(csv_files) > 0, f"Expected at least one CSV file, found {csv_files}"


# ---------------------------------------------------------------------------
# Config cache invalidation (ENT-006): reload_config()
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

_APP_CONFIG_JSON5 = '{"export_folder": "output",}'


@pytest.fixture
def local_app_service(tmp_path: Path) -> AppService:
    """Build an AppService backed by isolated temp config files."""
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    (settings_dir / "log_config.json5").write_text(_LOG_CONFIG_JSON5, encoding="utf-8")
    (settings_dir / "app_config.json5").write_text(_APP_CONFIG_JSON5, encoding="utf-8")
    # Copy template defaults so get_default_data_settings can resolve them
    src_defaults = APP_PATHS.app_settings_dir / "defaults"
    dst_defaults = settings_dir / "defaults"
    shutil.copytree(src_defaults, dst_defaults)

    app_paths = AppPaths(app_dir=tmp_path, app_name="test", user_dir=tmp_path)
    return AppService(app_paths=app_paths, resolver=PathResolver(tmp_path))


def test_app_config_is_cached(local_app_service: AppService) -> None:
    """Repeated access returns the same cached object."""
    first = local_app_service.app_config
    second = local_app_service.app_config
    assert first is second


def test_reload_config_invalidates_cache(local_app_service: AppService) -> None:
    """After reload_config(), app_config is a fresh object."""
    original = local_app_service.app_config
    local_app_service.reload_config()
    reloaded = local_app_service.app_config
    assert reloaded is not original


def test_reload_config_before_access_does_not_raise(
    local_app_service: AppService,
) -> None:
    """reload_config() is safe even when the cache entry is absent.

    Uses pop(..., None) so a missing cached_property key never raises.
    """
    local_app_service.__dict__.pop("app_config", None)
    local_app_service.__dict__.pop("export_folder", None)
    local_app_service.reload_config()  # must not raise
    assert local_app_service.app_config is not None


def test_reload_config_reapplies_logging(local_app_service: AppService, mocker) -> None:
    """reload_config() re-runs dictConfig without error."""
    mock_dict_config = mocker.patch("logging.config.dictConfig")
    local_app_service.reload_config()
    mock_dict_config.assert_called_once()


def test_reload_config_invalidates_export_folder(
    local_app_service: AppService,
) -> None:
    """After reload_config(), export_folder is re-resolved."""
    original = local_app_service.export_folder
    local_app_service.reload_config()
    reloaded = local_app_service.export_folder
    assert reloaded == original  # same resolved path
    assert reloaded is not None


# ---------------------------------------------------------------------------
# Credential existence guard (CFG-004): get_connection()
# ---------------------------------------------------------------------------


def test_get_connection_raises_on_missing_credential(
    local_app_service: AppService,
) -> None:
    """get_connection raises FileNotFoundError when credentials are missing."""
    # local_app_service fixture does NOT create a credentials/connections file
    with pytest.raises(FileNotFoundError, match="init"):
        local_app_service.get_connection(MediaType.TV)


# ---------------------------------------------------------------------------
# CFG-006: Missing defaults templates (TV/simple, TV_REG/timeband, TV_REG/simple)
# ---------------------------------------------------------------------------


def test_get_default_data_settings_missing_file() -> None:
    """All 6 (media, report_type) combinations resolve without FileNotFoundError.

    After creating the missing defaults templates, get_default_data_settings
    must succeed for (TV, simple), (TV_REG, timeband), and (TV_REG, simple)
    in addition to the pre-existing (TV, crosstab), (TV, timeband),
    (TV_REG, crosstab).
    """
    combos = [
        (MediaType.TV, ReportType.crosstab),
        (MediaType.TV, ReportType.timeband),
        (MediaType.TV, ReportType.simple),
        (MediaType.TV_REG, ReportType.crosstab),
        (MediaType.TV_REG, ReportType.timeband),
        (MediaType.TV_REG, ReportType.simple),
    ]
    for media, report_type in combos:
        defaults_path = (
            APP_PATHS.app_settings_dir
            / "defaults"
            / media.value.lower()
            / f"{report_type.value}.json5"
        )
        assert defaults_path.exists(), f"Missing defaults template: {defaults_path}"
        data = json5.loads(defaults_path.read_text(encoding="utf-8"))
        defaults = DataDefaults(**data)
        assert defaults.DATA_DEFAULTS is not None


def test_get_default_data_settings_missing_defaults_raises_wrong_data_settings(
    local_app_service: AppService,
) -> None:
    """Missing defaults file raises WrongDataSettings (not FileNotFoundError).

    Reproduces TPL-005: get_default_data_settings must check path.exists()
    and raise WrongDataSettings with a clear (media, report_type) message.
    """
    from mko_get_mediascope_data.core.errors import WrongDataSettings

    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        relative_path="tv/test",
        period={"last_time": {"unit_count": 1}, "time_unit": "m"},
    )
    # Remove the defaults file to trigger the missing-file path
    missing_path = local_app_service.app_paths.defaults_dir / "tv" / "crosstab.json5"
    missing_path.unlink()

    with pytest.raises(WrongDataSettings, match="crosstab"):
        local_app_service.get_default_data_settings(report_settings)

