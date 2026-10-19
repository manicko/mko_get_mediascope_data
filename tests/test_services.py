import json
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import json5

from tests.conftest import expected_output_columns
from mko_get_mediascope_data.core.models import (
    DataDefaults,
    Data,
    MediaType,
    ReportSettings,
    ReportSubtype,
    ReportType,
)
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.services import AppService

_LOG_CONFIG = """
{
  "version": 1,
  "disable_existing_loggers": false,
  "formatters": {
    "basic": {
      "class": "logging.Formatter",
      "format": "%(message)s",
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


def _compute_result_df(
    app_service: AppService, report_settings: ReportSettings
) -> pd.DataFrame:
    """Build a DataFrame whose columns match ``output_columns`` for the report.

    Loads default data settings, merges with report settings (applying
    ``en_to_ru`` for ``data_lang="ru"``), constructs ``Data``, then derives
    the column list via the shared ``expected_output_columns`` helper that
    mirrors ``TVMediaReport.set_output_columns`` (reports.py:282-296).
    """
    data_defaults = app_service.get_default_data_settings(report_settings)
    data_dict = AppService.merge_data_settings(data_defaults, report_settings)
    data_settings = Data(**data_dict)

    output_columns = expected_output_columns(data_settings, report_settings)
    columns = output_columns["slices"] + output_columns["statistics"]

    df = pd.DataFrame(columns=columns)
    df.loc[0] = 0
    return df


@pytest.mark.slow
async def test_run_report_with_real_models(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """Exercise the full pipeline through ``hermetic_app_service.run_report``.

    The external ``MediaVortexTask`` and ``NetworkClient`` are doubled via
    injection (not class-level patching); ``create_report`` runs for real
    and we assert on actual CSV output.
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
        hermetic_app_service, report_settings
    )

    # ==================== 4. Mock NetworkClient =================
    # Inject a mock NetworkClient instance so run_report() (which creates
    # NetworkClient() internally) receives our double instead of a real client.
    mock_network_client = MagicMock()

    async def mock_call_side_effect(func, *args, **kwargs):
        if getattr(func, "__name__", None) == "get_connection":
            return mock_report_service
        return func(*args, **kwargs)

    mock_network_client.call = AsyncMock(side_effect=mock_call_side_effect)
    mocker.patch(
        "mko_get_mediascope_data.core.services.NetworkClient",
        return_value=mock_network_client,
    )

    # ==================== 5. Mock asyncio.sleep ==================
    # Avoid 2s poll delay in wait_task (reports.py:278).
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ==================== 6. Run real pipeline ===================
    export_paths = await hermetic_app_service.run_report(test_json5)

    # ==================== 7. Assertions ===========================
    assert len(export_paths) == 1
    assert export_paths[0].name == "raw_data"
    assert export_paths[0].parts[-2] == "finance"

    csv_files = list(
        (hermetic_app_service.export_folder / "finance" / "raw_data").glob("*.csv.gz")
    )
    assert len(csv_files) > 0, f"Expected at least one CSV file, found {csv_files}"


# ---------------------------------------------------------------------------
# Config cache invalidation (ENT-006): reload_config()
# ---------------------------------------------------------------------------


def test_app_config_is_cached(hermetic_app_service: AppService) -> None:
    """Repeated access returns the same cached object (cached_property)."""
    first = hermetic_app_service.app_config
    second = hermetic_app_service.app_config
    assert first is second
    # Content assertion — verify the cached config has expected values
    assert first.app_settings.export_folder == Path("output")


def test_reload_config_invalidates_cache(hermetic_app_service: AppService) -> None:
    """After reload_config(), app_config is a fresh object."""
    original = hermetic_app_service.app_config
    hermetic_app_service.reload_config()
    reloaded = hermetic_app_service.app_config
    assert reloaded is not original


def test_reload_config_before_access_does_not_raise(
    hermetic_app_service: AppService,
) -> None:
    """reload_config() is safe even when the cache entry is absent.

    Uses pop(..., None) so a missing cached_property key never raises.
    """
    hermetic_app_service.__dict__.pop("app_config", None)
    hermetic_app_service.__dict__.pop("export_folder", None)
    hermetic_app_service.reload_config()  # must not raise
    assert hermetic_app_service.app_config is not None


def test_reload_config_reapplies_logging(
    hermetic_app_service: AppService, mocker
) -> None:
    """reload_config() re-runs dictConfig with the reloaded config dict."""
    mock_dict_config = mocker.patch("logging.config.dictConfig")
    hermetic_app_service.reload_config()
    mock_dict_config.assert_called_once()
    # Pin the config dict passed — not just the call count
    expected_config = hermetic_app_service.app_config.logging_settings.model_dump()
    mock_dict_config.assert_called_with(expected_config)


def test_reload_config_invalidates_export_folder(
    hermetic_app_service: AppService,
) -> None:
    """After reload_config(), export_folder is re-resolved."""
    original = hermetic_app_service.export_folder
    hermetic_app_service.reload_config()
    reloaded = hermetic_app_service.export_folder
    assert reloaded is not original  # identity check: cached_property was invalidated


# ---------------------------------------------------------------------------
# Credential existence guard (CFG-004): get_connection()
# ---------------------------------------------------------------------------


def test_get_connection_raises_on_missing_credential(
    hermetic_app_service: AppService,
) -> None:
    """get_connection raises FileNotFoundError when credentials are missing."""
    # hermetic_app_service fixture does NOT create a credentials/connections file
    with pytest.raises(FileNotFoundError, match="init"):
        hermetic_app_service.get_connection(MediaType.TV)


# ---------------------------------------------------------------------------
# CFG-006: Missing defaults templates (TV/simple, TV_REG/timeband, TV_REG/simple)
# ---------------------------------------------------------------------------


def test_bundled_defaults_templates_exist_and_parse() -> None:
    """All (media, report_type) combinations resolve without FileNotFoundError.

    After creating the missing defaults templates, get_default_data_settings
    must succeed for (TV, simple), (TV_REG, timeband), and (TV_REG, simple)
    in addition to the pre-existing (TV, crosstab), (TV, timeband),
    (TV_REG, crosstab).

    Uses itertools.product(MediaType, ReportType) to iterate all 6 combos
    instead of hardcoding the list.
    """
    import itertools

    for media, report_type in itertools.product(MediaType, ReportType):
        defaults_path = (
            APP_PATHS.app_settings_dir
            / "defaults"
            / media.value.lower()
            / f"{report_type.value}.json5"
        )
        assert defaults_path.exists(), f"Missing defaults template: {defaults_path}"
        data = json5.loads(defaults_path.read_text(encoding="utf-8"))
        defaults = DataDefaults(**data)
        assert "slices" in defaults.DATA_DEFAULTS  # real structural check


def test_get_default_data_settings_missing_defaults_raises_wrong_data_settings(
    hermetic_app_service: AppService,
) -> None:
    """Missing defaults file raises WrongDataSettings (not FileNotFoundError).

    Reproduces TPL-005: get_default_data_settings must check path.exists()
    and raise WrongDataSettings with a clear (media, report_type) message.
    """
    from mko_get_mediascope_data.core.errors import WrongDataSettings

    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"last_time": {"unit_count": 1}, "time_unit": "m"},  # type: ignore[arg-type]
    )
    # Remove the defaults file to trigger the missing-file path
    missing_path = hermetic_app_service.app_paths.defaults_dir / "tv" / "crosstab.json5"
    missing_path.unlink()

    with pytest.raises(WrongDataSettings, match="crosstab"):
        hermetic_app_service.get_default_data_settings(report_settings)


# ---------------------------------------------------------------------------
# CFG-007: get_app_service singleton + get_report_settings dict branch +
#          sub-type overlay in merge_data_settings
# ---------------------------------------------------------------------------


def test_get_app_service_singleton(mocker, tmp_path: Path) -> None:
    """get_app_service() returns the same instance on repeated calls."""
    from mko_get_mediascope_data.core.services import get_app_service

    # Point APP_PATHS at a hermetic temp dir so the singleton can load configs
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    (settings_dir / "log_config.json5").write_text(_LOG_CONFIG, encoding="utf-8")
    (settings_dir / "app_config.json5").write_text(
        '{"export_folder": "output"}', encoding="utf-8"
    )
    shutil.copytree(APP_PATHS.app_settings_dir / "defaults", settings_dir / "defaults")

    test_app_paths = AppPaths(app_dir=tmp_path, app_name="test", user_dir=tmp_path)
    mocker.patch("mko_get_mediascope_data.core.services.APP_PATHS", test_app_paths)

    # Reset singleton
    import mko_get_mediascope_data.core.services as svc_mod
    svc_mod._app_service = None

    first = get_app_service()
    second = get_app_service()
    assert first is second
    assert isinstance(first, AppService)

    # Cleanup
    svc_mod._app_service = None


def test_get_report_settings_normalizes_dict_to_list(tmp_path: Path) -> None:
    """get_report_settings normalizes a dict (not list) of entries to a list.

    services.py:92-93: ``if isinstance(report_configs, dict):
    report_configs = list(report_configs.values())``.
    """
    # Write a JSON5 file whose top-level is a dict mapping entry names
    # to report-config dicts (not a list).  The code normalizes this to
    # list(dict.values()) so ReportSettings receives proper dicts.
    single_entry = {
        "my_report": {
            "media": MediaType.TV.value,
            "report_type": ReportType.crosstab.value,
            "report_subtype": ReportSubtype.TOP_NAT_TV_ADVERTISERS.value,
            "relative_path": "tv/test",
            "period": {"last_time": {"unit_count": 1}, "time_unit": "m"},
        }
    }
    config_file = tmp_path / "single.json5"
    config_file.write_text(json5.dumps(single_entry), encoding="utf-8")

    settings_list = list(AppService.get_report_settings(config_file))
    assert len(settings_list) == 1
    assert settings_list[0].media == MediaType.TV


def test_merge_data_settings_applies_subtype_overlay() -> None:
    """merge_data_settings overlays subtype-specific defaults (services.py:130-132).

    When ``data_lang='en'``, the subtype overlay (e.g. TOP_NAT_TV_ADVERTISERS)
    is merged from DataDefaults into the base DATA_DEFAULTS dict.
    """
    # Load real defaults for TV/crosstab
    defaults_path = APP_PATHS.app_settings_dir / "defaults" / "tv" / "crosstab.json5"
    data = json5.loads(defaults_path.read_text(encoding="utf-8"))
    defaults = DataDefaults(**data)

    # ReportSettings with a specific subtype
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        data_lang="en",
        period={"last_time": {"unit_count": 1}, "time_unit": "m"},  # type: ignore[arg-type]
    )

    result = AppService.merge_data_settings(defaults, report_settings)

    # The subtype overlay (if present in defaults) should have been applied
    # to keys that are NOT also in ReportSettings.model_fields (those are
    # overwritten by report_settings values at services.py:134-136).
    overlay = getattr(defaults, ReportSubtype.TOP_NAT_TV_ADVERTISERS.value, None)
    if overlay:
        report_fields = set(ReportSettings.model_fields.keys())
        for key, val in overlay.items():
            if key not in report_fields:
                assert result[key] == val, (
                    f"Subtype overlay key '{key}' not applied to merge result"
                )


def test_prepare_app_resolves_handler_filename(mocker, tmp_path: Path) -> None:
    """prepare_app resolves handler 'filename' paths via PathResolver (services.py:60-64)."""
    # Build a log config with a file handler that has a filename
    log_config_with_file = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"basic": {"class": "logging.Formatter", "format": "%(message)s"}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "basic",
                "level": "INFO",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.FileHandler",
                "formatter": "basic",
                "level": "INFO",
                "filename": "logs/app.log",
            },
        },
        "root": {"level": "INFO", "handlers": ["console", "file"]},
        "loggers": {},
    }
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    (settings_dir / "log_config.json5").write_text(
        json5.dumps(log_config_with_file), encoding="utf-8"
    )
    (settings_dir / "app_config.json5").write_text('{"export_folder": "output"}', encoding="utf-8")
    shutil.copytree(APP_PATHS.app_settings_dir / "defaults", settings_dir / "defaults")

    app_paths = AppPaths(app_dir=tmp_path, app_name="test", user_dir=tmp_path)
    service = AppService(app_paths=app_paths, resolver=PathResolver(tmp_path))

    service.prepare_app()

    # The handler's filename should have been resolved to an absolute path
    handlers = service.app_config.logging_settings.handlers
    assert "file" in handlers
    file_handler = handlers["file"]
    assert "filename" in file_handler
    # Resolved path should be under the resolver's base_dir
    resolved = Path(file_handler["filename"])
    assert resolved.is_absolute()
    assert resolved.parent.exists()


async def test_run_report_multi_entry_yields_multiple_paths(
    tmp_path: Path,
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
) -> None:
    """run_report with a multi-entry JSON5 yields one export path per entry (services.py:167-178)."""
    entries = [
        {
            "media": MediaType.TV.value,
            "report_type": ReportType.crosstab.value,
            "report_subtype": ReportSubtype.TOP_NAT_TV_ADVERTISERS.value,
            "relative_path": "tv/raw_data",
            "data_lang": "ru",
            "period": {"last_time": {"unit_count": 1}, "time_unit": "m"},
            "target_audiences": {"all 18+": "age >= 18"},
        },
        {
            "media": MediaType.TV.value,
            "report_type": ReportType.crosstab.value,
            "report_subtype": ReportSubtype.DYNAMICS_BY_SPOTS.value,
            "relative_path": "tv/dynamic_spots",
            "data_lang": "ru",
            "period": {"last_time": {"unit_count": 1}, "time_unit": "m"},
            "target_audiences": {"all 18+": "age >= 18"},
        },
    ]
    config_file = tmp_path / "multi_entry.json5"
    config_file.write_text(json5.dumps(entries), encoding="utf-8")

    # Double MediaVortexTask + NetworkClient via injection
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}

    # Build result DataFrames for each entry
    settings_1 = ReportSettings.model_validate(entries[0])
    settings_2 = ReportSettings.model_validate(entries[1])
    result_df_1 = _compute_result_df(hermetic_app_service, settings_1)
    result_df_2 = _compute_result_df(hermetic_app_service, settings_2)
    mock_report_service.result2table.side_effect = [result_df_1, result_df_2]

    mock_network_client = MagicMock()

    async def mock_call_side_effect(func, *args, **kwargs):
        if getattr(func, "__name__", None) == "get_connection":
            return mock_report_service
        return func(*args, **kwargs)

    mock_network_client.call = AsyncMock(side_effect=mock_call_side_effect)
    mocker.patch(
        "mko_get_mediascope_data.core.services.NetworkClient",
        return_value=mock_network_client,
    )
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    export_paths = await hermetic_app_service.run_report(config_file)
    assert len(export_paths) == 2
    assert export_paths[0].name == "raw_data"
    assert export_paths[1].name == "dynamic_spots"
