from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from mko_get_mediascope_data.core.paths import AppPaths, PathResolver
from mko_get_mediascope_data.core.services import AppService, get_app_service


@pytest.mark.asyncio
async def test_run_report_with_real_models(tmp_path, mocker, tv_yaml_content):
    """Тест использует реальные ReportSettings + DataDefaults.
    Мокаем только сеть и создание отчёта — всё остальное работает как в проде.
    """

    # ==================== 1. Реальный yaml-файл (как в проекте) ====================

    test_yaml = tmp_path / "nat_tv_brands_last.yaml"
    test_yaml.write_text(tv_yaml_content, encoding="utf-8")

    # ==================== 2. Мокаем только сеть ====================
    dummy_df = pd.DataFrame(
        {"id": ["1"], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]}
    )

    # Fake MediaVortexTask
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period = MagicMock(return_value=dummy_df)
    mock_report_service.build_task = MagicMock()
    mock_report_service.send_task = MagicMock()
    mock_report_service.get_status = MagicMock()
    mock_report_service.get_result = MagicMock()
    mock_report_service.result2table = MagicMock(return_value=pd.DataFrame())

    # NetworkClient.call возвращает report_service при вызове get_connection
    async def mock_call_side_effect(func, *args, **kwargs):
        if func.__name__ == "get_connection":
            return mock_report_service
        return dummy_df

    mocker.patch(
        "mko_get_mediascope_data.core.network.NetworkClient.call",
        side_effect=mock_call_side_effect,
    )

    # ==================== 3. Мокаем тяжёлый метод ====================
    mocker.patch(
        "mko_get_mediascope_data.core.reports.TVMediaReport.create_report",
        new_callable=AsyncMock,
    )

    # ==================== 4. Запускаем реальный код ====================
    export_paths = await get_app_service().run_report(test_yaml)

    # ==================== 5. Проверки ====================
    assert len(export_paths) == 1
    assert export_paths[0].name == "raw_data"  # из relative_path
    assert export_paths[0].parts[-2] == "finance"  # проверка пути


# ---------------------------------------------------------------------------
# Config cache invalidation (ENT-006): reload_config()
# ---------------------------------------------------------------------------

_LOG_CONFIG_YAML = """
version: 1
disable_existing_loggers: false
formatters:
  basic:
    class: logging.Formatter
    format: "%(levelname)s %(message)s"
handlers:
  console:
    class: logging.StreamHandler
    formatter: basic
    level: INFO
    stream: ext://sys.stdout
root:
  level: INFO
  handlers:
    - console
loggers: {}
"""

_APP_CONFIG_YAML = "{ 'export_folder':'output', }"


@pytest.fixture
def local_app_service(tmp_path: Path) -> AppService:
    """Build an AppService backed by isolated temp config files."""
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    (settings_dir / "log_config.yaml").write_text(_LOG_CONFIG_YAML, encoding="utf-8")
    (settings_dir / "app_config.yaml").write_text(_APP_CONFIG_YAML, encoding="utf-8")

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


def test_reload_config_before_access_does_not_raise(local_app_service: AppService) -> None:
    """reload_config() is safe even when the cache entry is absent.

    Uses pop(..., None) so a missing cached_property key never raises.
    """
    local_app_service.__dict__.pop("app_config", None)
    local_app_service.__dict__.pop("export_folder", None)
    local_app_service.reload_config()  # must not raise
    assert local_app_service.app_config is not None


def test_reload_config_reapplies_logging(
    local_app_service: AppService, mocker
) -> None:
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
