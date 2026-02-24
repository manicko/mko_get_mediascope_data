import pytest
from unittest.mock import MagicMock, AsyncMock
import pandas as pd

from mko_get_mediascope_data.core.services import app_service



@pytest.mark.asyncio
async def test_run_report_with_real_models(tmp_path, mocker, tv_yaml_content):
    """Тест использует реальные ReportSettings + DataDefaults.
    Мокаем только сеть и создание отчёта — всё остальное работает как в проде.
    """

    # ==================== 1. Реальный yaml-файл (как в проекте) ====================

    test_yaml = tmp_path / "nat_tv_brands_last.yaml"
    test_yaml.write_text(tv_yaml_content, encoding="utf-8")

    # ==================== 2. Мокаем только сеть ====================
    dummy_df = pd.DataFrame({
        "id": ["1"],
        "periodFrom": ["2025-01-01"],
        "periodTo": ["2025-12-31"]
    })

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
        side_effect=mock_call_side_effect
    )

    # ==================== 3. Мокаем тяжёлый метод ====================
    mocker.patch(
        "mko_get_mediascope_data.core.reports.TVMediaReport.create_report",
        new_callable=AsyncMock
    )

    # ==================== 4. Запускаем реальный код ====================
    export_paths = await app_service.run_report(test_yaml)

    # ==================== 5. Проверки ====================
    assert len(export_paths) == 1
    assert export_paths[0].name == "raw_data"  # из relative_path
    assert export_paths[0].parts[-2] == "finance"  # проверка пути
