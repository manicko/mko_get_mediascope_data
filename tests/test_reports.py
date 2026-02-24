import pytest
import pandas as pd
from unittest.mock import MagicMock, AsyncMock

from mko_get_mediascope_data.core.reports import (
    TVMediaReport, DefaultTVTaskStrategy, TVRegTaskStrategy,
    DefaultDataStrategy, DictDataStrategy
)


@pytest.fixture
def mock_report(dummy_df):
    report = MagicMock()
    report.period = [("2025-01-01", "2025-01-07")]
    report.targets = {"all 18+": None}
    report.done_files = {}
    report.data_settings = MagicMock()
    report.data_settings.model_dump.return_value = {}
    report.report_settings = MagicMock()
    report.report_settings.model_dump.return_value = {}
    report.report_settings.relative_path = "test"
    report.subtype = MagicMock(value="crosstab")
    report.type = MagicMock(value="TV")
    report.network_client = MagicMock()
    report.network_client.call = AsyncMock(return_value=dummy_df)
    return report


@pytest.mark.asyncio
async def test_default_task_strategy(mock_report):
    strategy = DefaultTVTaskStrategy()
    tasks = [task async for task in strategy.generate(mock_report)]
    assert len(tasks) == 1
    assert tasks[0].name.startswith("2025-01-01")


@pytest.mark.asyncio
async def test_dict_data_strategy():
    strategy = DictDataStrategy()
    df = pd.DataFrame({"col1": [1], "col2": [2]})
    report_mock = MagicMock()
    report_mock.data_settings = {"slices": ["col1"]}
    result = await strategy.prepare(report_mock, df)
    assert result is not None
    assert "search_column_idx" in result.columns