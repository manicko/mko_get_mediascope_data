import logging
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from mko_get_mediascope_data.core.reports import (
    DefaultDataStrategy,
    DefaultTVTaskStrategy,
    DictDataStrategy,
    TVRegTaskStrategy,
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


# ---------------------------------------------------------------------------
# PIPE-008: DefaultDataStrategy.prepare logs task_name on KeyError
# ---------------------------------------------------------------------------

_REPORTER_LOGGER = "mko_get_mediascope_data.core.reports"


@pytest.mark.asyncio
async def test_default_data_strategy_prepare_logs_task_name(caplog):
    """PIPE-008: ``DefaultDataStrategy.prepare`` includes *task_name* in the
    ``KeyError`` error log and returns ``None``.

    The DataFrame columns intentionally omit a column referenced by
    ``report.output_columns`` so that the column-subset selection in
    ``DefaultDataStrategy.prepare`` raises ``KeyError``.
    """
    strategy = DefaultDataStrategy()
    df = pd.DataFrame({"col1": [1], "col2": [2]})
    report_mock = MagicMock()
    report_mock.output_columns = {"slices": ["col1", "nonexistent_column"]}

    with caplog.at_level(logging.ERROR, logger=_REPORTER_LOGGER):
        result = await strategy.prepare(report_mock, df, task_name="test_task_123")

    assert result is None
    assert "test_task_123" in caplog.text


# ---------------------------------------------------------------------------
# PIPE-011: TVRegTaskStrategy.generate logs warning and skips done files
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tv_reg_task_strategy_skip_logs_warning(caplog):
    """PIPE-011: ``TVRegTaskStrategy.generate`` logs a warning and skips a task
    when its generated name already appears in ``report.done_files``.

    With a single region (id=1, ename="Moscow") and the fixture's interval /
    target, the generated task name is::

        "2025-01-01_2025-01-07_all 18+_Moscow"

    Placing that name in ``done_files`` causes the strategy to emit the
    "already exists … skipping" warning and yield zero tasks.
    """
    strategy = TVRegTaskStrategy()

    report = MagicMock()
    report.data_settings = MagicMock()
    report.data_settings.model_dump.return_value = {}

    regions_df = pd.DataFrame({"id": [1], "ename": ["Moscow"]})
    report.network_client.call = AsyncMock(return_value=regions_df)

    report.report_settings = MagicMock()
    report.report_settings.model_dump.return_value = {}

    report.period = [("2025-01-01", "2025-01-07")]
    report.targets = {"all 18+": None}
    report.catalogs = MagicMock()

    expected_name = "2025-01-01_2025-01-07_all 18+_Moscow"
    report.done_files = {expected_name: "file_exists.csv.gz"}

    report.subtype = MagicMock(value="crosstab")
    report.type = MagicMock(value="TV_REG")

    with caplog.at_level(logging.WARNING, logger=_REPORTER_LOGGER):
        tasks = [task async for task in strategy.generate(report)]

    assert len(tasks) == 0
    assert "already exists" in caplog.text
