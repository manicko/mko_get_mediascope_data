import logging
from unittest.mock import AsyncMock, MagicMock

import pandas as pd

from mko_get_mediascope_data.core.models import Data, ReportSubtype, ReportType
from mko_get_mediascope_data.core.reports import (
    DefaultDataStrategy,
    DictDataStrategy,
    TVRegTaskStrategy,
)


async def test_dict_data_strategy():
    """DictDataStrategy melts a wide DICT-report DataFrame into a long-format
    "search dictionary" table.

    Verifies, end-to-end with a 30-row payload that includes duplicates,
    ``None``, empty strings, parenthetical suffixes, and repeated values
    across different columns:

    1. Selects only the columns listed in ``data_settings.slices``.
    2. Melts them into ``(column, value)`` rows, dropping NaN and duplicates.
    3. Assigns ``search_column_idx`` starting at a fixed shift (3) and sets
       ``action="r"`` / ``match="p"``.
    4. Strips parenthetical suffixes and whitespace into ``term``.
    5. Creates one ``{col}_clean`` column per slice, populated only on rows
       whose ``search_column_idx`` matches that slice's position.
    """
    strategy = DictDataStrategy()

    df = pd.DataFrame([
        ["Первый канал (HD)", "Новости (вечерние)", "Информация"],
        ["Россия 1", "Вести", "Информация"],
        ["НТВ", "Сегодня", "Новости"],
        ["ТНТ", "Комеди Клаб", "Развлечения"],
        ["СТС", "Кухня (сериал)", "Сериалы"],
        ["Первый канал (SD)", "Новости (утренние)", "Информация"],
        ["Россия 1", "Вести", "Информация"],
        ["НТВ", "Сегодня", "Новости"],
        ["ТНТ", "Однажды в России", "Развлечения"],
        ["СТС", "Воронины", "Сериалы"],
        ["РЕН ТВ", "Новости 24", "Информация"],
        ["Пятый канал", "След (сериал)", "Сериалы"],
        ["Матч ТВ", "Футбол (прямой эфир)", "Спорт"],
        ["Матч ТВ", "Хоккей (повтор)", "Спорт"],
        ["Россия 24", "Вести", "Новости"],
        ["Первый канал (HD)", "Новости (вечерние)", "Информация"],
        ["ТНТ", "Комеди Клаб", "Развлечения"],
        ["СТС", None, "Сериалы"],
        [None, "Вести", "Информация"],
        ["", "Новости", ""],
        ["  НТВ  ", "  Сегодня  ", "Новости"],
        ["Россия 1 (HD)", "Вести (20:00)", "Информация"],
        ["Россия 1 (SD)", "Вести (утро)", "Информация"],
        ["Первый канал", "Поле чудес", "Развлечения"],
        ["ТНТ", "Новости", "Развлечения"],
        ["Культура", "Большая опера", "Культура"],
        ["Карусель", "Смешарики", "Детские"],
        ["Домашний", "Тест на отцовство", "Развлечения"],
        ["ТВ Центр", "События", "Новости"],
        ["Звезда", "Военная тайна (архив)", "Документальные"],
    ], columns=["channel", "program", "category"])

    report_mock = MagicMock()
    report_mock.data_settings = Data(slices=["channel", "program", "category"])

    result = await strategy.prepare(report_mock, df)

    assert result is not None

    # Column order: metadata + one _clean column per slice (in slice order)
    assert list(result.columns) == [
        "action", "match", "search_column_idx", "value", "term",
        "channel_clean", "program_clean", "category_clean",
    ]

    # 30 rows × 3 slices = 90 melt rows; 2 None values dropped → 88;
    # duplicates removed (3 pair-dupes each for channel/program/category) → 51
    assert len(result) == 51
    assert not result.duplicated().any()

    # Row counts per slice match the user-specified output
    assert (result["search_column_idx"] == 3).sum() == 20  # channel
    assert (result["search_column_idx"] == 4).sum() == 22  # program
    assert (result["search_column_idx"] == 5).sum() == 9   # category

    # action and match are constants
    assert set(result["action"]) == {"r"}
    assert set(result["match"]) == {"p"}

    # Each _clean column is populated only on rows of its own slice type
    channel_rows = result[result["search_column_idx"] == 3]
    assert channel_rows["channel_clean"].notna().all()
    assert channel_rows["program_clean"].isna().all()
    assert channel_rows["category_clean"].isna().all()

    program_rows = result[result["search_column_idx"] == 4]
    assert program_rows["program_clean"].notna().all()
    assert program_rows["channel_clean"].isna().all()
    assert program_rows["category_clean"].isna().all()

    category_rows = result[result["search_column_idx"] == 5]
    assert category_rows["category_clean"].notna().all()
    assert category_rows["channel_clean"].isna().all()
    assert category_rows["program_clean"].isna().all()

    # Empty-string values survive dropna() but produce empty term / _clean
    empty_rows = result[result["value"] == ""]
    assert len(empty_rows) == 2  # one empty channel, one empty category
    assert (empty_rows["term"] == "").all()

    # Parenthetical suffixes are stripped in term and _clean columns
    paren_mask = result["value"].str.contains(r"\s+\(", na=False)
    paren_rows = result[paren_mask]
    assert not paren_rows["term"].str.contains(r"\(", regex=True).any()
    assert not paren_rows["term"].str.contains(r"\)", regex=True).any()
    # "Первый канал (HD)" and "Первый канал (SD)" collapse to the same term
    assert {
        row.term
        for row in paren_rows[
            paren_rows["value"].str.startswith("Первый канал")
        ].itertuples()
    } == {"Первый канал"}
    # Whitespace around values is trimmed in term / _clean
    ntvs = result[result["value"].str.strip() == "НТВ"]
    assert set(ntvs["term"]) == {"НТВ"}


# ---------------------------------------------------------------------------
# PIPE-008: DefaultDataStrategy.prepare logs task_name on KeyError
# ---------------------------------------------------------------------------

_REPORTER_LOGGER = "mko_get_mediascope_data.core.reports"


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
    report.report_settings.model_dump.return_value = {"regions": [1]}

    report.period = [("2025-01-01", "2025-01-07")]
    report.targets = {"all 18+": None}
    report.catalogs = MagicMock()

    expected_name = "2025-01-01_2025-01-07_all 18+_Moscow"
    report.done_files = {expected_name: "file_exists.csv.gz"}

    report.subtype = MagicMock(value=ReportSubtype.TOP_NAT_TV_ADVERTISERS.value)
    report.type = MagicMock(value=ReportType.crosstab.value)

    with caplog.at_level(logging.WARNING, logger=_REPORTER_LOGGER):
        tasks = [task async for task in strategy.generate(report)]

    assert len(tasks) == 0
    assert "already exists" in caplog.text


async def test_tv_reg_task_strategy_generates_tasks_for_regions():
    """TVRegTaskStrategy.generate yields tasks for explicit regions list.

    With ``regions=[1, 2, 99]`` in report_settings, region id 99 is
    skipped (reports.py:544 ``if int(reg_id) == 99: continue``), so
    exactly 2 tasks are yielded — not 3.
    """
    strategy = TVRegTaskStrategy()

    report = MagicMock()
    report.data_settings = MagicMock()
    report.data_settings.model_dump.return_value = {}

    regions_df = pd.DataFrame(
        {"id": [1, 2, 99], "ename": ["Moscow", "SPb", "Russia"]}
    )
    report.network_client.call = AsyncMock(return_value=regions_df)

    report.report_settings = MagicMock()
    report.report_settings.model_dump.return_value = {"regions": [1, 2, 99]}

    report.period = [("2025-01-01", "2025-01-07")]
    report.targets = {"all 18+": None}
    report.catalogs = MagicMock()
    report.done_files = {}

    report.subtype = MagicMock(value=ReportSubtype.TOP_NAT_TV_ADVERTISERS.value)
    report.type = MagicMock(value=ReportType.crosstab.value)

    tasks = [task async for task in strategy.generate(report)]

    # Region 99 is skipped → 2 tasks, not 3.
    assert len(tasks) == 2

    first_task = tasks[0]
    # company_filter for region 1: "regionId IN (1)"
    assert "regionId IN (1)" in first_task.settings["company_filter"]
    assert first_task.interval == ("2025-01-01", "2025-01-07")
    assert first_task.target == "all 18+"
