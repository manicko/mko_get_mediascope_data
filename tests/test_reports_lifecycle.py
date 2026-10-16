"""Lifecycle test for ``TVMediaReport.create_report``.

Exercises the real ``create_report`` -> ``get_period`` -> ``processing_tasks`` ->
``worker`` -> ``send_task`` -> ``wait_task`` -> ``extract_data`` -> ``prepare_data``
-> ``csv_to_file`` -> ``task_done`` pipeline against a doubled
``MediaVortexTask``.  Neither ``create_report`` nor ``NetworkClient.call`` is
mocked -- only the external Mediascope API client is doubled.
"""

import json
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from unidecode import unidecode

from mko_get_mediascope_data.core.models import Data, ReportSettings
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.reports import ReportFactory, TVMediaReport
from mko_get_mediascope_data.core.services import AppService

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


def _build_hermetic_app_service(tmp_path: Path) -> AppService:
    """Create an ``AppService`` backed by isolated temp config files."""
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    (settings_dir / "log_config.yaml").write_text(_LOG_CONFIG_YAML, encoding="utf-8")
    (settings_dir / "app_config.yaml").write_text(_APP_CONFIG_YAML, encoding="utf-8")
    shutil.copytree(APP_PATHS.app_settings_dir / "defaults", settings_dir / "defaults")
    app_paths = AppPaths(app_dir=tmp_path, app_name="test", user_dir=tmp_path)
    return AppService(app_paths=app_paths, resolver=PathResolver(tmp_path))


def _build_result_df(
    data_settings: Data, report_settings: ReportSettings
) -> pd.DataFrame:
    """Build a DataFrame whose columns match ``output_columns``.

    Mirrors ``TVMediaReport.set_output_columns`` (reports.py:207-221) so the
    doubled ``result2table`` returns columns that ``DefaultDataStrategy.prepare``
    can select without a ``KeyError``.
    """
    columns = list(data_settings.slices) + list(data_settings.statistics)
    if report_settings.target_audiences:
        columns.insert(0, "targetAudience")
    df = pd.DataFrame(columns=columns)
    df.loc[0] = 0
    return df


@pytest.mark.asyncio
async def test_create_report_writes_csv_and_marks_queue_done(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """Real ``create_report`` pipeline produces CSV output.

    Asserts that at least one ``.csv.gz`` file is written to the export path
    and that ``task_queue.join()`` completes without hanging (i.e. every
    ``task_done`` was called for every item placed on the queue).
    """

    # ---- 1. Parse ReportSettings from the fixture YAML ----------------
    yaml_data = json.loads(tv_yaml_content)
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask -----------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path + strategies ----------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    report_factory = ReportFactory(report_settings)

    # ---- 8. Construct report directly ----------------------------------
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=network_client,
        task_strategy=report_factory.task_strategy(),
        data_strategy=report_factory.data_strategy(),
        check_done=False,
    )

    # ---- 9. Run real pipeline ------------------------------------------
    await report.create_report()

    # ---- 10. Assertions ------------------------------------------------
    csv_files = list(export_path.glob("*.csv.gz"))
    assert len(csv_files) > 0, "Expected at least one CSV file to be written"

    # Queue accounting: if create_report returned without hanging, every
    # task_done was called for the single task that was enqueued.
    assert report.task_queue.qsize() == 0


@pytest.mark.asyncio
async def test_create_report_skips_done_files(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """Real ``create_report`` pipeline skips tasks whose CSV already exists.

    With ``check_done=True``, ``TVMediaReport`` scans the export folder for
    existing ``.csv.gz`` files and skips any task whose generated name matches.
    Asserts that exactly one CSV file remains (the pre-existing one) and that
    no API methods (``build_task``, ``send_task``, ``get_status``,
    ``get_result``) are invoked.
    """

    # ---- 1. Parse ReportSettings from the fixture YAML ----------------
    yaml_data = json.loads(tv_yaml_content)
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask -----------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) -----------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path + pre-create done file ------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # Compute the file name that DefaultTVTaskStrategy.generate would produce:
    #   name = "_".join(interval) + "_" + unidecode(target_name)
    # interval = ("2025-01-01", "2025-01-07") from date_filter + monthly slice
    # target_name = "all 18+" from target_audiences
    interval = ("2025-01-01", "2025-01-07")
    target_name = "all 18+"
    expected_name = "_".join(interval) + "_" + unidecode(target_name)
    done_file = export_path / f"{expected_name}.csv.gz"
    done_file.touch()

    # ---- 8. Construct report with check_done=True ---------------------
    report_factory = ReportFactory(report_settings)

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=network_client,
        task_strategy=report_factory.task_strategy(),
        data_strategy=report_factory.data_strategy(),
        check_done=True,
    )

    # ---- 9. Run real pipeline ----------------------------------------
    await report.create_report()

    # ---- 10. Assertions ------------------------------------------------
    csv_files = list(export_path.glob("*.csv.gz"))
    assert len(csv_files) == 1, "Expected only the pre-existing CSV, no new file"
    assert csv_files[0].name == f"{expected_name}.csv.gz"

    # The skipped task must not have invoked any API methods
    mock_report_service.build_task.assert_not_called()
    mock_report_service.send_task.assert_not_called()
    mock_report_service.get_status.assert_not_called()
    mock_report_service.get_result.assert_not_called()
