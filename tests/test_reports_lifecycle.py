"""Lifecycle test for ``TVMediaReport.create_report``.

Exercises the real ``create_report`` -> ``get_period`` -> ``processing_tasks`` ->
``worker`` -> ``send_task`` -> ``wait_task`` -> ``extract_data`` -> ``prepare_data``
-> ``csv_to_file`` -> ``task_done`` pipeline against a doubled
``MediaVortexTask``.  Neither ``create_report`` nor ``NetworkClient.call`` is
mocked -- only the external Mediascope API client is doubled.
"""

import asyncio
import json
import logging
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import yaml
from unidecode import unidecode

from mko_get_mediascope_data.core.errors import NoReportFoundError
from mko_get_mediascope_data.core.models import Data, ReportSettings
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_get_mediascope_data.core.reports import ReportFactory, TVMediaReport
from mko_get_mediascope_data.core.services import AppService
from mko_get_mediascope_data.core.tasks import TVTask

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


def _yielding_sleep(real_sleep):
    """Return an ``asyncio.sleep`` replacement that yields to the event loop.

    ``asyncio.wait_for`` schedules its deadline via ``loop.call_later``,
    not ``asyncio.sleep``, so mocking ``asyncio.sleep`` does not affect
    the timeout.  A real ``sleep(0)`` yields control so the deadline
    callback fires between fast poll iterations.  A plain ``AsyncMock``
    returns immediately without yielding, which would let ``wait_task``'s
    ``while True`` spin without ever letting the ``call_later`` timer fire.
    """

    async def _yielding(*args, **kwargs):
        await real_sleep(0)

    return _yielding


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


@pytest.mark.asyncio
async def test_create_report_timeout_does_not_hang(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """Worker must not hang when ``wait_task`` polls forever.

    Doubles ``get_status`` to return a non-terminal status
    (``"IN_PROGRESS"``) so ``TVMediaReport.wait_task``'s inner ``while True``
    poll loop would spin indefinitely.  A short ``timeout`` passed to
    ``create_report`` breaks the loop via ``asyncio.wait_for``, marks the
    task failed, and writes an error YAML artifact.

    **M11 guard** — the outer ``asyncio.wait_for(..., timeout=10)``
    ensures that if ``task_done`` is ever removed (mutation M11), the test
    fails within 10 s instead of hanging the suite past 240 s.
    """

    # ---- 1. Parse ReportSettings from the fixture YAML ----------------
    yaml_data = json.loads(tv_yaml_content)
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — get_status never terminal -------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    # Non-terminal status — wait_task inner loop never breaks
    mock_report_service.get_status.return_value = {"taskStatus": "IN_PROGRESS"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep to yield (enables wait_for timer) ------
    # asyncio.wait_for schedules its deadline via loop.call_later (not
    # asyncio.sleep), so the mock does not affect the timeout timer.
    # A real sleep(0) yields control so the deadline callback can fire
    # between wait_task iterations.
    real_sleep = asyncio.sleep
    mocker.patch("asyncio.sleep", new=_yielding_sleep(real_sleep))  # noqa: N812

    # ---- 7. Export path ------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 8. Construct report with check_done=False ---------------------
    report_factory = ReportFactory(report_settings)

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

    # ---- 9. Run with short inner timeout, bounded by outer wait_for ---
    # Inner timeout (0.5 s): breaks the infinite wait_task poll loop.
    # Outer timeout (10 s): catches M11 — if task_done is missing,
    #   queue.join() hangs and the outer wait_for fires at 10 s.
    await asyncio.wait_for(report.create_report(timeout=0.5), timeout=10.0)

    # ---- 10. Assertions ------------------------------------------------
    # task_done() was always called — queue is fully drained (no join hang)
    assert report.task_queue.qsize() == 0

    # Error YAML written for the timed-out task
    error_files = list(export_path.glob("errors/*.yaml"))
    assert len(error_files) == 1, (
        f"Expected 1 error YAML in {export_path / 'errors'}, "
        f"found {len(error_files)}"
    )


@pytest.mark.asyncio
async def test_create_report_zero_tasks_raises_no_report_found(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """PIPE-004: empty ``target_audiences`` produces zero tasks.

    With ``target_audiences`` set to ``{}``, ``DefaultTVTaskStrategy.generate``
    yields no tasks.  ``processing_tasks`` counts ``enqueued == 0``, cancels
    the idle workers, and raises ``NoReportFoundError`` (not the ``check_done``
    early-return, since ``check_done`` is ``False``).
    """

    # ---- 1. Parse ReportSettings with empty target_audiences --------------
    yaml_data = json.loads(tv_yaml_content)
    yaml_data[0]["target_audiences"] = {}
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings ---------------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns ----------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask --------------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) -------------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) --------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path ---------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 8. Construct report with check_done=False ------------------------
    report_factory = ReportFactory(report_settings)

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

    # ---- 9. Run — expect NoReportFoundError -------------------------------
    with pytest.raises(NoReportFoundError):
        await report.create_report()


@pytest.mark.asyncio
async def test_create_report_out_of_range_date_filter_raises(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """PIPE-005: out-of-range ``date_filter`` produces an inverted period.

    With ``date_filter`` set to ``["1900-01-01", "1900-02-01"]`` — outside the
    available data range (2025-01-01 .. 2025-12-31 from ``dummy_df``) —
    ``get_period`` clamps to ``(2025-01-01, 1900-02-01)`` which is inverted
    (``period[0] > period[1]``).  ``NoReportFoundError`` is raised with a
    "no overlap" message before any tasks are generated.
    """

    # ---- 1. Parse ReportSettings with out-of-range date_filter ------------
    yaml_data = json.loads(tv_yaml_content)
    yaml_data[0]["period"]["date_filter"] = ["1900-01-01", "1900-02-01"]
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings ---------------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns ----------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask --------------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) -------------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) --------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path ---------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 8. Construct report with check_done=False ------------------------
    report_factory = ReportFactory(report_settings)

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

    # ---- 9. Run — expect NoReportFoundError with "no overlap" -------------
    with pytest.raises(NoReportFoundError, match="no overlap"):
        await report.create_report()


@pytest.mark.asyncio
async def test_wait_task_max_polls_limit(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """PIPE-006: the inner ``max_polls`` guard breaks the ``wait_task`` loop.

    The existing timeout test only exercises the outer ``asyncio.wait_for``
    wrapper in ``worker`` — the inner ``max_polls`` / ``poll_deadline`` guards
    in ``wait_task`` are never triggered.  Here ``get_status`` is doubled to
    return a non-terminal ``IN_PROGRESS`` status so the poll loop would spin
    forever without the guard.

    The guard is ``if polls >= max_polls`` and is checked at the *top* of each
    iteration, before the status poll.  Therefore ``max_polls=2`` is the smallest
    value that lets exactly one real poll run (so ``get_status`` is called once,
    receiving the non-terminal status) and then trips the counter on the next
    iteration — terminating via the inner guard rather than any outer timeout.
    (``max_polls=1`` would trip the guard before any poll, calling ``get_status``
    zero times, which would not exercise the poll path.)
    """

    # ---- 1. Parse ReportSettings from the fixture YAML ----------------
    yaml_data = json.loads(tv_yaml_content)
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — non-terminal status -------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    # Non-terminal status — without the max_polls guard the loop would spin
    # indefinitely waiting for the (mocked) task to finish.
    mock_report_service.get_status.return_value = {"taskStatus": "IN_PROGRESS"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path + report construction -------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    report_factory = ReportFactory(report_settings)

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

    # ---- 8. Build a simple task and drive wait_task directly -----------
    # wait_task is called directly — no need for the full create_report()
    # pipeline or the outer asyncio.wait_for wrapper.
    task = TVTask("test_wait_task", {}, "DYNAMICS_BY_SPOTS", "crosstab")
    task.key = {"taskJson": "mock_key"}

    await report.wait_task(task, max_polls=2)

    # ---- 9. Assertions -------------------------------------------------
    # The inner max_polls guard terminated the loop -> task marked failed.
    assert task.status is False
    assert task.log_error is True
    assert "Poll deadline" in task.error

    # Exactly one poll ran before the guard tripped the counter.
    assert mock_report_service.get_status.call_count == 1


@pytest.mark.asyncio
async def test_worker_handles_exception_and_writes_error_yaml(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """PIPE-007: worker's ``except Exception`` branch writes the error YAML.

    ``send_task`` is doubled to raise ``RuntimeError`` — a non-timeout failure
    from the upstream API layer.  ``worker`` must catch it via
    ``except Exception`` (distinct from the ``except TimeoutError`` path
    covered by ``test_create_report_timeout_does_not_hang``), mark the task
    failed, and write an error YAML before draining the queue.
    """

    # ---- 1. Parse ReportSettings from the fixture YAML ----------------
    yaml_data = json.loads(tv_yaml_content)
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — send_task raises RuntimeError ---
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    # Non-timeout API failure — must be caught by ``except Exception``, not
    # the ``except TimeoutError`` branch exercised elsewhere.
    mock_report_service.send_task.side_effect = RuntimeError("API connection refused")
    # The real ``NetworkClient.call`` reads ``func.__name__`` while logging a
    # failed call.  A bare ``MagicMock`` raises ``AttributeError('__name__')``
    # on that access (masking the intended ``RuntimeError``), so give the mock
    # a ``__name__`` like a real bound method to let the original error through.
    mock_report_service.send_task.__name__ = "send_task"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path ------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 8. Construct report with check_done=False --------------------
    report_factory = ReportFactory(report_settings)

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

    # ---- 9. Run — worker must catch the RuntimeError, not re-raise -----
    # Inner timeout (1.0s) is per-task; the outer wait_for (10.0s) is the
    # safety net ensuring the suite cannot hang if task_done() is ever dropped.
    await asyncio.wait_for(report.create_report(timeout=1.0), timeout=10.0)

    # ---- 10. Assertions ------------------------------------------------
    # No status poll should occur: the task fails in send_task before
    # wait_task is ever reached.
    mock_report_service.get_status.assert_not_called()

    # Error YAML written for the failed task (task name + .yaml).
    error_files = list(export_path.glob("errors/*.yaml"))
    assert len(error_files) >= 1, (
        f"Expected an error YAML in {export_path / 'errors'}, "
        f"found {len(error_files)}"
    )

    # The recorded error is the API failure ("API connection refused"), NOT a
    # "Timeout after ..." message — proving the ``except Exception`` branch
    # (not ``except TimeoutError``) ran.
    error_payload = yaml.safe_load(error_files[0].read_text(encoding="utf-8"))
    assert error_payload["error"] == "API connection refused"
    assert error_payload["log_error"] is True

    # Queue accounting: every enqueued task was task_done()'d — no join hang.
    assert report.task_queue.qsize() == 0


# ---------------------------------------------------------------------------
# PIPE-009: worker log_error True/False branches
# ---------------------------------------------------------------------------

_REPORTER_LOGGER = "mko_get_mediascope_data.core.reports"


@pytest.mark.asyncio
async def test_worker_log_error_true_writes_yaml(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """PIPE-009 (``log_error=True``): worker writes error YAML on API key failure.

    ``send_task`` on the doubled ``MediaVortexTask`` is configured to return
    an empty string (falsy).  ``TVMediaReport.send_task`` then sets
    ``task.key = ""``; the ``not task.key`` guard marks the task as
    ``status=False, log_error=True``.

    The ``worker`` logs a "will be skipped" warning and calls ``to_yaml``,
    writing the error artifact to ``export_path/errors/`` with
    ``log_error=True``.
    """

    # ---- 1. Parse ReportSettings from the fixture YAML ----------------
    yaml_data = json.loads(tv_yaml_content)
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — send_task returns falsy key ----
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    # Empty string is falsy → TVMediaReport.send_task sets log_error=True
    mock_report_service.send_task.return_value = ""
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path ------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 8. Construct report with check_done=False ---------------------
    report_factory = ReportFactory(report_settings)

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

    # ---- 9. Run real pipeline ----------------------------------------
    await asyncio.wait_for(report.create_report(), timeout=10.0)

    # ---- 10. Assertions ------------------------------------------------
    # task_done() always called — queue fully drained (no join hang).
    assert report.task_queue.qsize() == 0

    # Error YAML written for the failed task.
    error_files = list(export_path.glob("errors/*.yaml"))
    assert len(error_files) == 1, (
        f"Expected 1 error YAML in {export_path / 'errors'}, "
        f"found {len(error_files)}"
    )

    # The recorded log_error field must be True.
    error_payload = yaml.safe_load(error_files[0].read_text(encoding="utf-8"))
    assert error_payload["log_error"] is True


@pytest.mark.asyncio
async def test_worker_log_error_false_no_yaml(
    tmp_path: Path,
    mocker,
    tv_yaml_content: str,
    dummy_df: pd.DataFrame,
):
    """PIPE-009 (``log_error=False``): worker logs "No data" and writes no YAML.

    ``send_task`` returns a valid key, ``get_status`` returns DONE,
    ``get_result`` returns a non-False result, and ``result2table`` returns
    an **empty** DataFrame.  ``extract_data`` then sets
    ``task.status=False, log_error=False`` (reports.py:433-436).

    The ``worker`` logs "No data for task … — skipping" and does **not**
    call ``to_yaml``.
    """

    # ---- 1. Parse ReportSettings from the fixture YAML ----------------
    yaml_data = json.loads(tv_yaml_content)
    report_settings = ReportSettings(**yaml_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = _build_hermetic_app_service(tmp_path)
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Empty result DataFrame — triggers df.empty branch ----------
    result_df = pd.DataFrame()

    # ---- 4. Doubled MediaVortexTask — successful path to empty df -----
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    # Empty DataFrame → df.empty is True → log_error=False
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # dictConfig (called by _build_hermetic_app_service) replaces the root
    # logger handlers, so caplog loses its handler.  Spy on the module-level
    # logger.warning instead — this intercepts calls regardless of handler
    # configuration.
    reports_logger = logging.getLogger(_REPORTER_LOGGER)
    mock_warning = mocker.spy(reports_logger, "warning")

    # ---- 7. Export path ------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 8. Construct report with check_done=False ---------------------
    report_factory = ReportFactory(report_settings)

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

    # ---- 9. Run real pipeline ----------------------------------------
    await asyncio.wait_for(report.create_report(), timeout=10.0)

    # ---- 10. Assertions ------------------------------------------------
    # task_done() always called — queue fully drained (no join hang).
    assert report.task_queue.qsize() == 0

    # No error YAML — the "No data" branch must not call to_yaml.
    error_files = list(export_path.glob("errors/*.yaml"))
    assert len(error_files) == 0, (
        f"Expected 0 error YAML files, found {len(error_files)} at "
        f"{export_path / 'errors'}"
    )

    # The "No data for task" warning confirms the log_error=False path.
    warning_messages = [
        str(call.args[0]) if call.args else ""
        for call in mock_warning.call_args_list
    ]
    assert any("No data" in msg for msg in warning_messages), (
        f"Expected a 'No data' warning, got calls: {mock_warning.call_args_list}"
    )
