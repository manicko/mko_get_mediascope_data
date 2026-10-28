"""Lifecycle test for ``TVMediaReport.create_report``.

Exercises the real ``create_report`` -> ``get_period`` -> ``processing_tasks`` ->
``worker`` -> ``send_task`` -> ``wait_task`` -> ``extract_data`` -> ``prepare_data``
-> ``csv_to_file`` -> ``task_done`` pipeline against a doubled
``MediaVortexTask``.  Neither ``create_report`` nor ``NetworkClient.call`` is
mocked -- only the external Mediascope API client is doubled.
"""

import asyncio
import hashlib
import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import json5
from unidecode import unidecode

from tests.conftest import MOCK_TASK_KEY, expected_output_columns
from mko_get_mediascope_data.core import utils
from mko_get_mediascope_data.core.errors import NoReportFoundError
from mko_get_mediascope_data.core.models import (
    Data,
    MediaType,
    ReportSettings,
    ReportSubtype,
    ReportType,
    SendTaskSettings,
)
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.reports import (
    DefaultDataStrategy,
    DefaultTVTaskStrategy,
    DictDataStrategy,
    ReportFactory,
    TVMediaReport,
    TVRegTaskStrategy,
)
from mko_get_mediascope_data.core.services import AppService
from mko_get_mediascope_data.core.tasks import TVTask


def _build_result_df(
    data_settings: Data, report_settings: ReportSettings
) -> pd.DataFrame:
    """Build a DataFrame whose columns match ``output_columns``.

    Delegates to the shared ``expected_output_columns`` helper (conftest)
    which mirrors ``TVMediaReport.set_output_columns`` (reports.py:282-296),
    including the ``frequencyDistInterval`` branch.  The doubled
    ``result2table`` thus returns columns that ``DefaultDataStrategy.prepare``
    can select without a ``KeyError``.
    """
    output_columns = expected_output_columns(data_settings, report_settings)
    columns = output_columns["slices"] + output_columns["statistics"]
    df = pd.DataFrame(columns=columns)
    df.loc[0] = 0
    return df


async def test_create_report_writes_csv_and_marks_queue_done(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """Real ``create_report`` pipeline produces CSV output.

    Asserts that at least one ``.csv.gz`` file is written to the export path
    and that ``task_queue.join()`` completes without hanging (i.e. every
    ``task_done`` was called for every item placed on the queue).
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask -----------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
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


async def test_create_report_skips_done_files(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """Real ``create_report`` pipeline skips tasks whose CSV already exists.

    With ``check_done=True``, ``TVMediaReport`` scans the export folder for
    existing ``.csv.gz`` files and skips any task whose generated name matches.
    Asserts that exactly one CSV file remains (the pre-existing one) and that
    no API methods (``build_task``, ``send_task``, ``get_status``,
    ``get_result``) are invoked.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask -----------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
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


def test_done_files_excludes_temp_csv(
    tmp_path: Path,
    tv_json5_content: str,
    hermetic_app_service: AppService,
):
    """``get_done_files`` must ignore atomic-publication temp CSV files.

    ``utils.csv_to_file`` publishes atomically by writing a temp file whose
    name starts with ``utils.CSV_TEMP_PREFIX`` and then ``os.replace``-ing it
    onto the final name.  A temp file left behind (e.g. a crashed writer) must
    never be recognized as published output during resume discovery, so
    ``get_done_files`` files it under the published key only.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Export path with a published file and a sibling temp file -
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    published_name = "2025-01-01_2025-01-07_all_18+"
    (export_path / f"{published_name}.csv.gz").touch()
    (export_path / f"{utils.CSV_TEMP_PREFIX}{published_name}.csv.gz").touch()

    # ---- 4. Minimal report (only export_path + report_settings used) --
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=MagicMock(),
        network_client=MagicMock(),
        task_strategy=MagicMock(),
        data_strategy=MagicMock(),
        check_done=True,
    )

    # ---- 5. Assertions ------------------------------------------------
    done_files = report.get_done_files()
    assert published_name in done_files
    assert f"{utils.CSV_TEMP_PREFIX}{published_name}" not in done_files
    assert len(done_files) == 1


async def test_create_report_timeout_does_not_hang(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """Worker must not hang when ``wait_task`` polls forever.

    Doubles ``get_status`` to return a non-terminal status
    (``"IN_PROGRESS"``) so ``TVMediaReport.wait_task``'s inner ``while True``
    poll loop would spin indefinitely.  A short ``timeout`` passed to
    ``create_report`` breaks the loop via ``asyncio.wait_for``, marks the
    task failed, and writes an error JSON5 artifact.

    **M11 guard** — the outer ``asyncio.wait_for(..., timeout=10)``
    ensures that if ``task_done`` is ever removed (mutation M11), the test
    fails within 10 s instead of hanging the suite past 240 s.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — get_status never terminal -------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    # Non-terminal status — wait_task inner loop never breaks
    mock_report_service.get_status.return_value = {"taskStatus": "IN_PROGRESS"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Patch asyncio.sleep with a real 0.3 s delay ------------------
    # asyncio.wait_for schedules its deadline via loop.call_later (not
    # asyncio.sleep), so the mock does not affect the timeout timer itself.
    # A real 0.3 s delay per poll means 2 polls (0.6 s) exceed wait_for's
    # 0.5 s inner deadline, triggering TimeoutError in worker's except clause.
    real_sleep = asyncio.sleep

    async def _delayed_sleep(*_args, **_kwargs):
        await real_sleep(0.3)

    mocker.patch("asyncio.sleep", new=_delayed_sleep)

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

    # Error JSON5 written for the timed-out task
    error_files = list(export_path.glob("errors/*.json5"))
    assert len(error_files) == 1, (
        f"Expected 1 error JSON5 in {export_path / 'errors'}, "
        f"found {len(error_files)}"
    )

    # The error message confirms the TimeoutError branch ran — not the
    # max_polls guard or poll_deadline fallback in wait_task.
    error_payload = json5.loads(error_files[0].read_text(encoding="utf-8"))
    assert "Timeout after" in error_payload["error"], (
        f"Expected 'Timeout after' in error, got: {error_payload['error']}"
    )


async def test_create_report_zero_tasks_raises_no_report_found(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PIPE-004: empty ``target_audiences`` produces zero tasks.

    With ``target_audiences`` set to ``{}``, ``DefaultTVTaskStrategy.generate``
    yields no tasks.  ``processing_tasks`` counts ``enqueued == 0``, cancels
    the idle workers, and raises ``NoReportFoundError`` (not the ``check_done``
    early-return, since ``check_done`` is ``False``).
    """

    # ---- 1. Parse ReportSettings with empty target_audiences --------------
    json5_data = json.loads(tv_json5_content)
    json5_data[0]["target_audiences"] = {}
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings ---------------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns ----------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask --------------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
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


async def test_create_report_out_of_range_date_filter_raises(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PIPE-005: out-of-range ``date_filter`` produces an inverted period.

    With ``date_filter`` set to ``["1900-01-01", "1900-02-01"]`` — outside the
    available data range (2025-01-01 .. 2025-12-31 from ``dummy_df``) —
    ``get_period`` clamps to ``(2025-01-01, 1900-02-01)`` which is inverted
    (``period[0] > period[1]``).  ``NoReportFoundError`` is raised with a
    "no overlap" message before any tasks are generated.
    """

    # ---- 1. Parse ReportSettings with out-of-range date_filter ------------
    json5_data = json.loads(tv_json5_content)
    json5_data[0]["period"]["date_filter"] = ["1900-01-01", "1900-02-01"]
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings ---------------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns ----------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask --------------------------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
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


async def test_wait_task_max_polls_limit(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PIPE-006: the inner ``max_polls`` guard breaks the ``wait_task`` loop.

    The existing timeout test only exercises the outer ``asyncio.wait_for``
    wrapper in ``worker`` — the inner ``max_polls`` / ``poll_deadline`` guards
    in ``wait_task`` are never triggered.  Here ``get_status`` is doubled to
    return a non-terminal ``IN_PROGRESS`` status so the poll loop would spin
    forever without the guard.

    The guard is ``if polls > max_polls`` and is checked at the *top* of each
    iteration, after the poll counter is incremented.  With ``max_polls=N``
    the loop performs exactly N ``get_status`` calls (the old ``>=``
    off-by-one performed only N-1).  ``max_polls=1`` is the smallest value
    that lets exactly one real poll run (so ``get_status`` is called once,
    receiving the non-terminal status) and then trips the counter on the next
    iteration — terminating via the inner guard rather than any outer timeout.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — non-terminal status -------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
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

    await report.wait_task(task, max_polls=1)

    # ---- 9. Assertions -------------------------------------------------
    # The inner max_polls guard terminated the loop -> task marked failed.
    assert task.status is False
    assert task.log_error is True
    assert "Poll deadline" in task.error

    # Exactly one poll ran before the guard tripped the counter.
    assert mock_report_service.get_status.call_count == 1


async def test_worker_handles_exception_and_writes_error_json5(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PIPE-007: worker's ``except Exception`` branch writes the error JSON5.

    ``send_task`` is doubled to raise ``RuntimeError`` — a non-timeout failure
    from the upstream API layer.  ``worker`` must catch it via
    ``except Exception`` (distinct from the ``except TimeoutError`` path
    covered by ``test_create_report_timeout_does_not_hang``), mark the task
    failed, and write an error JSON5 before draining the queue.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
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

    # Error JSON5 written for the failed task (task name + .json5).
    error_files = list(export_path.glob("errors/*.json5"))
    assert len(error_files) >= 1, (
        f"Expected an error JSON5 in {export_path / 'errors'}, "
        f"found {len(error_files)}"
    )

    # The recorded error is the API failure ("API connection refused"), NOT a
    # "Timeout after ..." message — proving the ``except Exception`` branch
    # (not ``except TimeoutError``) ran.
    error_payload = json5.loads(error_files[0].read_text(encoding="utf-8"))
    assert error_payload["error"] == "API connection refused"
    assert error_payload["log_error"] is True

    # Queue accounting: every enqueued task was task_done()'d — no join hang.
    assert report.task_queue.qsize() == 0


# ---------------------------------------------------------------------------
# PIPE-009: worker log_error True/False branches
# ---------------------------------------------------------------------------

_REPORTER_LOGGER = "mko_get_mediascope_data.core.reports"


async def test_worker_log_error_true_writes_json5(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PIPE-009 (``log_error=True``): worker writes error JSON5 on API key failure.

    ``send_task`` on the doubled ``MediaVortexTask`` is configured to return
    an empty string (falsy).  ``TVMediaReport.send_task`` then sets
    ``task.key = ""``; the ``not task.key`` guard marks the task as
    ``status=False, log_error=True``.

    The ``worker`` logs a "will be skipped" warning and calls ``to_file``,
    writing the error artifact to ``export_path/errors/`` with
    ``log_error=True``.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
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

    # Error JSON5 written for the failed task.
    error_files = list(export_path.glob("errors/*.json5"))
    assert len(error_files) == 1, (
        f"Expected 1 error JSON5 in {export_path / 'errors'}, "
        f"found {len(error_files)}"
    )

    # The recorded log_error field must be True.
    error_payload = json5.loads(error_files[0].read_text(encoding="utf-8"))
    assert error_payload["log_error"] is True


async def test_worker_log_error_false_no_json5(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PIPE-009 (``log_error=False``): worker logs "No data" and writes no JSON5.

    ``send_task`` returns a valid key, ``get_status`` returns DONE,
    ``get_result`` returns a non-False result, and ``result2table`` returns
    an **empty** DataFrame.  ``extract_data`` then sets
    ``task.status=False, log_error=False`` (reports.py:433-436).

    The ``worker`` logs "No data for task … — skipping" and does **not**
    call ``to_file``.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Empty result DataFrame — triggers df.empty branch ----------
    result_df = pd.DataFrame()

    # ---- 4. Doubled MediaVortexTask — successful path to empty df -----
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    # Empty DataFrame → df.empty is True → log_error=False
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # dictConfig (called by the hermetic_app_service fixture) replaces the root
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

    # No error JSON5 — the "No data" branch must not call to_file.
    error_files = list(export_path.glob("errors/*.json5"))
    assert len(error_files) == 0, (
        f"Expected 0 error JSON5 files, found {len(error_files)} at "
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


# ---------------------------------------------------------------------------
# EXT-003: FAILED / CANCELLED terminal-status reset in wait_task
# ---------------------------------------------------------------------------


async def test_wait_task_resets_status_on_failed(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """EXT-003: terminal FAILED status resets task fields and skips extract_data.

    ``get_status`` returns ``{"taskStatus": "FAILED"}`` immediately.
    ``wait_task`` must reset ``task.status=False``, ``task.log_error=True``,
    ``task.error="Task FAILED"`` and never reach ``extract_data``
    (which calls ``get_result``).
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — FAILED status -------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "FAILED"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) -------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path + report construction ---------------------------
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

    # ---- 8. Build a task and drive wait_task directly -------------------
    # wait_task is called directly — no need for the full create_report()
    # pipeline.  max_polls=5 lets the single terminal poll run before the
    # inner guard could trip.
    task = TVTask("test_wait_task_failed", {}, "DYNAMICS_BY_SPOTS", "crosstab")
    task.key = {"taskJson": "mock_key"}

    await report.wait_task(task, max_polls=5)

    # ---- 9. Assertions --------------------------------------------------
    # EXT-003: terminal FAILED resets status and records the error.
    assert task.status is False
    assert task.log_error is True
    assert task.error == "Task FAILED"

    # extract_data was never reached — get_result was not called.
    mock_report_service.get_result.assert_not_called()


async def test_wait_task_resets_status_on_cancelled(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """EXT-003: terminal CANCELLED status resets task fields and skips extract_data.

    Same as ``test_wait_task_resets_status_on_failed`` but with a
    ``CANCELLED`` terminal status from the API.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — CANCELLED status ----------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "CANCELLED"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) -------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path + report construction ---------------------------
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

    # ---- 8. Build a task and drive wait_task directly -------------------
    task = TVTask("test_wait_task_cancelled", {}, "DYNAMICS_BY_SPOTS", "crosstab")
    task.key = {"taskJson": "mock_key"}

    await report.wait_task(task, max_polls=5)

    # ---- 9. Assertions --------------------------------------------------
    # EXT-003: terminal CANCELLED resets status and records the error.
    assert task.status is False
    assert task.log_error is True
    assert task.error == "Task CANCELLED"

    # extract_data was never reached — get_result was not called.
    mock_report_service.get_result.assert_not_called()


async def test_worker_does_not_log_ready_on_failed(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """EXT-003: worker logs "will be skipped" (not "is ready") for FAILED tasks.

    The full ``create_report`` pipeline runs with ``get_status`` returning
    ``{"taskStatus": "FAILED"}``.  ``wait_task`` resets ``task.status=False`` /
    ``log_error=True``, so ``_process_item`` skips ``extract_data``.  The
    ``worker`` must log a "will be skipped" warning (not "is ready"),
    write an error JSON5, and never call ``get_result``.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — FAILED status -------------------
    # send_task returns a valid key (so task enters wait_task), then
    # get_status returns FAILED — exercising the EXT-003 reset path.
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "FAILED"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Spy on reports logger ---------------------------------------
    # dictConfig (called by the hermetic_app_service fixture) replaces the root
    # logger handlers, so caplog loses its handler.  Spy on the module-level
    # logger.info / logger.warning instead — this intercepts calls regardless
    # of handler configuration.
    reports_logger = logging.getLogger(_REPORTER_LOGGER)
    mock_warning = mocker.spy(reports_logger, "warning")
    mock_info = mocker.spy(reports_logger, "info")

    # ---- 8. Export path + report construction ---------------------------
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

    # ---- 9. Run real pipeline ----------------------------------------
    await asyncio.wait_for(report.create_report(), timeout=10.0)

    # ---- 10. Assertions ------------------------------------------------
    # task_done() always called — queue fully drained (no join hang).
    assert report.task_queue.qsize() == 0

    # "is ready" must NOT be logged for the failed task.
    info_messages = [
        str(call.args[0]) if call.args else ""
        for call in mock_info.call_args_list
    ]
    assert not any("is ready" in msg for msg in info_messages), (
        f"Expected 'is ready' NOT to be logged, got calls: {mock_info.call_args_list}"
    )

    # "will be skipped" must be logged.
    warning_messages = [
        str(call.args[0]) if call.args else ""
        for call in mock_warning.call_args_list
    ]
    assert any("will be skipped" in msg for msg in warning_messages), (
        f"Expected a 'will be skipped' warning, got calls: {mock_warning.call_args_list}"
    )

    # Error JSON5 written for the failed task.
    error_files = list(export_path.glob("errors/*.json5"))
    assert len(error_files) == 1, (
        f"Expected 1 error JSON5 in {export_path / 'errors'}, "
        f"found {len(error_files)}"
    )

    # extract_data was never reached — get_result was not called.
    mock_report_service.get_result.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 5.2 — Reporting pipeline gap coverage (13 new tests)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Factory strategy selection (reports.py:30-50)
# ---------------------------------------------------------------------------


def test_factory_selects_tvreg_task_strategy():
    """ReportFactory with TV_REG media selects TVRegTaskStrategy (reports.py:40-41)."""
    report_settings = ReportSettings(
        media=MediaType.TV_REG,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv_reg/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    factory = ReportFactory(report_settings)
    assert factory.task_strategy is TVRegTaskStrategy


def test_factory_selects_dict_data_strategy():
    """ReportFactory with DYNAMICS_BY_SPOTS_DICT subtype selects DictDataStrategy (reports.py:47-48)."""
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.DYNAMICS_BY_SPOTS_DICT,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    factory = ReportFactory(report_settings)
    assert factory.data_strategy is DictDataStrategy


def test_factory_default_strategies():
    """Default media/subtype selects DefaultTVTaskStrategy + DefaultDataStrategy."""
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    factory = ReportFactory(report_settings)
    assert factory.task_strategy is DefaultTVTaskStrategy
    assert factory.data_strategy is DefaultDataStrategy


# ---------------------------------------------------------------------------
# DictDataStrategy error paths (reports.py:616-623)
# ---------------------------------------------------------------------------


def test_dict_data_strategy_missing_columns_returns_none():
    """DictDataStrategy.prepare returns None when required columns are missing (reports.py:619-623)."""
    data_settings = Data(slices=["col_a", "col_b"])
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.DYNAMICS_BY_SPOTS_DICT,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    mock_service = MagicMock()
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=Path("/tmp/dummy_export"),
        report_service=mock_service,
        network_client=MagicMock(),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DictDataStrategy(),
        check_done=False,
    )
    df = pd.DataFrame({"other_col": [1]})
    strategy = DictDataStrategy()
    result = asyncio.run(strategy.prepare(report, df))
    assert result is None


def test_dict_data_strategy_empty_slices_returns_none():
    """DictDataStrategy.prepare returns None when slices list is empty (reports.py:616)."""
    data_settings = Data(slices=[])
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.DYNAMICS_BY_SPOTS_DICT,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    mock_service = MagicMock()
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=Path("/tmp/dummy_export"),
        report_service=mock_service,
        network_client=MagicMock(),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DictDataStrategy(),
        check_done=False,
    )
    df = pd.DataFrame({"col_a": [1]})
    strategy = DictDataStrategy()
    result = asyncio.run(strategy.prepare(report, df))
    assert result is None


# ---------------------------------------------------------------------------
# Poll cadence (reports.py:406, 422-423)
# ---------------------------------------------------------------------------


async def test_wait_task_poll_cadence_starts_at_30(
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """wait_task's first poll interval is 30s (EXT-004 fix), not 2s.

    reports.py:406 — ``sleep_interval = 30.0`` (was 2.0 in the regression).
    The first ``asyncio.sleep`` call in the poll loop must receive 30.0,
    not 2.0.  ``asyncio.sleep`` is mocked so no real delay occurs.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.get_status.return_value = {"taskStatus": "IN_PROGRESS"}

    network_client = NetworkClient(sleep_time=0, max_attempts=1)
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=network_client,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    # get_status returns IN_PROGRESS — we exit via max_polls guard (not sleep cadence)
    # but we need at least one asyncio.sleep call to assert the cadence.

    task = TVTask("test_cadence", {"options": {"kitId": 1}}, "crosstab", "TV")
    task.key = {"taskJson": "mock_key"}

    await report.wait_task(task, max_polls=5)

    # The first asyncio.sleep call (outer poll cadence, not NetworkClient smoothing)
    # must be 30.0 — the EXT-004 regression lowered it to 2.0.
    first_sleep_arg = mock_sleep.call_args_list[0].args[0]
    assert first_sleep_arg == 30.0, (
        f"Expected first poll interval 30.0s, got {first_sleep_arg}"
    )


# ---------------------------------------------------------------------------
# poll_deadline guard (reports.py:383-387)
# ---------------------------------------------------------------------------


async def test_wait_task_poll_deadline_exceeded(
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """wait_task's poll_deadline guard trips when wall-clock exceeds deadline.

    reports.py:383-387 — the ``loop.time() - start > poll_deadline`` branch.
    Distinct from ``max_polls`` (tested in test_wait_task_max_polls_limit).
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "IN_PROGRESS"}

    network_client = NetworkClient(sleep_time=0, max_attempts=1)
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=network_client,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    task = TVTask("test_poll_deadline", {}, "crosstab", "TV")
    task.key = {"taskJson": "mock_key"}

    # poll_deadline=0.0 → immediately exceeded; max_polls=60 so the
    # poll_deadline guard (not max_polls) is the one that trips.
    await report.wait_task(task, max_polls=60, poll_deadline=0.0)

    assert task.status is False
    assert task.log_error is True
    assert "Poll deadline exceeded" in task.error


# ---------------------------------------------------------------------------
# send_task is-False branch (reports.py:352-360)
# ---------------------------------------------------------------------------


def test_send_task_false_key_marks_no_data(
    hermetic_app_service: AppService,
    dummy_df: pd.DataFrame,
):
    """send_task: builder returns False → status=False, log_error=False.

    reports.py:369-390 — when ``task_json is False`` (builder returns False,
    e.g. period outside available DB range), ``send_task`` sets
    ``task.status=False, task.log_error=False`` so the worker skips without
    creating an error artifact.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = False

    mock_network_client = MagicMock()
    mock_network_client.call = AsyncMock(return_value=False)

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    task = TVTask("test_send_false", {}, "crosstab", "TV")
    asyncio.run(report.send_task(task))

    assert task.status is False
    assert task.log_error is False
    assert task.error == "Task could not be built — period or target may be outside available range."


# ---------------------------------------------------------------------------
# send_task config wiring (EXT-001: SendTaskSettings drives retryable/retry_on)
# ---------------------------------------------------------------------------


async def test_send_task_config_wiring(
    hermetic_app_service: AppService,
    dummy_df: pd.DataFrame,
):
    """SendTaskSettings config is wired into NetworkClient.call kwargs.

    EXT-001: ``send_task`` previously hardcoded ``retry_on=RETRIABLE_EXCEPTIONS``
    with ``retryable`` defaulting to ``True``.  Now ``SendTaskSettings`` drives
    both parameters through ``self.send_task_settings``.

    Two cases:
    1. ``retryable=False`` → ``call(retryable=False, retry_on=None)`` — triggers
       the single-attempt early-return path at network.py:182.
    2. ``retryable=True, retry_on_connection_error=False`` → ``call(
       retryable=True, retry_on=None)`` — selective retry scope disabled.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    # ---- Case 1: retryable=False → retryable=False, retry_on=None ----------
    mock_report_service_1 = MagicMock()
    mock_report_service_1.cats = MagicMock()
    mock_report_service_1.cats.get_availability_period.return_value = dummy_df
    mock_report_service_1.build_task.return_value = {"task_json": "mock"}

    mock_network_client_1 = MagicMock()
    mock_network_client_1.call = AsyncMock(
        side_effect=[{"task_json": "mock"}, MOCK_TASK_KEY]
    )

    report_1 = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test1",
        report_service=mock_report_service_1,
        network_client=mock_network_client_1,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
        send_task_settings=SendTaskSettings(retryable=False),
    )

    task_1 = TVTask(
        "test_retryable_false", {"options": {"kitId": 1}}, "crosstab", "TV"
    )
    await report_1.send_task(task_1)

    # The second call (self.sender POST) must carry retryable=False, retry_on=None
    send_call_1 = mock_network_client_1.call.call_args_list[1]
    assert send_call_1.kwargs["retryable"] is False
    assert send_call_1.kwargs["retry_on"] is None
    assert task_1.key == MOCK_TASK_KEY

    # ---- Case 2: retryable=True, retry_on_connection_error=False → retry_on=None
    mock_report_service_2 = MagicMock()
    mock_report_service_2.cats = MagicMock()
    mock_report_service_2.cats.get_availability_period.return_value = dummy_df
    mock_report_service_2.build_task.return_value = {"task_json": "mock"}

    mock_network_client_2 = MagicMock()
    mock_network_client_2.call = AsyncMock(
        side_effect=[{"task_json": "mock"}, MOCK_TASK_KEY]
    )

    report_2 = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test2",
        report_service=mock_report_service_2,
        network_client=mock_network_client_2,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
        send_task_settings=SendTaskSettings(
            retryable=True, retry_on_connection_error=False
        ),
    )

    task_2 = TVTask(
        "test_no_conn_retry", {"options": {"kitId": 1}}, "crosstab", "TV"
    )
    await report_2.send_task(task_2)

    # When retryable=True but retry_on_connection_error=False, retry_on must be
    # None (not RETRIABLE_EXCEPTIONS).
    send_call_2 = mock_network_client_2.call.call_args_list[1]
    assert send_call_2.kwargs["retryable"] is True
    assert send_call_2.kwargs["retry_on"] is None
    assert task_2.key == MOCK_TASK_KEY


# ---------------------------------------------------------------------------
# extract_data branches (reports.py:422-441)
# ---------------------------------------------------------------------------


def test_extract_data_get_result_false_marks_failed(
    hermetic_app_service: AppService,
    dummy_df: pd.DataFrame,
):
    """extract_data: get_result returns False → task.error='Task json is not returned' (reports.py:422-425)."""
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.get_result.return_value = False

    mock_network_client = MagicMock()
    mock_network_client.call = AsyncMock(return_value=False)

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    task = TVTask("test_get_result_false", {"options": {"kitId": 1}}, "crosstab", "TV")
    task.key = {"taskJson": "mock_key"}
    task.status = True
    asyncio.run(report.extract_data(task))

    assert task.status is False
    assert task.log_error is True
    assert task.error == "Task json is not returned"


def test_extract_data_result2table_none_marks_failed(
    hermetic_app_service: AppService,
    dummy_df: pd.DataFrame,
):
    """extract_data: result2table returns None → task.error='Got empty dataframe' (reports.py:430-433)."""
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.get_result.return_value = {"result": "mock"}
    mock_report_service.result2table.return_value = None

    mock_network_client = MagicMock()
    mock_network_client.call = AsyncMock(
        side_effect=[{"result": "mock"}, None]
    )

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    task = TVTask("test_result2table_none", {"options": {"kitId": 1}}, "crosstab", "TV")
    task.key = {"taskJson": "mock_key"}
    task.status = True
    asyncio.run(report.extract_data(task))

    assert task.status is False
    assert task.log_error is True
    assert task.error == "Got empty dataframe"


def test_extract_data_prepare_data_none_marks_failed(
    hermetic_app_service: AppService,
    dummy_df: pd.DataFrame,
):
    """extract_data: prepare_data returns None → log_error=False (reports.py:439-441).

    ``prepare_data`` returns ``None`` when ``df.empty`` is True. We give
    result2table a DataFrame with correct columns but zero rows.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.get_result.return_value = {"result": "mock"}

    result_df = _build_result_df(data_settings, report_settings)
    empty_df = result_df.iloc[0:0]
    mock_report_service.result2table.return_value = empty_df

    mock_network_client = MagicMock()
    mock_network_client.call = AsyncMock(
        side_effect=[{"result": "mock"}, empty_df]
    )

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=mock_network_client,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    task = TVTask("test_prepare_none", {"options": {"kitId": 1}}, "crosstab", "TV")
    task.key = {"taskJson": "mock_key"}
    task.status = True
    asyncio.run(report.extract_data(task))

    assert task.status is False
    assert task.log_error is False


# ---------------------------------------------------------------------------
# frequency_dist_conditions (reports.py:293-294)
# ---------------------------------------------------------------------------


def test_output_columns_includes_frequency_dist_interval(
    hermetic_app_service: AppService,
):
    """set_output_columns appends 'frequencyDistInterval' when frequency_dist_conditions is set (reports.py:293-294)."""
    data_settings = Data.model_validate({
        "slices": ["channel"],
        "statistics": ["avg"],
        "frequency_dist_conditions": {"start": 0, "end": 10, "step": 1},
    })
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    mock_service = MagicMock()
    mock_service.cats = MagicMock()
    mock_service.cats.get_availability_period.return_value = pd.DataFrame(
        {"id": ["1"], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]}
    )
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=Path("/tmp/dummy"),
        report_service=mock_service,
        network_client=MagicMock(),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )
    assert "frequencyDistInterval" in report.output_columns["statistics"]


# ---------------------------------------------------------------------------
# multiple_files=False (reports.py:336-338)
# ---------------------------------------------------------------------------


async def test_get_period_multiple_files_false_returns_single_period(
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """With multiple_files=False, get_period returns [period] not sliced intervals (reports.py:505-507)."""
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        multiple_files=False,
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=NetworkClient(sleep_time=0, max_attempts=1),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    period = await report.get_period()
    assert len(period) == 1  # single interval, not sliced
    # Intervals must be ISO-format *strings*, not date objects — date objects
    # are not JSON-serializable and crash the vendor's build_task -> json.dumps
    # (PIPE date-serialization regression).
    interval = period[0]
    assert all(isinstance(d, str) for d in interval)
    assert interval == ("2025-01-01", "2025-01-07")


async def test_send_task_with_multiple_files_false_passes_string_date_filter(
    mocker,
    dummy_df: pd.DataFrame,
    tv_json5_content: str,
    hermetic_app_service: AppService,
):
    """PIPE date-serialization: multiple_files=False must not pass date objects to build_task.

    Reproduces the original bug: with ``multiple_files=False`` and an explicit
    ``date_filter``, ``get_period`` returned ``date`` objects which the vendor's
    ``build_task`` -> ``json.dumps`` cannot serialize, producing
    ``TypeError: Object of type date is not JSON serializable``.

    This test drives ``create_report`` end-to-end (generate + send_task) with the
    ``DYNAMICS_BY_SPOTS_DICT`` subtype and ``multiple_files=False``, asserting
    that ``build_task`` receives ``date_filter`` containing **str** dates
    suitable for JSON serialization.
    """
    from datetime import date as date_cls

    json5_data = json.loads(tv_json5_content)
    # Override to the failing config from the bug report: DICT subtype,
    # multiple_files=False, explicit date_filter.
    json5_data[0]["report_subtype"] = ReportSubtype.DYNAMICS_BY_SPOTS_DICT.value
    json5_data[0]["multiple_files"] = False
    json5_data[0]["period"] = {"date_filter": ["2025-01-01", "2025-01-07"]}
    report_settings = ReportSettings(**json5_data[0])
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    result_df = _build_result_df(data_settings, report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    network_client = NetworkClient(sleep_time=0, max_attempts=1)
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    export_path = hermetic_app_service.export_folder / "test_dict_nofilesplit"
    export_path.mkdir(parents=True, exist_ok=True)

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

    await report.create_report()

    # build_task must have been called — verify the date_filter argument it
    # received contains str dates, not date objects.
    assert mock_report_service.build_task.called
    call_kwargs = mock_report_service.build_task.call_args.kwargs
    date_filter = call_kwargs.get("date_filter")
    assert date_filter is not None, "Expected date_filter in build_task kwargs"
    # date_filter is a list of [start, end] tuples; every element must be a
    # JSON-serializable string (not a date object).
    for interval in date_filter:
        for d in interval:
            assert not isinstance(d, date_cls), (
                f"date_filter contains non-serializable date object: {d!r} "
                "(must be str)"
            )
            assert isinstance(d, str), f"Expected str date, got {type(d)}: {d!r}"
            json.dumps(d)  # must not raise TypeError


# ---------------------------------------------------------------------------
# last_time path (reports.py:309-317)
# ---------------------------------------------------------------------------


async def test_get_period_last_time_calls_get_last_period(
    mocker,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """get_period calls utils.get_last_period when date_filter is None (reports.py:309-317)."""
    from datetime import date as date_cls

    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"last_time": {"unit_count": 1}, "time_unit": "m"},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=NetworkClient(sleep_time=0, max_attempts=1),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    mock_get_last_period = mocker.patch(
        "mko_get_mediascope_data.core.utils.get_last_period",
        return_value=(date_cls(2025, 1, 1), date_cls(2025, 1, 7)),
    )

    period = await report.get_period()
    mock_get_last_period.assert_called_once()
    assert len(period) >= 1


# ---------------------------------------------------------------------------
# wait_task status-False (reports.py:394-399)
# ---------------------------------------------------------------------------


async def test_wait_task_status_false_marks_max_retries(
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """wait_task: get_status returns False → task.error='Max retries exceeded' (reports.py:394-399)."""
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.get_status.return_value = False

    network_client = NetworkClient(sleep_time=0, max_attempts=1)
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=network_client,
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    task = TVTask("test_status_false", {"options": {"kitId": 1}}, "crosstab", "TV")
    task.key = {"taskJson": "mock_key"}

    await report.wait_task(task, max_polls=5)

    assert task.status is False
    assert task.log_error is True
    assert task.error == "Max retries exceeded"


# ---------------------------------------------------------------------------
# PERF-002: result-size guard in extract_data (reports.py:429-444)
# ---------------------------------------------------------------------------


async def test_large_result_is_processed(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """No row-count cap: ``extract_data`` processes oversized ``resultBody``.

    Historically (PERF-002) ``extract_data`` rejected any ``resultBody`` longer
    than a 10 000-row cap and skipped extraction.  That guard has been removed:
    there is no row-count limit, so an oversized result is processed like any
    other — ``result2table`` is called, a CSV is written, and no error artifact
    is produced.
    """
    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 3. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 4. Doubled MediaVortexTask — oversized resultBody (no cap) -------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    # 10 001 entries — previously exceeded the 10 000-row cap.
    # With the cap removed this is processed like any other result.
    mock_report_service.get_result.return_value = {
        "resultBody": [{"dummy": i} for i in range(10001)]
    }
    mock_report_service.result2table.return_value = result_df

    # ---- 5. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 6. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 7. Export path ------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 8. Construct report -------------------------------------------
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

    # ---- 9. Run real pipeline (bounded by outer wait_for for safety) --
    await asyncio.wait_for(report.create_report(), timeout=10.0)

    # ---- 10. Assertions ------------------------------------------------
    # result2table is now called even for oversized result bodies — there is no
    # row-count cap. A CSV is produced and the queue drains cleanly.
    mock_report_service.result2table.assert_called()

    csv_files = list(export_path.glob("*.csv.gz"))
    assert len(csv_files) > 0, "Expected at least one CSV file for an oversized result"

    # No error artifact — the task succeeded, not rejected.
    error_files = list(export_path.glob("errors/*.json5"))
    assert len(error_files) == 0, (
        f"Expected no error JSON5 for oversized result, found {len(error_files)}"
    )

    # Queue accounting: every enqueued task was task_done()'d — no join hang.
    assert report.task_queue.qsize() == 0


# ---------------------------------------------------------------------------
# PERF-003: per-task latency instrumentation (reports.py:137-159, 358-381, 383-440, 442-510)
# ---------------------------------------------------------------------------


async def test_worker_emits_per_task_duration(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PERF-003: the full pipeline emits structured per-task timing logs.

    Mirrors ``test_create_report_writes_csv_and_marks_queue_done`` (success
    path) and the spy pattern of ``test_worker_does_not_log_ready_on_failed``.
    Asserts that ``reports_logger.info`` receives ``task_complete`` and
    ``task_stage_complete`` records carrying float durations and str task
    names, and that no timing format string contains "is ready" or
    "will be skipped".
    """

    # ---- 1. Spy on reports logger.info (success path emits no warnings) --
    reports_logger = logging.getLogger(_REPORTER_LOGGER)
    mock_info = mocker.spy(reports_logger, "info")

    # ---- 2. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 3. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
    data_settings = app_service.get_data_settings(report_settings)

    # ---- 4. Compute result2table DataFrame columns --------------------
    result_df = _build_result_df(data_settings, report_settings)

    # ---- 5. Doubled MediaVortexTask (success path) --------------------
    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = MOCK_TASK_KEY
    mock_report_service.get_status.return_value = {"taskStatus": "DONE"}
    mock_report_service.get_result.return_value = {"result": "mock_data"}
    mock_report_service.result2table.return_value = result_df

    # ---- 6. Real NetworkClient (sleep_time=0, max_attempts=1) ----------
    network_client = NetworkClient(sleep_time=0, max_attempts=1)

    # ---- 7. Mock asyncio.sleep (no real delays) ------------------------
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # ---- 8. Export path + strategies ----------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    report_factory = ReportFactory(report_settings)

    # ---- 9. Construct report directly ----------------------------------
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

    # ---- 10. Run real pipeline (bounded by outer wait_for for safety) --
    await asyncio.wait_for(report.create_report(), timeout=10.0)

    # ---- 11. Assertions ------------------------------------------------

    # Queue drained — no hang (every task_done was called).
    assert report.task_queue.qsize() == 0

    # Collect every info call captured by the spy.
    info_calls = mock_info.call_args_list

    # At least one "task_complete" record (per-task total from _process_item).
    total_calls = [c for c in info_calls if "task_complete" in str(c.args[0])]
    assert len(total_calls) >= 1, (
        f"Expected at least one 'task_complete' info call, got: {info_calls}"
    )

    # At least one "task_stage_complete" record (stage-level duration).
    stage_calls = [
        c for c in info_calls if "task_stage_complete" in str(c.args[0])
    ]
    assert len(stage_calls) >= 1, (
        f"Expected at least one 'task_stage_complete' info call, "
        f"got: {info_calls}"
    )

    timing_calls = total_calls + stage_calls

    # Negative: no timing format string may contain "is ready" or
    # "will be skipped" — these belong to worker's success / skip branches
    # and must not collide with the structured instrumentation records.
    for call in timing_calls:
        fmt = str(call.args[0])
        assert "is ready" not in fmt, (
            f"Timing format string must not contain 'is ready': {fmt}"
        )
        assert "will be skipped" not in fmt, (
            f"Timing format string must not contain 'will be skipped': {fmt}"
        )

    # Every timing call must carry a float duration and a non-empty str
    # task name among its non-format arguments.
    for call in timing_calls:
        args = call.args[1:]
        assert any(isinstance(a, float) for a in args), (
            f"Timing call {call.args[0]!r} missing a float duration; args={args}"
        )
        assert any(isinstance(a, str) and a for a in args), (
            f"Timing call {call.args[0]!r} missing a non-empty str task name; "
            f"args={args}"
        )

    # Cross-reference: extract the task name from the worker's success-path
    # ``logger.info(f"Task {item.name} is ready.")`` f-string call (args[0] is
    # the fully rendered string) and assert that the same name appears among
    # the str() values of at least one timing call's data arguments.
    ready_calls = [c for c in info_calls if "is ready" in str(c.args[0])]
    assert len(ready_calls) >= 1, (
        f"Expected at least one 'is ready' info call from worker, got: {info_calls}"
    )
    ready_rendered = str(ready_calls[0].args[0])
    ready_name = ready_rendered.removeprefix("Task ").removesuffix(" is ready.")

    name_found = any(
        ready_name in str(arg)
        for call in timing_calls
        for arg in call.args[1:]
    )
    assert name_found, (
        f"Task name '{ready_name}' not found among timing-call arguments; "
        f"timing calls: {timing_calls}"
    )


# ---------------------------------------------------------------------------
# PIPE-010: "will be skipped" warning includes item.error (reports.py:208)
# ---------------------------------------------------------------------------


async def test_worker_will_be_skipped_warning_includes_error_reason(
    tmp_path: Path,
    mocker,
    tv_json5_content: str,
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """PIPE-010: the 'will be skipped' warning includes the task error reason.

    When ``item.log_error`` is ``True``, the worker logs a "will be skipped"
    warning.  The message now includes ``Reason: {item.error}`` so operators
    can see why the task failed without inspecting the error JSON5 artifact.

    ``send_task`` on the doubled ``MediaVortexTask`` returns an empty string
    (falsy) → ``TVMediaReport.send_task`` sets ``task.error =
    "Task.key is not received from API."`` and ``task.log_error = True``.
    The worker must log that error in the "will be skipped" warning.
    """

    # ---- 1. Parse ReportSettings from the fixture JSON5 ----------------
    json5_data = json.loads(tv_json5_content)
    report_settings = ReportSettings(**json5_data[0])

    # ---- 2. Hermetic AppService + data settings -----------------------
    app_service = hermetic_app_service
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

    # ---- 7. Spy on reports logger.warning ------------------------------
    reports_logger = logging.getLogger(_REPORTER_LOGGER)
    mock_warning = mocker.spy(reports_logger, "warning")

    # ---- 8. Export path ------------------------------------------------
    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    # ---- 9. Construct report with check_done=False ---------------------
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

    # ---- 10. Run real pipeline -----------------------------------------
    await asyncio.wait_for(report.create_report(), timeout=10.0)

    # ---- 11. Assertions ------------------------------------------------
    warning_messages = [
        str(call.args[0]) if call.args else ""
        for call in mock_warning.call_args_list
    ]
    skip_warnings = [
        msg for msg in warning_messages if "will be skipped" in msg
    ]
    assert len(skip_warnings) >= 1, (
        f"Expected a 'will be skipped' warning, got: {mock_warning.call_args_list}"
    )
    for msg in skip_warnings:
        assert "Reason:" in msg, (
            f"Expected 'Reason:' in skip warning, got: {msg}"
        )
        assert "Task.key is not received from API." in msg, (
            f"Expected error reason in warning, got: {msg}"
        )


# ---------------------------------------------------------------------------
# ASY-010: worker lifetime cleanup on every exit path from processing_tasks
# ---------------------------------------------------------------------------


def _build_hermetic_report(
    tmp_path: Path,
    hermetic_app_service: AppService,
    *,
    check_done: bool = False,
    max_tasks: int = 3,
    reporter: object | None = None,
) -> TVMediaReport:
    """Construct a hermetic ``TVMediaReport`` for worker-lifecycle tests.

    Uses a doubled ``MediaVortexTask`` whose API methods never actually run
    (the tests drive ``processing_tasks`` directly with a custom
    ``generate_tasks``), plus a real ``NetworkClient``.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.DYNAMICS_BY_SPOTS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()

    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True)

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=NetworkClient(sleep_time=0, max_attempts=1),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=check_done,
        max_tasks=max_tasks,
        reporter=reporter,  # type: ignore[arg-type]
    )
    return report


def _capture_worker_tasks(mocker) -> list[asyncio.Task[None]]:
    """Patch ``asyncio.create_task`` to record every task created.

    ``processing_tasks`` constructs its worker group via
    ``asyncio.create_task``; recording the created tasks lets the tests assert
    that the owned workers are cancelled/gathered (never left pending) on every
    exit path.
    """
    created: list[asyncio.Task[None]] = []
    real_create_task = asyncio.create_task

    def _recording_create_task(coro, **kwargs):
        task = real_create_task(coro, **kwargs)
        created.append(task)
        return task

    mocker.patch("asyncio.create_task", side_effect=_recording_create_task)
    return created


class _RecordingReporter:
    """Minimal recording reporter for report_completed assertions."""

    def __init__(self) -> None:
        self.started: list[tuple[str, int]] = []
        self.completed: list[str] = []

    def report_started(self, report_id: str, total_tasks: int) -> None:
        self.started.append((report_id, total_tasks))

    def task_status_changed(self, task, old_status, new_status) -> None: ...

    def report_completed(self, report_id: str) -> None:
        self.completed.append(report_id)


async def test_processing_tasks_cleans_workers_on_generation_failure(
    tmp_path: Path,
    mocker,
    hermetic_app_service: AppService,
):
    """ASY-010: a generator failure after yielding must not strand workers.

    ``generate_tasks`` yields one task then raises ``RuntimeError``.  The
    single encompassing ``try/finally`` must cancel and gather the owned
    workers, and the ORIGINAL exception must propagate — it must NOT be masked
    by the ASY-008 supervision guard (which sees an exception already in
    flight via ``sys.exc_info()`` and therefore stays silent).
    """
    report = _build_hermetic_report(tmp_path, hermetic_app_service, max_tasks=3)
    worker_tasks = _capture_worker_tasks(mocker)

    async def _failing_generate():
        task = TVTask("gen_fail_task", {}, "DYNAMICS_BY_SPOTS", "crosstab")
        yield task
        raise RuntimeError("generation blew up after the first task")

    mocker.patch.object(report, "generate_tasks", _failing_generate)

    with pytest.raises(RuntimeError, match="generation blew up"):
        await asyncio.wait_for(report.processing_tasks(), timeout=10.0)

    # Worker group was created and owned by processing_tasks.
    assert len(worker_tasks) == 3
    # Every owned worker is done — none left pending in the live loop.
    assert all(t.done() for t in worker_tasks)
    assert not any(t.done() and not t.cancelled() for t in worker_tasks)


async def test_processing_tasks_cleans_workers_on_enqueue_cancel(
    tmp_path: Path,
    mocker,
    hermetic_app_service: AppService,
):
    """ASY-010: cancellation while blocked on ``queue.put`` cleans up workers.

    A single worker is parked on a never-set event so it never drains the
    bounded queue (``maxsize == max_tasks == 1``); the enqueue loop therefore
    blocks on ``queue.put``.  Cancelling ``processing_tasks`` must cancel and
    gather the owned worker before the ``CancelledError`` propagates.
    """
    report = _build_hermetic_report(
        tmp_path, hermetic_app_service, max_tasks=1
    )
    worker_tasks = _capture_worker_tasks(mocker)

    blocker = asyncio.Event()

    async def _blocking_process_item(item):
        await blocker.wait()

    mocker.patch.object(report, "_process_item", _blocking_process_item)

    tasks = [
        TVTask(f"cancel_task_{i}", {}, "DYNAMICS_BY_SPOTS", "crosstab")
        for i in range(3)
    ]

    async def _generate():
        for task in tasks:
            yield task

    mocker.patch.object(report, "generate_tasks", _generate)

    processing = asyncio.create_task(report.processing_tasks())
    # Let the coroutine reach the blocked ``await self.task_queue.put(...)``.
    for _ in range(20):
        await asyncio.sleep(0)
        if report.task_queue.full():
            break
    # Ensure at least one enqueue completed so a put is genuinely blocking.
    assert report.task_queue.qsize() >= 1

    processing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(processing, timeout=10.0)

    # Owned worker(s) were cancelled/gathered — none pending.
    worker_owned = [t for t in worker_tasks if t is not processing]
    assert len(worker_owned) == 1
    assert all(t.done() for t in worker_owned)
    assert all(t.cancelled() for t in worker_owned)


async def test_processing_tasks_zero_tasks_cleans_workers(
    tmp_path: Path,
    mocker,
    hermetic_app_service: AppService,
):
    """ASY-010: the zero-task path raises inside the try and cleans workers.

    ``generate_tasks`` yields nothing → ``NoReportFoundError`` is raised inside
    the encompassing ``try``.  The ``finally`` cancels/gathers all workers and
    must NOT call ``report_completed`` (``report_started`` never fired).
    """
    reporter = _RecordingReporter()
    report = _build_hermetic_report(
        tmp_path, hermetic_app_service, max_tasks=3, reporter=reporter
    )
    worker_tasks = _capture_worker_tasks(mocker)

    async def _empty_generate():
        return
        yield  # pragma: no cover

    mocker.patch.object(report, "generate_tasks", _empty_generate)

    with pytest.raises(NoReportFoundError):
        await asyncio.wait_for(report.processing_tasks(), timeout=10.0)

    assert len(worker_tasks) == 3
    assert all(t.done() for t in worker_tasks)

    # report_started never fired → report_completed must NOT fire.
    assert reporter.started == []
    assert reporter.completed == []


async def test_processing_tasks_all_skipped_cleans_workers(
    tmp_path: Path,
    mocker,
    hermetic_app_service: AppService,
):
    """ASY-010: the all-skipped path returns inside the try and cleans workers.

    All generated tasks already have output files (``check_done=True``), so the
    enqueue loop is skipped entirely.  ``report_completed`` must be called
    (``report_started`` fired with the full total) and no worker may remain
    pending.
    """
    reporter = _RecordingReporter()
    report = _build_hermetic_report(
        tmp_path, hermetic_app_service, max_tasks=3, check_done=True, reporter=reporter
    )
    worker_tasks = _capture_worker_tasks(mocker)

    skipped_names = ["skipped_a", "skipped_b"]
    # Pre-create the output files so every generated task is skipped.
    export_path = tmp_path / "output" / "tv/test"
    for name in skipped_names:
        (export_path / f"{name}.csv.gz").touch()
    report.done_files = report.get_done_files()

    async def _generate():
        for name in skipped_names:
            yield TVTask(name, {}, "DYNAMICS_BY_SPOTS", "crosstab")

    mocker.patch.object(report, "generate_tasks", _generate)

    await asyncio.wait_for(report.processing_tasks(), timeout=10.0)

    assert len(worker_tasks) == 3
    assert all(t.done() for t in worker_tasks)

    # report_started fired with the full total, then report_completed once.
    assert len(reporter.started) == 1
    assert reporter.started[0][1] == 2
    assert len(reporter.completed) == 1



# ---------------------------------------------------------------------------
# ASY-008: error-artifact persistence isolation in worker
# ---------------------------------------------------------------------------


async def test_error_persistence_failure_does_not_strand_queue_join(
    tmp_path: Path,
    mocker,
    hermetic_app_service: AppService,
):
    """ASY-008: a failing ``to_file`` must not kill the worker or strand join.

    One worker (``max_tasks=1``) drains two tasks.  The first task fails, so
    ``worker`` invokes ``_write_error_artifact`` — with ``TVTask.to_file``
    patched to raise ``OSError``.  Before the fix that exception escaped the
    ``except`` block, killed the worker, and left the second queued item with
    no consumer, so ``queue.join()`` hung.  With ``_write_error_artifact`` the
    persistence failure is logged once and swallowed, so the worker proceeds to
    the second task and ``queue.join()`` completes.

    The outer ``asyncio.wait_for(..., timeout=10.0)`` is a hang guard: if the
    worker dies, the test fails within 10 s instead of hanging the suite.
    """
    report = _build_hermetic_report(tmp_path, hermetic_app_service, max_tasks=1)
    worker_tasks = _capture_worker_tasks(mocker)

    # Record the order in which items are processed so we can prove the second
    # task was handled after the first task's persistence failure.
    processed: list[str] = []

    async def _failing_process_item(item):
        processed.append(item.name)
        if item.name == "first":
            item.status = False
            item.log_error = True
            item.error = "synthetic failure"

    mocker.patch.object(report, "_process_item", _failing_process_item)

    tasks = [
        TVTask("first", {}, "DYNAMICS_BY_SPOTS", "crosstab"),
        TVTask("second", {}, "DYNAMICS_BY_SPOTS", "crosstab"),
    ]

    async def _generate():
        for task in tasks:
            yield task

    mocker.patch.object(report, "generate_tasks", _generate)

    # The error-artifact writer fails — the ASY-008 death trigger.
    mock_to_file = mocker.patch.object(
        TVTask, "to_file", side_effect=OSError("disk full")
    )

    # Spy on the module-level logger.error to assert the persistence failure is
    # logged once (dictConfig from the fixture replaces caplog's handler).
    reports_logger = logging.getLogger(_REPORTER_LOGGER)
    mock_error = mocker.spy(reports_logger, "error")

    await asyncio.wait_for(report.processing_tasks(), timeout=10.0)

    # Both tasks were processed despite the first task's to_file failure.
    assert processed == ["first", "second"]

    # queue.join() completed — every enqueued item got task_done().
    assert report.task_queue.qsize() == 0

    # The single owned worker was cancelled/gathered — none left pending.
    assert len(worker_tasks) == 1
    assert all(t.done() for t in worker_tasks)

    # to_file was attempted exactly once (the failing first task), no retry.
    assert mock_to_file.call_count == 1

    # The persistence failure was logged exactly once.
    persistence_logs = [
        str(call.args[0]) if call.args else ""
        for call in mock_error.call_args_list
    ]
    matching = [
        msg
        for msg in persistence_logs
        if "Failed to persist error artifact for task 'first'" in msg
    ]
    assert len(matching) == 1, (
        f"Expected exactly one persistence-failure log, got: {persistence_logs}"
    )


async def test_write_error_artifact_swallows_and_logs(
    tmp_path: Path,
    mocker,
    hermetic_app_service: AppService,
):
    """ASY-008: ``_write_error_artifact`` swallows a ``to_file`` failure and logs it.

    Drives the method directly: ``TVTask.to_file`` raises ``OSError``; the
    method must not propagate the exception and must log the failure, including
    the task name and the original task error.
    """
    report = _build_hermetic_report(tmp_path, hermetic_app_service, max_tasks=1)

    task = TVTask("artifact_task", {}, "DYNAMICS_BY_SPOTS", "crosstab")
    task.error = "original task error"

    mocker.patch.object(TVTask, "to_file", side_effect=OSError("disk full"))

    reports_logger = logging.getLogger(_REPORTER_LOGGER)
    mock_error = mocker.spy(reports_logger, "error")

    # Must not raise.
    await report._write_error_artifact(task)

    error_messages = [
        str(call.args[0]) if call.args else ""
        for call in mock_error.call_args_list
    ]
    assert any(
        "artifact_task" in msg and "original task error" in msg
        for msg in error_messages
    ), f"Expected task name and original error in log, got: {error_messages}"


# ---------------------------------------------------------------------------
# PIPE-014: collision-safe task identity and deterministic disambiguation
# ---------------------------------------------------------------------------


def _build_collision_report(
    tmp_path: Path,
    hermetic_app_service: AppService,
    targets: dict[str, str],
) -> TVMediaReport:
    """Construct a hermetic ``TVMediaReport`` for PIPE-014 collision tests.

    Uses the real ``DefaultTVTaskStrategy`` so the generated task names follow
    production transliteration, and a doubled ``MediaVortexTask`` whose API
    methods are never invoked (the tests drive ``generate_tasks`` directly).
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
        target_audiences=targets,
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    export_path = tmp_path / "output" / report_settings.relative_path
    export_path.mkdir(parents=True, exist_ok=True)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()

    return TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=export_path,
        report_service=mock_report_service,
        network_client=NetworkClient(sleep_time=0, max_attempts=1),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )


async def _collect_generated_tasks(report: TVMediaReport) -> list[TVTask]:
    """Materialise the report's generated tasks exactly like processing_tasks."""
    tasks: list[TVTask] = []
    async for task in report.generate_tasks():
        tasks.append(task)
    return tasks


async def test_distinct_audience_names_cannot_collide_after_transliteration(
    tmp_path: Path,
    hermetic_app_service: AppService,
):
    """PIPE-014: distinct audiences that transliterate identically stay unique.

    ``"Аудитория"`` (leading Cyrillic "А", U+0410) and ``"Aудитория"``
    (leading Latin "A", U+0041) are distinct audiences with distinct filters,
    yet both ``unidecode`` to the same string.  Without disambiguation both
    tasks would share one persisted filename.  After
    ``Report._disambiguate_task_names`` each name is unique and carries the
    deterministic 8-hex SHA-256 suffix; rerunning yields identical names.
    """
    cyrillic_target = "\u0410удитория"
    latin_target = "Aудитория"
    assert cyrillic_target != latin_target
    assert unidecode(cyrillic_target) == unidecode(latin_target)

    targets = {cyrillic_target: "age >= 18", latin_target: "age >= 25"}

    report = _build_collision_report(tmp_path, hermetic_app_service, targets)
    report.period = [("2025-01-01", "2025-01-07")]

    tasks = await _collect_generated_tasks(report)

    # Precondition: the raw transliterated names genuinely collide.
    assert len(tasks) == 2
    assert tasks[0].name == tasks[1].name

    # Disambiguate exactly as processing_tasks does.
    TVMediaReport._disambiguate_task_names(tasks)

    names = [task.name for task in tasks]
    assert len(set(names)) == 2, f"Expected unique names, got: {names}"

    legacy_name = tasks[0].name[:-9]
    for task in tasks:
        assert task.name.startswith(legacy_name + "_")
        assert task._collision_id is not None
        digest = hashlib.sha256(task._collision_id.encode("utf-8")).hexdigest()[:8]
        assert task.name == legacy_name + "_" + digest
        assert len(digest) == 8

    # Stability: regenerating and re-disambiguating yields the same names.
    report_again = _build_collision_report(
        tmp_path, hermetic_app_service, targets
    )
    report_again.period = [("2025-01-01", "2025-01-07")]
    tasks_again = await _collect_generated_tasks(report_again)
    TVMediaReport._disambiguate_task_names(tasks_again)

    assert [task.name for task in tasks_again] == names


async def test_non_colliding_names_preserved_without_hash(
    tmp_path: Path,
    hermetic_app_service: AppService,
):
    """PIPE-014: a single/non-colliding audience keeps its exact legacy name.

    With one audience the transliterated name is unique, so
    ``_disambiguate_task_names`` must leave it verbatim (no hash suffix) so
    resume discovery of legacy output files is unchanged.
    """
    target = "all 18+"
    report = _build_collision_report(
        tmp_path, hermetic_app_service, {target: "age >= 18"}
    )
    report.period = [("2025-01-01", "2025-01-07")]

    tasks = await _collect_generated_tasks(report)
    assert len(tasks) == 1

    legacy_name = "_".join(("2025-01-01", "2025-01-07")) + "_" + unidecode(target)
    assert tasks[0].name == legacy_name

    TVMediaReport._disambiguate_task_names(tasks)

    assert tasks[0].name == legacy_name, (
        "Non-colliding name must stay verbatim for resume back-compat"
    )


def test_disambiguation_fallback_is_unique_without_collision_id():
    """PIPE-014: colliding tasks lacking ``_collision_id`` still disambiguate.

    Both real strategies always set ``_collision_id``, so the fallback path is
    normally unreachable.  This guards it defensively: when a future task
    source omits the identity and two tasks share the same ``name``, the
    index-based fallback must still yield distinct suffixes so collisions are
    always broken (no silent overwrite).
    """
    tasks = [
        TVTask(name="same", settings={}, report_subtype="s", report_type="t"),
        TVTask(name="same", settings={}, report_subtype="s", report_type="t"),
    ]
    assert all(task._collision_id is None for task in tasks)

    TVMediaReport._disambiguate_task_names(tasks)

    first, second = tasks[0].name, tasks[1].name
    assert first != second, f"Fallback failed to disambiguate: {[first, second]}"
    for index, task in enumerate(tasks):
        expected = hashlib.sha256(f"same|{index}".encode()).hexdigest()[:8]
        assert task.name == "same_" + expected
        assert len(expected) == 8


# ---------------------------------------------------------------------------
# 366-day API limit — year splitting in get_period (reports.py:505-527)
# ---------------------------------------------------------------------------


def _make_long_availability_df() -> pd.DataFrame:
    """Availability DataFrame covering 2023-01-01 .. 2026-12-31."""
    return pd.DataFrame(
        {"id": ["7"], "periodFrom": ["2023-01-01"], "periodTo": ["2026-12-31"]}
    )


async def test_get_period_long_period_multiple_files_false_splits_by_year(
    hermetic_app_service: AppService,
):
    """>366-day period with multiple_files=False splits into year chunks.

    Previously get_period returned a single unsliced interval covering the
    entire clamped period — exceeding the Mediascope API's 366-day task
    limit.  Now each calendar-year chunk becomes its own interval, each
    <= 366 days.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        multiple_files=False,
        period={  # type: ignore[arg-type]
            "date_filter": ["2023-01-01", "2026-12-31"],
            "time_unit": "w",
        },
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = (
        _make_long_availability_df()
    )

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=NetworkClient(sleep_time=0, max_attempts=1),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    period = await report.get_period()

    # 4 calendar-year chunks for a 4-year span.
    assert len(period) == 4
    assert period == [
        ("2023-01-01", "2023-12-31"),
        ("2024-01-01", "2024-12-31"),
        ("2025-01-01", "2025-12-31"),
        ("2026-01-01", "2026-12-31"),
    ]

    # Every chunk must be <= MAX_PERIOD_DAYS (366) and contain ISO strings.
    for start_str, end_str in period:
        assert isinstance(start_str, str)
        assert isinstance(end_str, str)
        start_date = utils.str_to_date(start_str)
        end_date = utils.str_to_date(end_str)
        duration = (end_date - start_date).days + 1
        assert duration <= utils.MAX_PERIOD_DAYS, (
            f"Chunk ({start_str}, {end_str}) spans {duration} days, "
            f"exceeding MAX_PERIOD_DAYS ({utils.MAX_PERIOD_DAYS})"
        )


async def test_get_period_long_period_multiple_files_true_slices_by_time_unit(
    hermetic_app_service: AppService,
):
    """>366-day period with multiple_files=True slices directly by time_unit.

    Year pre-splitting is intentionally NOT applied for ``multiple_files=True``
    so that time_unit chunks (e.g. weeks) can naturally cross calendar-year
    boundaries.  Previously the year-first split fragmented a week like
    2024-12-30..2025-01-05 into 2024-12-30..2024-12-31 and 2025-01-01..2025-01-05,
    causing duplicate period coverage.  Now the full period is sliced by time_unit
    in one pass, producing contiguous non-overlapping intervals.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        multiple_files=True,
        period={  # type: ignore[arg-type]
            "date_filter": ["2023-01-01", "2026-12-31"],
            "time_unit": "w",
        },
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = (
        _make_long_availability_df()
    )

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=NetworkClient(sleep_time=0, max_attempts=1),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    period = await report.get_period()

    # Weekly slicing of ~4 years yields far more intervals than 4 year chunks.
    assert len(period) > 4

    # Every interval must be <= MAX_PERIOD_DAYS.
    for start_str, end_str in period:
        assert isinstance(start_str, str)
        assert isinstance(end_str, str)
        start_date = utils.str_to_date(start_str)
        end_date = utils.str_to_date(end_str)
        duration = (end_date - start_date).days + 1
        assert duration <= utils.MAX_PERIOD_DAYS, (
            f"Interval ({start_str}, {end_str}) spans {duration} days, "
            f"exceeding MAX_PERIOD_DAYS ({utils.MAX_PERIOD_DAYS})"
        )

    # No duplicate or overlapping intervals — this is the core regression guard.
    parsed = [
        (utils.str_to_date(s), utils.str_to_date(e)) for s, e in period
    ]
    for i in range(len(parsed)):
        for j in range(i + 1, len(parsed)):
            s1, e1 = parsed[i]
            s2, e2 = parsed[j]
            assert s1 != s2 or e1 != e2, (
                f"Duplicate interval ({s1}, {e1})"
            )
            assert e1 < s2 or e2 < s1, (
                f"Overlapping intervals ({s1}, {e1}) and ({s2}, {e2})"
            )


async def test_get_period_short_period_unchanged_multiple_files_false(
    dummy_df: pd.DataFrame,
    hermetic_app_service: AppService,
):
    """<366-day single-year period with multiple_files=False is unchanged.

    A period that fits within a single calendar year and is < 366 days
    produces a single chunk — identical to the pre-split behavior.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",  # type: ignore[arg-type]
        multiple_files=False,
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df

    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=NetworkClient(sleep_time=0, max_attempts=1),
        task_strategy=DefaultTVTaskStrategy(),
        data_strategy=DefaultDataStrategy(),
        check_done=False,
    )

    period = await report.get_period()

    # Single chunk for a single-year period — behavior unchanged.
    assert len(period) == 1
    assert period[0] == ("2025-01-01", "2025-01-07")
