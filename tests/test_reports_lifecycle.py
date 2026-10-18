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
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import json5
from unidecode import unidecode

from tests.conftest import expected_output_columns
from mko_get_mediascope_data.core.errors import NoReportFoundError
from mko_get_mediascope_data.core.models import (
    Data,
    MediaType,
    ReportSettings,
    ReportSubtype,
    ReportType,
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
    mock_report_service.send_task.return_value = "mock_task_key_123"
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

    The guard is ``if polls >= max_polls`` and is checked at the *top* of each
    iteration, before the status poll.  Therefore ``max_polls=2`` is the smallest
    value that lets exactly one real poll run (so ``get_status`` is called once,
    receiving the non-terminal status) and then trips the counter on the next
    iteration — terminating via the inner guard rather than any outer timeout.
    (``max_polls=1`` would trip the guard before any poll, calling ``get_status``
    zero times, which would not exercise the poll path.)
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
    mock_report_service.send_task.return_value = "mock_task_key_123"
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
    mock_report_service.send_task.return_value = "mock_task_key_123"
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
    mock_report_service.send_task.return_value = "mock_task_key_123"
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
    mock_report_service.send_task.return_value = "mock_task_key_123"
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
        relative_path="tv_reg/test",
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
        relative_path="tv/test",
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
        relative_path="tv/test",
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
        relative_path="tv/test",
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    mock_service = MagicMock()
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=Path("/tmp/dummy_export"),
        report_service=mock_service,
        network_client=MagicMock(),
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DictDataStrategy,
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
        relative_path="tv/test",
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    mock_service = MagicMock()
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=Path("/tmp/dummy_export"),
        report_service=mock_service,
        network_client=MagicMock(),
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DictDataStrategy,
        check_done=False,
    )
    df = pd.DataFrame({"col_a": [1]})
    strategy = DictDataStrategy()
    result = asyncio.run(strategy.prepare(report, df))
    assert result is None


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
        relative_path="tv/test",
        period={"date_filter": ["2025-01-01", "2025-01-07"]},  # type: ignore[arg-type]
    )
    data_settings = hermetic_app_service.get_data_settings(report_settings)

    mock_report_service = MagicMock()
    mock_report_service.cats = MagicMock()
    mock_report_service.cats.get_availability_period.return_value = dummy_df
    mock_report_service.build_task.return_value = {"task_json": "mock"}
    mock_report_service.send_task.return_value = "mock_task_key_123"
    mock_report_service.get_status.return_value = {"taskStatus": "IN_PROGRESS"}

    network_client = NetworkClient(sleep_time=0, max_attempts=1)
    report = TVMediaReport(
        report_settings=report_settings,
        data_settings=data_settings,
        export_path=hermetic_app_service.export_folder / "test",
        report_service=mock_report_service,
        network_client=network_client,
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
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


def test_send_task_false_key_marks_task_failed(
    hermetic_app_service: AppService,
    dummy_df: pd.DataFrame,
):
    """send_task: builder returns False → task.key is empty → status=False, log_error=True.

    reports.py:349-360 — when ``task_json is False`` (builder returns False),
    ``send_task`` skips key assignment; ``not task.key`` guard sets
    ``task.error = "Task.key is not received from API."``.
    """
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
        check_done=False,
    )

    task = TVTask("test_send_false", {}, "crosstab", "TV")
    asyncio.run(report.send_task(task))

    assert task.status is False
    assert task.log_error is True
    assert task.error == "Task.key is not received from API."


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
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
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
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
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
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
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
    data_settings = Data(
        slices=["channel"],
        statistics=["avg"],
        frequency_dist_conditions={"start": 0, "end": 10, "step": 1},
    )
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
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
    """With multiple_files=False, get_period returns [period] not sliced intervals (reports.py:336-338)."""
    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        report_subtype=ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
        check_done=False,
    )

    period = await report.get_period()
    assert len(period) == 1  # single interval, not sliced


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
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
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
        relative_path="tv/test",
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
        task_strategy=DefaultTVTaskStrategy,
        data_strategy=DefaultDataStrategy,
        check_done=False,
    )

    task = TVTask("test_status_false", {"options": {"kitId": 1}}, "crosstab", "TV")
    task.key = {"taskJson": "mock_key"}

    await report.wait_task(task, max_polls=5)

    assert task.status is False
    assert task.log_error is True
    assert task.error == "Max retries exceeded"
