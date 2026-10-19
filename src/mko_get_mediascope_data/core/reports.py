from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from datetime import date
from os import PathLike
from typing import Any

from mediascope_api.mediavortex.tasks import MediaVortexTask
from pandas import DataFrame
from unidecode import unidecode

import mko_get_mediascope_data.core.utils as utils
from mko_get_mediascope_data.core.errors import NoReportFoundError
from mko_get_mediascope_data.core.models import (
    Data,
    MediaType,
    ReportSettings,
    ReportSubtype,
    ReportType,
)
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.tasks import TVTask

logger = logging.getLogger(__name__)


class ReportFactory:
    def __init__(self, report_settings: ReportSettings):
        self.report_media: MediaType = report_settings.media
        self.report_subtype: ReportSubtype = report_settings.report_subtype

        self._task_strategy: type[TaskGenerationStrategy] = DefaultTVTaskStrategy
        self._data_strategy: type[DataPreparationStrategy] = DefaultDataStrategy

    @property
    def task_strategy(self):
        if self.report_media == MediaType.TV_REG:
            return TVRegTaskStrategy
        else:
            return self._task_strategy

    @property
    def data_strategy(self):
        if self.report_subtype == ReportSubtype.DYNAMICS_BY_SPOTS_DICT:
            return DictDataStrategy
        else:
            return self._data_strategy


class Report:
    def __init__(
        self,
        report_settings: ReportSettings,
        data_settings: Data,
        export_path: PathLike[str],
        check_done: bool | None = True,
    ):
        # print('init Report')

        self.report_settings: ReportSettings = report_settings.model_copy()

        self.data_settings: Data = data_settings.model_copy()

        self.export_path = export_path

        self.media: MediaType = self.report_settings.media
        self.type: ReportType = self.report_settings.report_type
        self.subtype: ReportSubtype = self.report_settings.report_subtype

        self.done_files: dict[str, Any] = {}
        self.check_done = check_done
        if check_done:
            self.done_files = self.get_done_files()

    def get_done_files(self):
        sufix = utils.get_files_suffix(self.report_settings.compression)
        return utils.dir_content_to_dict(
            utils.list_files_in_directory(self.export_path, extensions=(sufix,)),
            suffix=sufix,
        )


class MediaReport(Report):
    def __init__(
        self,
        report_settings: ReportSettings,
        data_settings: Data,
        export_path: PathLike[str],
        report_service: MediaVortexTask,
        network_client: NetworkClient,
        max_tasks: int = 100,
        check_done: bool | None = True,
    ):
        super().__init__(
            report_settings=report_settings,
            data_settings=data_settings,
            export_path=export_path,
            check_done=check_done,
        )
        # print('init MediaReport')
        # helps to override Mediascope API connection errors limit in case of bad network

        self.report_service = report_service
        self.network_client = network_client

        self.catalogs = self.report_service.cats

        self.builder = self.report_service.build_task
        self.sender = self.report_service.send_task

        # limits maximum tasks sent to Mediascope API
        self.max_tasks = max_tasks
        self.task_queue: asyncio.Queue[TVTask] = asyncio.Queue(maxsize=self.max_tasks)

        self.period: list[tuple[str, str]] | list[tuple[date, date]] | None = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def generate_tasks(self) -> AsyncGenerator[TVTask, None]:
        """Generate tasks. Must be an async generator."""
        return
        yield

    async def send_task(self, task: TVTask) -> None:
        pass

    async def wait_task(self, task: TVTask) -> None:
        pass

    async def extract_data(self, task: TVTask) -> None:
        pass

    async def _process_item(self, item: TVTask) -> None:
        """Execute the per-item pipeline: send, wait, extract.

        Extracted from ``worker`` so the caller can wrap it with
        ``asyncio.wait_for`` and isolate per-task failures.
        """
        start = time.perf_counter()
        try:
            await self.send_task(item)
            if item.status is True:
                await self.wait_task(item)
            if item.status is True:
                await self.extract_data(item)
        finally:
            elapsed = time.perf_counter() - start
            logger.info(
                "task_complete name=%s target=%s interval=%s total=%.4f",
                item.name,
                item.target,
                item.interval,
                elapsed,
            )

    async def worker(self, timeout: float | None = None) -> None:
        """Manage task queue sequence: send, wait, extract, mark done.

        Wraps each item in ``_process_item`` with ``asyncio.wait_for`` when
        *timeout* is set, and isolates per-item failures via try/except/finally
        so ``task_done()`` is always called (prevents ``queue.join()`` hang).
        """
        while True:
            item = await self.task_queue.get()
            logger.debug(f"Queue depth after get: {self.task_queue.qsize()}")
            try:
                coro = self._process_item(item)
                if timeout is not None:
                    await asyncio.wait_for(coro, timeout=timeout)
                else:
                    await coro
                if item.status is False:
                    if item.log_error:
                        logger.warning(f"Calculation: '{item.name}' will be skipped.")
                        await asyncio.to_thread(item.to_file, self.export_path)
                    else:
                        logger.warning(f"No data for task '{item.name}' — skipping.")
                else:
                    logger.info(f"Task {item.name} is ready.")
            except TimeoutError:
                logger.error(
                    f"Task '{item.name}' timed out after {timeout}s — skipping."
                )
                item.status = False
                item.log_error = True
                item.error = f"Timeout after {timeout}s"
                await asyncio.to_thread(item.to_file, self.export_path)
            except Exception as exc:
                logger.error(f"Task '{item.name}' failed: {exc} — skipping.")
                item.status = False
                item.log_error = True
                item.error = str(exc)
                await asyncio.to_thread(item.to_file, self.export_path)
            finally:
                self.task_queue.task_done()

    async def processing_tasks(self, timeout: float | None = None) -> None:
        """creates group of task within a max_tasks limit,
        put tasks from generator function to the queue and wits till all tasks are done
        """
        task_group = [
            asyncio.create_task(self.worker(timeout=timeout))
            for _ in range(self.max_tasks)
        ]
        enqueued = 0
        async for task in self.generate_tasks():
            await self.task_queue.put(task)
            logger.debug(f"Queue depth after put: {self.task_queue.qsize()}")
            enqueued += 1
        if enqueued == 0:
            for t in task_group:
                t.cancel()
            await asyncio.gather(*task_group, return_exceptions=True)
            if self.check_done and self.done_files:
                logger.warning(
                    "All tasks were skipped because their output files "
                    "already exist (check_done=True). Nothing to do."
                )
                return
            raise NoReportFoundError(
                "No tasks were generated for this report. Check that "
                "target_audiences and the period produce at least one "
                "interval — the requested period may be empty or outside "
                "the available data range."
            )
        await self.task_queue.join()
        for t in task_group:
            t.cancel()
        await asyncio.gather(*task_group, return_exceptions=True)

    async def create_report(self, timeout: float | None = None) -> None:
        """sync function to run async part"""
        self.period = await self.get_period()
        await self.processing_tasks(timeout=timeout)

    async def get_period(self) -> list[tuple[str, str]] | list[tuple[date, date]]:
        raise NotImplementedError


class TVMediaReport(MediaReport):
    def __init__(
        self,
        report_settings: ReportSettings,
        data_settings: Data,
        export_path: PathLike[str],
        report_service: MediaVortexTask,
        network_client: NetworkClient,
        task_strategy: TaskGenerationStrategy | None = None,
        data_strategy: DataPreparationStrategy | None = None,
        max_tasks: int = 100,
        check_done: bool | None = True,
        max_result_rows: int = 10000,
    ):
        super().__init__(
            report_settings=report_settings,
            data_settings=data_settings,
            export_path=export_path,
            report_service=report_service,
            network_client=network_client,
            max_tasks=max_tasks,
            check_done=check_done,
        )

        self.max_result_rows = max_result_rows

        self.task_strategy = task_strategy or DefaultTVTaskStrategy()
        self.data_strategy = data_strategy or DefaultDataStrategy()

        # common fields (used by strategies)
        self.targets = self.report_settings.target_audiences
        options = self.data_settings.options
        self.base_id: int | str = (
            options.get("kitId", 1) if isinstance(options, dict) else 1
        )

        self.output_columns = self.set_output_columns()

    async def get_available_period(self) -> list[date]:
        """
        Checks available dates in the Mediascope database using API
        """
        df: DataFrame = await self.network_client.call(
            self.catalogs.get_availability_period
        )

        raw_periods: list[list[str]] = df.loc[df["id"] == str(self.base_id)][
            ["periodFrom", "periodTo"]
        ].values.tolist()
        available_period: list[date] = [utils.str_to_date(d) for d in raw_periods[0]]
        return available_period

    def set_output_columns(self) -> dict[str, list[str]]:
        """Prepare columns to be used in the report
        :return: dict of columns for the report
        """
        output_columns = {
            "slices": list(self.data_settings.slices),
            "statistics": list(self.data_settings.statistics),
        }
        if self.targets:
            output_columns["slices"].insert(0, "targetAudience")

        if getattr(self.data_settings, "frequency_dist_conditions", None):
            output_columns["statistics"].append("frequencyDistInterval")

        return output_columns

    async def get_period(self) -> list[tuple[str, str]] | list[tuple[date, date]]:
        """Prepare period to load data.
        Slice intervals based on frequency from settings
        Checks the latest available date in the Mediascope database using API

        :return: Generator with intervals in a format of tuple ('2023-02-01', '2023-03-22')
        """
        # check available dates in the database
        available_period = await self.get_available_period()
        try:
            period_model = self.report_settings.period
            last_time = period_model.last_time
            if period_model.date_filter is None:
                # if period is not set explicitly, build it from the last available date
                assert last_time is not None
                raw_period: tuple[date, date] | list[date] = utils.get_last_period(
                    today=available_period[1],
                    settings=last_time,
                    time_unit=period_model.time_unit,
                )
            else:
                raw_period = period_model.date_filter
            frequency = period_model.time_unit
            period: tuple[date, date] = (
                max(available_period[0], raw_period[0]),
                min(available_period[1], raw_period[1]),
            )
            if period[0] > period[1]:
                raise NoReportFoundError(
                    f"Requested period {raw_period} has no overlap with the "
                    f"available data range {tuple(available_period)}."
                )
            if period != (raw_period[0], raw_period[1]):
                logger.warning(
                    f"Requested period {tuple(raw_period)} partially overlaps "
                    f"available range {tuple(available_period)}; "
                    f"using clamped intersection {period}."
                )
            if self.report_settings.multiple_files:
                return utils.slice_period(period, frequency)
            return [period]
        except Exception as e:
            logger.exception(e)
            raise

    async def send_task(self, task: TVTask):
        """Sends request based on task settings to Mediascope database using API
        :param task: task object
        :return: Nothing
        """
        stage_start = time.perf_counter()
        # print(f'debug task.settings = {task.settings}')
        task_json = await self.network_client.call(
            self.builder, task.report_type, **task.settings, api_call=False
        )
        if task_json is not False:
            task.key = {}
            task.key = await self.network_client.call(self.sender, task_json, retryable=False)

            logger.info(
                "task_stage_complete stage=send name=%s target=%s duration=%.4f",
                task.name,
                task.target,
                time.perf_counter() - stage_start,
            )
        if not task.key:
            task.status = False
            task.log_error = True
            task.error = "Task.key is not received from API."

    async def wait_task(
        self, task, *, max_polls: int = 60, poll_deadline: float = 3600.0
    ):
        """
        Requesting status of the task using Mediascope API
        :param task: Task object keeping params for the request
        :param max_polls: Maximum number of poll iterations before giving up.
        :param poll_deadline: Maximum wall-clock seconds before giving up.
        :return: Nothing
        """
        loop = asyncio.get_running_loop()
        start = loop.time()
        stage_start = time.perf_counter()
        polls = 0
        sleep_interval = 2.0
        while True:
            polls += 1
            if polls >= max_polls:
                task.status = False
                task.log_error = True
                task.error = f"Poll deadline exceeded ({max_polls} polls)"
                break
            if loop.time() - start > poll_deadline:
                task.status = False
                task.log_error = True
                task.error = f"Poll deadline exceeded ({poll_deadline}s)"
                break

            await asyncio.sleep(sleep_interval)

            status = await self.network_client.call(
                self.report_service.get_status, task.key, sleep_time=2
            )
            if status is False:
                task.status = False
                task.log_error = True
                task.error = "Max retries exceeded"
                logger.warning("Max retries exceeded")
                break
            elif isinstance(status, dict):
                # print(f'\n Task {task.name} status:
                # {status.get('taskStatus', None)}, {status.get('message', None)}')
                terminal = status.get("taskStatus", None)
                if terminal in ("DONE", "FAILED", "CANCELLED"):
                    if terminal in ("FAILED", "CANCELLED"):
                        task.status = False
                        task.log_error = True
                        task.error = f"Task {terminal}"
                    break

            sleep_interval = min(sleep_interval * 2, 60.0)

        logger.info(
            "task_stage_complete stage=wait name=%s target=%s duration=%.4f",
            task.name,
            task.target,
            time.perf_counter() - stage_start,
        )

    async def extract_data(self, task):
        """Exports data to the CSV file
        :param task: Task object with task settings
        :return: Nothing
        """
        stage_start = time.perf_counter()
        # retrieve result
        res_json = await self.network_client.call(
            self.report_service.get_result, task.key
        )
        if res_json is False:
            task.status = False
            task.log_error = True
            task.error = "Task json is not returned"
        if task.status:
            # PERF-002: guard against oversized result bodies before materialization
            result_body = res_json.get("resultBody", [])
            if len(result_body) > self.max_result_rows:
                task.status = False
                task.log_error = True
                task.error = (
                    f"Result size {len(result_body)} exceeds maximum "
                    f"of {self.max_result_rows} rows"
                )
                logger.error(
                    f"Task '{task.name}' result size {len(result_body)} "
                    f"exceeds max_result_rows {self.max_result_rows} — "
                    "skipping extraction."
                )
                logger.info(
                    "task_stage_complete stage=extract name=%s target=%s duration=%.4f",
                    task.name,
                    task.target,
                    time.perf_counter() - stage_start,
                )
                return
            df: DataFrame | None = await self.network_client.call(
                self.report_service.result2table, res_json, project_name=task.target,
                api_call=False,
            )
            if df is None:
                task.status = False
                task.log_error = True
                task.error = "Got empty dataframe"
            elif df.empty:
                task.status = False
                task.log_error = False
            else:
                df = await self.prepare_data(df, task_name=task.name)
                if df is None:
                    task.status = False
                    task.log_error = False
                else:
                    await asyncio.to_thread(
                        utils.csv_to_file,
                        df,
                        file_prefix=task.name,
                        add_time=False,
                        csv_path_out=self.export_path,
                        compression=self.report_settings.compression,
                    )
                    # print(f'\n saving {task.name} to file')

        logger.info(
            "task_stage_complete stage=extract name=%s target=%s duration=%.4f",
            task.name,
            task.target,
            time.perf_counter() - stage_start,
        )

    async def generate_tasks(self) -> AsyncGenerator[TVTask, None]:
        async for task in self.task_strategy.generate(self):
            yield task

    async def prepare_data(
        self, df: DataFrame, task_name: str | None = None
    ) -> DataFrame | None:
        return await self.data_strategy.prepare(self, df, task_name=task_name)


# ====================== STRATEGIES ======================


class TaskGenerationStrategy(ABC):
    @abstractmethod
    async def generate(self, report: TVMediaReport) -> AsyncGenerator[TVTask, None]:
        """Generate tasks. Must be an async generator."""
        raise NotImplementedError
        yield  # pragma: no cover


class DataPreparationStrategy(ABC):
    @abstractmethod
    async def prepare(
        self, report: TVMediaReport, df: DataFrame, task_name: str | None = None
    ) -> DataFrame | None:
        """Prepare DataFrame.

        :param task_name: Name of the task being processed, included in
            error logging for traceability (PIPE-008).
        """


class DefaultTVTaskStrategy(TaskGenerationStrategy):
    async def generate(self, report: TVMediaReport) -> AsyncGenerator[TVTask, None]:
        """
        Generator function returning task objects with
        report settings sliced by intervals and demographic profiles
        :return: generator
        """
        settings: dict[str, Any] = report.data_settings.model_dump()
        if report.period is None or report.targets is None:
            return
        for interval in report.period:
            settings["date_filter"] = [interval]
            name: str = "_".join(str(part) for part in interval)
            for t_name, t_filter in report.targets.items():
                settings["basedemo_filter"] = t_filter
                task = TVTask(
                    name, settings.copy(), report.subtype.value, report.type.value
                )
                if t_name:
                    task.name += "_" + unidecode(t_name)
                task.interval = interval
                task.target = t_name
                if task.name in report.done_files:
                    logger.warning(
                        f"File '{task.name}' already exists in folder, skipping."
                    )
                    continue
                yield task


class TVRegTaskStrategy(TaskGenerationStrategy):
    async def generate(self, report: TVMediaReport) -> AsyncGenerator[TVTask, None]:
        settings: dict[str, Any] = report.data_settings.model_dump()
        available_regions = await report.network_client.call(
            report.catalogs.get_tv_region
        )
        if available_regions is None:
            return
        regions_list = (
            report.report_settings.model_dump().get("regions")
            or available_regions["id"].to_list()
        )
        regions_names = dict(
            zip(
                available_regions["id"],
                available_regions["ename"],
                strict=True,
            )
        )

        company_filter = ""
        if isinstance(settings.get("company_filter"), str):
            company_filter = settings["company_filter"] + " AND "
        company_filter += "regionId IN ({reg_id})"

        if report.period is None or report.targets is None:
            return
        for reg_id in regions_list:
            if int(reg_id) == 99:
                continue
            settings["company_filter"] = company_filter.format(reg_id=reg_id)
            for interval in report.period:
                settings["date_filter"] = [interval]
                for t_name, t_filter in report.targets.items():
                    settings["basedemo_filter"] = t_filter
                    name = "_".join(
                        filter(
                            None,
                            map(
                                unidecode,
                                (
                                    *map(str, interval),
                                    t_name,
                                    regions_names[int(reg_id)],
                                ),
                            ),
                        )
                    )
                    task = TVTask(
                        name,
                        settings.copy(),
                        report.subtype.value,
                        report.type.value,
                    )
                    task.interval = interval
                    task.target = t_name
                    if task.name in report.done_files:
                        logger.warning(
                            f"File '{task.name}' already exists in folder, skipping."
                        )
                        continue
                    yield task


class DefaultDataStrategy(DataPreparationStrategy):
    async def prepare(
        self, report: TVMediaReport, df: DataFrame, task_name: str | None = None
    ) -> DataFrame | None:
        """
        Prepare DataFrame with data
        :param df: pandas DataFrame
        :param report: TVMediaReport
        :param task_name: Name of the task being processed (for error logging).
        :return:
        """
        try:
            if "prj_name" in df.columns:
                df.rename(columns={"prj_name": "targetAudience"}, inplace=True)
            df = df[
                [col for col_list in report.output_columns.values() for col in col_list]
            ]
            if "frequencyDistInterval" in report.output_columns.get("statistics", []):
                df = utils.pivot_df_frequency(df, report.output_columns["slices"])
            return df
        except KeyError as err:
            logger.error(f"Error '{err}' while processing data for task '{task_name}'.")
            return None


class DictDataStrategy(DataPreparationStrategy):
    async def prepare(
        self, report: TVMediaReport, df: DataFrame, task_name: str | None = None
    ) -> DataFrame | None:
        data_settings = report.data_settings

        if isinstance(data_settings, dict):
            output_columns = data_settings.get("slices")
        else:
            output_columns = getattr(data_settings, "slices", None)

        if not output_columns:
            return None

        missing = set(output_columns) - set(df.columns)

        if missing:
            logger.error(f"Missing columns: {missing}")
            return None

        shift = 3

        df_long = (
            df[output_columns]
            .melt(var_name="column", value_name="value")
            .dropna(subset=["value"])
            .drop_duplicates()
            .copy()
        )

        col_position_map = {
            col: idx for idx, col in enumerate(output_columns, start=shift)
        }

        df_long["search_column_idx"] = df_long["column"].map(col_position_map)

        df_long["term"] = (
            df_long["value"]
            .astype(str)
            .str.replace(r"\s+\(.*\)", "", regex=True)
            .str.strip()
        )

        df_long["action"] = "r"
        df_long["match"] = "p"

        clean_columns = []

        for i, col_name in enumerate(output_columns):
            clean_col = f"{col_name}_clean"
            clean_columns.append(clean_col)

            mask = df_long["search_column_idx"] == shift + i

            df_long[clean_col] = df_long["term"].where(mask)

        result_columns = [
            "action",
            "match",
            "search_column_idx",
            "value",
            "term",
            *clean_columns,
        ]

        return df_long.reindex(columns=result_columns)
