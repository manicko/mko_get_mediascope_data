from __future__ import annotations
from datetime import date
from abc import ABC, abstractmethod
from pandas import DataFrame
from unidecode import unidecode
import logging
from os import PathLike
import asyncio
from mediascope_api.mediavortex.tasks import MediaVortexTask
import mko_get_mediascope_data.core.utils as utils
from mko_get_mediascope_data.core.network import NetworkClient
from mko_get_mediascope_data.core.tasks import TVTask
from mko_get_mediascope_data.core.models import ReportSettings, Data, MediaType

logger = logging.getLogger(__name__)


class ReportFactory:
    def __init__(self, report_settings: ReportSettings):
        self.report_media: str = report_settings.media.upper()
        self.report_subtype: str = report_settings.report_subtype.upper()

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
        if "DICT" in self.report_subtype:
            return TVRegTaskStrategy
        else:
            return self._data_strategy


class Report:
    def __init__(self,
                 report_settings: ReportSettings,
                 data_settings: Data,
                 export_path: PathLike,
                 check_done: bool = True
                 ):
        # print('init Report')

        self.report_settings = report_settings.model_copy()

        self.data_settings = data_settings.model_copy()

        self.export_path = export_path

        self.media = self.report_settings.media
        self.type = self.report_settings.report_type
        self.subtype = self.report_settings.report_subtype

        self.done_files = {}
        self.check_done = check_done
        if check_done:
            self.done_files = self.get_done_files()

    def get_done_files(self):
        sufix = utils.get_files_suffix(self.report_settings.compression)
        return utils.dir_content_to_dict(utils.list_files_in_directory(self.export_path, extensions=(sufix,)),
                                         suffix=sufix)


class MediaReport(Report):
    def __init__(self, *args,
                 report_settings: ReportSettings,
                 data_settings: Data,
                 export_path: PathLike,
                 report_service: MediaVortexTask,
                 network_client: NetworkClient,
                 max_tasks: int = 100,
                 **kwargs
                 ):
        super(MediaReport, self).__init__(*args, report_settings=report_settings, data_settings=data_settings,
                                          export_path=export_path, **kwargs)
        # print('init MediaReport')
        # helps to override Mediascope API connection errors limit in case of bad network

        self.report_service = report_service
        self.network_client = network_client

        self.catalogs = self.report_service.cats

        self.builder = self.report_service.build_task
        self.sender = self.report_service.send_task

        # limits maximum tasks sent to Mediascope API
        self.max_tasks = max_tasks
        self.task_queue = asyncio.Queue(maxsize=self.max_tasks)

        self.period: list[tuple[str, str]] | list[tuple[date, date]] | None = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def generate_tasks(self):
        yield None

    async def send_task(self, task):
        pass

    async def wait_task(self, task):
        pass

    async def extract_data(self, task):
        pass

    async def worker(self):
        """managing task queue sequence:
        sends task request to the base
        waits for response
        extracts data or task settings in case of error
        marks task as done
        """
        while True:
            item = await self.task_queue.get()
            await self.send_task(item)
            if item.status is True:
                await self.wait_task(item)
            if item.status is True:
                await self.extract_data(item)
            if item.status is False:
                logger.warning(f"Расчет: '{item.name}' будет пропущен.")
                await asyncio.to_thread(item.to_yaml, self.export_path)
            else:
                logger.info(f'Задача {item.name} готова.')
            self.task_queue.task_done()

    async def processing_tasks(self):
        """creates group of task within a max_tasks limit,
        put tasks from generator function to the queue and wits till all tasks are done"""
        task_group = [asyncio.create_task(self.worker()) for _ in range(self.max_tasks)]
        [await self.task_queue.put(t) async for t in self.generate_tasks()]
        await self.task_queue.join()
        [t.cancel() for t in task_group]

    async def create_report(self):
        """ sync function to run async part"""
        self.period = await self.get_period()
        await self.processing_tasks()



    async def get_period(self):
        pass

class TVMediaReport(MediaReport):

    def __init__(
            self,
            *args,
            task_strategy: TaskGenerationStrategy | None = None,
            data_strategy: DataPreparationStrategy | None = None,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.task_strategy = task_strategy or DefaultTVTaskStrategy()
        self.data_strategy = data_strategy or DefaultDataStrategy()

        # общие поля (используются стратегиями)
        self.targets = self.report_settings.target_audiences
        self.base_id = self.data_settings.options.get('kitId', 1)

        self.output_columns = self.set_output_columns()

    async def get_available_period(self):
        """
        Checks available dates in the Mediascope database using API
        """
        df: DataFrame = await self.network_client.call(self.catalogs.get_availability_period)

        available_period = df.loc[df['id'] == str(self.base_id)][['periodFrom', 'periodTo']].values.tolist()
        available_period = list(map(utils.str_to_date, available_period[0]))
        return available_period

    def set_output_columns(self) -> dict[str, list[str]]:
        """Prepare columns to be used in the report
        :return: dict of columns for the report
        """
        output_columns = {
            'slices': list(self.data_settings.slices),
            'statistics': list(self.data_settings.slices)
        }
        if 'target_audiences' in self.report_settings:
            output_columns['slices'].insert(0, 'targetAudience')

        if getattr(self.data_settings, 'frequency_dist_conditions', None):
            output_columns['statistics'].append('frequencyDistInterval')

        return output_columns

    async def get_period(self) -> list[tuple[str, str]] | list[tuple[date, date]]:
        """ Prepare period to load data.
        Slice intervals based on frequency from settings
        Checks the latest available date in the Mediascope database using API

        :return: Generator with intervals in a format of tuple ('2023-02-01', '2023-03-22')
        """
        # проверяем доступные даты в базе
        available_period = await self.get_available_period()
        try:
            if self.report_settings.period.date_filter is None:
                # если период не задан явно, строим его на основе последней доступной даты
                period = utils.get_last_period(
                    today=available_period[1],
                    settings=self.report_settings.period.last_time
                )  # ('2023-02-01', '2023-03-22')  #
            else:
                period = self.report_settings.period.date_filter.copy()
            frequency = self.report_settings.period.last_time.frequency
            period = max(available_period[0], period[0]), min(available_period[1], period[1])
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
        # print(f'debug task.settings = {task.settings}')
        task_json = await self.network_client.call(
            self.builder,
            task.report_type,
            **task.settings
        )
        if task_json is not False:
            task.key = {}
            task.key = await self.network_client.call(
                self.sender,
                task_json
            )

            print('=', end='', flush=True)
        if not task.key:
            task.status = False
            task.log_error = True
            task.error = 'Task.key is not received from API.'

    async def wait_task(self, task):
        """
        Requesting status of the task using Mediascope API
        :param task: Task object keeping params for the request
        :return: Nothing
        """
        while True:
            await asyncio.sleep(2)
            status = await self.network_client.call(
                self.report_service.get_status,
                task.key,
                sleep_time=60
            )
            if status is False:
                task.status = False
                task.log_error = True
                task.error = 'Max retries exceeded'
                logger.warning('Max retries exceeded')
                break
            elif isinstance(status, dict):
                # print(f'\n Статус расчета задачи {task.name}:
                # {status.get('taskStatus', None)}, {status.get('message', None)}')
                if status.get('taskStatus', None) in ('DONE', 'FAILED', 'CANCELLED'):
                    break

    async def extract_data(self, task):
        """ Exports data to the CSV file
        :param task: Task object with task settings
        :return: Nothing
        """
        # Получаем результат
        res_json = await self.network_client.call(
            self.report_service.get_result,
            task.key
        )
        if res_json is False:
            task.status = False
            task.log_error = True
            task.error = 'Task json is not returned'
        if task.status:
            df: DataFrame = await self.network_client.call(
                self.report_service.result2table,
                res_json,
                project_name=task.target
            )
            if df is None:
                task.status = False
                task.log_error = True
                task.error = 'Got empty dataframe'
            elif df.empty:
                task.status = False
                task.log_error = False
            else:
                df = await self.prepare_data(df)
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
                        compression=self.report_settings.compression
                    )
                    # print(f'\n сохраняю {task.name} в файл')

    async def generate_tasks(self):
        async for task in self.task_strategy.generate(self):
            yield task

    async def prepare_data(self, df: DataFrame) -> DataFrame | None:
        return await self.data_strategy.prepare(self, df)


# ====================== STRATEGIES ======================

class TaskGenerationStrategy(ABC):
    @abstractmethod
    async def generate(self, report: "TVMediaReport"):
        """Генерирует задачи. Должен быть async generator."""


class DataPreparationStrategy(ABC):
    @abstractmethod
    async def prepare(self, report: "TVMediaReport", df: DataFrame) -> DataFrame | None:
        """Подготавливает DataFrame."""


class DefaultTVTaskStrategy(TaskGenerationStrategy):
    async def generate(self, report: "TVMediaReport"):
        """
        Generator function returning task objects with
        report settings sliced by intervals and demographic profiles
        :return: generator
        """
        settings: dict = report.data_settings.model_dump()
        for interval in report.period:
            settings['date_filter'] = [interval]
            name: str = "_".join(interval)
            for t_name, t_filter in report.targets.items():
                settings['basedemo_filter'] = t_filter
                task = TVTask(
                    name, settings.copy(), report.subtype.value, report.type.value
                )
                if t_name:
                    task.name += '_' + unidecode(t_name)
                task.interval = interval
                task.target = t_name
                if task.name in report.done_files:
                    logger.warning(f"Файл: '{task.name}' уже есть в папке, пропускаем.")
                    continue
                yield task


class TVRegTaskStrategy(TaskGenerationStrategy):
    async def generate(self, report: "TVMediaReport"):
        settings = report.data_settings.model_dump()
        available_regions = report.catalogs.get_tv_region()
        regions_list = report.report_settings.model_dump().get('regions') or available_regions['id'].to_list()
        regions_names = dict(zip(available_regions['id'], available_regions['ename']))

        company_filter = ''
        if isinstance(settings.get('company_filter'), str):
            company_filter = settings['company_filter'] + ' AND '
        company_filter += 'regionId IN ({reg_id})'

        for reg_id in regions_list:
            if int(reg_id) == 99:
                continue
            settings['company_filter'] = company_filter.format(reg_id=reg_id)
            for interval in report.period:
                settings['date_filter'] = [interval]
                for t_name, t_filter in report.targets.items():
                    settings['basedemo_filter'] = t_filter
                    name = '_'.join(
                        filter(None, map(unidecode, (*interval, t_name, regions_names[int(reg_id)])))
                    )
                    task = TVTask(name, settings.copy(), report.subtype.value, report.type.value)
                    task.interval = interval
                    task.target = t_name
                    if task.name in report.done_files:
                        continue
                    yield task


class DefaultDataStrategy(DataPreparationStrategy):
    async def prepare(self, report: "TVMediaReport", df: DataFrame) -> DataFrame | None:
        """
        Prepare DataFrame with data
        :param df: pandas DataFrame
        :param report: TVMediaReport
        :return:
        """
        try:
            if 'prj_name' in df.columns:
                df.rename(columns={'prj_name': 'targetAudience'}, inplace=True)
            df = df[[col for col_list in report.output_columns.values() for col in col_list]]
            if 'frequencyDistInterval' in report.output_columns.get('statistics', []):
                df = utils.pivot_df_frequency(df, report.output_columns['slices'])
            return df
        except KeyError as err:
            logger.error(f"Ошибка '{err}' при обработке данных.")
            return None


class DictDataStrategy(DataPreparationStrategy):
    async def prepare(self, report: "TVMediaReport", df: DataFrame) -> DataFrame | None:
        output_columns = getattr(report.data_settings, 'slices', None) or report.data_settings.get('slices', None)
        if not output_columns:
            return None
        missing = set(output_columns) - set(df.columns)
        if missing:
            logger.error(f"Отсутствуют колонки: {missing}")
            return None

        df = df[output_columns]
        df_long = (
            df.melt(var_name="column", value_name="value")
            .dropna(subset=["value"])
            .drop_duplicates()
        )

        shift = 2
        action = 'upd'
        d_col_names = [
            'action', 'search_column_idx', 'value', 'term', 'cat', 'adv', 'bra',
            'sbr', 'mdl', 'cln_0', 'cln_1', 'cln_2', 'cln_3', 'cln_4', 'cln_5'
        ]

        col_position_map = {col: idx for idx, col in enumerate(output_columns, start=shift)}
        df_long["search_column_idx"] = df_long["column"].map(col_position_map)
        df_long["term"] = '"' + df_long["value"].astype(str) + '"'
        df_long["action"] = action

        start = d_col_names.index("cat")
        end = d_col_names.index("mdl") + 1
        for i, col_name in enumerate(d_col_names[start:end]):
            df_long[col_name] = None
            mask = df_long["search_column_idx"] == shift + i
            df_long.loc[mask, col_name] = df_long["value"]

        return df_long.reindex(columns=d_col_names)
