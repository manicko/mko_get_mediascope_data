from pandas import DataFrame
from unidecode import unidecode
import logging
from os import PathLike
import asyncio
from mediascope_api.mediavortex.tasks import MediaVortexTask
from mko_get_mediascope_data.core.errors import NoReportFoundError
import mko_get_mediascope_data.core.utils as utils
from typing import Type
from mko_get_mediascope_data.core.network_handler import network_handler
from mko_get_mediascope_data.core.tasks import TVTask
from mko_get_mediascope_data.core.models import ReportType, ReportSettings, Data

logger = logging.getLogger(__name__)


class ReportFactory:
    REPORT_TYPES: dict[ReportType, Type] = {}

    @classmethod
    def register_report(cls, report_type: ReportType):
        def inner(report_cls: Type) -> Type:
            cls.REPORT_TYPES[report_type] = report_cls
            return report_cls

        return inner

    @classmethod
    def run(cls, report_type: ReportType):
        try:
            return cls.REPORT_TYPES[report_type]
        except KeyError:
            logger.error(
                "Report type '%s' not found. Available types: %s",
                report_type,
                list(cls.REPORT_TYPES.keys())
            )
            raise NoReportFoundError(f"Report '{report_type}' not registered") from None


class Report:
    def __init__(self,
                 report_settings: ReportSettings,
                 data_settings: Data,
                 export_path: PathLike,
                 check_done: bool = False
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
        ext = utils.get_files_extension(self.report_settings.compression)
        return utils.dir_content_to_dict(utils.get_dir_content(self.export_path, ext=ext), ext)


class MediaReport(Report):
    def __init__(self, *args,
                 report_settings: ReportSettings,
                 data_settings: Data,
                 export_path: PathLike,
                 report_service: MediaVortexTask,
                 max_tasks: int = 100,
                 connection_errors_limit: int = 200,
                 **kwargs
                 ):
        super(MediaReport, self).__init__(*args, report_settings=report_settings, data_settings=data_settings,
                                          export_path=export_path, **kwargs)
        # print('init MediaReport')
        # helps to override Mediascope API connection errors limit in case of bad network
        self.connection_errors_limit = connection_errors_limit

        self.report_service = report_service
        self.catalogs = self.report_service.cats

        self.builder = self.report_service.build_task
        self.sender = self.report_service.send_task

        # limits maximum tasks sent to Mediascope API
        self.max_tasks = max_tasks
        self.task_queue = asyncio.Queue(maxsize=self.max_tasks)

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

    def create_report(self):
        """ sync function to run async part"""
        asyncio.run(self.processing_tasks())


class TVMediaReport(MediaReport):

    def __init__(self, *args, **kwargs):
        super(TVMediaReport, self).__init__(*args, **kwargs)
        # print('init TVMediaReport')
        self.task_intervals = {}  # переменная для хранения интервалов
        self.temp_tasks = []
        self.targets = self.report_settings.target_audiences
        self.base_id = self.data_settings.options.get('kitId', 1)
        self.period = self.get_period()
        self.output_columns = self.set_output_columns()

    def get_available_period(self):
        """
        Checks available dates in the Mediascope database using API
        """
        df: DataFrame = asyncio.run(
            network_handler(
                self.catalogs.get_availability_period,
                max_attempts=self.connection_errors_limit
            )
        )
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

    def get_period(self):
        """ Prepare period to load data.
        Slice intervals based on frequency from settings
        Checks the latest available date in the Mediascope database using API

        :return: Generator with intervals in a format of tuple ('2023-02-01', '2023-03-22')
        """
        # проверяем доступные даты в базе
        available_period = self.get_available_period()

        if (period := self.report_settings.period.date_filter.copy()) is None:
            # если период не задан явно, строим его на основе последней доступной даты
            period = utils.get_last_period(
                today=available_period[1],
                settings=self.report_settings.period.last_time
            )  # ('2023-02-01', '2023-03-22')  #
        # разбиваем период на интервалы и выгружаем в отдельные файлы
        frequency = self.report_settings.period.last_time.frequency
        period = max(available_period[0], period[0]), min(available_period[1], period[1])
        if self.report_settings.multiple_files:
            return utils.slice_period(period, frequency)
        return period

    async def send_task(self, task: TVTask):
        """Sends request based on task settings to Mediascope database using API
        :param task: task object
        :return: Nothing
        """
        # print(f'debug task.settings = {task.settings}')
        task_json = await network_handler(
            self.builder,
            task.report_type,
            sleep_time=2,
            max_attempts=self.connection_errors_limit,
            **task.settings
        )
        if task_json is not False:
            task.key = {}
            task.key = await network_handler(
                self.sender,
                task_json,
                sleep_time=2,
                max_attempts=self.connection_errors_limit
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
            status = await network_handler(
                self.report_service.get_status,
                task.key,
                sleep_time=60,
                max_attempts=self.connection_errors_limit
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
        res_json = await network_handler(
            self.report_service.get_result,
            task.key,
            sleep_time=2,
            max_attempts=self.connection_errors_limit
        )
        if res_json is False:
            task.status = False
            task.log_error = True
            task.error = 'Task json is not returned'
        if task.status:
            df: DataFrame = await network_handler(
                self.report_service.result2table,
                res_json,
                sleep_time=2,
                max_attempts=self.connection_errors_limit,
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

    async def prepare_data(self, df: DataFrame) -> DataFrame | None:
        """
        Prepare DataFrame with data
        :param df: pandas DataFrame        :return:
        """
        try:

            if 'prj_name' in df.columns:
                df.rename(columns={'prj_name': 'targetAudience'}, inplace=True)
            df = df[[col for col_list in self.output_columns.values() for col in col_list]]
            if 'frequencyDistInterval' in self.output_columns['statistics']:
                df = utils.pivot_df_frequency(df, self.output_columns['slices'])
        except KeyError as err:
            logger.error(f"Ошибка '{err}' при выгрузке интервала.")
            return None
        else:
            return df

    async def generate_tasks(self):
        """
        Generator function returning task objects with
        report settings sliced by intervals and demographic profiles
        :return: generator
        """
        settings: dict = self.data_settings.model_dump()
        for interval in self.period:
            settings['date_filter'] = [interval]
            name: str = "_".join(interval)
            for t_name, t_filter in self.targets.items():
                settings['basedemo_filter'] = t_filter
                task = TVTask(name, settings.copy(), self.subtype.value, self.type.value)
                if t_name:
                    task.name += '_' + unidecode(t_name)
                task.interval = interval
                task.target = t_name
                if task.name in self.done_files:
                    logger.warning(f"Файл с расчетом: '{task.name}' уже есть "
                                   f"в папке, расчет будет пропущен.")
                    continue
                yield task

# @ReportFactory.register_report(ReportType.crosstab)
# class TVCrossTab(TVMediaReport):
#     def __init__(self, *args, **kwargs):
#         super(TVCrossTab, self).__init__(*args, **kwargs)
#
#     def get_builder(self):
#         return self.connection.build_crosstab_task
#
#     def get_sender(self):
#         return self.connection.send_crosstab_task
#
#
# @ReportFactory.register_report(ReportType.simple)
# class TVSimple(TVMediaReport):
#     def __init__(self, *args, **kwargs):
#         super(TVSimple, self).__init__(*args, **kwargs)
#
#     def get_builder(self):
#         return self.connection.build_simple_task
#
#     def get_sender(self):
#         return self.connection.send_simple_task
#
#
# @ReportFactory.register_report(ReportType.timeband)
# class TVTimeBand(TVMediaReport):
#     def __init__(self, *args, **kwargs):
#         super(TVTimeBand, self).__init__(*args, **kwargs)
#
#     def get_builder(self):
#         return self.connection.build_timeband_task
#
#     def get_sender(self):
#         return self.connection.send_timeband_task


#
# class NatTVReport(TVMediaReport):
#     def __init__(self, *args, **kwargs):
#         super(NatTVReport, self).__init__(*args, **kwargs)
#         # print('init NatTVReport')

# @ReportFactory.register_report(ReportType.TV_DICT_CROSSTAB)
# class TVDictCrossTab(TVCrossTab):
#     """
#     Methods to load data for dictionary to clean the main report data
#     """
#
#     def __init__(self, *args, **kwargs):
#         super(TVDictCrossTab, self).__init__(*args, **kwargs)
#         self.output_columns = self.data_settings.get('slices', None)
#
#     async def prepare_data(self, df):
#         shift = 2  # номер колонки с рекламодателем advertiser в выгрузке
#         action = 'upd'  # действие по умолчанию
#         d_col_names = [
#             'action',
#             'search_column_idx',
#             'value',
#             'term',
#             'cat',
#             'adv',
#             'bra',
#             'sbr',
#             'mdl',
#             'cln_0',
#             'cln_1',
#             'cln_2',
#             'cln_3',
#             'cln_4',
#             'cln_5'
#         ]
#
#         if not self.output_columns:
#             return None
#
#         missing = set(self.output_columns) - set(df.columns)
#         if missing:
#             logger.error(f"Ошибка: отсутствуют колонки {missing}")
#             return None
#
#         df = df[self.output_columns]
#
#         df_long = (
#             df.melt(var_name="column", value_name="value")
#             .dropna(subset=["value"])
#             .drop_duplicates()
#         )
#
#         col_position_map = {
#             col: idx
#             for idx, col in enumerate(self.output_columns, start=shift)
#         }
#
#         df_long["search_column_idx"] = df_long["column"].map(col_position_map)
#
#         df_long["term"] = '"' + df_long["value"].astype(str) + '"'
#         df_long["action"] = action
#
#         start = d_col_names.index("cat")
#         end = d_col_names.index("mdl") + 1
#         target_cols = d_col_names[start:end]
#
#         for i, col_name in enumerate(target_cols):
#             df_long[col_name] = None
#             mask = df_long["search_column_idx"] == shift + i
#             df_long.loc[mask, col_name] = df_long["value"]
#
#         df_final = df_long.reindex(columns=d_col_names)
#
#         return df_final
#
#
# @ReportFactory.register_report(ReportType.TV_REG_CROSSTAB)
# class TVRegCrossTab(TVCrossTab):
#     def __init__(self, *args, **kwargs):
#         super(TVRegCrossTab, self).__init__(*args, **kwargs)
#         self.available_regions = self.get_available_regions()
#         self.regions_list = self.set_regions_list()
#         # base settings
#         self.data_settings['options']['kitId'] = self.get_base_id()
#         if 'regionName' not in self.data_settings['slices']:
#             self.data_settings['slices'].append('regionName')
#         self.data_settings['add_city_to_basedemo_from_region'] = True
#         self.data_settings['add_city_to_targetdemo_from_region'] = True
#
#     def get_base_id(self):
#         return 3  # base id in mediascope API
#
#     def set_regions_list(self):
#         if 'regions' in self.settings:
#             return self.settings['regions']
#         return self.available_regions['id'].to_list()
#
#     def get_available_regions(self) -> DataFrame:
#         return self.catalogs.get_tv_region()
#
#     async def generate_tasks(self):
#         """
#         Generator function returning task objects with
#         report settings sliced by intervals and demographic profiles
#         :return: generator
#         """
#         settings = self.data_settings.copy()
#         company_filter = ''
#         if isinstance(['company_filter'], str):
#             company_filter = settings['company_filter'] + ' AND '
#         company_filter += 'regionId IN ({reg_id})'
#         regions_names = dict(zip(self.available_regions['id'], self.available_regions['ename']))
#         for reg_id in self.regions_list:
#             if int(reg_id) == 99:  # skip Network broadcasting
#                 continue
#             settings['company_filter'] = company_filter.format(reg_id=reg_id)
#             for interval in self.period:
#                 settings['date_filter'] = [interval]
#                 for t_name, t_filter in self.targets.items():
#                     settings['basedemo_filter'] = t_filter
#                     name = map(unidecode, filter(None, (*interval, t_name, regions_names[int(reg_id)])))
#                     name = '_'.join(name)
#                     task = TVTask(name, settings.copy(), self.subtype, self.type)
#                     task.interval = interval
#                     task.target = t_name
#                     if task.name in self.done_files:
#                         logger.warning(f"Файл с расчетом: '{task.name}' уже есть "
#                                        f"в папке, расчет будет пропущен.")
#                         continue
#                     yield task
#
#
# @ReportFactory.register_report(ReportType.TV_REG_DICT_CROSSTAB)
# class RegTVDictCrossTab(TVDictCrossTab, TVRegCrossTab):
#     def __init__(self, *args, **kwargs):
#         super(RegTVDictCrossTab, self).__init__(*args, **kwargs)
#
#
# class BudgetMediaReport(MediaReport):
#     pass
