from pandas import (concat, DataFrame)
from unidecode import unidecode
from os import PathLike
from pathlib import Path
import asyncio
from requests.exceptions import (ConnectTimeout, HTTPError, ConnectionError, Timeout, RetryError)
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.core.errors import HTTP404Error

import mko_get_mediascope_data.core.utils as utils

from mko_get_mediascope_data.core.tasks import TVTask

ROOT_DIR = Path(__file__).absolute().parent.parent

MEDIASCOPE_CONNECTION_SETTINGS = 'settings/connections/mediascope.json'
PATHS_TO_DEFAULTS = 'settings/defaults/media'

MEDIASCOPE_CONNECTION_SETTINGS = Path.joinpath(ROOT_DIR, MEDIASCOPE_CONNECTION_SETTINGS)
PATHS_TO_DEFAULTS = Path.joinpath(ROOT_DIR, PATHS_TO_DEFAULTS)


class Report:
    def __init__(self, settings, output_path: [str | bytes | PathLike | None] = None,
                 defaults_file: str | bytes | PathLike | None = None):
        # print('init Report')

        self.media = settings.get('media', 'TV').lower()
        self.type = settings['report_type']
        self.subtype = settings['report_subtype']
        self.settings = settings

        # get default settings for the report
        self.data_settings = self.get_data_settings(defaults_file)

        # use report subtype or category_name from settings as a name for the report
        # will be used as extract folder
        self.name = self.subtype
        if 'category_name' in self.settings:
            self.name = unidecode(self.settings['category_name']).lower()

        self.path = self.get_subpath()
        self.path = utils.get_output_path(output_path, self.path)
        # log file will be used to report connection errors
        self.log_file = Path.joinpath(self.path, 'error.log')

    def get_subpath(self):
        """makes relative path string to the report folder using
        name of the report and folder if it's specified in settings"""
        dir_name = []
        if self.name:
            dir_name.append(self.name)
        if 'folder' in self.settings and self.settings['folder']:
            dir_name.append(unidecode(self.settings['folder']).lower())
        return '/'.join(dir_name)

    def get_data_settings(self, defaults_file: str | bytes | PathLike | None = None):
        pass


class MediaReport(Report):
    def __init__(self, *args, settings,
                 connection_settings_file: str | bytes | PathLike = MEDIASCOPE_CONNECTION_SETTINGS,
                 max_tasks=100,
                 connection_errors_limit=200,
                 **kwargs
                 ):
        super(MediaReport, self).__init__(*args, settings=settings, **kwargs)
        # print('init MediaReport')

        # helps to override Mediascope API connection errors limit in case of bad network
        self.connection_errors_limit = connection_errors_limit

        # get connection to Mediascope API
        self.connection_settings_file = connection_settings_file
        self.connection = asyncio.run(self.network_handler(self.connect_to_base))
        self.catalogs = self.connection.cats

        self.builder = self.get_builder()
        self.sender = self.get_sender()

        # limits maximum tasks sent to Mediascope API
        self.max_tasks = max_tasks
        self.task_queue = asyncio.Queue(maxsize=self.max_tasks)

    def connect_to_base(self):
        pass

    def get_builder(self):
        pass

    def get_sender(self):
        pass

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
                print(f"Расчет: '{item.name} будет пропущен.'")
                await asyncio.to_thread(item.to_yaml, self.path)
            else:
                print(f'Задача {item.name} готова.')
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

    async def network_handler(self, func, sleep_time: int = 2, *args, **kwargs):
        """Wrapping function to handle network errors, override error limit of Mediascope API,
        log error to the log file in the report folder.
        :param func: function, name of a function sending request to Mediascope API
        :param sleep_time: int, time to wait until nest request
        :param args: any, params of a passed function
        :param kwargs: any, named params of a passed function
        :return: result of a function request
        """
        count_errors = 0
        while True:
            await asyncio.sleep(sleep_time)
            try:
                result = await asyncio.to_thread(
                    func,
                    *args,
                    **kwargs)
            except HTTP404Error as err:
                await utils.log_to_file(self.log_file, f'\n mtask err : {err} \n')
                count_errors += 1
            # except (ConnectTimeoutError, MaxRetryError, TimeoutError) as err:
            #     print('urllib', err)
            #     count_errors += 1
            except (ConnectTimeout, HTTPError, ConnectionError, Timeout, RetryError) as err:
                await utils.log_to_file(self.log_file, f'\n request err: {err}')
                count_errors += 1
            except Exception as hz:
                await utils.log_to_file(self.log_file, f'\n Unexpected err: "{hz}" of type "{type(hz)}" '
                                                       f'\n in function "{func}" '
                                                       f'\n with args= "{args}" '
                                                       f'and \n kwargs= {kwargs} ')
                count_errors += 1
            else:
                return result
            finally:
                if count_errors >= self.connection_errors_limit:
                    print(
                        f'\n Не удалось получить расчет задачи. '
                        f'Превышен лимит неуспешных соединений '
                        f'{self.connection_errors_limit}')
                    ask = input("Press 'y' to continue, 'n' to break ")
                    if ask == 'n':
                        return False
                    else:
                        count_errors = 0


class TVMediaReport(MediaReport):

    def __init__(self, *args, **kwargs):
        super(TVMediaReport, self).__init__(*args, **kwargs)
        # print('init TVMediaReport')
        self.task_intervals = {}  # переменная для хранения интервалов
        self.temp_tasks = []
        self.targets = self.settings.get('target_audiences', None)
        if self.targets is None:
            self.targets = {'not_set': None}
        self.base_id = self.get_base_id()
        self.period = self.get_period()

    def get_available_period(self):
        """
        Checks available dates in the Mediascope database using API
        """
        df = asyncio.run(self.network_handler(self.catalogs.get_availability_period))
        available_period = df.loc[df['id'] == str(self.base_id)][['periodFrom', 'periodTo']].values.tolist()
        available_period = list(map(utils.str_to_date, available_period[0]))
        return available_period

    def get_defaults_file(self):
        defaults_dir = Path.joinpath(PATHS_TO_DEFAULTS, self.media)
        defaults_files_dic = utils.dir_content_to_dict(utils.get_dir_content(defaults_dir, 'yaml'))
        return defaults_files_dic.get(self.type, None)

    # defaults_file[self.type])

    def get_data_settings(self, defaults_file: str | bytes | PathLike | None = None):
        """ reads default settings yaml file and returns dictionary with
        default settings basing on the self report subtype

        :param defaults_file: str or path, absolute path to yaml file
        :return: dict, dictionary with default settings
        """
        # getting path corresponding to the current report subtype and combine settings
        if defaults_file is None:
            defaults_file = self.get_defaults_file()

        data = utils.yaml_to_dict(defaults_file)
        data_settings = data['DATA_DEFAULTS']
        if self.subtype in data:
            data_settings.update(data[self.subtype])
        for k, v in self.settings.items():
            if k in data_settings:
                data_settings[k] = v
        if self.settings['data_lang'] == 'ru':
            data_settings['slices'] = utils.en_to_ru(data_settings['slices'])
        return data_settings

    def get_base_id(self):
        if 'options' in self.data_settings and 'kitId' in self.data_settings['options']:
            return self.data_settings['options']['kitId']
        return 1

    def get_period(self):
        """ Prepare period to load data.
        Slice intervals based on frequency from settings
        Checks the latest available date in the Mediascope database using API

        :return: Generator with intervals in a format of tuple ('2023-02-01', '2023-03-22')
        """
        # задаем период выгрузки и частоту для разбивки периода на интервалы
        period = self.settings['period']['date_filter']

        frequency = utils.get_frequency(self.settings)
        # проверяем, если фильтр дат не задан явно, задаем его
        base_available_dates = self.get_available_period()
        if period is None:
            period = utils.get_last_period(
                today=base_available_dates[1],
                **self.settings['period']['last_time']
            )  # ('2023-02-01', '2023-03-22')  #
        # разбиваем период на интервалы и выгружаем в отдельные файлы
        period = utils.check_period(period, base_available_dates)
        intervals = utils.slice_period(period, frequency)
        return intervals

    def connect_to_base(self):
        """
        :return: Mediascope API task object providing methods to
        request and receive data from the Mediascope TV database
        """
        return cwt.MediaVortexTask(settings_filename=self.connection_settings_file)

    async def send_task(self, task):
        """Sends request based on task settings to Mediascope database using API
        :param task: task object
        :return: Nothing
        """
        # print(f'debug task.settings = {task.settings}')
        task_json = await self.network_handler(
            self.builder, 2, **task.settings)
        if task_json is not False:
            task.key = {}
            task.key = await self.network_handler(
                self.sender, 2, task_json)
            print('=', end='')
        if not task.key:
            task.status = False
            task.log_error = True

    async def wait_task(self, task):
        """
        Requesting status of the task using Mediascope API
        :param task: Task object keeping params for the request
        :return: Nothing
        """
        while True:
            await asyncio.sleep(2)
            status = await self.network_handler(
                self.connection.get_status,
                20,
                task.key)
            if status is False:
                task.status = False
                task.log_error = True
                print('wait problem')
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
        res_json = await self.network_handler(
            self.connection.get_result,
            2,
            task.key)
        if res_json is False:
            task.status = False
            task.log_error = True
        if task.status is True:
            df = await self.network_handler(
                self.connection.result2table,
                2,
                res_json,
                project_name=task.target)
            if df.empty:
                task.status = False
                task.log_error = False
            elif df is None:
                task.status = False
                task.log_error = True
            else:
                columns = await self.prepare_extract_columns(df)
                df = await self.prepare_data(df, columns)
                if df is None:
                    task.status = False
                    task.log_error = False
                else:
                    await asyncio.to_thread(
                        utils.csv_to_file,
                        df,
                        sub_folder=None,
                        file_prefix=task.name,
                        add_time=False,
                        csv_path_out=self.path,
                        compression=self.settings.get('compression', None)
                    )
                    # print(f'\n сохраняю {task.name} в файл')

    async def prepare_data(self, df, columns):
        """
        Prepare DataFrame with data
        :param df: pandas DataFrame
        :param columns: list, columns to be used in a report
        :return: pandas DataFrame
        """
        try:
            df = df[columns]
        except KeyError as err:
            print(f"Ошибка '{err}' при выгрузке интервала.", end=" ")
            return None
        else:
            return df

    async def prepare_extract_columns(self, df):
        """ Prepare columns to be used in the report
        :param df: pandas DataFrame
        :return: list, list of columns for the report
        """
        columns = self.data_settings['slices'] + self.data_settings['statistics']
        if 'prj_name' in df:
            df.rename(columns={'prj_name': 'targetAudience'}, inplace=True)
            columns = ['targetAudience'] + columns
        return columns

    async def generate_tasks(self):
        """
        Generator function returning task objects with
        report settings sliced by intervals and demographic profiles
        :return: generator
        """
        settings = self.data_settings.copy()
        for interval in self.period:
            settings['date_filter'] = [interval]
            name = "_".join(interval)
            for t_name, t_filter in self.targets.items():
                settings['basedemo_filter'] = t_filter
                task = TVTask(name, settings.copy(), self.subtype)
                if t_filter:
                    task.name += '_' + unidecode(t_name)
                task.interval = interval
                task.target = t_name
                yield task


class TVCrossTab(TVMediaReport):
    def __init__(self, *args, **kwargs):
        super(TVCrossTab, self).__init__(*args, **kwargs)

    def get_builder(self):
        return self.connection.build_crosstab_task

    def get_sender(self):
        return self.connection.send_crosstab_task


class TVSimple(TVMediaReport):
    def __init__(self, *args, **kwargs):
        super(TVSimple, self).__init__(*args, **kwargs)

    def get_builder(self):
        return self.connection.build_simple_task

    def get_sender(self):
        return self.connection.send_simple_task


class TVTimeBand(TVMediaReport):
    def __init__(self, *args, **kwargs):
        super(TVTimeBand, self).__init__(*args, **kwargs)

    def get_builder(self):
        return self.connection.build_timeband_task

    def get_sender(self):
        return self.connection.send_timeband_task


class NatTVReport(TVMediaReport):
    def __init__(self, *args, **kwargs):
        super(NatTVReport, self).__init__(*args, **kwargs)
        # print('init NatTVReport')


class TVGetDictCrossTab(TVCrossTab):
    """
    Methods to load data for dictionary to clean the main report data
    """

    def __init__(self, *args, **kwargs):
        super(TVGetDictCrossTab, self).__init__(*args, **kwargs)
        # print('init TVGetDictCrossTab')

    async def prepare_extract_columns(self, df=None):
        columns = self.data_settings['slices']
        return columns

    async def prepare_data(self, df, columns):
        shift = 2  # номер колонки с рекламодателем advertiser в выгрузке
        action = 'upd'  # действие по умолчанию
        d_col_names = [
            'action',
            'search_column_idx',
            'value',
            'term',
            'cat',
            'adv',
            'bra',
            'sbr',
            'mdl',
            'cln_0',
            'cln_1',
            'cln_2',
            'cln_3',
            'cln_4',
            'cln_5'
        ]
        try:
            df = df[columns]
        except KeyError as err:
            print(f"Ошибка '{err}' при выгрузке интервала.", end=" ")
            return None
        # формируем словарь с уникальными значениями колонок
        data = {}
        for search_id, col in enumerate(df, start=shift):
            data[search_id] = df[col].unique().tolist()

        # переносим словарь в Dataframe и добавляем индекс
        df = DataFrame.from_dict(data, orient='index')
        df = df.T.unstack().dropna().reset_index(level=1, drop=True).reset_index()
        # переименовываем колонки
        df.columns = ['search_column_idx', 'value']
        # добавляем колонку с поисковыми условиями и колонку с action
        # df['term'] = '"col_' + df['search_column_idx'].astype(str) + '":"' + df['value'].astype(str) + '"'
        df['term'] = '"' + df['value'].astype(str) + '"'
        df['action'] = action

        # заполняем колонки с 'cat' или 'adv'по 'mdl'
        start = d_col_names.index('cat')
        end = d_col_names.index('mdl') + 1
        for i, col_name in enumerate(d_col_names[start:end]):
            df[col_name] = None
            df.loc[df['search_column_idx'] == shift + i, col_name] = df['value']

        # Альтернативный вариант, через массивы:
        # col_number = len(df.columns)
        # data = [[search_id, v, f'"col_{search_id}":"{v}"'] for search_id, rows in data.items() for v in rows]
        #
        # # помещаем значение в соответствующую колонку
        # for row in data:
        #     row.extend([None] * col_number)
        #     search_id, value = row[:2]
        #     row[search_id + 1] = value
        #
        # df = DataFrame(
        #     data,
        #     columns=[
        #         'search_column_idx',
        #         'value',
        #         'term',
        #         'adv',
        #         'bra',
        #         'sbr',
        #         'mdl'
        #     ]
        # )
        # # print(df)
        df = await asyncio.to_thread(
            concat,
            [df, DataFrame(columns=[col for col in d_col_names if col not in df.columns])]
        )
        df = df[d_col_names]
        return df


class RegTVCrossTab(TVCrossTab):
    def __init__(self, *args, **kwargs):
        super(RegTVCrossTab, self).__init__(*args, **kwargs)
        self.available_regions = self.get_available_regions()
        self.regions_list = self.set_regions_list()
        # print(f' debug self.regions_list ={self.regions_list }')
        # base settings
        self.data_settings['options']['kitId'] = self.get_base_id()
        if 'regionName' not in self.data_settings['slices']:
            self.data_settings['slices'].append('regionName')
        self.data_settings['add_city_to_basedemo_from_region'] = True
        self.data_settings['add_city_to_targetdemo_from_region'] = True

    def get_base_id(self):
        return 3  # base id in mediascope API

    def set_regions_list(self):
        if 'regions' in self.settings:
            return self.settings['regions']
        return self.available_regions['id'].to_list()

    def get_available_regions(self) -> DataFrame:
        return self.catalogs.get_tv_region()

    async def generate_tasks(self):
        """
        Generator function returning task objects with
        report settings sliced by intervals and demographic profiles
        :return: generator
        """
        settings = self.data_settings.copy()
        company_filter = ''
        if isinstance(['company_filter'], str):
            company_filter = settings['company_filter'] + ' AND '
        company_filter += 'regionId IN ({reg_id})'
        regions_names = dict(zip(self.available_regions['id'], self.available_regions['ename']))
        for reg_id in self.regions_list:
            if int(reg_id) == 99:  # skip Network broadcasting
                continue
            settings['company_filter'] = company_filter.format(reg_id=reg_id)
            for interval in self.period:
                settings['date_filter'] = [interval]
                name = "_".join(interval)
                for t_name, t_filter in self.targets.items():
                    settings['basedemo_filter'] = t_filter
                    task = TVTask(name, settings.copy(), self.subtype)
                    if t_filter:
                        task.name += '_' + unidecode(t_name) + '_' + regions_names[int(reg_id)]
                    task.interval = interval
                    task.target = t_name
                    yield task


class BudgetMediaReport(MediaReport):
    pass
