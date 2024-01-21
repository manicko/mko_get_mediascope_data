import time

from pandas import (concat, DataFrame)
from unidecode import unidecode
from os import PathLike

from requests.exceptions import (ConnectTimeout, HTTPError, ConnectionError, Timeout, RetryError)
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc
from mediascope_api.core.errors import HTTP404Error

from urllib3.exceptions import (
    ConnectTimeoutError,
    MaxRetryError
)

from core.utils import (
    en_to_ru,
    slice_period,
    write_to_file,
    yaml_to_dict,
    get_frequency,
    get_last_period,
    str_to_date
)

MEDIASCOPE_CONNECTION_SETTINGS = 'settings/connections/mediascope.json'
PATHS_TO_DEFAULTS = 'settings/defaults/navigation.yaml'


class Report:
    def __init__(self, settings, defaults_file: str | bytes | PathLike | None = None):
        # print('init Report')
        self.type = settings['report_subtype']
        self.settings = settings
        self.data_settings = self.get_data_settings(defaults_file)
        self.name = self.type
        if 'category_name' in self.settings:
            self.name = unidecode(self.settings['category_name']).lower()

    def get_data_settings(self, defaults_file: str | bytes | PathLike | None = None):
        pass


class MediaReport(Report):
    def __init__(self, *args, settings,
                 connection_settings_file: str | bytes | PathLike = MEDIASCOPE_CONNECTION_SETTINGS, **kwargs):
        super(MediaReport, self).__init__(*args, settings=settings, **kwargs)
        # print('init MediaReport')
        self.connection_settings_file = connection_settings_file
        self.connection = self.connect_to_base()
        self.catalogs = cwc.MediaVortexCats(settings_filename=self.connection_settings_file)
        self.builder = self.get_builder()
        self.sender = self.get_sender()

    def get_builder(self):
        pass

    def connect_to_base(self):
        pass

    def request_data(self):
        pass

    def wait_data(self):
        pass

    def load_data(self):
        self.request_data()
        self.wait_data()

    def extract_data(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class TVMediaReport(MediaReport):
    def __init__(self, *args, **kwargs):
        super(TVMediaReport, self).__init__(*args, **kwargs)
        # print('init TVMediaReport')
        self.targets = self.settings.get('target_audiences', None)
        if self.targets is None:
            self.targets = {'not_set': None}
        self.period = self.get_period()

    def get_period(self):
        pass

    def connect_to_base(self):
        return cwt.MediaVortexTask(settings_filename=self.connection_settings_file)

    def to_csv(self, df, file_prefix):
        dir_name = []
        if self.name:
            dir_name.append(self.name)
        if 'folder' in self.settings and self.settings['folder']:
            dir_name.append(unidecode(self.settings['folder']).lower())
        dir_name = '/'.join(dir_name)
        write_to_file(
            df,
            folder=dir_name,
            file_prefix=file_prefix
        )
        print('.', end='')


class NatTVReport(TVMediaReport):
    def __init__(self, *args, **kwargs):
        super(NatTVReport, self).__init__(*args, **kwargs)
        # print('init NatTVReport')
        self.task_intervals = {}  # переменная для хранения интервалов
        self.temp_tasks = []

    def get_data_settings(self, defaults_file: str | bytes | PathLike | None = None):
        # getting path corresponding to the current report type and combine settings
        if defaults_file is None:
            defaults_file = yaml_to_dict(PATHS_TO_DEFAULTS)
        data = yaml_to_dict(defaults_file[self.type])
        data_settings = data['DATA_DEFAULTS']
        if self.type in data:
            data_settings.update(data[self.type])
        for k, v in self.settings.items():
            if k in data_settings:
                data_settings[k] = v
        if self.settings['data_lang'] == 'ru':
            data_settings['slices'] = en_to_ru(data_settings['slices'])
        return data_settings

    def get_period(self):
        # задаем период выгрузки и частоту для разбивки периода на интервалы
        period = self.settings['period']['date_filter']
        frequency = get_frequency(self.settings)
        # проверяем, если фильтр дат не задан явно, задаем его
        if period is None:
            period = get_last_period(**self.settings['period']['last_time'])  # ('2023-02-01', '2023-03-22')  #
        # разбиваем период на интервалы и выгружаем в отдельные файлы
        period = self.check_period(period)
        intervals = slice_period(period, frequency)
        return intervals

    def check_period(self, period):
        # проверяем доступный период в каталоге
        df = self.catalogs.get_availability_period()
        available_period = df.loc[df['name'] == 'TV Index All Russia'][['periodFrom', 'periodTo']].values.tolist()
        available_period = list(map(str_to_date, available_period[0]))
        test_period = list(map(str_to_date, period))
        output = max(available_period[0], test_period[0]), min(available_period[1], test_period[1])
        return tuple(map(lambda x: f'{x:%Y-%m-%d}', output))

    def request_data(self):
        for interval in self.period:
            self.data_settings['date_filter'] = [interval]
            interval_name = "_".join([self.name] + list(interval))
            self.task_intervals[interval_name] = {}

            for t_name, t_filter in self.targets.items():
                # Отправляем задание
                self.data_settings['basedemo_filter'] = t_filter
                task_json = self.builder(**self.data_settings)
                self.task_intervals[interval_name][t_name] = {'task': self.sender(task_json)}
                self.temp_tasks.append(self.task_intervals[interval_name][t_name])
                time.sleep(2)
                print('.', end='')
        print()

    def wait_data(self):
        while True:
            try:
                # ждем выполнения
                self.connection.wait_task(self.temp_tasks)
            except HTTP404Error as err:
                print('mtask', err)
                ask = input("Press 'y' to continue, 'n' to break")
                if ask == 'n':
                    break
            except (ConnectTimeoutError, MaxRetryError) as err:
                print('urllib', err)
                ask = input("Press 'y' to continue, 'n' to break")
                if ask == 'n':
                    break

            except (ConnectTimeout, HTTPError, ConnectionError, Timeout, RetryError) as err:
                print('request', err)
                ask = input("Press 'y' to continue, 'n' to break")
                if ask == 'n':
                    break

            except Exception as hz:
                print('hz', type(hz), hz)
                ask = input("Press 'y' to continue, 'n' to break")
                if ask == 'n':
                    break
            else:
                break
        del self.temp_tasks
        print('Расчет завершен, получаем результат')

    def extract_data(self):
        # Получаем результат
        for interval_name, interval in self.task_intervals.items():
            results = []
            # print(interval)
            for t_name in self.targets.keys():
                # print(t_name)
                df = self.connection.result2table(self.connection.get_result(interval[t_name]['task']),
                                                  project_name=t_name)
                if not df.empty:
                    results.append(df)
                else:
                    print(f"Интервал: '{interval_name}. будет пропущен.'")
            if results:
                df = concat(results)
                # настраиваем колонки данных на выходе
                columns = self.prepare_extract_columns(df)
                df = NatTVReport.prepare_data(columns, df)
                if df is None:
                    print(f"Интервал: '{interval_name}. будет пропущен.'")
                    continue
                self.to_csv(df, interval_name)

    @staticmethod
    def prepare_data(columns, df):
        try:
            df = df[columns]
        except KeyError as err:
            print(f"Ошибка '{err}' при выгрузке интервала.", end=" ")
            return None
        else:
            return df

    def prepare_extract_columns(self, df):
        columns = self.data_settings['slices'] + self.data_settings['statistics']
        if 'prj_name' in df:
            df.rename(columns={'prj_name': 'targetAudience'}, inplace=True)
            columns = ['targetAudience'] + columns
        return columns


class NatTVCrossTab(NatTVReport):
    def __init__(self, *args, **kwargs):
        super(NatTVCrossTab, self).__init__(*args, **kwargs)

    def get_builder(self):
        return self.connection.build_crosstab_task

    def get_sender(self):
        return self.connection.send_crosstab_task


class NatCrossTabDict(NatTVCrossTab):
    def __init__(self, *args, **kwargs):
        super(NatCrossTabDict, self).__init__(*args, **kwargs)
        # print('init NatCrossTabDict')

    def prepare_extract_columns(self, df=None):
        columns = self.data_settings['slices']
        return columns

    def prepare_data(self, df):
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

        # формируем словарь с уникальными значениями колонок
        data = {}
        for search_id, col in enumerate(df, start=shift):
            data[search_id] = df[col].unique().tolist()

        # переносим словарь в датафрейм и добавляем индекс
        df = DataFrame.from_dict(data, orient='index')
        df = df.T.unstack().dropna().reset_index(level=1, drop=True).reset_index()

        # переименовываем колонки
        df.columns = ['search_column_idx', 'value']
        # добавляем колонку с поисковыми условиями и колонку с action
        df['term'] = '"col_' + df['search_column_idx'].astype(str) + '":"' + df['value'] + '"'
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
        df = concat([df, DataFrame(columns=[col for col in d_col_names if col not in df.columns])])
        df = df[d_col_names]
        return df


class NatTVSimple(NatTVReport):
    def get_builder(self):
        return self.connection.build_simple_task

    def get_sender(self):
        return self.connection.send_simple_task


class NatTVTimeBand(NatTVReport):
    def get_builder(self):
        return self.connection.build_timeband_task

    def get_sender(self):
        return self.connection.send_timeband_task


class RegTVReport(TVMediaReport):
    pass


class BudgetMediaReport(MediaReport):
    pass
