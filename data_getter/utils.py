from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import yaml
import time
from pandas import (concat, DataFrame)


def str_to_date(d: str):
    try:
        d = datetime.strptime(d, '%Y-%m-%d').date()
    except (ValueError, TypeError) as err:
        print(f'wrong format of date {d}')
        print(err)
    else:
        return d


def get_frequency(settings: dict) -> str | None:
    if 'multiple_files' not in settings or settings['multiple_files'] is False:
        return None
    if 'last_time' not in settings['period']:
        return None
    if 'period_type' not in settings['period']['last_time']:
        return None
    return settings['period']['last_time']['period_type']


def slice_period(period: tuple, period_type: str = 'm'):
    if period_type is None:
        yield period
        return

    # добавить проверку на дату старта интервала - начало недели, начало месяца и т.д.
    allowed_periods = {'y': 'years', 'm': 'months', 'w': 'weeks'}
    if period_type not in allowed_periods:
        print(f'wrong period type {period_type} should be either "y" - years, "m" -months, "w" - weeks')
        return
    period_type = allowed_periods[period_type]
    first, last = map(str_to_date, period)
    delta_period = relativedelta(**{period_type: 1})
    delta_day = relativedelta(days=-1)

    start_next = first

    while start_next + delta_period <= last:
        start = start_next
        # adding the required period (week or month, vs 1 day, so the end - is an and of a month or week)
        start_next = start + delta_period
        end = start_next + delta_day
        yield f'{start:%Y-%m-%d}', f'{end:%Y-%m-%d}'

    # if last date is earlier vs last date of new interval, we set the last day as last
    if start_next <= last:
        yield f'{start_next:%Y-%m-%d}', f'{last:%Y-%m-%d}'


def get_last_period(period_type: str = 'w', period_num: int = 2, include_current: bool = False) -> tuple:
    """
    Returns period as a tuple for last 'period_num' months, weeks or years starting from now
    :param period_type: str, 'y' for years, 'm' for months, 'w' for weeks
    :param period_num: int, number of periods ago in terms of 'period_type'
    :param include_current: bool, set to True to set period until today
    :return:
    """
    allowed_types = {'y', 'm', 'w'}

    if period_type not in allowed_types:
        print('Period should be either "y" - years, "m" -months, "w" - weeks')
        return '', ''
    if type(period_num) is not int:
        try:
            period_num = int(period_num)
        except TypeError:
            print('period_num should be num')
            return '', ''

    if period_num <= 0:
        print('period_num should be > 0')
        return '', ''

    first_day_of_period = today = date.today()
    last_day_of_period = today

    if period_type == 'y':
        start_year = today.year - period_num
        first_day_of_period = today.replace(day=1, month=1, year=start_year)
        if include_current is False:
            last_day_of_period = today.replace(day=1, month=1) + relativedelta(days=-1)

    if period_type == 'm':
        first_day_of_period = today.replace(day=1) + relativedelta(months=-period_num)
        if include_current is False:
            last_day_of_period = today.replace(day=1) + relativedelta(days=-1)

    if period_type == 'w':
        weekday = today.weekday()
        first_day_of_period = today + relativedelta(days=-weekday, weeks=-period_num)
        if include_current is False:
            last_day_of_period = first_day_of_period + relativedelta(days=6, weeks=period_num - 1)

    output = (first_day_of_period, last_day_of_period)
    output = tuple(map(lambda x: f'{x:%Y-%m-%d}', output))
    return output


def write_to_file(data_frame, folder: str = None, csv_path_out: str = None, file_prefix: str = None,
                  add_time: bool = True):
    if csv_path_out is None:
        # path to folder containing SQLight databases
        root_dir = Path().absolute()
        # folder containing data for import export CSV
        csv_path = Path.joinpath(root_dir, r'data/')
        # folder to output CSV from database
        csv_path_out = Path.joinpath(csv_path, 'output/')
    if file_prefix is None:
        file_prefix = 'out'
    if folder is not None:
        csv_path_out = Path.joinpath(csv_path_out, folder)
    # creating folder with subfolders
    csv_path_out.mkdir(parents=True, exist_ok=True)
    # try:
    #     csv_path_out.mkdir(parents=True)  # could use flag exist_ok=True to skip check for folder exists
    # except FileExistsError:
    #     print(f'folder: {csv_path_out} already exists')
    # finally:
    time_str = ''
    if add_time is True:
        time_str = '_' + time.strftime("%Y%m%d_%H%M%S")
    out_file = Path(csv_path_out, f'{file_prefix}{time_str}.csv')
    try:
        data_frame.to_csv(path_or_buf=out_file, index=False, mode='x')
    except FileExistsError:
        print(f'File {out_file} already exists. Skip it.')


def en_to_ru(slices_en: list) -> list:
    if type(slices_en) is not list:
        print(f'parameter should be list {type(slices_en)} is given')
        return slices_en
    slices_ru = [[] for _ in range(len(slices_en))]
    for i, param in enumerate(slices_en):
        slices_ru[i] = param.replace('EName', 'Name')
    return slices_ru


def yaml_to_dict(file: str):
    with open(file, "r", encoding="utf8") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
        else:
            return data


def prepare_dict(df):
    shift = 2  # column number in raw data with advertiser
    data = {}
    col_number = len(df.columns)
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
    for search_id, col in enumerate(df, start=shift):
        data[search_id] = df[col].unique().tolist()

    data = [[search_id, v, f'"col_{search_id}":"{v}"'] for search_id, rows in data.items() for v in rows]

    # помещаем значение в соответствующую колонку
    for row in data:
        row.extend([None] * col_number)
        search_id, value = row[:2]
        row[search_id + 1] = value

    df = DataFrame(
        data,
        columns=[
            'search_column_idx',
            'value',
            'term',
            'adv',
            'bra',
            'sbr',
            'mdl'
        ]
    )
    # print(df)
    df = concat([df, DataFrame(columns=[col for col in d_col_names if col not in df.columns])])
    df = df[d_col_names]
    return df
