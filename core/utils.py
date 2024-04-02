from os import PathLike
from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import yaml
import time
import aiofiles as aiof


def str_to_date(date_string: str):
    """
    converts string to date format
    :param date_string: str in format  %Y-%m-%d
    :return: date
    """
    try:
        date_string = datetime.strptime(date_string, '%Y-%m-%d').date()
    except (ValueError, TypeError) as err:
        print(f'wrong format of date_string {date_string}')
        print(err)
    else:
        return date_string


def get_frequency(settings: dict) -> str | None:
    """
    Read settings to get frequency for slicing date intervals 'y': 'years', 'm': 'months', 'w': 'weeks'
    :param settings: dict, settings of the report
    :return: str, string 'y', 'm' or 'w'
    """
    if 'multiple_files' not in settings or settings['multiple_files'] is False:
        return None
    if 'last_time' not in settings['period']:
        return None
    if 'period_type' not in settings['period']['last_time']:
        return None
    return settings['period']['last_time']['period_type']


def slice_period(period: tuple, period_type: str = 'm'):
    if period_type is None:
        if isinstance(period, list):
            period = tuple(period)
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

    first_day_of_period = None
    last_day_of_period = today = date.today()

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


def get_output_path():
    # absolute path to sub_folder
    root_dir = Path(__file__).absolute().parent.parent
    # sub_folder containing data for import export CSV
    path = Path.joinpath(root_dir, 'data', 'output')
    return path


def get_log_file(sub_folder):
    log_file = get_output_path()
    log_file = Path.joinpath(log_file, sub_folder)
    log_file.mkdir(parents=True, exist_ok=True)
    log_file = Path.joinpath(log_file, 'error.log')
    return log_file


async def log_to_file(log_file, data):
    async with aiof.open(log_file, 'a', encoding="utf-8") as outfile:
        await outfile.write(f"{data}\r\n")
        await outfile.flush()


def csv_to_file(data_frame, sub_folder: str = None, csv_path_out: str = None, file_prefix: str = None,
                add_time: bool = True, *args, **kwargs):
    if csv_path_out is None:
        csv_path_out = get_output_path()
    if file_prefix is None:
        file_prefix = 'out'
    if sub_folder is not None:
        csv_path_out = Path.joinpath(csv_path_out, sub_folder)
    # creating sub_folder with subfolder
    csv_path_out.mkdir(parents=True, exist_ok=True)
    # try:
    #     csv_path_out.mkdir(parents=True)  # could use flag exist_ok=True to skip check for sub_folder exists
    # except FileExistsError:
    #     print(f"sub_folder: {csv_path_out} already exists")
    # finally:
    time_str = ''
    if add_time is True:
        time_str = '_' + time.strftime("%Y%m%d_%H%M%S")
    out_file = Path(csv_path_out, f'{file_prefix}{time_str}.csv')
    if 'compression' in kwargs and isinstance(kwargs['compression'], dict) and 'method' in kwargs['compression']:
        suffix = '.' + kwargs['compression']['method']
        suffix = suffix.replace('.gzip', '.gz')
        out_file = Path(out_file).with_suffix(suffix)
    try:
        data_frame.to_csv(path_or_buf=out_file, index=False, mode='x', decimal=',', sep=';', *args, **kwargs)
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


def yaml_to_dict(file: str | PathLike):
    with open(file, "r", encoding="utf8") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
        else:
            return data
