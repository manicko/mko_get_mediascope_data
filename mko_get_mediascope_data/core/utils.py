from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import time
import aiofiles as aiof
from pandas import DataFrame
import logging
from os import PathLike
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

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
        return [period]
    # добавить проверку на дату старта интервала - начало недели, начало месяца и т.д.
    allowed_periods = {'y': 'years', 'm': 'months', 'w': 'weeks'}
    if period_type not in allowed_periods:
        print(f'wrong period type {period_type} should be either "y" - years, "m" -months, "w" - weeks')
        return None
    period_type = allowed_periods[period_type]
    first, last = map(str_to_date, period)
    delta_period = relativedelta(**{period_type: 1})
    delta_day = relativedelta(days=-1)

    start_next = first
    date_list = []
    while start_next + delta_period <= last:
        start = start_next
        # adding the required period (week or month, vs 1 day, so the end - is an and of a month or week)
        start_next = start + delta_period
        end = start_next + delta_day
        date_list.append((f'{start:%Y-%m-%d}', f'{end:%Y-%m-%d}'))

    # if last date is earlier vs last date of new interval, we set the last day as last
    if start_next <= last:
        date_list.append((f'{start_next:%Y-%m-%d}', f'{last:%Y-%m-%d}'))
    return date_list


def check_period(period: tuple[str], available_period: list | None):
    """
    Adjust the report period with available dates accordingly
    :param available_period: list,
    :param period: tuple of dates in format of strings, ('2023-02-01', '2023-03-22')

    :return: tuple, example: ('2023-02-01', '2023-03-22')
    """
    # проверяем доступный период в каталоге
    test_period = list(map(str_to_date, period))
    output = max(available_period[0], test_period[0]), min(available_period[1], test_period[1])
    return tuple(map(lambda x: f'{x:%Y-%m-%d}', output))


def get_last_period(period_type: str = 'w',
                    period_num: int = 2,
                    include_current: bool = False,
                    today: datetime = None) -> tuple:
    """
    Returns period as a tuple for last 'period_num' months, weeks or years starting from now
    :param period_type: str, 'y' for years, 'm' for months, 'w' for weeks
    :param period_num: int, number of periods ago in terms of 'period_type'
    :param include_current: bool, set to True to set period until today
    :param today: date from which to get calculation
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

    if today is None:
        today = date.today()

    first_day_of_period = None
    last_day_of_period = today

    if period_type == 'y':
        start_year = today.year - period_num
        first_day_of_period = today.replace(day=1, month=1, year=start_year)
        if not include_current:
            last_day_of_period = today.replace(day=1, month=1) + relativedelta(days=-1)

    if period_type == 'm':
        first_day_of_period = today.replace(day=1) + relativedelta(months=-period_num)
        if not include_current:
            last_day_of_period = today.replace(day=1) + relativedelta(days=-1)

    if period_type == 'w':
        weekday = today.weekday()
        first_day_of_period = today + relativedelta(days=-weekday, weeks=-period_num)
        if not include_current:
            last_day_of_period = first_day_of_period + relativedelta(days=6, weeks=period_num - 1)

    output = (first_day_of_period, last_day_of_period)
    output = tuple(map(lambda x: f'{x:%Y-%m-%d}', output))

    return output


def get_output_path(root_dir: str | PathLike = None, path: str | PathLike = None):
    root_dir = Path(root_dir)
    if root_dir is None or not root_dir.exists():
        # absolute path to sub_folder
        root_dir = Path(__file__).absolute().parent.parent
        # sub_folder containing data for import export CSV
        root_dir = Path.joinpath(root_dir, 'data', 'output')
    path = Path.joinpath(root_dir, path)
    path.mkdir(parents=True, exist_ok=True)
    return path


async def log_to_file(log_file, data):
    time_stamp = datetime.now().replace(microsecond=0)
    async with aiof.open(log_file, 'a', encoding="utf-8") as outfile:
        await outfile.write(f"{time_stamp}  {data}\r\n")
        await outfile.flush()


def get_files_extension(**kwargs):
    ext = '.csv'
    if 'compression' in kwargs and isinstance(kwargs['compression'], dict) and 'method' in kwargs['compression']:
        ext += '.' + kwargs['compression']['method']
        ext = ext.replace('.gzip', '.gz')
    return ext


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
    time_str = ''
    if add_time:
        time_str = '_' + time.strftime("%Y%m%d_%H%M%S")
    ext = get_files_extension(**kwargs)
    out_file = Path(csv_path_out, f'{file_prefix}{time_str}{ext}')

    try:
        encoding = kwargs.pop('encoding', 'utf-8-sig')
        data_frame.to_csv(
            path_or_buf=out_file,
            index=False, mode='x',
            decimal=',',
            sep=';',
            encoding=encoding,
            *args,
            **kwargs)
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
        try:
            with open(file, "r", encoding="utf8") as cfg:
                data = yaml.safe_load(cfg)
        except yaml.YAMLError as err:
            logger.error(err)
        except FileNotFoundError as err:
            logger.error(f"No such file or directory{file} {err}")
        else:
            return data


def get_dir_content(path: str | PathLike, ext: str = 'yaml', subfolders=True):
    try:
        subfolders = '**/' if subfolders else ''
        files = Path(path).glob(f'{subfolders}*.{ext.strip(".")}')
    except Exception as err:
        raise err
    else:
        return files


def dir_content_to_dict(files, ext: str = 'yaml'):
    return {file.name.removesuffix(ext).rstrip('.'): file for file in files}


def pivot_df_frequency(df: DataFrame, dim_cols) -> DataFrame:
    if 'frequencyDistInterval' not in df.columns:
        return df

    try:
        #  Берем колонки, где одно значение в строке
        base_df = (
            df[df['frequencyDistInterval'] == '-']
            .drop(columns='frequencyDistInterval')
            .dropna(axis=1, how='all')
            .groupby(dim_cols, as_index=False)
            .first()
        )
        # Частоты - тут набор значений в строки их приводим в pivot
        freq_df = (
            df[df['frequencyDistInterval'] != '-']
            .pivot_table(
                index=dim_cols,
                columns='frequencyDistInterval',
                values='FrequencyDistPer',
                aggfunc='first'
            )
            .reset_index()
        )
        # сортировка частотных интервалов
        freq_df = freq_df.reindex(
            sorted(
                freq_df.columns,
                key=lambda x: int(x[1:-2]) if x.startswith('[') else -1
            ),
            axis=1
        )

        # Склейка двух массивов
        result = base_df.merge(freq_df, on=dim_cols, how='left')

        return result
    except Exception as err:
        print(f"Ошибка '{err}' при обработке частот.", end=" ")
        return df



def list_files_in_directory(
    path: str | PathLike[str],
    extensions: tuple[str, ...] = ("yaml", "json"),
    include_subfolders: bool = False,
) -> list[Path]:
    """
    Lists files in a directory with specific extensions.

    Args:
        path (Union[str, PathLike]): The directory path.
        extensions (Tuple[str, ...]): Allowed file extensions (default: ('csv', 'txt')).
        include_subfolders (bool): Whether to include subfolders (default: False).

    Returns:
        List[Path]: A list of file paths matching the given extensions.
    """
    try:
        files: list[Path] = []
        subfolder_pattern = "**/" if include_subfolders else ""
        for ext in extensions:
            files.extend(Path(path).glob(f'{subfolder_pattern}*.{ext.strip(".")}'))
        return files
    except Exception as err:
        logger.error(f"Error reading directory {path}: {err}")
        return []


def get_path(path: str | Path, base_dir: Path | None = None) -> Path:
    path = Path(path).expanduser()

    if not path.is_absolute():
        base_dir = base_dir or Path.cwd()
        path = (base_dir / path).resolve()

    return path

def ensure_path_exists(path: Path) -> None:
    """
    Ensures that a given path exists, creating directories if necessary.

    Args:
        path (Path): Path to a file or directory.

    Raises:
        ValueError: If the path cannot be created.
    """
    try:
        if path.exists():
            return  # Path already exists, no action needed
        if path.suffix:  # If it's a file, create its parent directory
            path.parent.mkdir(parents=True, exist_ok=True)
        else:  # If it's a directory, create it
            path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise ValueError(f"Failed to create path {path}: {e}") from e


def resolve_path(path: str | Path, base_dir: Path | None = None) -> Path:
    """
    Resolves an absolute path, creating it if necessary.

    If a relative path is given, it is resolved against `base_dir`.

    Args:
        path (Union[str, Path]): The path to resolve (can be absolute or relative).
        base_dir (Union[Path, None], optional): The base directory for resolving relative paths.
            Defaults to the parent directory of this script.

    Returns:
        Path: The resolved absolute path.

    Raises:
        ValueError: If the path cannot be found or created.
    """
    path = Path(path).expanduser()  # Expands `~` (home directory)

    # If the path is absolute and exists, return it immediately
    if path.is_absolute():
        if path.exists():
            return path
        ensure_path_exists(path)  # If not found, attempt to create it
        return path

    # Ensure base_dir is a valid Path
    base_dir = base_dir or Path(__file__).resolve().parent.parent
    resolved_path = (base_dir / path).resolve()

    if not resolved_path.exists():
        ensure_path_exists(resolved_path)  # Create the path if it does not exist

    return resolved_path


def load_config(path: Path) -> dict[str, Any]:
    """
    Loads configuration from a YAML file.

    Args:
        path (Path): Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: Parsed configuration dictionary, or an empty dict if the file does not exist or is invalid.
    """
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def merge_dicts(dict1: dict[Any, Any], dict2: dict[Any, Any]) -> dict[Any, Any]:
    """
    Recursively merges two dictionaries.

    If both dictionaries have the same key and the value is also a dictionary, it merges them recursively.
    Otherwise, `dict2`'s value overwrites `dict1`'s value.

    Args:
        dict1 (Dict[Any, Any]): The first dictionary.
        dict2 (Dict[Any, Any]): The second dictionary.

    Returns:
        Dict[Any, Any]: The merged dictionary.
    """
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        return dict2
    for k in dict2:
        if k in dict1:
            dict1[k] = merge_dicts(dict1[k], dict2[k])
        else:
            dict1[k] = dict2[k]
    return dict1