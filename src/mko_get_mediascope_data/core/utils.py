import logging
import time
from collections.abc import Iterator
from datetime import date, datetime
from os import PathLike
from pathlib import Path
from typing import Any, Literal, overload

import yaml
from dateutil.relativedelta import relativedelta
from pandas import DataFrame

from mko_get_mediascope_data.core.models import LastTimeModel

logger = logging.getLogger(__name__)


def str_to_date(date_string: str) -> date:
    """
    converts string to date format
    :param date_string: str in format  %Y-%m-%d
    :return: date
    """
    try:
        result: date = datetime.strptime(date_string, "%Y-%m-%d").date()
    except (ValueError, TypeError) as err:
        logger.error(f"wrong format of date_string {date_string}: {err}")
        raise err
    else:
        return result


def iter_period_slices(
    overall_start: date, overall_end: date, frequency: Literal["d", "w", "m", "y"]
) -> Iterator[tuple[date, date]]:
    """
    Generate sequential intervals within [overall_start, overall_end]
    with a step corresponding to frequency.
    """
    if overall_start > overall_end:
        return

    current = overall_start
    delta: relativedelta = relativedelta()
    match frequency:
        case "d":
            delta = relativedelta(days=1)
        case "w":
            delta = relativedelta(weeks=1)
        case "m":
            delta = relativedelta(months=1)
        case "y":
            delta = relativedelta(years=1)
        case _:
            raise ValueError(f"Unknown frequency: {frequency}")

    while current <= overall_end:
        next_start = current + delta
        # end of current slice = day before next start
        slice_end = next_start - relativedelta(days=1)
        # but do not exceed overall_end
        slice_end = min(slice_end, overall_end)

        yield current, slice_end

        current = next_start


def slice_period(
    period: list[date] | tuple[date, date],
    frequency: Literal["d", "w", "m", "y"],
    as_strings: bool = True,
) -> list[tuple[str, str]] | list[tuple[date, date]]:
    """
    Splits a period into sub-intervals according to frequency.
    """

    start, end = period

    slices = list(iter_period_slices(start, end, frequency))

    if as_strings:
        return [(s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")) for s, e in slices]

    return slices


@overload
def get_last_period(
    settings: LastTimeModel,
    today: date | None = None,
    *,
    time_unit: Literal["d", "w", "m", "y"] = "w",
    as_strings: Literal[False] = False,
) -> tuple[date, date]: ...

@overload
def get_last_period(
    settings: LastTimeModel,
    today: date | None = None,
    *,
    time_unit: Literal["d", "w", "m", "y"] = "w",
    as_strings: Literal[True],
) -> tuple[str, ...]: ...

def get_last_period(
    settings: LastTimeModel,
    today: date | None = None,
    *,
    time_unit: Literal["d", "w", "m", "y"] = "w",
    as_strings: bool = False,
) -> tuple[date, date] | tuple[str, ...]:
    """
    Return the start and end of the period for the last N periods (days/weeks/months/years).

    The ``time_unit`` is now supplied explicitly rather than read from
    ``settings`` (it lives on ``PeriodModel``, not ``LastTimeModel``).
    """

    today = date.today() if today is None else today
    end = start = today

    start_of_month = today.replace(day=1)
    start_of_year = today.replace(month=1, day=1)
    start_of_week = today - relativedelta(days=today.weekday())

    match time_unit:
        case "y":
            start = start_of_year.replace(year=today.year - settings.unit_count)
        case "m":
            start = start_of_month - relativedelta(months=settings.unit_count)
        case "w":
            start = start_of_week - relativedelta(weeks=settings.unit_count)
        case "d":
            start = today - relativedelta(days=settings.unit_count)
        case _:
            raise ValueError(f"Unknown frequency: {time_unit}")

    if not settings.include_current:
        match time_unit:
            case "y":
                end = start_of_year - relativedelta(days=1)
            case "m":
                end = start_of_month - relativedelta(days=1)
            case "w":
                end = start + relativedelta(weeks=settings.unit_count - 1, days=6)
            case "d":
                end = today - relativedelta(days=1)

    result = (start, end)

    if as_strings:
        return tuple(d.strftime("%Y-%m-%d") for d in result)

    return result


def get_files_suffix(compression: str | dict[str, Any] | None = None) -> str:
    """Return the full file extension accounting for compression.
    Always normalize gzip → .gz (standard and reliable extension).
    """
    base = ".csv"

    if compression is None or compression == "infer" or not compression:
        return base

    # get compression method
    if isinstance(compression, dict):
        method = compression.get("method", "").lower().strip()
    else:
        method = str(compression).lower().strip()

    # gzip normalization (most common issue)
    if method in ("gzip", ".gzip", "gz", ".gz"):
        return base + ".gz"

    # Other popular compression formats
    elif method in ("bz2", "bzip2"):
        return base + ".bz2"
    elif method in ("xz",):
        return base + ".xz"
    elif method in ("zip",):
        return base + ".zip"
    elif method in ("zstd",):
        return base + ".zst"

    else:
        clean = method.strip(".")
        return base + "." + clean


def csv_to_file(
    data_frame: DataFrame,
    csv_path_out: PathLike[str],
    file_prefix: str = "",
    compression: (
        Literal["infer", "gzip", "bz2", "zip", "xz", "zstd", "tar"]
        | None
        | dict[str, Any]
    ) = "infer",
    add_time: bool = True,
    **kwargs: Any,
) -> None:
    time_str = ""
    if add_time:
        time_str = "_" + time.strftime("%Y%m%d_%H%M%S")
    ext = get_files_suffix(compression)
    out_file = Path(csv_path_out, f"{file_prefix}{time_str}{ext}")

    encoding = kwargs.pop("encoding", "utf-8-sig")
    data_frame.to_csv(
        path_or_buf=out_file,
        index=False,
        mode="w",
        decimal=",",
        sep=";",
        encoding=encoding,
        compression=compression,
        **kwargs,
    )


def en_to_ru(slices_en: list[Any]) -> list[Any]:
    try:
        slices_ru: list[list[Any]] = [[] for _ in range(len(slices_en))]
        for i, param in enumerate(slices_en):
            slices_ru[i] = param.replace("EName", "Name")
        return slices_ru
    except (TypeError, AttributeError) as err:
        logger.error(
            f"Element must be a string, got {type(slices_en)}"
        )
        raise err


def pivot_df_frequency(df: DataFrame, dim_cols) -> DataFrame:
    if "frequencyDistInterval" not in df.columns:
        return df

    try:
        # Take columns where there is one value per row
        base_df = (
            df[df["frequencyDistInterval"] == "-"]
            .drop(columns="frequencyDistInterval")
            .dropna(axis=1, how="all")
            .groupby(dim_cols, as_index=False)
            .first()
        )
        # Frequencies - pivot the set of values into rows here
        freq_df = (
            df[df["frequencyDistInterval"] != "-"]
            .pivot_table(
                index=dim_cols,
                columns="frequencyDistInterval",
                values="FrequencyDistPer",
                aggfunc="first",
            )
            .reset_index()
        )
        # sort frequency intervals
        freq_df = freq_df.reindex(
            sorted(
                freq_df.columns, key=lambda x: int(x[1:-2]) if x.startswith("[") else -1
            ),
            axis=1,
        )

        # Merge two arrays
        result = base_df.merge(freq_df, on=dim_cols, how="left")

        return result
    except Exception as err:
        logger.exception(f"Error '{err}' while processing frequencies.")
        return df


def dir_content_to_dict(files, suffix: str = "yaml"):
    return {file.name.removesuffix(suffix): file for file in files}


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
            files.extend(Path(path).glob(f"{subfolder_pattern}*.{ext.strip('.')}"))
        return files
    except Exception as err:
        logger.exception(f"Error reading directory {path}: {err}")
        return []


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


def yaml_to_dict(file: str | PathLike[str]) -> dict[str, Any] | list[Any]:
    """
    Loads configuration from a YAML file.

    Args:
        file (Path): Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: Parsed configuration dictionary. For an empty file,
        returns an empty dict (``yaml.safe_load`` returns ``None`` for null
        nodes, which is normalized to ``{}``).

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If the file contains invalid YAML.
    """
    try:
        with open(file, encoding="utf8") as cfg:
            return yaml.safe_load(cfg) or {}
    except yaml.YAMLError as err:
        logger.error(err)
        raise
    except FileNotFoundError as err:
        logger.error(f"No such file or directory: {file} — {err}")
        raise


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
