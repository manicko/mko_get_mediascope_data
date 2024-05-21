import mko_get_mediascope_data.core.reports as reports
from os import PathLike
from mko_get_mediascope_data.core.utils import (yaml_to_dict)
from datetime import datetime
from pathlib import Path


def get_data(
        report_settings_file: [str, PathLike] = None,
        output_path: [str, PathLike] = None,
        connection_settings_file: [str, PathLike] = None
):
    REPORT_TYPES = {
        'TV_CROSSTAB': reports.TVCrossTab,
        'TV_TIMEBAND': reports.TVTimeBand,
        'TV_SIMPLE': reports.TVSimple,
        'TV_DICT_CROSSTAB': reports.TVGetDictCrossTab,
        'REG_TV_CROSSTAB': reports.RegTVCrossTab,
        'REG_TV_DICT_CROSSTAB': reports.RegTVGetDictCrossTab,
    }
    if not Path.is_file(report_settings_file):
        raise FileNotFoundError(f'The system cannot '
                                f'find the file specified:"{report_settings_file}"')
    if not Path.is_file(connection_settings_file):
        raise FileNotFoundError(f'The system cannot '
                                f'find the file specified:"{report_settings_file}"')
    if not Path.is_dir(output_path):
        raise FileNotFoundError(f'The system cannot '
                                f'find the directory specified: "{report_settings_file}"')

    folders = set()
    start_time = datetime.now().replace(microsecond=0)
    tasks = yaml_to_dict(report_settings_file)

    print(f'\nОбработка стартовала {str(start_time)}.\nКоличество отчетов: {len(tasks)}.')
    for i, task_settings in enumerate(tasks, start=1):
        print(f'\nПриступаем к отчету {i}\n')
        rep = task_settings['report_type']
        t = REPORT_TYPES[rep](settings=task_settings,
                              output_path=output_path,
                              connection_settings_file=connection_settings_file
                              )
        t.create_report()
        folders.add(t.name)

    end_time = datetime.now().replace(microsecond=0)
    print(f'\nПодготовка отчетов завершена в {str(end_time)}. \nПодготовка заняла {str(end_time - start_time)}.')

    return list(folders)


if __name__ == '__main__':
    REPORT_SETTINGS = 'sovcombank/nat_tv_by_spots.yaml'
    root_dir = Path(__file__).absolute().parent  # root_dir = Path().absolute()
    rep_settings_file = Path.joinpath(root_dir, 'settings/reports/', REPORT_SETTINGS)
    out_path = Path.joinpath(root_dir.parent, "data/output")
    connections = Path.joinpath(root_dir, "settings/connections/mediascope2.json")

    get_data(
        report_settings_file=rep_settings_file,
        output_path=out_path,
        connection_settings_file=connections
    )
