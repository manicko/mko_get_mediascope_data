from core.reports import (
    NatCrossTabDict,
    NatTVCrossTab,
    NatTVTimeBand
)
from os import PathLike
from core.utils import (yaml_to_dict)
from datetime import datetime
from pathlib import Path

REPORT_SETTINGS = 'tv_report.yaml'


def get_data(
        report_settings_file: [str, PathLike] = None,
        output_path: [str, PathLike] = None,
        connection_settings_file: [str, PathLike] = None
):
    REPORT_TYPES = {
        'DYNAMICS_BY_SPOTS': NatTVCrossTab,
        'DYNAMICS_BY_SPOTS_DICT': NatCrossTabDict,
        'TOP_NAT_TV_ADVERTISERS': NatTVCrossTab,
        'TOP_NAT_TV_PROGRAMS': NatTVCrossTab,
        'NAT_TV_CHANNELS_BA': NatTVCrossTab,
        'NAT_TV_CHANNELS_ATV': NatTVCrossTab,
        'NAT_TV_CHANNELS_TVR': NatTVTimeBand,
        'NAT_TV_CHANNELS_SOC_DEM': NatTVTimeBand
    }
    folders = set()
    start_time = datetime.now().replace(microsecond=0)
    tasks = yaml_to_dict(report_settings_file)

    print(f'Обработка стартовала {str(start_time)}. Количество отчетов: {len(tasks)}.')
    for i, task_settings in enumerate(tasks, start=1):
        print(f'Приступаем к отчету {i}')
        rep = task_settings['report_subtype']
        t = REPORT_TYPES[rep](settings=task_settings,
                              output_path=output_path,
                              connection_settings_file=connection_settings_file
                              )
        t.create_report()
        folders.add(t.name)

    print(f'\n Подготовка отчета заняла {str(datetime.now().replace(microsecond=0) - start_time)}')

    return folders


if __name__ == '__main__':
    root_dir = Path(__file__).absolute().parent  # root_dir = Path().absolute()
    report_settings_file = Path.joinpath(root_dir, 'settings/reports/', REPORT_SETTINGS)
    output_path = f"C:/py_exp/mko_get_mediascope_data/data/output"
    connections = f'C:/py_exp/mko_get_mediascope_data/src/mko_get_mediascope_data/settings/connections/mediascope.json'
    get_data(
        report_settings_file,
        output_path,
        connections
    )
