from core.reports import (
    NatCrossTabDict,
    NatTVCrossTab,
    NatTVTimeBand
)

from core.utils import (yaml_to_dict)
from datetime import datetime
from pathlib import Path

REPORT_SETTINGS = 'tv_report.yaml'


def main(report_settings_file):
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
    root_dir = Path(__file__).absolute().parent  # root_dir = Path().absolute()
    report_settings_file = Path.joinpath(root_dir, 'settings/reports/', report_settings_file)
    tasks = yaml_to_dict(report_settings_file)

    print(f'Обработка стартовала {str(start_time)}. Количество отчетов: {len(tasks)}.')
    for i, task_settings in enumerate(tasks, start=1):
        print(f'Приступаем к отчету {i}')
        rep = task_settings['report_subtype']
        t = REPORT_TYPES[rep](settings=task_settings)
        t.create_report()
        folders.add(t.name)

    print(f'\n Подготовка отчета заняла {str(datetime.now().replace(microsecond=0) - start_time)}')

    return folders


if __name__ == '__main__':
    main(REPORT_SETTINGS)
