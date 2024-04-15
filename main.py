import core.reports as reports
from core.utils import (yaml_to_dict)
from datetime import datetime
from pathlib import Path

REPORT_SETTINGS = 'settings/reports/tv_report.yaml'
REPORT_TYPES = {
    'DYNAMICS_BY_SPOTS': reports.NatTVCrossTab,
    'DYNAMICS_BY_SPOTS_DICT': reports.NatCrossTabDict,
    'TOP_NAT_TV_ADVERTISERS': reports.NatTVCrossTab,
    'TOP_NAT_TV_PROGRAMS': reports.NatTVCrossTab,
    'NAT_TV_CHANNELS_BA': reports.NatTVCrossTab,
    'NAT_TV_CHANNELS_ATV': reports.NatTVCrossTab,
    'NAT_TV_CHANNELS_TVR': reports.NatTVTimeBand,
    'NAT_TV_CHANNELS_SOC_DEM': reports.NatTVTimeBand
}


def main(report_settings_file):
    folders = set()
    start_time = datetime.now().replace(microsecond=0)
    root_dir = Path().absolute()
    report_settings_file = Path.joinpath(root_dir, report_settings_file)
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
