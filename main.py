import core.reports as reports
from core.utils import (yaml_to_dict)

REPORT_SETTINGS = 'settings/reports/tv_report.yaml'
REPORT_TYPES = {
    'DYNAMICS_BY_SPOTS': reports.NatTVCrossTab,
    'DYNAMICS_BY_SPOTS_DICT': reports.NatCrossTabDict,
    'TOP_NAT_TV_ADVERTISERS': reports.NatTVCrossTab,
    'TOP_NAT_TV_PROGRAMS': reports.NatTVCrossTab,
    'NAT_TV_CHANNELS_BA': reports.NatTVCrossTab,
    'NAT_TV_CHANNELS_ATV': reports.NatTVTimeBand,
    'NAT_TV_CHANNELS_TVR': reports.NatTVTimeBand,
    'NAT_TV_CHANNELS_SOC_DEM': reports.NatTVTimeBand
}

if __name__ == '__main__':

    tasks = yaml_to_dict(REPORT_SETTINGS)

    print(f'Начинаем подготовку {len(tasks)} отчетов')
    for i, task_settings in enumerate(tasks, start=1):
        print(f'Приступаем к отчету {i}')
        rep = task_settings['report_subtype']
        t = REPORT_TYPES[rep](settings=task_settings)
        t.create_report()
