import core.reports as reports
from core.utils import (yaml_to_dict)

REPORT_SETTINGS = 'settings/reports/report_settings_cars.yaml'
REPORT_TYPES = {
    'DYNAMICS_BY_SPOTS': reports.DynamicsBySpots,
    'DYNAMICS_BY_SPOTS_DICT': reports.DynamicsBySpotsDict,
    'TOP_ADVERTISERS': reports.TopNatTVAdvertisers,
    'TOP_PROGRAMS': reports.TopNatTVPrograms
}

if __name__ == '__main__':

    tasks = yaml_to_dict(REPORT_SETTINGS)
    # a = reports.DynamicsBySpots(settings=re[0])
    print(f'Начинаем подготовку {len(tasks)} отчетов')
    for i, task_settings in enumerate(tasks, start=1):
        print(f'Приступаем к отчету {i}')
        t = REPORT_TYPES[task_settings['report_subtype']](settings=task_settings)
        t.load_data()
        t.extract_data()
