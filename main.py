from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from data_getter.utils import (en_to_ru, slice_period, get_last_period, write_to_file, yaml_to_dict)
import yaml
import time
import pandas as pd
import os as os
from unidecode import unidecode

from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

default_settings_yaml = "default_report_settings.yaml"
report_settings = 'report_settings.yaml'

DEFAULT_SETTINGS = yaml_to_dict(default_settings_yaml)
SETTINGS = yaml_to_dict(report_settings)
# reading report settings
# print(DEFAULT_SETTINGS, SETTINGS, sep='\n')

if __name__ == '__main__':

    # подключаемся к базе и создаем объекты для работы с TVI API
    mnet = mscore.MediascopeApiNetwork()
    mtask = cwt.MediaVortexTask()
    cats = cwc.MediaVortexCats()

    # формируем настройки выгрузки
    settings = DEFAULT_SETTINGS['CROSS_TAB']['DEFAULT_DATA_PARAMS'].copy()
    settings.update(DEFAULT_SETTINGS['CROSS_TAB']['DYNAMICS_BY_SPOTS'])

    for k, v in SETTINGS.items():
        if k in settings:
            settings[k] = v

    settings.update(DEFAULT_SETTINGS['CROSS_TAB']['DYNAMICS_BY_SPOTS'])
    category = unidecode(SETTINGS['category_name']).lower()
    targets = SETTINGS['target_audiences']
    # меняем настройки выгрузки на русский, если необходимо
    if SETTINGS['data_lang'] == 'ru':
        settings['slices'] = en_to_ru(settings['slices'])

    # задаем период выгрузки и частоту для разбивки периода на интервалы
    period = None
    frequency = 'm'  # разбивка интервала по умолчанию по месяцам
    # проверяем, если фильтр дат не задан явно, задаем его
    if SETTINGS['period']['date_filter'] is None:
        period = ('2023-02-01', '2023-03-22')  # get_last_period(**SETTINGS['period']['last_time'])
        frequency = SETTINGS['period']['last_time']['period_type']

    # разбиваем период на интервалы и выгружаем в отдельные файлы
    task_intervals = {}  # переменная для хранения интервалов
    temp_tasks = []
    print('Готовим задачи к расчету')
    for interval in slice_period(period, frequency):
        settings['date_filter'] = [interval]
        interval_name = f'{category}_{"_".join(interval)}'
        task_intervals[interval_name] = {}
        for t_name, t_filter in targets.items():
            # Отправляем задание
            settings['basedemo_filter'] = t_filter
            task_json = mtask.build_crosstab_task(**settings)
            task_intervals[interval_name][t_name] = {'task': mtask.send_crosstab_task(task_json)}
            temp_tasks.append(task_intervals[interval_name][t_name])
            time.sleep(2)
            print('.', end='')
    print()
    # ждем выполнения
    mtask.wait_task(temp_tasks)
    temp_tasks = []
    print('Расчет завершен, получаем результат и сохраняем в файлы')
    # Получаем результат

    for interval_name, interval in task_intervals.items():
        results = []
        for t_name in targets.keys():
            df = mtask.result2table(mtask.get_result(interval[t_name]['task']), project_name=t_name)
            results.append(df)
        df = pd.concat(results)
        df = df[['prj_name'] + settings['slices'] + settings['statistics']]
        write_to_file(
            df,
            folder=category,
            file_prefix=interval_name
        )
        print('.', end='')
    print()
    print('Готово')
