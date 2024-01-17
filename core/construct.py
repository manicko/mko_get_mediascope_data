from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from urllib3.exceptions import (
    ConnectTimeoutError,
    MaxRetryError
)

from core.utils import (
    en_to_ru,
    slice_period,
    write_to_file,
    yaml_to_dict,
    get_frequency,
    get_last_period,
    prepare_dict,
    check_period
)
import yaml
import time
import pandas as pd
import os as os
from unidecode import unidecode

from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

# загружаем базовые настройки и настройки заданной выгрузки
# DEFAULT_SETTINGS_YAML = "../settings/default_report_settings.yaml"
# REPORT_SETTINGS = '../settings/report_settings.yaml'
DEFAULT_SETTINGS_YAML = "settings/default_report_settings.yaml"


def report_load_export(report_settings, mtask, ct):
    # загружаем базовые настройки и настройки заданной выгрузки

    default_settings = yaml_to_dict(DEFAULT_SETTINGS_YAML)

    # формируем настройки выгрузки
    report_type = default_settings['REPORT_TYPES'][report_settings['report_subtype']]
    settings = default_settings[report_type]['DEFAULT_DATA_PARAMS'].copy()
    settings.update(default_settings[report_type][report_settings['report_subtype']])

    for k, v in report_settings.items():
        if k in settings:
            settings[k] = v
    category = ''
    if 'category_name' in report_settings:
        category = unidecode(report_settings['category_name']).lower()
    targets = report_settings['target_audiences']
    # меняем настройки выгрузки на русский, если необходимо
    if report_settings['data_lang'] == 'ru':
        settings['slices'] = en_to_ru(settings['slices'])

    # задаем период выгрузки и частоту для разбивки периода на интервалы
    period = report_settings['period']['date_filter']
    frequency = get_frequency(report_settings)
    # проверяем, если фильтр дат не задан явно, задаем его
    if period is None:
        period = get_last_period(**report_settings['period']['last_time'])  # ('2023-02-01', '2023-03-22')  #

    # разбиваем период на интервалы и выгружаем в отдельные файлы
    task_intervals = {}  # переменная для хранения интервалов
    temp_tasks = []
    period = check_period(period)
    intervals = slice_period(period, frequency)

    print('Готовим задачи к расчету')
    for interval in intervals:
        settings['date_filter'] = [interval]
        interval_name = "_".join([category] + list(interval))
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
    while True:
        try:
            # ждем выполнения
            mtask.wait_task(temp_tasks)
        except (mtask.ConnectTimeout, mtask.ConnectTimeoutError, mtask.MaxRetryError) as err:
            print('mtask', err)
            ask = input("Press 'y' to continue, 'n' to break")
            if ask == 'n':
                break
        except (ConnectTimeoutError, MaxRetryError):
            print('urllib', err)
            ask = input("Press 'y' to continue, 'n' to break")
            if ask == 'n':
                break
        except Exception as hz:
            print('hz', type(hz), hz)
        else:
            break

    del temp_tasks

    print('Расчет завершен, получаем результат')

    # Получаем результат
    for interval_name, interval in task_intervals.items():
        results = []
        # print(interval)
        for t_name in targets.keys():
            # print(t_name)
            df = mtask.result2table(mtask.get_result(interval[t_name]['task']), project_name=t_name)
            if not df.empty:
                results.append(df)
        if results:
            df = pd.concat(results)
            # настраиваем колонки данных на выходе
            columns = settings['slices'] + settings['statistics']
            if 'prj_name' in df:
                df.rename(columns={'prj_name': 'targetAudience'}, inplace=True)
                columns = ['targetAudience'] + columns
            try:
                df = df[columns]
            except KeyError as err:
                print(f"Ошибка '{err}' при выгрузке интервала: '{interval_name}. Он будет пропущен.'")
            else:
                if report_settings['report_subtype'] == 'DYNAMICS_BY_SPOTS_DICT':
                    df = prepare_dict(df[settings['slices']])
                # записываем файл в соответствующую папку
                dir_name = []
                if category:
                    dir_name.append(category)
                if 'folder' in report_settings:
                    dir_name.append(unidecode(report_settings['folder']).lower())
                dir_name = '/'.join(dir_name)
                write_to_file(
                    df,
                    folder=dir_name,
                    file_prefix=interval_name
                )
                print('.', end='')

    print()
    print('Готово')

# mt = cwt.MediaVortexTask(settings_filename='../settings/mediascope_connection_settings.json')
# r_settings = yaml_to_dict(REPORT_SETTINGS)
# for report in r_settings:
#     report_load_export(report, mt)
