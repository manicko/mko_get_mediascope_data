from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta
from core.utils import get_last_period
import csv as csv
import time
import pandas as pd
# import matplotlib.pyplot as plt
# from IPython.display import JSON

from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

# Настраиваем отображение

# Включаем отображение всех колонок
# pd.set_option('display.max_columns', None)

# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

# Задаем параметры по умолчанию для выгрузки из медиаскоп
GET_DATA_PARAMS = {
    'date_filter': None,  # Фильтр указывается в виде списка ('Начало', 'Конец'). Можно указать несколько периодов
    'weekday_filter': None,  # Задаем дни недели
    'daytype_filter': None,  # Задаем тип дня
    'company_filter': None,  # Задаем каналы
    'location_filter': None,  # Задаем место просмотра
    'basedemo_filter': None,  # Задаем ЦА
    'targetdemo_filter': None,
    # Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index
    'program_filter': None,  # Фильтр программ
    'break_filter': None,  # Фильтр блоков
    'ad_filter': None,  # Фильтр роликов:
    'slices': None,  # Указываем список срезов
    'statistics': None,  # Указываем список статистик для расчета
    'sortings': None,  # Задаем условия сортировки:
    'options': None  # Задаем опции расчета
}

# Функция для задания периода - последние несколько месяцев, лет или недель
# сначала указываем тип: 'y' -год, 'm' месяц, 'w' - неделя, затем количество и, наконец True - если включаем текущий день и нет, если нет.
period = get_last_period('m', 1)

# Фильтр указывается в виде списка ('Начало', 'Конец'). Можно указать несколько периодов
GET_DATA_PARAMS['date_filter'] = [period]
# GET_DATA_PARAMS['date_filter'] = [('2023-09-01', '2023-09-31')]

print(GET_DATA_PARAMS['date_filter'])

# basedemo_filter = 'age >= 30 AND age <= 60 AND incomeGroupRussia IN (2,3)'  # all 30-60 BC
GET_DATA_PARAMS['basedemo_filter'] = None

# Фильтр роликов: Товарная категория 2 - УСЛУГИ ФИНАНСОВЫЕ;  Статус события - Реальный
# GET_DATA_PARAMS['ad_filter'] = 'articleLevel2Id = 2272 and adIssueStatusId = R'
# МАГАЗИНЫ ПАРФЮМЕРИИ И КОСМЕТИКИ ИНТЕРНЕТ-МАГАЗИНЫ ПАРФЮМЕРИИ И КОСМЕТИКИ
GET_DATA_PARAMS['ad_filter'] = 'articleLevel4Id in (4767, 4911)  and adIssueStatusId = R'
category_name = 'МАГАЗИНЫ ПАРФЮМЕРИИ И КОСМЕТИКИ'
# Указываем список статистик для расчета
GET_DATA_PARAMS['statistics'] = [
    'QuantitySum'  # количество эфирных событий
]

# Задаем опции расчета
GET_DATA_PARAMS['options'] = {
    # id набора данных (1-Russia all, 2-Russia 100+, 3-Cities, # 4-TVI+ Russia all, 5-TVI+ Russia 100+, 6-Moscow)
    # Возможно указать только одно значение
    "kitId": 1,
    "issueType": "AD",  # Тип события для расчета отчета Кросс-таблица (Crosstab): PROGRAM, BREAKS, AD
    "standRtg": {
        "useRealDuration": True,  # расчет по реальной длительности ролика.
        "standardDuration": 20  # стандартная длительность 20 сек.
    }
}

# Указываем список срезов
slices_en = [
    'advertiserEName',  # Список рекламодателей
    'brandEName',  # Список брендов
    'subbrandEName',  # Список суббрендов
    'modelEName',  # Список продуктов
]
slices_ru = slices_en.copy()
for i, param in enumerate(slices_ru):
    slices_ru[i] = param.replace('EName', 'Name')

GET_DATA_PARAMS['slices'] = slices_ru

# Формируем задание для API TV Index в формате JSON
task_json = mtask.build_crosstab_task(**GET_DATA_PARAMS)

# Отправляем задание на расчет и ждем выполнения
task_crosstab = mtask.wait_task(mtask.send_crosstab_task(task_json))

# Получаем результат
df = mtask.result2table(mtask.get_result(task_crosstab))

df = df[slices_ru]

# переносим данные датафрейма в словарь
d_col_names = [
    'action',
    'search_column_idx',
    'value',
    'term',
    'cat',
    'adv',
    'bra',
    'sbr',
    'mdl',
    'cln_0',
    'cln_1',
    'cln_2',
    'cln_3',
    'cln_4',
    'cln_5'
]

# path to folder containing SQLight databases
ROOT_DIR = Path().absolute()

# folder containing data for import export CSV
CSV_PATH = Path.joinpath(ROOT_DIR, r'data/')

# folder to output CSV from database
CSV_PATH_OUT = Path.joinpath(CSV_PATH, 'output/')

file_prefix = 'dict'
time_str = 1  # time.strftime("%Y%m%d-%H%M%S")
out_file = Path(CSV_PATH_OUT, f'{file_prefix}_{time_str}.csv')
# df.to_csv(path_or_buf=out_file, index=False)
with open(out_file, 'w', newline='', encoding='utf-8') as csvfile:
    # creating a csv writer object
    csv_writer = csv.writer(csvfile)
    # writing header
    csv_writer.writerow(d_col_names)
    # writing data
    shift = 2
    for search_id, col in enumerate(df, start=shift):
        data = df[col].unique().tolist()
        row = ['upd'] + [search_id]
        for cell in data:
            term = f'"col_{search_id}":"{cell}"'
            csv_writer.writerow(row + [cell] + [term] + [category_name] + [None] * (search_id - shift) + [cell])
