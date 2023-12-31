from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta
from data_getter.utils import get_last_period

import time
import pandas as pd


from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc


# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

# Найдем id товарная категория 2-го уровня - "УСЛУГИ ФИНАНСОВЫЕ"
# Для этого в параметр level передадим 2, в параметр name - название категории
# cats.get_tv_article(levels=['2'], name=['УСЛУГИ ФИНАНСОВЫЕ'])

# Получим список всех демографических переменных
# cats.get_tv_demo_attribute()

# Обратимся к словарю типов распространения
# cats.get_tv_breaks_distribution()
# Для типа распространения "Сетевой" идентификатор - N

# Обратимся к словарю статусов события
# cats.get_tv_issue_status()
# Для "Реальный", будем использовать id R

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

# Указываем список срезов
slices_en = [
    # 'researchDate', # день разрешено указывать не более 1 временного среза на задачу.
    'researchMonth',  # месяц разрешено указывать не более 1 временного среза на задачу.
    # 'researchWeek',  # неделя разрешено указывать не более 1 временного среза на задачу.
    'advertiserListEName',  # Список рекламодателей
    # 'advertiserListId',  # Список рекламодателей id
    'brandListEName',  # Список брендов
    # 'brandListId',  # Список брендов id
    'subbrandListEName',  # Список суббрендов
    # 'subbrandListId',  # Список суббрендов id
    'modelListEName',  # Список продуктов
    # 'modelListId',  # Список продуктов id
    'articleList3EName',  # Список товарных категорий 3
    'articleList4EName',  # Список товарных категорий 4
    'adEName',  # Ролик
    'adStandardDuration',  # Ролик ожидаемая длительность
    'adDistributionTypeEName',  # Ролик распространение
    'adTypeName',  # Ролик тип
    'adPositionId',  # Ролик позиция в блоке
    'adPositionTypeEName',  # Ролик позиционирование
    'adPrimeTimeStatusId',  # Прайм/ОфПрайм роликов id' 1 - Prime-time, 2 - Off-prime
    'adPrimeTimeStatusEName',  # Прайм/ОфПрайм роликов (англ.)
    'adId',  # Ролик ID
    'adNotes',  # Ролик описание
    'tvCompanyEName',  # телекомпания
    'programEName',  # Программа
    'programCategoryEName',  # Программа категория (англ.)
    'programCategoryShortEName',  # Программа короткая категория
    'programTypeEName'  # Программа жанр
]

# Задаем опции расчета
options = {
    # id набора данных (1-Russia all, 2-Russia 100+, 3-Cities, # 4-TVI+ Russia all, 5-TVI+ Russia 100+, 6-Moscow)
    # Возможно указать только одно значение
    "kitId": 1,
    "issueType": "AD",  # Тип события для расчета отчета Кросс-таблица (Crosstab): PROGRAM, BREAKS, AD
    "standRtg": {
        "useRealDuration": True,  # расчет по реальной длительности ролика.
        "standardDuration": 20  # стандартная длительность 20 сек.
    }
}

slices_ru = slices_en.copy()
for i, param in enumerate(slices_ru):
    slices_ru[i] = param.replace('EName', 'Name')

# Функция для задания периода - последние несколько месяцев, лет или недель
# сначала указываем тип: 'y' -год, 'm' месяц,
# w' - неделя, затем количество и, наконец True - если включаем текущий день и нет, если нет.
period = get_last_period('m', 11)
GET_DATA_PARAMS['date_filter'] = [period]
# GET_DATA_PARAMS['date_filter'] = [('2023-09-01', '2023-09-31')]

print(GET_DATA_PARAMS['date_filter'])

# GET_DATA_PARAMS['basedemo_filter'] = 'age >= 30 AND age <= 60 AND incomeGroupRussia IN (2,3)'  # all 30-60 BC
GET_DATA_PARAMS['basedemo_filter'] = 'sex = 2 AND age >= 25 AND age <= 45'  # w 25-45 BC

# Фильтр роликов: Товарная категория 2 - УСЛУГИ ФИНАНСОВЫЕ;  Статус события - Реальный
# GET_DATA_PARAMS['ad_filter'] = 'articleLevel2Id = 2272 and adIssueStatusId = R'
# МАГАЗИНЫ ПАРФЮМЕРИИ И КОСМЕТИКИ ИНТЕРНЕТ-МАГАЗИНЫ ПАРФЮМЕРИИ И КОСМЕТИКИ
GET_DATA_PARAMS['ad_filter'] = 'articleLevel4Id in (4767, 4911)  and adIssueStatusId = R'

# Указываем список статистик для расчета
GET_DATA_PARAMS['statistics'] = [
    'RtgPerSum',  # суммарный рейтинг группы событий в %
    'StandRtgPerSum',
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

df = df[GET_DATA_PARAMS['slices'] + GET_DATA_PARAMS['statistics']]

# path to folder containing SQLight databases
ROOT_DIR = Path().absolute()

# folder containing data for import export CSV
CSV_PATH = Path.joinpath(ROOT_DIR, r'data/')

# folder to output CSV from database
CSV_PATH_OUT = Path.joinpath(CSV_PATH, 'output/')
file_prefix = 'out'
time_str = time.strftime("%Y%m%d-%H%M%S")
out_file = Path(CSV_PATH_OUT, f'{file_prefix}_{time_str}.csv')
df.to_csv(path_or_buf=out_file, index=False)
