from pathlib import Path
import time
import pandas as pd
from core.utils import get_last_period

from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

# Настраиваем отображение

# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем максимальное количество выводимых строк. Раскомментируйте нужную строку
# 200 строк
# pd.set_option("display.max_rows", 200)
# Отображаем все строки. ВАЖНО! Отображение большого DataFrame требует много ресурсов
# pd.set_option("display.max_rows", None)

# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

# Задаем период
# Период указывается в виде списка ('Начало', 'Конец'). Можно указать несколько периодов
period = get_last_period('m', 3)

print(period)
date_filter = [period]
date_filter  = [('2023-02-01', '2023-04-22')]
# Задаем дни недели
weekday_filter = None

# Задаем тип дня
daytype_filter = None

# Задаем временной интервал - минимум 60 минут
time_filter = None

# Задаем ЦА
basedemo_filter = None

# Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index
targetdemo_filter = None

# Задаем место просмотра
location_filter = None

# Задаем каналы
company_filter = None

# Фильтр программ
program_filter = None

# Фильтр блоков
break_filter = None

# Фильтр роликов: Товарная категория 2 - УСЛУГИ ФИНАНСОВЫЕ;  Статус события - Реальный
ad_filter = None

# Указываем список срезов
slices_en = [
    # 'researchDate', # день разрешено указывать не более 1 временного среза на задачу.
    'researchMonth',  # месяц разрешено указывать не более 1 временного среза на задачу.
    # 'researchWeek',  # неделя разрешено указывать не более 1 временного среза на задачу.
    'programEName', #Название программы
    # 'tvNetEName', # телесеть
    'tvCompanyEName', #Телекомпания
    'programTypeEName', #Программа жанр
]

# Указываем список статистик для расчета
statistics = [
    'RtgPerAvg',
    'ShareAvg'
]

# Задаем условия сортировки: Телекомпания (от а до я), рекламодатель (от а до я), бренд (от а до я), товар (от а до я)
sortings = {
    'researchMonth': 'ASC',
    'RtgPerAvg': 'DESC'
}

# Задаем опции расчета
options = {
    # id набора данных (1-Russia all, 2-Russia 100+, 3-Cities, # 4-TVI+ Russia all, 5-TVI+ Russia 100+, 6-Moscow)
    # Возможно указать только одно значение
    "kitId": 1,
    "issueType": "PROGRAM",   #Тип события для расчета отчета Кросс-таблица (Crosstab): PROGRAM, BREAKS, AD
}

slices_ru = slices_en.copy()
for i, param in enumerate(slices_ru):
    slices_ru[i] = param.replace('EName', 'Name')

# Задаем необходимые группы
targets = {
    'all 30-60 BC': 'age >= 30 AND age <= 60 AND incomeGroupRussia IN (2,3)',
    'w 25-55': 'sex = 2 and age >= 25 AND age <= 55'
}

# Посчитаем задания в цикле
tasks = []
print("Отправляем задания на расчет")

# Для каждой ЦА формируем задание и отправляем на расчет
for target, syntax in targets.items():
    # Подставляем значения словаря в параметры
    project_name = target
    basedemo_filter = syntax

    # Формируем задание для API TV Index в формате JSON
    task_json = mtask.build_crosstab_task(date_filter=date_filter, weekday_filter=weekday_filter,
                                          daytype_filter=daytype_filter, company_filter=company_filter,
                                          location_filter=location_filter, basedemo_filter=basedemo_filter,
                                          targetdemo_filter=targetdemo_filter, program_filter=program_filter,
                                          break_filter=break_filter, ad_filter=ad_filter,
                                          slices=slices_ru, statistics=statistics, sortings=sortings, options=options)

    # Для каждого этапа цикла формируем словарь с параметрами и отправленным заданием на расчет
    tsk = {}
    tsk['project_name'] = project_name
    tsk['task'] = mtask.send_crosstab_task(task_json)
    tasks.append(tsk)
    time.sleep(2)
    print('.', end='')

print(f"\nid: {[i['task']['taskId'] for i in tasks]}")

print('')
# Ждем выполнения
print('Ждем выполнения')
tsks = mtask.wait_task(tasks)
print('Расчет завершен, получаем результат')

# Получаем результат
results = []
print('Собираем таблицу')
for t in tasks:
    tsk = t['task']
    df_result = mtask.result2table(mtask.get_result(tsk), project_name=t['project_name'])
    results.append(df_result)
    print('.', end='')
df = pd.concat(results)

# Приводим порядок столбцов в соответствие с условиями расчета
df = df[['prj_name'] + slices_ru + statistics]



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
