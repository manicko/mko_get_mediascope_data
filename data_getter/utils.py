
from datetime import date
from dateutil.relativedelta import relativedelta

# Задаем период
# Период указывается в виде списка ('Начало', 'Конец'). Можно указать несколько периодов

def get_last_period(period_type: str = 'w', period_num: int = 2, include_current: bool = False) -> tuple:
    '''
    Returns period as a tuple for last 'period_num' months, weeks or years starting from now
    :param period_type: str, 'y' for years, 'm' for months, 'w' for weeks
    :param period_num: int, number of periods ago in terms of 'period_type'
    :param include_current: bool, set to True to set period until today
    :return:
    '''
    allowed_types = {'y', 'm', 'w'}

    if period_type not in allowed_types:
        print('Period should either "y" - years, "m" -months, "w" - weeks')
        return ('', '')
    if period_num <= 0:
        print('period_num should be > 0')
        return ('', '')

    today = date.today()
    last_day_of_period = today

    if period_type == 'y':
        start_year = today.year - period_num
        first_day_of_period = today.replace(day=1, month=1, year=start_year)
        if include_current is False:
            last_day_of_period = today.replace(day=1, month=1) + relativedelta(days=-1)

    if period_type == 'm':
        first_day_of_period = today.replace(day=1) + relativedelta(months=-period_num)
        if include_current is False:
            last_day_of_period = today.replace(day=1) + relativedelta(days=-1)

    if period_type == 'w':
        weekday = today.weekday()
        first_day_of_period = today + relativedelta(days=-weekday, weeks=-period_num)
        if include_current is False:
            last_day_of_period = first_day_of_period + relativedelta(days=6, weeks=period_num-1)

    output = (first_day_of_period, last_day_of_period)
    output = tuple(map(lambda x: f'{x:%Y-%m-%d}', output))
    return output

