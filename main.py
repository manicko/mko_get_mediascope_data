from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from core.utils import (en_to_ru, slice_period, get_last_period, write_to_file, yaml_to_dict)
from core.construct import (report_load_export)
import yaml
import time
import pandas as pd
import os as os
from unidecode import unidecode

from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc


# настройки заданной выгрузки
REPORT_SETTINGS = 'settings/report_settings_cars.yaml'

if __name__ == '__main__':
    mt = cwt.MediaVortexTask(settings_filename='settings/mediascope_connection_settings.json')
    r_settings = yaml_to_dict(REPORT_SETTINGS)
    print(f'Получено задач к расчету:{len(r_settings)}')
    for i, report in enumerate(r_settings):
        print(f'Приступаем к {i+1} задаче')
        report_load_export(report, mt)
