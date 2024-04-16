from yaml import dump
from os import PathLike
from pathlib import Path


class Task:
    pass


class TVTask(Task):
    def __init__(self, name, settings, report_subtype):
        self.name = name
        self.settings = settings
        self.key = None
        self.status = True
        self.log_error = False
        self.interval = None
        self.target = None
        self.report_subtype = report_subtype

    def to_yaml(self, folder: str | bytes | PathLike = None):
        if folder is not None:
            folder = Path(folder)
            root_dir = Path().absolute()
            folder = Path.joinpath(root_dir, 'data', 'output', folder, 'errors')
            folder.mkdir(parents=True, exist_ok=True)
            export_file = Path.joinpath(folder, self.name + '.yaml')
            dump_settings = {
                'name': self.name,
                'report_subtype': self.report_subtype,
                'target': self.target,
                'settings': self.settings
            }
            # print(f'Сохраняю настройки задачи {self.name}: {dump_settings} в файл {export_file}')
            with open(export_file, 'w', encoding="utf-8") as outfile:
                dump(dump_settings, outfile, default_flow_style=False, allow_unicode=True, encoding=None)
