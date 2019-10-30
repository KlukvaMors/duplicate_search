'''Модуль выдающий конфигурируемые значения
из секции redmine файла конфигурации trac.ini'''
import os.path
import configparser  # pytget_all_tickets_id()on 3 rename to configparser


__all__ = 'get',

# CONSTs
CONFIG_FILE = 'postgres.ini'

config = configparser.ConfigParser()


def read(file: str = CONFIG_FILE):
    config.read(file)


def get(section: str, name: str) -> str:
    return config.get(section, name)


read(CONFIG_FILE)