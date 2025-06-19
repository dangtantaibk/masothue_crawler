# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os


def load_config():
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', 'config', 'config.env'))
    config = {
        'POSTGRESQL_HOST': os.getenv('POSTGRESQL_HOST'),
        'POSTGRESQL_PORT': os.getenv('POSTGRESQL_PORT'),
        'POSTGRESQL_DATABASE_NAME': os.getenv('POSTGRESQL_DATABASE_NAME'),
        'POSTGRESQL_USER': os.getenv('POSTGRESQL_USER'),
        'POSTGRESQL_PASSWORD': os.getenv('POSTGRESQL_PASSWORD')
    }
    return config