# -*- coding: utf-8 -*-

import psycopg2
from config_loader import load_config


def get_db_connection():
    config = load_config()
    conn = psycopg2.connect(
        host=config['POSTGRESQL_HOST'],
        port=config['POSTGRESQL_PORT'],
        dbname=config['POSTGRESQL_DATABASE_NAME'],
        user=config['POSTGRESQL_USER'],
        password=config['POSTGRESQL_PASSWORD']
    )
    return conn


def end_db_connection(cur, conn):
    conn.commit()
    cur.close()
    conn.close()
    return True
