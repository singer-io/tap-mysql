import os
import pymysql
import singer
import tap_mysql
import tap_mysql.sync_strategies.common as common
from tap_mysql.connection import MySQLConnection

DB_NAME='tap_mysql_test'

def get_db_config():
    config = {}
    config['host'] = os.environ.get('TAP_MYSQL_HOST')
    config['port'] = int(os.environ.get('TAP_MYSQL_PORT'))
    config['user'] = os.environ.get('TAP_MYSQL_USER')
    config['password'] = os.environ.get('TAP_MYSQL_PASSWORD')
    config['charset'] = 'utf8'
    if not config['password']:
        del config['password']

    return config


def get_test_connection():
    db_config = get_db_config()

    con = pymysql.connect(**db_config)

    try:
        with con.cursor() as cur:
            try:
                cur.execute('DROP DATABASE {}'.format(DB_NAME))
            except:
                pass
            cur.execute('CREATE DATABASE {}'.format(DB_NAME))
    finally:
        con.close()

    db_config['database'] = DB_NAME
    db_config['autocommit'] = True

    mysql_conn = MySQLConnection(db_config)
    mysql_conn.autocommit_mode = True

    return mysql_conn


def discover_catalog(connection, catalog):
    catalog = tap_mysql.discover_catalog(connection, catalog)
    streams = []

    for stream in catalog.streams:
        database_name = common.get_database_name(stream)

        if database_name == DB_NAME:
            streams.append(stream)

    catalog.streams = streams

    return catalog


def set_replication_method_and_key(stream, r_method, r_key):
    new_md = singer.metadata.to_map(stream.metadata)
    old_md = new_md.get(())
    if r_method:
        old_md.update({'replication-method': r_method})

    if r_key:
        old_md.update({'replication-key': r_key})

    stream.metadata = singer.metadata.to_list(new_md)
    return stream
