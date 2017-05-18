
#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time
import collections

import attr
import pendulum

import pymysql
import singer
import singer.stats
from singer import utils

import pymysql.constants.FIELD_TYPE as FIELD_TYPE

ColumnDesc = collections.namedtuple(
    'ColumnDesc',
    ['name', 'type_code', 'display_size', 'internal_size', 'precision', 'scale', 'null_ok'])

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password',
    'database'
]

LOGGER = singer.get_logger()


def open_connection(config):
    return pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
    )


def schema_for_column(c):
    
    # if c.type_code == FIELD_TYPE.DECIMAL:
    #     return c
    # elif c.type_code == FIELD_TYPE.TINY:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.SHORT:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.LONG:
    #     print(c)
    # elif c.type_code == FIELD_TYPE.FLOAT:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.DOUBLE:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.NULL:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.TIMESTAMP:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.LONGLONG:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.INT24:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.DATE:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.TIME:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.DATETIME:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.YEAR:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.NEWDATE:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.VARCHAR:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.BIT:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.JSON:
    #     raise Exception('Unrecognized type')
    if c.type_code == FIELD_TYPE.NEWDECIMAL:
        return {
            'type': 'number',
            'maximum': 10 ** c.precision - 1,
            }
    # elif c.type_code == FIELD_TYPE.ENUM:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.SET:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.TINY_BLOB:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.MEDIUM_BLOB:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.LONG_BLOB:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.BLOB:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.VAR_STRING:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.STRING:
    #     raise Exception('Unrecognized type')
    # elif c.type_code == FIELD_TYPE.GEOMETRY:
    #     raise Exception('Unrecognized type')
    raise Exception('Unrecognized column description {}'.format(c))
    
def schema_for_table(connection, table):
    schema = {
        'type': 'object',
        'properties': {}
    }
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM {} LIMIT 1'.format(table))
        for column_desc in cursor.description:
            column_desc = ColumnDesc(*column_desc)
            schema['properties'][column_desc.name] = schema_for_column(column_desc)
        return schema


def do_discover(connection):
    with connection.cursor() as cursor:
        cursor.execute('SHOW DATABASES')
        dbs = [row[0] for row in cursor.fetchall()]

    LOGGER.info("Databases are %s", dbs)
    for db in dbs:
        with connection.cursor() as cursor:
            # TODO: Cleanse the db name
            cursor.execute('USE {}'.format(db))
            cursor.execute('SHOW TABLES')
            tables = [row[0] for row in cursor.fetchall()]
            LOGGER.info('DB %s has tables %s', db, tables)
        for table in tables:
            pass


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    connection = open_connection(args.config)
    if args.discover:
        do_discover(connection)
    elif args.properties:
        print("I would do sync")
    else:
        LOGGER.info("No properties were selected")
