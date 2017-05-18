
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

@attr.s
class Column(object):

    name = attr.ib(default=None)
    type_code = attr.ib(default=None)
    display_size = attr.ib(default=None)
    internal_size = attr.ib(default=None)
    precision = attr.ib(default=None)
    scale = attr.ib(default=None)
    null_ok = attr.ib(default=None)

    sql_datatype = attr.ib(default=None)
    key = attr.ib(default=None)
    

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
    
    if c.type_code == FIELD_TYPE.TINY:
        return {
            'type': 'integer',
            }
    elif c.type_code == FIELD_TYPE.SHORT:
        return {
            'type': 'integer',
            }
    elif c.type_code == FIELD_TYPE.LONG:
        return {
            'type': 'integer',
            }
    elif c.type_code == FIELD_TYPE.FLOAT:
        return {
            'type': 'number'
        }
    elif c.type_code == FIELD_TYPE.DOUBLE:
        return {
            'type': 'number'
        }
    elif c.type_code == FIELD_TYPE.LONGLONG:
        return {
            'type': 'integer',
            }    
    elif c.type_code == FIELD_TYPE.INT24:
        return {
            'type': 'integer',
            }    
    elif c.type_code == FIELD_TYPE.NEWDECIMAL:
        return {
            'type': 'number',
            'exclusiveMaximum': 10 ** (c.precision - c.scale),
            'multipleOf': 10 ** (0 - c.scale),
            }
    else:
        return {
            'inclusion': 'unsupported',
            'description': 'Unsupported column type {}'.format(c.sql_datatype)
        }

    
def schema_for_table(connection, table):
    schema = {
        'type': 'object',
        'properties': {}
    }

    columns = {}
    with connection.cursor() as cursor:
        cursor.execute('SHOW columns FROM {}'.format(table))
        
        for c in cursor.fetchall():
            (name, datatype, _, _, _, _) = c
            column = Column()
            column.name = name
            column.sql_datatype = datatype
            columns[name] = column

    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM {} LIMIT 1'.format(table))
        for description in cursor.description:

            # Destructure column desc
            (name, type_code, display_size, internal_size,
             precision, scale, null_ok) = description
            col = columns[name]
            col.type_code = type_code
            col.display_size = display_size
            col.internal_size = internal_size
            col.precision = precision
            col.scale = scale
            col.null_ok = null_ok
            
            schema['properties'][col.name] = schema_for_column(col)

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
