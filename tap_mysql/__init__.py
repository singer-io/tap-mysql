
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

Column = collections.namedtuple('Column', [
    "table_schema", 
    "table_name", 
    "column_name", 
    "data_type", 
    "character_maximum_length", 
    "numeric_precision", 
    "numeric_scale", 
    "datetime_precision", 
    "column_type", 
    "column_key"]
)

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

STRING_TYPES = set([
    'char',
    'enum',
    'longtext',
    'mediumtext',
    'text',
    'varchar'
])

BYTES_FOR_INTEGER_TYPE = {
    'tinyint' : 1,
    'smallint': 2,
    'mediumint' : 3,
    'int': 4,
    'bigint': 8
}

FLOAT_TYPES = set(['float', 'double'])

class InputException(Exception):
    pass

def schema_for_column(c):

    t = c.data_type

    result = {}

    if c.column_key == 'PRI':
        result['inclusion'] = 'automatic'
    else:
        result['inclusion'] = 'available'
    
    if t in BYTES_FOR_INTEGER_TYPE:
        result['type'] = 'integer'
        bits = BYTES_FOR_INTEGER_TYPE[t] * 8
        if 'unsigned' in c.column_type:
            result['minimum'] = 0
            result['maximum'] = 2 ** bits
        else:
            result['minimum'] = 0 - 2 ** (bits - 1)
            result['maximum'] = 2 ** (bits - 1) - 1

    elif t in FLOAT_TYPES:
        result['type'] = 'number'
    elif t == 'decimal':
        result['type'] = 'number'
        result['exclusiveMaximum'] = 10 ** (c.numeric_precision - c.numeric_scale)
        result['multipleOf'] = 10 ** (0 - c.numeric_scale)
    elif t in STRING_TYPES:
        result['type'] = 'string'
        result['maxLength'] = c.character_maximum_length
    else:
        result['inclusion'] = 'unsupported'
        result['description'] = 'Unsupported column type {}'.format(c.column_type)
    return result

    
def discover_schemas(connection):
    
    with connection.cursor() as cursor:

        if connection.db:
            cursor.execute("""
                SELECT table_schema, 
                       table_name, 
                       column_name, 
                       data_type, 
                       character_maximum_length, 
                       numeric_precision, 
                       numeric_scale, 
                       datetime_precision, 
                       column_type, 
                       column_key
                  FROM information_schema.columns
                 WHERE table_schema = %s""",
                           (connection.db,))
        else:
            cursor.execute("""
                SELECT table_schema, 
                       table_name, 
                       column_name, 
                       data_type, 
                       character_maximum_length, 
                       numeric_precision, 
                       numeric_scale, 
                       datetime_precision, 
                       column_type, 
                       column_key
                  FROM information_schema.columns
                 WHERE table_schema NOT IN (
                          'information_schema',
                          'performance_schema',
                          'mysql')
            """)

        data = {}
        rec = cursor.fetchone()
        while rec is not None:
            col = Column(*rec)
            db = col.table_schema
            table = col.table_name
            if db not in data:
                data[db] = {}
            if table not in data[db]:
                data[db][table] = {
                    'type': 'object',
                    'properties': {}
                }
            data[db][table]['properties'][col.column_name] = schema_for_column(col)

            rec = cursor.fetchone()
        result = {'streams': {}}
        for db in data:
            for table in data[db]:
                result['streams'][table] = {'schema': data[db][table]}
        return result

def do_discover(connection):
    json.dump(discover_schemas(connection), sys.stdout, indent=2)


def primary_key_columns(connection, db, table):
    with connection.cursor() as cur:
        cur.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE column_key = 'pri'
              AND table_schema = ?
              AND table_name = ?
        """
        (db, table))
        return set([c.column_name for c in cur.fetchall()])


def translate_selected_properties(properties):
    '''Turns the raw annotated schemas input into a map a map from table name
    to set of columns selected. For every (table, column) tuple where

      properties['streams'][table]['schema']['properties'][column]['selected']

    is true, there will be an entry in 

      result[table][column]

    '''
    result = {}
    if not isinstance(properties, dict):
        raise InputException('properties must contain "streams" key')
    if not isinstance(properties['streams'], dict):
        raise InputException('properties["streams"] must be a dictionary')

    for stream_name, stream_selections in properties['streams'].items():
        if not isinstance(stream_selections, dict):
            raise InputException('streams.{} must be a dictionary'.format(stream_name))
        if not stream_selections.get('selected'):
            continue
        result[stream_name] = set()

        path = 'streams.' + stream_name + '.schema'
        schema = stream_selections.get('schema')
        if not schema:
            raise InputException(path + ' not defined')
        if not isinstance(schema, dict):
            raise InputException(path + ' must be a dictionary')

        path += '.properties'
        props = schema.get('properties')
        if not props:
            raise InputException(path + ' not defined')
        if not isinstance(props, dict):
            raise InputException(path + ' must be a dictionary')

        cols = set()
        for col_name, col_schema in props.items():
            if not isinstance(col_schema, dict):
                raise InputException(path + '.' + col_name + ' must be a dictionary')
            if col_schema.get('selected'):
                cols.add(col_name)
        result[stream_name] = cols
    return result


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    connection = open_connection(args.config)
    if args.discover:
        do_discover(connection)
    elif args.properties:
        do_sync(connection, args.properties)
    else:
        LOGGER.info("No properties were selected")
