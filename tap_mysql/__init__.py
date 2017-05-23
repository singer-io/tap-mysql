
#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time
import collections
import itertools

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

    # We want to automatically include all primary key columns
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
        # TODO: What about unsigned decimals?
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


        columns = []
        rec = cursor.fetchone()
        while rec is not None:
            columns.append(Column(*rec))
            rec = cursor.fetchone()

        streams = []
        for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
            (table_schema, table_name) = k
            streams.append({
                'database': table_schema,
                'table': table_name,
                'schema': {
                    'type': 'object',
                    'properties': {c.column_name: schema_for_column(c) for c in cols}
                }
            })
        return {'streams': streams}


def do_discover(connection):
    json.dump(discover_schemas(connection), sys.stdout, indent=2)


def primary_key_columns(connection, table):
    '''Return a list of names of columns that are primary keys in the given table'''
    with connection.cursor() as cur:
        select = """
            SELECT column_name
              FROM information_schema.columns
             WHERE column_key = 'pri'
              AND table_schema = %s
              AND table_name = %s
        """
        cur.execute(select, (connection.db, table))
        return set([c[0] for c in cur.fetchall()])


def translate_selected_properties(properties):
    '''Turns the raw annotated schemas input into a map a map from table name
    to set of columns selected. For every (table, column) tuple where

      properties['streams'][i]['schema']['properties'][column]['selected']

    is true, there will be an entry in 

      result[db][table][column]

    '''
    result = {}
    if not isinstance(properties, dict):
        raise InputException('properties must contain "streams" key')
    if not isinstance(properties['streams'], list):
        raise InputException('properties["streams"] must be a list')

    for i, stream in enumerate(properties['streams']):
        if not isinstance(stream, dict):
            raise InputException('streams[{}] must be a dictionary'.format(i))
        if not stream.get('selected'):
            continue

        database = stream['database']
        table = stream['table']
        if database not in result:
            result[database] = {}
        result[database][table] = set()

        path = 'streams.' + str(i) + '.schema'
        schema = stream.get('schema')
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

        for col_name, col_schema in props.items():
            if not isinstance(col_schema, dict):
                raise InputException(path + '.' + col_name + ' must be a dictionary')
            if col_schema.get('selected'):
                result[database][table].add(col_name)

    return result


def columns_to_select(connection, table, user_selected_columns):
    pks = primary_key_columns(connection, table)
    pks_to_add = pks.difference(user_selected_columns)
    if pks_to_add:
        LOGGER.info('For table %s, columns %s are primary keys but were not selected. Automatically adding them.',
                    table, pks_to_add)
    return user_selected_columns.union(pks)


def sync_table(connection, table, selected_columns):
    columns = columns_to_select(connection, table, selected_columns)

    if not columns:
        LOGGER.warn('There are no columns selected for table %s, skipping it', table)
        return
     
    with connection.cursor() as cursor:
        # TODO: Escape the columns
        select = 'SELECT {} FROM {}'.format(','.join(columns), table)
        LOGGER.info('Running %s', select)
        cursor.execute(select)
        row = cursor.fetchone()
        while row:
            rec = dict(zip(columns, row))
            singer.write_record(table, rec)
            row = cursor.fetchone()

def do_sync(config, raw_selections):
    cooked_selections = translate_selected_properties(raw_selections)

    for db in cooked_selections:
        con = pymysql.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=db)
        try:
            for table, columns in cooked_selections[db].items():
                sync_table(con, table, columns)
        finally:
            con.close()


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    connection = open_connection(args.config)
    if args.discover:
        do_discover(connection)
    elif args.properties:
        do_sync(args.config, args.properties)
    else:
        LOGGER.info("No properties were selected")

# TODO: How to deal with primary keys for views
