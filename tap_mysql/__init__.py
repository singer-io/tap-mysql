
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
#         cursor.execute("""
#           SELECT table_schema, 
#                  table_name, 
#                  column_name, 
#                  data_type, 
#                  character_maximum_length, 
#                  numeric_precision, 
#                  numeric_scale, 
#                  datetime_precision, 
#                  column_type, 
#                  column_key from information_schema.columns
#             WHERE table_schema NOT IN (
#               'information_schema',
#               'performance_schema',
#               'mysql')
# """)
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
                  column_key from information_schema.columns
             WHERE table_schema = %s""",
                        (connection.db,))
        
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
        result = {}
        for db in data:
            for table in data[db]:
                result[table] = {'schema': data[db][table]}
        return result

def do_discover(connection):
    streams = discover_schemas(connection)
    print('hi')
    json.dump({'streams': streams}, sys.stdout, indent=2)


def primary_key_columns(connection, table):
    with connection.cursor() as cursor:
        curser.execute("SELECT column_name FROM %s WHERE column_key = 'PRI'")
        return [row[0] for row in cursor.fetchall()]


def selected_columns(selections, table):
    props = selections['streams'][table]['schema']['properties']
    return set([k for (k, v) in props.items() if v.get('selected')])

def automatic_col_names(schema):
    cols = set()
    for (k, v) in schema['properties'].items():
        if v.get('inclusion') == 'automatic':
            cols.add(k)
    return cols

def selected_col_names(schema):
    cols = set()
    for (k, v) in schema['properties'].items():
        if v.get('selected'):
            cols.add(k)
    return cols

def all_col_names(schema):
    return schema['properties'].keys()


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


def columns_to_include(connection, selections, table):
    cols = []
    schema = selections['streams'][table]['schema']
    schema_current = schema_for_table(connection, stream)

    cols_all = all_col_names(schema_current)
    cols_auto = automatic_col_names(schema_current)
    cols_selected = selected_col_names(schema_in).intersection(cols_all)

    return cols_selected.union(cols_auto)


def interpret_properties(selected_properties):
    '''Given a raw set of annotated schemas, returns a map from table name to
    set of columns selected. For every (table, column) where 

    selected_properties['streams'][table]['schema']['properties'][column]['selected']

    is true, there will be an entry in 

    result[table][column]

    '''
    result = {}
    if not isinstance(selected_properties, dict):
        raise InputException('properties must contain "streams" key')
    if not isinstance(selected_properties['streams'], dict):
        raise InputException('properties["streams"] must be a dictionary')

    for stream_name, stream_selections in selected_properties['streams'][stream]:
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
