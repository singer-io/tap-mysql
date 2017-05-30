
#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time
import collections
import itertools
import copy

import attr
import pendulum

import pymysql
import pymysql.constants.FIELD_TYPE as FIELD_TYPE

import singer
import singer.stats
from singer import utils



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
    "column_key"])

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

DATETIME_TYPES = set(['datetime', 'timestamp'])

class InputException(Exception):
    pass

@attr.s
class StreamState(object):
    stream = attr.ib()
    replication_key = attr.ib()
    replication_key_value = attr.ib()

    def update(self, record):
        self.replication_key_value = record[self.replication_key]


def replication_key_by_table(raw_selections):
    result = {}
    for stream in raw_selections['streams']:
        key = stream.get('replication_key')
        if key is not None:
            result[stream['stream']] = key
    return result


class State(object):
    def __init__(self, state, selections):
        self.current_stream = None
        self.streams = []

        current_stream = state.get('current_stream')
        if current_stream:
            self.current_stream = current_stream

        for selected_stream in selections['streams']:
            selected_rep_key = selected_stream.get('replication_key')
            if selected_rep_key:
                selected_stream_name = selected_stream['stream']
                stored_stream_state = None
                value = None
                for s in state.get('streams', []):
                    if s['stream'] == selected_stream_name:
                        stored_stream_state = s
                if stored_stream_state and stored_stream_state['replication_key'] == selected_rep_key: # pylint: disable=line-too-long
                    value = stored_stream_state['replication_key_value']
                stream_state = StreamState(
                    stream=selected_stream_name,
                    replication_key=selected_rep_key,
                    replication_key_value=value)
                self.streams.append(stream_state)


    def get_stream_state(self, stream):
        for stream_state in self.streams:
            if stream_state.stream == stream:
                return stream_state

    def make_state_message(self):
        result = {}
        if self.current_stream:
            result['current_stream'] = self.current_stream
        result['streams'] = [s.__dict__ for s in self.streams]
        return singer.StateMessage(value=result)

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

    elif t in DATETIME_TYPES:
        result['type'] = 'string'
        result['format'] = 'date-time'

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
                       table_rows
                  FROM information_schema.tables
                 WHERE table_schema = %s""",
                           (connection.db,))
        else:
            cursor.execute("""
                SELECT table_schema,
                       table_name,
                       table_rows
                  FROM information_schema.tables
                 WHERE table_schema NOT IN (
                          'information_schema',
                          'performance_schema',
                          'mysql')
            """)
        row_counts = {}
        for (db, table, rows) in cursor.fetchall():
            if db not in row_counts:
                row_counts[db] = {}
            row_counts[db][table] = rows

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
            cols = list(cols)
            (table_schema, table_name) = k

            stream = {
                'database': table_schema,
                'table': table_name,
                'key_properties': [c.column_name for c in cols if c.column_key == 'PRI'],
                'schema': {
                    'type': 'object',
                    'properties': {c.column_name: schema_for_column(c) for c in cols}
                },
            }
            if table_schema in row_counts and table_name in row_counts[table_schema]:
                stream['row_count'] = row_counts[table_schema][table_name]
            streams.append(stream)

        return {'streams': streams}


def do_discover(connection):
    json.dump(discover_schemas(connection), sys.stdout, indent=2)


def primary_key_columns(connection, db, table):
    '''Return a list of names of columns that are primary keys in the given
    table in the given db.'''
    with connection.cursor() as cur:
        select = """
            SELECT column_name
              FROM information_schema.columns
             WHERE column_key = 'pri'
              AND table_schema = %s
              AND table_name = %s
        """
        cur.execute(select, (db, table))
        return set([c[0] for c in cur.fetchall()])


def index_schema(schemas):
    '''Turns the discovered stream schemas into a nested map of column schemas
    indexed by database, table, and column name.

      schemas['streams'][i]['schema']['schemas']['column'] { the column schema }

    to

      result[db][table][column] { the column schema }'''

    result = {}

    for stream in schemas['streams']:

        database = stream['database']
        table = stream['table']
        if database not in result:
            result[database] = {}
        result[database][table] = {}

        for col_name, col_schema in stream['schema']['properties'].items():
            result[database][table][col_name] = col_schema

    return result


def remove_unwanted_columns(selected, indexed_schema, database, table):

    selected = set(selected)
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in indexed_schema[database][table].items():
        all_columns.add(column)
        inclusion = column_schema['inclusion']
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning('For database %s, table %s, columns %s were selected but are not supported. Skipping them.', # pylint: disable=line-too-long
                       database, table, selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning('For databasee %s, table %s, columns %s were selected but do not exist.',
                       database, table, selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning('For database %s, table %s, columns %s are primary keys but were not selected. Automatically adding them.', # pylint: disable=line-too-long
                       database, table, not_selected_but_automatic)

    keep = selected.intersection(available).union(automatic)
    remove = all_columns.difference(keep)
    for col in remove:
        del indexed_schema[database][table][col]


def sync_table(connection, db, table, columns, state):
    if not columns:
        LOGGER.warning('There are no columns selected for table %s, skipping it', table)
        return

    with connection.cursor() as cursor:
        # TODO: escape column names
        select = 'SELECT {} FROM {}.{}'.format(','.join(columns), db, table)
        params = {}
        stream_state = state.get_stream_state(table)
        if stream_state and stream_state.replication_key_value is not None:
            key = stream_state.replication_key
            value = stream_state.replication_key_value
            select += ' WHERE `{}` >= %(replication_key_value)s ORDER BY `{}` ASC'.format(key, key)
            params['replication_key_value'] = value
        elif stream_state:
            key = stream_state.replication_key
            select += ' ORDER BY `{}` ASC'.format(key)

        LOGGER.info('Running %s', select)
        cursor.execute(select, params)
        row = cursor.fetchone()
        counter = 0
        while row:
            counter += 1
            row_to_persist = ()
            for elem in row:
                if isinstance(elem, datetime.datetime):
                    row_to_persist += (pendulum.instance(elem).to_iso8601_string(),)
                else:
                    row_to_persist += (elem,)
            rec = dict(zip(columns, row_to_persist))
            if stream_state:
                stream_state.update(rec)
            yield singer.RecordMessage(stream=table, record=rec)
            if counter % 1000 == 0:
                yield state.make_state_message()
            row = cursor.fetchone()
        yield state.make_state_message()



def generate_messages(con, raw_selections, raw_state):
    indexed_schema = index_schema(discover_schemas(con))
    state = State(raw_state, raw_selections)

    for stream in raw_selections['streams']:
        if not stream.get('selected'):
            continue

        database = stream['database']
        table = stream['table']

        # TODO: How to handle a table that's missing
        if database not in indexed_schema:
            raise Exception('No database called {}'.format(database))
        if table not in indexed_schema[database]:
            raise Exception('No table called {} in database {}'.format(table, database))

        selected = [k for k, v in stream['schema']['properties'].items() if v.get('selected')]

        remove_unwanted_columns(selected, indexed_schema, database, table)
        schema = {
            'type': 'object',
            'properties': indexed_schema[database][table]
        }
        columns = schema['properties'].keys()
        yield singer.SchemaMessage(
            stream=table,
            schema=schema,
            key_properties=stream['key_properties'])
        for message in sync_table(con, database, table, columns, state):
            yield message


def do_sync(con, raw_selections, raw_state):
    with con.cursor() as cur:
        cur.execute('SET time_zone="+0:00"')
    for message in generate_messages(con, raw_selections, raw_state):
        singer.write_message(message)


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    connection = open_connection(args.config)

    if args.discover:
        do_discover(connection)
    elif args.properties:
        do_sync(connection, args.properties, args.state)
    else:
        LOGGER.info("No properties were selected")

# TODO: How to deal with primary keys for views
