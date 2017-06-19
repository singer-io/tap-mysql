
#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time
import collections
import itertools
from itertools import dropwhile
import copy

import attr
import pendulum

import pymysql
import pymysql.constants.FIELD_TYPE as FIELD_TYPE

import singer
import singer.metrics as metrics
import singer.schema
from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "column_type",
    "column_key"])

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password'
]

LOGGER = singer.get_logger()


def open_connection(config):
    connection_args = {'host': config['host'],
                       'user': config['user'],
                       'password': config['password']}
    database = config.get('database')
    if database:
        connection_args['database'] = database
    return pymysql.connect(**connection_args)

STRING_TYPES = set([
    'char',
    'enum',
    'longtext',
    'mediumtext',
    'text',
    'varchar'
])

BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'smallint': 2,
    'mediumint': 3,
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
    for stream_meta in raw_selections:
        if stream_meta.replication_key is not None:
            result[stream_meta.stream] = stream_meta.replication_key
    return result


class State(object):
    def __init__(self, state, catalog):
        self.current_stream = None
        self.streams = []

        current_stream = state.get('current_stream')
        if current_stream:
            self.current_stream = current_stream
        for selected_stream in catalog.streams:

            selected_rep_key = selected_stream.replication_key
            if selected_rep_key:
                selected_stream_name = selected_stream.stream
                stored_stream_state = None
                value = None
                for s in state.get('streams', []):
                    if s['stream'] == selected_stream_name:
                        stored_stream_state = s
                if stored_stream_state and stored_stream_state['replication_key'] == selected_rep_key:  # pylint: disable=line-too-long
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

    # We want to automatically include all primary key columns
    if c.column_key == 'PRI':
        inclusion = 'automatic'
    else:
        inclusion = 'available'

    result = Schema(inclusion=inclusion)
    result.sqlDatatype = c.column_type

    if t in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[t] * 8
        if 'unsigned' in c.column_type:
            result.minimum = 0
            result.maximum = 2 ** bits
        else:
            result.minimum = 0 - 2 ** (bits - 1)
            result.maximum = 2 ** (bits - 1) - 1

    elif t in FLOAT_TYPES:
        result.type = ['null', 'number']

    elif t == 'decimal':
        result.type = ['null', 'number']
        result.exclusiveMaximum = 10 ** (c.numeric_precision - c.numeric_scale)
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        if 'unsigned' in c.column_type:
            result.minimum = 0
        else:
            result.exclusiveMinimum = -10 ** (c.numeric_precision - c.numeric_scale)
        return result

    elif t in STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length

    elif t in DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        sqlDatatype=c.column_type,
                        description='Unsupported column type {}'.format(c.column_type))
    return result



def discover_catalog(connection):

    with connection.cursor() as cursor:
        if connection.db:
            cursor.execute("""
                SELECT table_schema,
                       table_name,
                       table_type,
                       table_rows
                  FROM information_schema.tables
                 WHERE table_schema = %s""",
                           (connection.db,))
        else:
            cursor.execute("""
                SELECT table_schema,
                       table_name,
                       table_type,
                       table_rows
                  FROM information_schema.tables
                 WHERE table_schema NOT IN (
                          'information_schema',
                          'performance_schema',
                          'mysql')
            """)
        table_info = {}

        for (db, table, table_type, rows) in cursor.fetchall():
            if db not in table_info:
                table_info[db] = {}
            table_info[db][table] = {
                'row_count': rows,
                'is_view': table_type == 'VIEW'
            }

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

        entries = []
        for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
            cols = list(cols)
            (table_schema, table_name) = k
            schema = Schema(type='object',
                            properties={c.column_name: schema_for_column(c) for c in cols})
            entry = CatalogEntry(
                database=table_schema,
                table=table_name,
                schema=schema)
            key_properties = [c.column_name for c in cols if c.column_key == 'PRI']
            if key_properties:
                entry.key_properties = key_properties

            if table_schema in table_info and table_name in table_info[table_schema]:
                entry.row_count = table_info[table_schema][table_name]['row_count']
                entry.is_view = table_info[table_schema][table_name]['is_view']
            entries.append(entry)

        return Catalog(entries)


def do_discover(connection):
    discover_catalog(connection).dump()

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


def index_catalog(catalog):
    '''Turns the discovered stream schemas into a nested map of column schemas
    indexed by database, table, and column name.

      schemas['streams'][i]['schema']['schemas']['column'] { the column schema }

    to

      result[db][table][column] { the column schema }'''

    result = {}

    for stream in catalog.streams:
        if stream.database not in result:
            result[stream.database] = {}
        result[stream.database][stream.table] = {}

        for col_name, col_schema in stream.schema.properties.items():
            result[stream.database][stream.table][col_name] = col_schema

    return result


def remove_unwanted_columns(selected, indexed_schema, database, table):

    selected = set(selected)
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in indexed_schema[database][table].items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
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
        LOGGER.warning('For database %s, table %s, columns %s were selected but are not supported. Skipping them.',  # pylint: disable=line-too-long
                       database, table, selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning('For databasee %s, table %s, columns %s were selected but do not exist.',
                       database, table, selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning('For database %s, table %s, columns %s are primary keys but were not selected. Automatically adding them.',  # pylint: disable=line-too-long
                       database, table, not_selected_but_automatic)

    keep = selected.intersection(available).union(automatic)
    remove = all_columns.difference(keep)
    for col in remove:
        del indexed_schema[database][table][col]

def escape(string):
    if '`' in string:
        raise Exception("Can't escape identifier {} because it contains a backtick"
                        .format(string))
    return '`' + string + '`'

def sync_table(connection, db, table, columns, state):
    if not columns:
        LOGGER.warning('There are no columns selected for table %s, skipping it', table)
        return

    with connection.cursor() as cursor:
        escaped_db = escape(db)
        escaped_table = escape(table)
        escaped_columns = [escape(c) for c in columns]
        select = 'SELECT {} FROM {}.{}'.format(
            ','.join(escaped_columns),
            escaped_db,
            escaped_table)
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
        rows_saved = 0

        with metrics.record_counter(None) as counter:
            counter.tags['database'] = db
            counter.tags['table'] = table
            while row:
                counter.increment()
                rows_saved += 1
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
                if rows_saved % 1000 == 0:
                    yield state.make_state_message()
                row = cursor.fetchone()
        yield state.make_state_message()


def generate_messages(con, catalog, raw_state):
    indexed_schema = index_catalog(discover_catalog(con))
    state = State(raw_state, catalog)

    streams = list(filter(lambda stream: stream.is_selected(), catalog.streams))
    LOGGER.info('%d streams total, %d are selected', len(catalog.streams), len(streams))
    if state.current_stream:
        streams = dropwhile(lambda s: s.stream != state.current_stream, streams)

    for stream in streams:
        state.current_stream = stream.stream
        yield state.make_state_message()

        database = stream.database
        table = stream.table

        # TODO: How to handle a table that's missing
        if database not in indexed_schema:
            raise Exception('No database called {}'.format(database))
        if table not in indexed_schema[database]:
            raise Exception('No table called {} in database {}'.format(table, database))

        selected = [k for k, v in stream.schema.properties.items() if v.selected]

        remove_unwanted_columns(selected, indexed_schema, database, table)
        schema = Schema(
            type='object',
            properties=indexed_schema[database][table])
        columns = schema.properties.keys() # pylint: disable=no-member
        yield singer.SchemaMessage(
            stream=table,
            schema=schema.to_dict(),
            key_properties=stream.key_properties)
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database
            timer.tags['table'] = table
            for message in sync_table(con, database, table, columns, state):
                yield message
    state.current_stream = None
    yield state.make_state_message()


def do_sync(con, raw_selections, raw_state):
    with con.cursor() as cur:
        cur.execute('SET time_zone="+0:00"')
    for message in generate_messages(con, raw_selections, raw_state):
        singer.write_message(message)


def log_server_params(con):
    with con.cursor() as cur:
        cur.execute('''
            SELECT VERSION() as version,
                   @@SESSION.wait_timeout as wait_timeout,
                   @@SESSION.innodb_lock_wait_timeout as innodb_lock_wait_timeout,
                   @@SESSION.max_allowed_packet as max_allowed_packet,
                   @@SESSION.interactive_timeout as interactive_timeout''')
        row = cur.fetchone()
        LOGGER.info('Server Parameters: ' +
                    'version: %s, ' +
                    'wait_timeout: %s, ' +
                    'innodb_lock_wait_timeout: %s, ' +
                    'max_allowed_packet: %s, ' +
                    'interactive_timeout: %s',
                    *row)


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    connection = open_connection(args.config)
    log_server_params(connection)
    if args.discover:
        do_discover(connection)
    elif args.catalog:
        do_sync(connection, args.catalog, args.state)
    elif args.properties:
        do_sync(connection, Catalog.from_dict(args.properties), args.state)
    else:
        LOGGER.info("No properties were selected")
