#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name

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


# TODO: Maybe put in common library. Not singer-python. singer-db-utils?
@attr.s
class StreamState(object):
    '''Represents the state for a single stream.

    The state for a stream consists of four properties:

      * tap_stream_id - the identifier for the stream
      * replication_key (optional, string) - name of the field used for
        bookmarking
      * replication_key_value (optional, string or int) - current value of
        the bookmark
      * version (int) - the version number of the stream.

    Use StreamState.from_dict(raw, catalog_entry) to build a StreamState
    based on the raw dict and the singer.catalog.CatalogEntry for the
    stream.

    '''
    tap_stream_id = attr.ib()
    replication_key = attr.ib(default=None)
    replication_key_value = attr.ib(default=None)
    version = attr.ib(default=None)


    def update(self, record):
        self.replication_key_value = record[self.replication_key]

    @classmethod
    def from_dict(cls, raw, catalog_entry):
        '''Builds a StreamState from a raw dictionary containing the entry for
        this stream in the state file, as well as a CatalogEntry.

        '''
        result = StreamState(catalog_entry.tap_stream_id)
        if catalog_entry.replication_key:
            result.replication_key = catalog_entry.replication_key
            if raw and raw.get('replication_key') == catalog_entry.replication_key:
                result.replication_key_value = raw['replication_key_value']
        if raw and 'version' in raw:
            result.version = raw['version']
        else:
            result.version = int(time.time() * 1000)
        return result


def replication_key_by_table(raw_selections):
    result = {}
    for stream_meta in raw_selections:
        if stream_meta.replication_key is not None:
            result[stream_meta.stream] = stream_meta.replication_key
    return result


# TODO: Maybe put in common library
@attr.s
class State(object):
    '''Represents the full state.

    Two properties:

      * current_stream - When the tap is in the middle of syncing a
        stream, this will be set to the tap_stream_id for that stream.
      * streams - List of StreamState objects, one for each selected
        stream.

    Use State.from_dict(raw, catalog) to build a StreamState based
    on the raw dict and the singer.catalog.Catalog.

    '''
    current_stream = attr.ib()
    streams = attr.ib()

    @classmethod
    def from_dict(cls, raw, catalog):
        current_stream = raw.get('current_stream')

        LOGGER.info('Building State from raw %s and catalog %s', raw, catalog.to_dict())
        stream_states = []
        for catalog_entry in catalog.streams:
            raw_stream_state = None
            for raw_stream in raw.get('streams', []):
                if raw_stream['tap_stream_id'] == catalog_entry.tap_stream_id:
                    raw_stream_state = raw_stream
            stream_states.append(StreamState.from_dict(raw_stream_state, catalog_entry))
        return State(current_stream, stream_states)

    def get_stream_state(self, tap_stream_id):
        for stream_state in self.streams:
            if stream_state.tap_stream_id == tap_stream_id:
                return stream_state
        msg = 'No state for stream {}, states are {}'.format(
            tap_stream_id, [s.tap_stream_id for s in self.streams])
        raise Exception(msg)

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

    result = Schema(inclusion=inclusion, selected=False)
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
        result.exclusiveMaximum = True
        result.maximum = 10 ** (c.numeric_precision - c.numeric_scale)
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        if 'unsigned' in c.column_type:
            result.minimum = 0
        else:
            result.exclusiveMinimum = True
            result.minimum = -10 ** (c.numeric_precision - c.numeric_scale)
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
                            selected=False,
                            properties={c.column_name: schema_for_column(c) for c in cols})
            entry = CatalogEntry(
                database=table_schema,
                table=table_name,
                stream=table_name,
                tap_stream_id=table_schema + '-' + table_name,
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


# TODO: Generalize this. Use tap_stream_id rather than database and table.
# Maybe make it a method on Catalog or CatalogEntry.
def desired_columns(selected, column_schemas):

    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in column_schemas.items():
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
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
                       not_selected_but_automatic)

    return selected.intersection(available).union(automatic)


def escape(string):
    if '`' in string:
        raise Exception("Can't escape identifier {} because it contains a backtick"
                        .format(string))
    return '`' + string + '`'


def sync_table(connection, db, table, columns, catalog_entry, state):
    if not columns:
        LOGGER.warning('There are no columns selected for table %s, skipping it', table)
        return

    stream_state = state.get_stream_state(catalog_entry.tap_stream_id)

    with connection.cursor() as cursor:
        escaped_db = escape(db)
        escaped_table = escape(table)
        escaped_columns = [escape(c) for c in columns]
        select = 'SELECT {} FROM {}.{}'.format(
            ','.join(escaped_columns),
            escaped_db,
            escaped_table)
        params = {}
        if stream_state.replication_key_value is not None:
            key = stream_state.replication_key
            value = stream_state.replication_key_value
            select += ' WHERE `{}` >= %(replication_key_value)s ORDER BY `{}` ASC'.format(key, key)
            params['replication_key_value'] = value
        elif stream_state.replication_key is not None:
            key = stream_state.replication_key
            select += ' ORDER BY `{}` ASC'.format(key)

        LOGGER.info('Running %s', select)
        cursor.execute(select, params)
        row = cursor.fetchone()
        rows_saved = 0

        activate_version_message = singer.ActivateVersionMessage(
            stream=catalog_entry.stream,
            version=stream_state.version
        )

        # If there's a replication key, we want to emit an
        # ACTIVATE_VERSION message at the beginning so the records show up
        # right away.
        if stream_state.replication_key:
            yield activate_version_message

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
                if stream_state.replication_key is not None:
                    stream_state.update(rec)
                yield singer.RecordMessage(
                    stream=catalog_entry.stream,
                    record=rec,
                    version=stream_state.version)
                if rows_saved % 1000 == 0:
                    yield state.make_state_message()
                row = cursor.fetchone()
        yield state.make_state_message()

        # If there is no replication key, we're doing "full table"
        # replication, and we need to activate this version at the end.
        if not stream_state.replication_key:
            yield activate_version_message

def generate_messages(con, catalog, raw_state):
    discovered_catalog = discover_catalog(con)
    state = State.from_dict(raw_state, catalog)

    streams = list(filter(lambda stream: stream.is_selected(), catalog.streams))
    LOGGER.info('%d streams total, %d are selected', len(catalog.streams), len(streams))
    LOGGER.info('State is %s', state.make_state_message())
    if state.current_stream:
        streams = dropwhile(lambda s: s.tap_stream_id != state.current_stream, streams)
 
    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams:
        state.current_stream = catalog_entry.tap_stream_id
        yield state.make_state_message()

        database = catalog_entry.database
        table = catalog_entry.table

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           catalog_entry.database, catalog_entry.table)
        discovered_column_schemas = discovered_table.schema.properties
        
        selected = set([k for k, v in catalog_entry.schema.properties.items()
                        if v.selected or k == catalog_entry.replication_key])

        columns = desired_columns(selected, discovered_column_schemas)
        out_schema = Schema(
            type='object',
            properties={col: discovered_column_schemas[col]
                        for col in columns})
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=out_schema.to_dict(),
            key_properties=catalog_entry.key_properties)
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database
            timer.tags['table'] = table
            for message in sync_table(con, database, table, columns, catalog_entry, state):
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
