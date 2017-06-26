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
    '''Returns an open connection to the database based on the config.'''
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


# TODO: Maybe put in a singer-db-utils library.
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
        '''Updates replication key value based on the value in the record.'''
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


# TODO: Maybe put in a singer-db-utils library.
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
    '''Returns the Schema object for the given Column.'''
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
    '''Returns a Catalog describing the structure of the database.'''

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


# TODO: Maybe put in a singer-db-utils library.
def desired_columns(selected, table_schema):

    '''Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    '''
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
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


def sync_table(connection, catalog_entry, state):
    columns = list(catalog_entry.schema.properties.keys())
    if not columns:
        LOGGER.warning(
            'There are no columns selected for table %s, skipping it',
            catalog_entry.table)
        return

    stream_state = state.get_stream_state(catalog_entry.tap_stream_id)

    with connection.cursor() as cursor:
        escaped_db = escape(catalog_entry.database)
        escaped_table = escape(catalog_entry.table)
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
            counter.tags['database'] = catalog_entry.database
            counter.tags['table'] = catalog_entry.table
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

# TODO: Maybe put in a singer-db-utils library.
def resolve_catalog(con, catalog, state):
    '''Returns the Catalog of data we're going to sync.

    Takes the Catalog we read from the input file and turns it into a
    Catalog representing exactly which tables and columns we're going to
    emit in this process. Compares the input Catalog to a freshly
    discovered Catalog to determine the resulting Catalog. Returns a new
    instance. The result may differ from the input Catalog in the
    following ways:

      * It will only include streams marked as "selected".
      * We will remove any streams and columns that were selected but do
        not actually exist in the database right now.
      * If the state has a current_stream, we will skip to that stream and
        drop all streams appearing before it in the catalog.
      * We will add any columns that were not selected but should be
        automatically included. For example, primary key columns and
        columns used as replication keys.

    '''
    discovered = discover_catalog(con)

    # Filter catalog to include only selected streams
    streams = list(filter(lambda stream: stream.is_selected(), catalog.streams))

    # If the state says we were in the middle of processing a stream, skip
    # to that stream.
    if state.current_stream:
        streams = dropwhile(lambda s: s.tap_stream_id != state.current_stream, streams)

    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams:

        discovered_table = discovered.get_stream(catalog_entry.tap_stream_id)
        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           catalog_entry.database, catalog_entry.table)
            continue
        selected = set([k for k, v in catalog_entry.schema.properties.items()
                        if v.selected or k == catalog_entry.replication_key])

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            key_properties=catalog_entry.key_properties,
            stream=catalog_entry.stream,
            database=catalog_entry.database,
            table=catalog_entry.table,
            replication_key=catalog_entry.replication_key,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            )
        ))

    return result


def generate_messages(con, catalog, state):
    catalog = resolve_catalog(con, catalog, state)

    for catalog_entry in catalog.streams:
        state.current_stream = catalog_entry.tap_stream_id

        # Emit a STATE message to indicate that we've started this stream
        yield state.make_state_message()

        # Emit a SCHEMA message before we sync any records
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=catalog_entry.key_properties)

        # Emit a RECORD message for each record in the result set
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = catalog_entry.database
            timer.tags['table'] = catalog_entry.table
            for message in sync_table(con, catalog_entry, state):
                yield message

    # If we get here, we've finished processing all the streams, so clear
    # current_stream from the state and emit a STATE message.
    state.current_stream = None
    yield state.make_state_message()


def do_sync(con, catalog, state):
    with con.cursor() as cur:
        cur.execute('SET time_zone="+0:00"')
    for message in generate_messages(con, catalog, state):
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
        state = State.from_dict(args.state, args.catalog)
        do_sync(connection, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = State.from_dict(args.state, catalog)
        do_sync(connection, catalog, state)
    else:
        LOGGER.info("No properties were selected")
