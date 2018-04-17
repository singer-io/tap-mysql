#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,duplicate-code

import datetime
import collections
import itertools
from itertools import dropwhile
import copy

import pendulum

import pymysql
from pymysql.constants import CLIENT

import singer
import singer.metrics as metrics
import singer.schema

from tap_mysql.connection import MySQLConnection

from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from singer import metadata

import tap_mysql.sync_strategies.binlog as binlog
import tap_mysql.sync_strategies.full_table as full_table
import tap_mysql.sync_strategies.incremental as incremental

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

pymysql.converters.conversions[pendulum.Pendulum] = pymysql.converters.escape_datetime

def parse_internal_hostname(hostname):
    # special handling for google cloud
    if ":" in hostname:
        parts = hostname.split(":")
        if len(parts) == 3:
            return parts[0] + ":" + parts[2]
        return parts[0] + ":" + parts[1]

    return hostname


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

DATETIME_TYPES = set(['datetime', 'timestamp', 'date', 'time'])

def build_state(raw_state, catalog):
    LOGGER.info('Building State from raw state %s and catalog %s', raw_state, catalog.to_dict())

    state = {}

    currently_syncing = singer.get_currently_syncing(raw_state)
    if currently_syncing:
        state = singer.set_currently_syncing(state, currently_syncing)

    for catalog_entry in catalog.streams:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)


        replication_key = catalog_metadata.get((), {}).get('replication-key')

        if replication_key:

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key',
                                          replication_key)
            # Only keep the existing replication_key_value if the
            # replication_key hasn't changed.
            raw_replication_key = singer.get_bookmark(raw_state,
                                                      catalog_entry.tap_stream_id,
                                                      'replication_key')
            if raw_replication_key == replication_key:
                raw_replication_key_value = singer.get_bookmark(raw_state,
                                                                catalog_entry.tap_stream_id,
                                                                'replication_key_value')
                state = singer.write_bookmark(state,
                                              catalog_entry.tap_stream_id,
                                              'replication_key_value',
                                              raw_replication_key_value)

        # Persist any existing version, even if it's None
        if raw_state.get('bookmarks', {}).get(catalog_entry.tap_stream_id):
            raw_stream_version = singer.get_bookmark(raw_state,
                                                     catalog_entry.tap_stream_id,
                                                     'version')

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'version',
                                          raw_stream_version)
    return state


def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    data_type = c.data_type.lower()
    column_type = c.column_type.lower()

    inclusion = 'available'
    # We want to automatically include all primary key columns
    if c.column_key.lower() == 'pri':
        inclusion = 'automatic'

    result = Schema(inclusion=inclusion)

    if data_type == 'bit' or column_type.startswith('tinyint(1)'):
        result.type = ['null', 'boolean']

    elif data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        if 'unsigned' in c.column_type:
            result.minimum = 0
            result.maximum = 2 ** bits - 1
        else:
            result.minimum = 0 - 2 ** (bits - 1)
            result.maximum = 2 ** (bits - 1) - 1

    elif data_type in FLOAT_TYPES:
        result.type = ['null', 'number']

    elif data_type == 'decimal':
        result.type = ['null', 'number']
        result.exclusiveMaximum = True
        result.maximum = 10 ** (c.numeric_precision - c.numeric_scale)
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        if 'unsigned' in column_type:
            result.minimum = 0
        else:
            result.exclusiveMinimum = True
            result.minimum = -10 ** (c.numeric_precision - c.numeric_scale)
        return result

    elif data_type in STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported column type {}'.format(column_type))
    return result


def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'sql-datatype',
                               c.column_type.lower())

    return metadata.to_list(mdata)


def discover_catalog(connection):
    '''Returns a Catalog describing the structure of the database.'''

    with connection.cursor() as cursor:
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
               ORDER BY table_schema, table_name
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
            md = create_column_metadata(cols)
            entry = CatalogEntry(
                database=table_schema,
                table=table_name,
                stream=table_name,
                metadata=md,
                tap_stream_id=table_schema + '-' + table_name,
                schema=schema)
            column_is_key_prop = lambda c, s: (
                c.column_key == 'PRI' and
                s.properties[c.column_name].inclusion != 'unsupported'
            )
            key_properties = [c.column_name for c in cols if column_is_key_prop(c, schema)]
            md.append({'metadata': {'table-key-properties' : key_properties}, 'breadcrumb': ()})

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


def log_engine(connection, catalog_entry):
    if catalog_entry.is_view:
        LOGGER.info("Beginning sync for view %s.%s", catalog_entry.database, catalog_entry.table)
    else:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT engine
                  FROM information_schema.tables
                 WHERE table_schema = %s
                   AND table_name   = %s
            """, (catalog_entry.database, catalog_entry.table))

            row = cursor.fetchone()

            if row:
                LOGGER.info("Beginning sync for %s table %s.%s",
                            row[0],
                            catalog_entry.database,
                            catalog_entry.table)


def is_selected(stream):
    table_md = metadata.to_map(stream.metadata).get((), {})

    return table_md.get('selected') or stream.is_selected()


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
      * If the state has a currently_syncing, we will skip to that stream and
        drop all streams appearing before it in the catalog.
      * We will add any columns that were not selected but should be
        automatically included. For example, primary key columns and
        columns used as replication keys.

    '''
    discovered = discover_catalog(con)

    # Filter catalog to include only selected streams
    streams = list(filter(lambda stream: is_selected(stream), catalog.streams))

    # If the state says we were in the middle of processing a stream, skip
    # to that stream.
    currently_syncing = singer.get_currently_syncing(state)
    if currently_syncing:
        streams = dropwhile(lambda s: s.tap_stream_id != currently_syncing, streams)

    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered.get_stream(catalog_entry.tap_stream_id)
        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           catalog_entry.database, catalog_entry.table)
            continue
        selected = set([k for k, v in catalog_entry.schema.properties.items()
                        if v.selected or k == replication_key])

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.stream,
            database=catalog_entry.database,
            table=catalog_entry.table,
            is_view=catalog_entry.is_view,
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
        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        yield singer.StateMessage(value=copy.deepcopy(state))

        md_map = metadata.to_map(catalog_entry.metadata)

        replication_method = md_map.get((), {}).get('replication-method')
        replication_key = singer.get_bookmark(state,
                                              catalog_entry.tap_stream_id,
                                              'replication_key')

        if catalog_entry.is_view:
            key_properties = md_map.get((), {}).get('view-key-properties')
        else:
            key_properties = md_map.get((), {}).get('table-key-properties')

        # Emit a SCHEMA message before we sync any records
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=key_properties,
            bookmark_properties=replication_key
        )

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = catalog_entry.database
            timer.tags['table'] = catalog_entry.table

            log_engine(con, catalog_entry)

            if replication_method == 'INCREMENTAL':
                LOGGER.info("Stream %s is using incremental replication", catalog_entry.stream)

                for message in incremental.sync_table(con, catalog_entry, state):
                    yield message
            elif replication_method == 'LOG_BASED':
                LOGGER.info("Stream %s is using binlog replication", catalog_entry.stream)
                log_file = singer.get_bookmark(state,
                                               catalog_entry.tap_stream_id,
                                               'log_file')

                log_pos = singer.get_bookmark(state,
                                              catalog_entry.tap_stream_id,
                                              'log_pos')

                if log_file and log_pos:
                    for message in binlog.sync_table(con, catalog_entry, state):
                        yield message
                else:
                    LOGGER.info("Performing initial full table sync")
                    log_file, log_pos = binlog.fetch_current_log_file_and_pos(con)

                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'log_file',
                                                  log_file)

                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'log_pos',
                                                  log_pos)

                    for message in full_table.sync_table(con, catalog_entry, state):
                        yield message
            elif replication_method == 'FULL_TABLE':
                LOGGER.info("Stream %s is using full table replication", catalog_entry.stream)

                for message in full_table.sync_table(con, catalog_entry, state):
                    yield message

                # Clear the stream's version from the state so that subsequent invocations will
                # emit a distinct stream version.
                state = singer.write_bookmark(state, catalog_entry.tap_stream_id, 'version', None)
                yield singer.StateMessage(value=copy.deepcopy(state))
            else:
                raise Exception("only INCREMENTAL, LOG_BASED, and FULL TABLE replication methods are supported")

    # if we get here, we've finished processing all the streams, so clear
    # currently_syncing from the state and emit a state message.
    state = singer.set_currently_syncing(state, None)
    yield singer.StateMessage(value=copy.deepcopy(state))


def do_sync(con, catalog, state):
    for message in generate_messages(con, catalog, state):
        singer.write_message(message)


def log_server_params(con):
    try:
        with con.cursor() as cur:
            cur.execute('''
                SELECT VERSION() as version,
                    @@session.wait_timeout as wait_timeout,
                    @@session.innodb_lock_wait_timeout as innodb_lock_wait_timeout,
                    @@session.max_allowed_packet as max_allowed_packet,
                    @@session.interactive_timeout as interactive_timeout''')
            row = cur.fetchone()
            LOGGER.info('Server Parameters: ' +
                        'version: %s, ' +
                        'wait_timeout: %s, ' +
                        'innodb_lock_wait_timeout: %s, ' +
                        'max_allowed_packet: %s, ' +
                        'interactive_timeout: %s',
                        *row)
    except pymysql.err.InternalError as e:
        LOGGER.warning("Encountered error checking server params. Error: (%s) %s", *e.args)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    connection = MySQLConnection(args.config)
    warnings = []
    with connection.cursor() as cur:
        try:
            cur.execute('SET @@session.time_zone="+0:00"')
        except pymysql.err.InternalError as e:
            warnings.append('Could not set session.time_zone. Error: ({}) {}'.format(*e.args))

        try:
            cur.execute('SET @@session.wait_timeout=2700')
        except pymysql.err.InternalError as e:
            warnings.append('Could not set session.wait_timeout. Error: ({}) {}'.format(*e.args))

        try:
            cur.execute('SET @@session.innodb_lock_wait_timeout=2700')
        except pymysql.err.InternalError as e:
            warnings.append(
                'Could not set session.innodb_lock_wait_timeout. Error: ({}) {}'.format(*e.args)
                )

    if warnings:
        LOGGER.info(("Encountered non-fatal errors when configuring MySQL session that could "
                     "impact performance:"))
    for w in warnings:
        LOGGER.warning(w)

    log_server_params(connection)
    if args.discover:
        do_discover(connection)
    elif args.catalog:
        state = build_state(args.state, args.catalog)
        do_sync(connection, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = build_state(args.state, catalog)
        do_sync(connection, catalog, state)
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
