#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,too-many-branches,invalid-name,duplicate-code,too-many-statements

import datetime
import collections
import itertools
from itertools import dropwhile
import copy
import os
import pendulum

import pymysql

import singer
import singer.metrics as metrics
import singer.schema

from singer import bookmarks
from singer import metadata
from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

import tap_mysql.sync_strategies.binlog as binlog
import tap_mysql.sync_strategies.common as common
import tap_mysql.sync_strategies.full_table as full_table
import tap_mysql.sync_strategies.incremental as incremental

from tap_mysql.connection import connect_with_backoff, MySQLConnection


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

    elif data_type == 'json':
        result.type = ['null', 'string']

    elif data_type == 'decimal':
        result.type = ['null', 'number']
        result.multipleOf = 10 ** (0 - c.numeric_scale)
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


def discover_catalog(mysql_conn, config):
    '''Returns a Catalog describing the structure of the database.'''


    filter_dbs_config = config.get('filter_dbs')


    if filter_dbs_config:
        filter_dbs_clause = ",".join(["'{}'".format(db)
                                         for db in filter_dbs_config.split(",")])

        table_schema_clause = "WHERE table_schema IN ({})".format(filter_dbs_clause)
    else:
        table_schema_clause = """
        WHERE table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'mysql'
        )"""

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("""
            SELECT table_schema,
                   table_name,
                   table_type,
                   table_rows
                FROM information_schema.tables
                {}
            """.format(table_schema_clause))

            table_info = {}

            for (db, table, table_type, rows) in cur.fetchall():
                if db not in table_info:
                    table_info[db] = {}

                table_info[db][table] = {
                    'row_count': rows,
                    'is_view': table_type == 'VIEW'
                }

            cur.execute("""
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
                    {}
                    ORDER BY table_schema, table_name
            """.format(table_schema_clause))

            columns = []
            rec = cur.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = cur.fetchone()

            entries = []
            for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
                cols = list(cols)
                (table_schema, table_name) = k
                schema = Schema(type='object',
                                properties={c.column_name: schema_for_column(c) for c in cols})
                md = create_column_metadata(cols)
                md_map = metadata.to_map(md)

                md_map = metadata.write(md_map,
                                        (),
                                        'database-name',
                                        table_schema)

                is_view = table_info[table_schema][table_name]['is_view']

                if table_schema in table_info and table_name in table_info[table_schema]:
                    row_count = table_info[table_schema][table_name].get('row_count')

                    if row_count is not None:
                        md_map = metadata.write(md_map,
                                                (),
                                                'row-count',
                                                row_count)

                    md_map = metadata.write(md_map,
                                            (),
                                            'is-view',
                                            is_view)

                column_is_key_prop = lambda c, s: (
                    c.column_key == 'PRI' and
                    s.properties[c.column_name].inclusion != 'unsupported'
                )

                key_properties = [c.column_name for c in cols if column_is_key_prop(c, schema)]

                if not is_view:
                    md_map = metadata.write(md_map,
                                            (),
                                            'table-key-properties',
                                            key_properties)

                entry = CatalogEntry(
                    table=table_name,
                    stream=table_name,
                    metadata=metadata.to_list(md_map),
                    tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                    schema=schema)

                entries.append(entry)

    return Catalog(entries)


def do_discover(mysql_conn, config):
    discover_catalog(mysql_conn, config).dump()


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


def log_engine(mysql_conn, catalog_entry):
    is_view = common.get_is_view(catalog_entry)
    database_name = common.get_database_name(catalog_entry)

    if is_view:
        LOGGER.info("Beginning sync for view %s.%s", database_name, catalog_entry.table)
    else:
        with connect_with_backoff(mysql_conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute("""
                    SELECT engine
                      FROM information_schema.tables
                     WHERE table_schema = %s
                       AND table_name   = %s
                """, (database_name, catalog_entry.table))

                row = cur.fetchone()

                if row:
                    LOGGER.info("Beginning sync for %s table %s.%s",
                                row[0],
                                database_name,
                                catalog_entry.table)


def is_valid_currently_syncing_stream(selected_stream, state):
    stream_metadata = metadata.to_map(selected_stream.metadata)
    replication_method = stream_metadata.get((), {}).get('replication-method')

    if replication_method != 'LOG_BASED':
        return True

    if replication_method == 'LOG_BASED' and binlog_stream_requires_historical(selected_stream, state):
        return True

    return False

def binlog_stream_requires_historical(catalog_entry, state):
    log_file = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'log_file')

    log_pos = singer.get_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'log_pos')

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    if (log_file and log_pos) and (not max_pk_values and not last_pk_fetched):
        return False

    return True


def resolve_catalog(discovered_catalog, streams_to_sync):
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           database_name, catalog_entry.table)
            continue

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.stream,
            table=catalog_entry.table,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            )
        ))

    return result


def get_non_binlog_streams(mysql_conn, catalog, config, state):
    '''Returns the Catalog of data we're going to sync for all SELECT-based
    streams (i.e. INCREMENTAL, FULL_TABLE, and LOG_BASED that require a historical
    sync). LOG_BASED streams that require a historical sync are inferred from lack
    of any state.

    Using the Catalog provided from the input file, this function will return a
    Catalog representing exactly which tables and columns that will be emitted
    by SELECT-based syncs. This is achieved by comparing the input Catalog to a
    freshly discovered Catalog to determine the resulting Catalog.

    The resulting Catalog will include the following any streams marked as
    "selected" that currently exist in the database. Columns marked as "selected"
    and those labled "automatic" (e.g. primary keys and replication keys) will be
    included. Streams will be prioritized in the following order:
      1. currently_syncing if it is SELECT-based
      2. any streams that do not have state
      3. any streams that do not have a replication method of LOG_BASED

    '''
    discovered = discover_catalog(mysql_conn, config)

    # Filter catalog to include only selected streams
    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method')
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)

        if not stream_state:
            if replication_method == 'LOG_BASED':
                LOGGER.info("LOG_BASED stream %s requires full historical sync", stream.tap_stream_id)

            streams_without_state.append(stream)
        elif stream_state and replication_method == 'LOG_BASED' and binlog_stream_requires_historical(stream, state):
            is_view = common.get_is_view(stream)

            if is_view:
                raise Exception("Unable to replicate stream({}) with binlog because it is a view.".format(stream.stream))

            LOGGER.info("LOG_BASED stream %s will resume its historical sync", stream.tap_stream_id)

            streams_with_state.append(stream)
        elif stream_state and replication_method != 'LOG_BASED':
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s.tap_stream_id == currently_syncing and is_valid_currently_syncing_stream(s, state),
            streams_with_state))

        non_currently_syncing_streams = list(filter(lambda s: s.tap_stream_id != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return resolve_catalog(discovered, streams_to_sync)


def get_binlog_streams(mysql_conn, catalog, config, state):
    discovered = discover_catalog(mysql_conn, config)

    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))
    binlog_streams = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method')
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)

        if replication_method == 'LOG_BASED' and not binlog_stream_requires_historical(stream, state):
            binlog_streams.append(stream)

    return resolve_catalog(discovered, binlog_streams)


def write_schema_message(catalog_entry, bookmark_properties=[]):
    key_properties = common.get_key_properties(catalog_entry)

    singer.write_message(singer.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=key_properties,
        bookmark_properties=bookmark_properties
    ))


def do_sync_incremental(mysql_conn, catalog_entry, state, columns, optional_limit=None):
    LOGGER.info("Stream %s is using incremental replication", catalog_entry.stream)

    md_map = metadata.to_map(catalog_entry.metadata)
    replication_key = md_map.get((), {}).get('replication-key')

    if not replication_key:
        raise Exception("Cannot use INCREMENTAL replication for table ({}) without a replication key.".format(catalog_entry.stream))

    write_schema_message(catalog_entry=catalog_entry,
                         bookmark_properties=[replication_key])

    if optional_limit:
        LOGGER.info("Incremental Stream %s is using an optional limit clause of %d", catalog_entry.stream, int(optional_limit))
        incremental.sync_table(mysql_conn, catalog_entry, state, columns, int(optional_limit))
    else:
        incremental.sync_table(mysql_conn, catalog_entry, state, columns)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync_historical_binlog(mysql_conn, config, catalog_entry, state, columns):
    binlog.verify_binlog_config(mysql_conn)

    is_view = common.get_is_view(catalog_entry)
    key_properties = common.get_key_properties(catalog_entry)

    if is_view:
        raise Exception("Unable to replicate stream({}) with binlog because it is a view.".format(catalog_entry.stream))

    log_file = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'log_file')

    log_pos = singer.get_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'log_pos')

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    if log_file and log_pos and max_pk_values:
        LOGGER.info("Resuming initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)
        full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)

    else:
        LOGGER.info("Performing initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)

        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'initial_binlog_complete',
                                      False)

        current_log_file, current_log_pos = binlog.fetch_current_log_file_and_pos(mysql_conn)
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'version',
                                      stream_version)

        if full_table.sync_is_resumable(mysql_conn, catalog_entry):
            # We must save log_file and log_pos across FULL_TABLE syncs when performing
            # a resumable full table sync
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_file',
                                          current_log_file)

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_pos',
                                          current_log_pos)

            full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)
        else:
            full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_file',
                                          current_log_file)

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_pos',
                                          current_log_pos)


def do_sync_full_table(mysql_conn, config, catalog_entry, state, columns):
    LOGGER.info("Stream %s is using full table replication", catalog_entry.stream)
    key_properties = common.get_key_properties(catalog_entry)

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)

    # Prefer initial_full_table_complete going forward
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_non_binlog_streams(mysql_conn, non_binlog_catalog, config, state):
    for catalog_entry in non_binlog_catalog.streams:
        columns = list(catalog_entry.schema.properties.keys())

        if not columns:
            LOGGER.warning('There are no columns selected for stream %s, skipping it.', catalog_entry.stream)
            continue

        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        md_map = metadata.to_map(catalog_entry.metadata)

        replication_method = md_map.get((), {}).get('replication-method')

        database_name = common.get_database_name(catalog_entry)

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = catalog_entry.table

            log_engine(mysql_conn, catalog_entry)

            if replication_method == 'INCREMENTAL':
                optional_limit = config.get('incremental_limit')
                do_sync_incremental(mysql_conn, catalog_entry, state, columns, optional_limit)
            elif replication_method == 'LOG_BASED':
                do_sync_historical_binlog(mysql_conn, config, catalog_entry, state, columns)
            elif replication_method == 'FULL_TABLE':
                do_sync_full_table(mysql_conn, config, catalog_entry, state, columns)
            else:
                raise Exception("only INCREMENTAL, LOG_BASED, and FULL TABLE replication methods are supported")

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_binlog_streams(mysql_conn, binlog_catalog, config, state):
    if binlog_catalog.streams:
        for stream in binlog_catalog.streams:
            write_schema_message(stream)

        with metrics.job_timer('sync_binlog') as timer:
            binlog.sync_binlog_stream(mysql_conn, config, binlog_catalog.streams, state)


def do_sync(mysql_conn, config, catalog, state):
    non_binlog_catalog = get_non_binlog_streams(mysql_conn, catalog, config, state)
    binlog_catalog = get_binlog_streams(mysql_conn, catalog, config, state)

    sync_non_binlog_streams(mysql_conn, non_binlog_catalog, config, state)
    sync_binlog_streams(mysql_conn, binlog_catalog, config, state)

def log_server_params(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        try:
            with open_conn.cursor() as cur:
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
            with open_conn.cursor() as cur:
                cur.execute('''
                show session status where Variable_name IN ('Ssl_version', 'Ssl_cipher')''')
                rows = cur.fetchall()
                mapped_row = dict(rows)
                LOGGER.info('Server SSL Parameters (blank means SSL is not active): ' +
                            '[ssl_version: %s], ' +
                            '[ssl_cipher: %s]',
                            mapped_row['Ssl_version'],
                            mapped_row['Ssl_cipher'])

        except pymysql.err.InternalError as e:
            LOGGER.warning("Encountered error checking server params. Error: (%s) %s", *e.args)

@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    #NB> this code will only work correctly when the local time is set to UTC because of calls to the  timestamp() method.
    os.environ['TZ'] = 'UTC'

    mysql_conn = MySQLConnection(args.config)
    log_server_params(mysql_conn)

    if args.discover:
        do_discover(mysql_conn, args.config)
    elif args.catalog:
        state = args.state or {}
        do_sync(mysql_conn, args.config, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = args.state or {}
        do_sync(mysql_conn, args.config, catalog, state)
    else:
        LOGGER.info("No properties were selected")

if __name__ == "__main__":
    main()
