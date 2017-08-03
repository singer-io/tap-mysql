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
import ssl

import attr
import pendulum

import pymysql
from pymysql.constants import CLIENT

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

CONNECT_TIMEOUT_SECONDS = 300
READ_TIMEOUT_SECONDS = 3600

# We need to hold onto this for self-signed SSL
match_hostname = ssl.match_hostname

pymysql.converters.conversions[pendulum.Pendulum] = pymysql.converters.escape_datetime

def parse_internal_hostname(hostname):
    # special handling for google cloud
    if ":" in hostname:
        parts = hostname.split(":")
        if len(parts) == 3:
            return parts[0] + ":" + parts[2]
        return parts[0] + ":" + parts[1]

    return hostname


def open_connection(config):
    # Google Cloud's SSL involves a self-signed certificate. This certificate's
    # hostname matches the form {instance}:{box}. The hostname displayed in the
    # Google Cloud UI is of the form {instance}:{region}:{box} which
    # necessitates the "parse_internal_hostname" function to get the correct
    # hostname to match.
    # The "internal_hostname" config variable allows for matching the SSL
    # against a host that doesn't match the host we are connecting to. In the
    # case of Google Cloud, we will be connecting to an IP, not the hostname
    # the SSL certificate expects.
    # The "ssl.match_hostname" function is patched to check against the
    # internal hostname rather than the host of the connection. In the event
    # that the connection fails, the patch is reverted by reassigning the
    # patched out method to it's original spot.

    args = {
        "user": config["user"],
        "password": config["password"],
        "host": config["host"],
        "port": int(config["port"]),
        "cursorclass": pymysql.cursors.SSCursor,
        "connect_timeout": CONNECT_TIMEOUT_SECONDS,
        "read_timeout": READ_TIMEOUT_SECONDS,
    }

    if config.get("database"):
        args["database"] = config["database"]

    # Attempt self-signed SSL if config vars are present
    if config.get("ssl_ca") and config.get("ssl_cert") and config.get("ssl_key"):
        try:
            LOGGER.info("Attempting SSL connection with custom ca")
            ssl_arg = {
                "ca": config["ssl_ca"],
                "cert": config["ssl_cert"],
                "key": config["ssl_key"],
            }

            # override match hostname for google cloud
            if config.get("internal_hostname"):
                parsed_hostname = parse_internal_hostname(config["internal_hostname"])
                ssl.match_hostname = lambda cert, hostname: match_hostname(cert, parsed_hostname)

            return pymysql.connect(ssl=ssl_arg, **args)
        except: # pylint: disable=bare-except
            ssl.match_hostname = match_hostname
            LOGGER.error("SSL connection with custom ca failed.")

    # Attempt SSL if SSL is enabled
    if config.get("ssl", False):
        try:
            LOGGER.info("Attempting SSL connection")
            conn = pymysql.Connection(defer_connect=True, **args)
            conn.ssl = True
            conn.ctx = ssl.create_default_context()
            conn.ctx.check_hostname = False
            conn.ctx.verify_mode = ssl.CERT_NONE
            conn.client_flag |= CLIENT.SSL
            conn.connect()
            return conn
        except: # pylint: disable=bare-except
            LOGGER.error("SSL connection failed")

    # Attempt unencrypted connection
    try:
        LOGGER.info("Attempting connection")
        return pymysql.connect(**args)
    except: # pylint: disable=bare-except
        LOGGER.error("Connection failed")


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
        if catalog_entry.replication_key:
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key',
                                          catalog_entry.replication_key)

            # Only keep the existing replication_key_value if the
            # replication_key hasn't changed.
            raw_replication_key = singer.get_bookmark(raw_state,
                                                      catalog_entry.tap_stream_id,
                                                      'replication_key')
            if raw_replication_key == catalog_entry.replication_key:
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
    t = c.data_type

    inclusion = 'available'
    # We want to automatically include all primary key columns
    if c.column_key == 'PRI':
        inclusion = 'automatic'

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

    elif t == 'bit':
        result.type = ['null', 'boolean']

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        sqlDatatype=c.column_type,
                        description='Unsupported column type {}'.format(c.column_type))
    return result


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

def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version

def row_to_singer_record(stream, version, row, columns):
    row_to_persist = ()
    for elem in row:
        if isinstance(elem, datetime.datetime):
            row_to_persist += (elem.isoformat() + '+00:00',)

        elif isinstance(elem, datetime.date):
            row_to_persist += (elem.isoformat() + 'T00:00:00+00:00',)

        elif isinstance(elem, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + elem
            row_to_persist += (timedelta_from_epoch.isoformat() + '+00:00',)

        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False and anything else as True
            boolean_representation = elem != b'\x00'
            row_to_persist += (boolean_representation,)

        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=stream,
        record=rec,
        version=version)

def sync_table(connection, catalog_entry, state):
    columns = list(catalog_entry.schema.properties.keys())
    if not columns:
        LOGGER.warning(
            'There are no columns selected for table %s, skipping it',
            catalog_entry.table)
        return

    with connection.cursor() as cursor:
        escaped_db = escape(catalog_entry.database)
        escaped_table = escape(catalog_entry.table)
        escaped_columns = [escape(c) for c in columns]
        select = 'SELECT {} FROM {}.{}'.format(
            ','.join(escaped_columns),
            escaped_db,
            escaped_table)
        params = {}
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value')
        replication_key = singer.get_bookmark(state,
                                              catalog_entry.tap_stream_id,
                                              'replication_key')

        bookmark_is_empty = state.get('bookmarks', {}).get(catalog_entry.tap_stream_id) is None

        stream_version = get_stream_version(catalog_entry.tap_stream_id, state)
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'version',
                                      stream_version)

        activate_version_message = singer.ActivateVersionMessage(
            stream=catalog_entry.stream,
            version=stream_version
        )

        # If there's a replication key, we want to emit an ACTIVATE_VERSION
        # message at the beginning so the records show up right away. If
        # there's no bookmark at all for this stream, assume it's the very
        # first replication. That is, clients have never seen rows for this
        # stream before, so they can immediately acknowledge the present
        # version.
        if replication_key or bookmark_is_empty:
            yield activate_version_message

        if replication_key_value is not None:
            if catalog_entry.schema.properties[replication_key].format == 'date-time':
                replication_key_value = pendulum.parse(replication_key_value)
                select += ' WHERE `{}` >= %(replication_key_value)s ORDER BY `{}` ASC'.format(
                    replication_key,
                    replication_key)
                params['replication_key_value'] = replication_key_value
        elif replication_key is not None:
            select += ' ORDER BY `{}` ASC'.format(replication_key)

        query_string = cursor.mogrify(select, params)
        LOGGER.info('Running %s', query_string)
        cursor.execute(select, params)
        row = cursor.fetchone()
        rows_saved = 0

        with metrics.record_counter(None) as counter:
            counter.tags['database'] = catalog_entry.database
            counter.tags['table'] = catalog_entry.table
            while row:
                counter.increment()
                rows_saved += 1
                record_message = row_to_singer_record(catalog_entry.stream,
                                                      stream_version,
                                                      row,
                                                      columns)
                yield record_message
                if replication_key is not None:
                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'replication_key_value',
                                                  record_message.record[replication_key])
                if rows_saved % 1000 == 0:
                    yield singer.StateMessage(value=copy.deepcopy(state))
                row = cursor.fetchone()

        # If there is no replication key, we're doing "full table" replication,
        # and we need to activate this version at the end. Also clear the
        # stream's version from the state so that subsequent invocations will
        # emit a distinct stream version.
        if not replication_key:
            yield activate_version_message
            state = singer.write_bookmark(state, catalog_entry.tap_stream_id, 'version', None)

        yield singer.StateMessage(value=copy.deepcopy(state))

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
      * If the state has a currently_syncing, we will skip to that stream and
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
    currently_syncing = singer.get_currently_syncing(state)
    if currently_syncing:
        streams = dropwhile(lambda s: s.tap_stream_id != currently_syncing, streams)

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
        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        yield singer.StateMessage(value=copy.deepcopy(state))

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
    # currently_syncing from the state and emit a state message.
    state = singer.set_currently_syncing(state, None)
    yield singer.StateMessage(value=copy.deepcopy(state))


def do_sync(con, catalog, state):
    for message in generate_messages(con, catalog, state):
        singer.write_message(message)

def log_server_params(con):
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


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    connection = open_connection(args.config)
    with connection.cursor() as cur:
        cur.execute('SET @@session.time_zone="+0:00"')
        cur.execute('SET @@session.wait_timeout=2700')
        cur.execute('SET @@session.innodb_lock_wait_timeout=2700')
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
