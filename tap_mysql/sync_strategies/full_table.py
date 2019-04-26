#!/usr/bin/env python3
# pylint: disable=duplicate-code,too-many-locals,simplifiable-if-expression,too-many-arguments

import copy
import datetime
import singer
from singer import metadata

import tap_mysql.sync_strategies.binlog as binlog
import tap_mysql.sync_strategies.common as common

from tap_mysql.connection import connect_with_backoff, MySQLConnection

LOGGER = singer.get_logger()


def generate_bookmark_keys(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get('replication-method')

    base_bookmark_keys = {'last_pk_fetched', 'max_pk_values', 'version', 'initial_full_table_complete'}

    if replication_method == 'FULL_TABLE':
        bookmark_keys = base_bookmark_keys
    else:
        bookmark_keys = base_bookmark_keys.union(binlog.BOOKMARK_KEYS)

    return bookmark_keys


RESUMABLE_PK_TYPES = set([
    'tinyint',
    'smallint'
    'mediumint',
    'int',
    'bigint',
    'char',
    'varchar',

    # NB: Below types added so we can resume when they are a part of a composite PK.
    'datetime',
    'timestamp',
    'date',
    'time',
])

def sync_is_resumable(mysql_conn, catalog_entry):
    ''' In order to resume a full table sync, a table requires
    '''
    database_name = common.get_database_name(catalog_entry)
    key_properties = common.get_key_properties(catalog_entry)

    if not key_properties:
        return False

    sql = """SELECT data_type
               FROM information_schema.columns
              WHERE table_schema = '{}'
                AND table_name = '{}'
                AND column_name = '{}'
    """

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            for pk in key_properties:
                cur.execute(sql.format(database_name,
                                          catalog_entry.table,
                                          pk))

                result = cur.fetchone()

                if not result:
                    raise Exception("Primary key column {} does not exist.".format(pk))

                if result[0] not in RESUMABLE_PK_TYPES:
                    LOGGER.warn("Found primary key column %s with type %s. Will not be able " +
                                "to resume interrupted FULL_TABLE sync using this key.",
                                pk, result[0])
                    return False

    return True


def get_max_pk_values(cursor, catalog_entry):
    database_name = common.get_database_name(catalog_entry)
    escaped_db = common.escape(database_name)
    escaped_table = common.escape(catalog_entry.table)

    key_properties = common.get_key_properties(catalog_entry)
    escaped_columns = [common.escape(c) for c in key_properties]

    sql = """SELECT {}
               FROM {}.{}
    """

    select_column_clause = ", ".join(["max(" + pk + ")" for pk in escaped_columns])

    cursor.execute(sql.format(select_column_clause,
                           escaped_db,
                           escaped_table))
    result = cursor.fetchone()
    processed_results = []
    for bm in result:
        if isinstance(bm, (datetime.date, datetime.datetime, datetime.timedelta)):
            processed_results += [common.to_utc_datetime_str(bm)]
        elif bm is not None:
            processed_results += [bm]

    max_pk_values = {}
    if processed_results:
        max_pk_values = dict(zip(key_properties, processed_results))

    return max_pk_values

def generate_pk_clause(catalog_entry, state):
    key_properties = common.get_key_properties(catalog_entry)
    escaped_columns = [common.escape(c) for c in key_properties]

    where_clause = " AND ".join([pk + " > `{}`" for pk in escaped_columns])
    order_by_clause = ", ".join(['`{}`, ' for pk in escaped_columns])

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    pk_comparisons = []

    if not max_pk_values:
        return ""

    if last_pk_fetched:
        for pk in key_properties:
            column_type = catalog_entry.schema.properties.get(pk).type

            # quote last/max PK val if column is VARCHAR
            if 'string' in column_type:
                last_pk_val = "'" + last_pk_fetched[pk] + "'"
                max_pk_val = "'" + max_pk_values[pk] + "'"
            else:
                last_pk_val = last_pk_fetched[pk]
                max_pk_val = max_pk_values[pk]

            pk_comparisons.append("({} > {} AND {} <= {})".format(common.escape(pk),
                                                                  last_pk_val,
                                                                  common.escape(pk),
                                                                  max_pk_val))
    else:
        for pk in key_properties:
            column_schema = catalog_entry.schema.properties.get(pk)
            column_type = column_schema.type

            # quote last/max PK val if column is VARCHAR
            if 'string' in column_type:
                pk_val = "'{}'".format(max_pk_values[pk])
            else:
                pk_val = max_pk_values[pk]

            pk_comparisons.append("{} <= {}".format(common.escape(pk), pk_val))

    sql = " WHERE {} ORDER BY {} ASC".format(" AND ".join(pk_comparisons),
                                             ", ".join(escaped_columns))

    return sql

def update_incremental_full_table_state(catalog_entry, state, cursor):
    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values') or get_max_pk_values(cursor, catalog_entry)


    if not max_pk_values:
        LOGGER.info("No max value for PK found for table {}".format(catalog_entry.table))
    else:
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'max_pk_values',
                                      max_pk_values)

    return state

def sync_table(mysql_conn, catalog_entry, state, columns, stream_version):
    common.whitelist_bookmark_keys(generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state)

    bookmark = state.get('bookmarks', {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if 'version' in bookmark else False

    initial_full_table_complete = singer.get_bookmark(state,
                                                      catalog_entry.tap_stream_id,
                                                      'initial_full_table_complete')

    state_version = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'version')

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    perform_resumable_sync = sync_is_resumable(mysql_conn, catalog_entry)

    pk_clause = ""

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns)

            if perform_resumable_sync:
                LOGGER.info("Full table sync is resumable based on primary key definition, will replicate incrementally")

                state = update_incremental_full_table_state(catalog_entry, state, cur)
                pk_clause = generate_pk_clause(catalog_entry, state)

            select_sql += pk_clause
            params = {}

            common.sync_query(cur,
                              catalog_entry,
                              state,
                              select_sql,
                              columns,
                              stream_version,
                              params)

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')

    singer.write_message(activate_version_message)
