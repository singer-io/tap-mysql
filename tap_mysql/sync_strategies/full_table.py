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


def quote_where_clause_value(value, column_type):
    if 'string' in column_type:
        return "'" + str(value) + "'"

    return str(value)


def generate_pk_bookmark_clause(key_properties, last_pk_fetched, catalog_entry):
    """
    Generates a bookmark clause based on `key_properties`, and
    `last_pk_fetched` bookmark. This ensures that the stream is resumed at
    the location in the data set per primary key component. Inclusivity is
    not maintained, since these are primary keys.

    Example:

    key_properties = ['name','birthday']
    last_pk_fetched = {'name': "Phil Collins", 'birthday': "1951-01-30"}

    Returns:
    "(`name` > 'Phil Collins') OR (`name` = 'Phil Collins' AND `birthday` > '1951-01-30')
    """
    assert last_pk_fetched is not None, \
        "Must call generate_pk_bookmark with a non-null 'last_pk_fetched' dict"

    clause_terms = []
    inclusive_pk_values = []
    for pk in key_properties:
        term = []
        for prev_pk, prev_pk_val, prev_col_type in inclusive_pk_values:
            term.append(common.escape(prev_pk) + ' = ' + quote_where_clause_value(prev_pk_val, prev_col_type))

        column_type = catalog_entry.schema.properties.get(pk).type
        term.append(common.escape(pk) + ' > ' + quote_where_clause_value(last_pk_fetched[pk], column_type))
        inclusive_pk_values.append((pk, last_pk_fetched[pk], column_type))

        clause_terms.append(' AND '.join(term))
    return '({})'.format(') OR ('.join(clause_terms)) if clause_terms else ''


def generate_pk_clause(catalog_entry, state):
    key_properties = common.get_key_properties(catalog_entry)

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    last_pk_clause = ''
    max_pk_comparisons = []


    if not max_pk_values:
        return ""

    if last_pk_fetched:
        for pk in key_properties:
            column_type = catalog_entry.schema.properties.get(pk).type

            # Add AND to interpolate along with max_pk_values clauses
            last_pk_clause = '({}) AND '.format(generate_pk_bookmark_clause(key_properties,
                                                                            last_pk_fetched,
                                                                            catalog_entry))
            max_pk_comparisons.append("{} <= {}".format(common.escape(pk),
                                                        quote_where_clause_value(max_pk_values[pk],
                                                                                 column_type)))
    else:
        for pk in key_properties:
            column_schema = catalog_entry.schema.properties.get(pk)
            column_type = column_schema.type

            pk_val = quote_where_clause_value(max_pk_values[pk],
                                              column_type)

            max_pk_comparisons.append("{} <= {}".format(common.escape(pk), pk_val))

    order_by_columns = [common.escape(c) for c in key_properties]
    sql = " WHERE {}{} ORDER BY {} ASC".format(last_pk_clause,
                                               " AND ".join(max_pk_comparisons),
                                               ", ".join(order_by_columns))

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
