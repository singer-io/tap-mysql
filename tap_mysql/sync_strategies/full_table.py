#!/usr/bin/env python3
# pylint: disable=duplicate-code

import copy
import singer

import tap_mysql.sync_strategies.common as common

LOGGER = singer.get_logger()

BOOKMARK_KEYS = {'version', 'initial_full_table_complete'}


def pks_are_autoincrementing(connection, catalog_entry, key_properties):
    database_name = common.get_database_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_table = escape(catalog_entry.table)

    if not key_properties:
        return False

    sql = """SELECT 1
               FROM information_schema.columns
              WHERE table_schema = '{}'
                AND table_name = '{}'
                AND column_name = '{}'
                AND extra LIKE '%auto_increment%'
"""

    with connection.cursor() as cur:
        for pk in key_properties:
            import pdb
            pdb.set_trace()
            cur.execute(sql.format(escaped_db,
                                   escaped_table,
                                   common.escape(pk)))

            result = cur.fetchone()

            if not result:
                return False

    return True


def sync_table(connection, catalog_entry, state, columns, stream_version):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

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

    with connection.cursor() as cursor:
        select_sql = common.generate_select_sql(catalog_entry, columns)

        params = {}

        common.sync_query(cursor,
                          catalog_entry,
                          state,
                          select_sql,
                          columns,
                          stream_version,
                          params)

    singer.write_message(activate_version_message)
