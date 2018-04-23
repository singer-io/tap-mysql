#!/usr/bin/env python3
# pylint: disable=duplicate-code

import copy
import singer

import tap_mysql.sync_strategies.common as common

LOGGER = singer.get_logger()

def sync_table(connection, catalog_entry, state, columns):
    bookmark = state.get('bookmarks', {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if 'version' in bookmark else False

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    # If there is no prior version for this stream, assume it is the
    # very first replication. Emity an ACTIVATE_VERSION message at the
    # beginning so the recors show up right away.
    if not version_exists:
        yield activate_version_message

    with connection.cursor() as cursor:
        select_sql = common.generate_select_sql(catalog_entry, columns)

        params = {}

        for message in common.sync_query(cursor,
                                         catalog_entry,
                                         state,
                                         select_sql,
                                         columns,
                                         stream_version,
                                         params):
            yield message

    yield activate_version_message
