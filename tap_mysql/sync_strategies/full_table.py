#!/usr/bin/env python3

import singer

import tap_mysql.sync_strategies.common as common

def sync_table(connection, catalog, state):
    columns = common.generate_column_list(catalog)

    if not columns:
        LOGGER.warning(
            'There are no columns selected for table %s, skipping it',
            catalog.table)
        return

    bookmark_is_empty = state.get('bookmarks', {}).get(catalog.tap_stream_id) is None


    stream_version = common.get_stream_version(catalog.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog.tap_stream_id,
                                  'version',
                                  stream_version)

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog.stream,
        version=stream_version
    )

    # If there is no bookmark at all for this stream, assume it is the
    # very first replication. Emity an ACTIVATE_VERSION message at the
    # beginning so the recors show up right away.
    if bookmark_is_empty:
       yield activate_version_message

    with connection.cursor() as cursor:
        select = common.generate_select(catalog, columns)
        select += ' ORDER BY `{}` ASC'.format(replication_key)

        params = {}

        common.sync_query(cursor, catalog, state, select, params)

    # Clear the stream's version from the state so that subsequent invocations will
    # emit a distinct stream version.
    state = singer.write_bookmark(state, catalog_entry.tap_stream_id, 'version', None)

    yield activate_version_message
    yield singer.StateMessage(value=copy.deepcopy(state))
