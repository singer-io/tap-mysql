#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pendulum
import singer
from singer import metadata

import tap_mysql.sync_strategies.common as common

LOGGER = singer.get_logger()

BOOKMARK_KEYS = {'replication_key', 'replication_key_value', 'version'}

def sync_table(connection, catalog_entry, state, columns):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    replication_key_metadata = stream_metadata.get('replication-key')
    replication_key_state = singer.get_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key')

    replication_key = replication_key_state or replication_key_metadata
    replication_key_value = None

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value')
    else:
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'replication_key',
                                      replication_key)
        state = singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    yield singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    with connection.cursor() as cursor:
        select_sql = common.generate_select_sql(catalog_entry, columns)
        params = {}

        if replication_key_value is not None:
            if catalog_entry.schema.properties[replication_key].format == 'date-time':
                replication_key_value = pendulum.parse(replication_key_value)

            select_sql += ' WHERE `{}` >= %(replication_key_value)s ORDER BY `{}` ASC'.format(
                replication_key,
                replication_key)

            params['replication_key_value'] = replication_key_value
        elif replication_key is not None:
            select_sql += ' ORDER BY `{}` ASC'.format(replication_key)

        for message in common.sync_query(cursor,
                                         catalog_entry,
                                         state,
                                         select_sql,
                                         columns,
                                         stream_version,
                                         params):
            yield message
