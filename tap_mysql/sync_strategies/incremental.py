#!/usr/bin/env python3

import singer

import tap_mysql.sync_strategies.common as common

def sync_table(connection, catalog_entry, state):
    columns = common.generate_column_list(catalog_entry)

    if not columns:
        LOGGER.warning(
            'There are no columns selected for table %s, skipping it',
            catalog_entry.table)
        return

    replication_key_value = singer.get_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key_value')

    replication_key = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key')

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    # If there's a replication key, we want to emit an ACTIVATE_VERSION
    # message at the beginning so the records show up right away. If
    # there's no bookmark at all for this stream, assume it's the very
    # first replication. That is, clients have never seen rows for this
    # stream before, so they can immediately acknowledge the present
    # version.
    if replication_key:
        yield singer.ActivateVersionMessage(
            stream=catalog_entry.stream,
            version=stream_version
        )

    with connection.cursor() as cursor:
        select = common.generate_select(catalog_entry, columns)
        params = {}

        if replication_key_value is not None:
            if catalog_entry.schema.properties[replication_key].format == 'date-time':
                replication_key_value = pendulum.parse(replication_key_value)

            select += ' WHERE `{}` >= %(replication_key_value)s ORDER BY `{}` ASC'.format(
                replication_key,
                replication_key)

            params['replication_key_value'] = replication_key_value
        elif replication_key is not None:
            select += ' ORDER BY `{}` ASC'.format(replication_key)

        common.sync_query(cursor, catalog_entry, state, select, params)
