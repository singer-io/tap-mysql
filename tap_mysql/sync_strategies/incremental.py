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

    replication_key_value = singer.get_bookmark(state,
                                                catalog.tap_stream_id,
                                                'replication_key_value')

    replication_key = singer.get_bookmark(state,
                                          catalog.tap_stream_id,
                                          'replication_key')

    stream_version = common.get_stream_version(catalog.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog.tap_stream_id,
                                  'version',
                                  stream_version)

    yield singer.ActivateVersionMessage(
        stream=catalog.stream,
        version=stream_version
    )

    with connection.cursor() as cursor:
        select = common.generate_select(catalog, columns)
        params = {}

        if replication_key_value is not None:
            if catalog.schema.properties[replication_key].format == 'date-time':
                replication_key_value = pendulum.parse(replication_key_value)

            select += ' WHERE `{}` >= %(replication_key_value)s ORDER BY `{}` ASC'.format(
                replication_key,
                replication_key)

            params['replication_key_value'] = replication_key_value
        elif replication_key is not None:
            select += ' ORDER BY `{}` ASC'.format(replication_key)

        common.sync_query(cursor, catalog, state, select, params)
