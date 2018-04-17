#!/usr/bin/env python3
# pylint: disable=duplicate-code

import singer

import tap_mysql.sync_strategies.common as common

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

LOGGER = singer.get_logger()

def verify_binlog_config(connection, catalog_entry):
    cur = connection.cursor()
    cur.execute("""
        SELECT  @@binlog_format    AS binlog_format,
                @@binlog_row_image AS binlog_row_image;
        """)
    binlog_format, binlog_row_image = cur.fetchone()

    if binlog_format != 'ROW':
        raise Exception("""
           Unable to replicate with binlog for stream({}) because binlog_format is not set to 'ROW': {}.
           """.format(catalog_entry.stream, binlog_format))

    if binlog_row_image != 'FULL':
        raise Exception("""
           Unable to replicate with binlog for stream({}) because binlog_row_image is not set to 'FULL': {}.
           """.format(catalog_entry.stream, binlog_row_image))

def fetch_current_log_file_and_pos(connection):
    cur = connection.cursor()
    cur.execute("SHOW MASTER STATUS")
    result = cur.fetchone()

    current_log_file, current_log_pos = result[0:2]

    return current_log_file, current_log_pos

def sync_table(connection, catalog_entry, state):
    verify_binlog_config(connection, catalog_entry)

    columns = common.generate_column_list(catalog_entry)

    if not columns:
        LOGGER.warning(
            'There are no columns selected for table %s, skipping it',
            catalog_entry.table)
        return

    log_file = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'log_file')

    log_pos = singer.get_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'log_pos')

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    yield singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )
