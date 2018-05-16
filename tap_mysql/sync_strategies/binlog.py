#!/usr/bin/env python3
# pylint: disable=duplicate-code,too-many-locals

import copy

import datetime
import pytz
import tzlocal

import singer
from singer import metadata
from singer import utils
from singer.schema import Schema

import pymysql.connections
import tap_mysql.sync_strategies.common as common

from tap_mysql.connection import make_connection_wrapper

from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

LOGGER = singer.get_logger()

SDC_DELETED_AT = "_sdc_deleted_at"

UPDATE_BOOKMARK_PERIOD = 1000

BOOKMARK_KEYS = {'log_file', 'log_pos', 'version'}

mysql_timestamp_types = {
    FIELD_TYPE.TIMESTAMP,
    FIELD_TYPE.TIMESTAMP2
}

def add_automatic_properties(catalog_entry, columns):
    catalog_entry.schema.properties[SDC_DELETED_AT] = Schema(
        type=["null", "string"],
        format="date-time"
        )

    columns.append(SDC_DELETED_AT)

    return columns


def verify_binlog_config(connection, catalog_entry):
    with connection.cursor() as cur:
        cur.execute("""
        SELECT  @@binlog_format    AS binlog_format,
        @@binlog_row_image AS binlog_row_nimage;
        """)

        binlog_format, binlog_row_image = cur.fetchone()

        if binlog_format != 'ROW':
            raise Exception("Unable to replicate stream({}) with binlog because binlog_format is not set to 'ROW': {}."
                            .format(catalog_entry.stream, binlog_format))

        if binlog_row_image != 'FULL':
            raise Exception("Unable to replicate stream({}) with binlog because binlog_row_image is not set to 'FULL': {}."
                            .format(catalog_entry.stream, binlog_row_image))


def verify_log_file_exists(connection, catalog_entry, log_file, log_pos):
    with connection.cursor() as cur:
        cur.execute("SHOW BINARY LOGS")
        result = cur.fetchall()

        existing_log_file = list(filter(lambda log: log[0] == log_file, result))

        if not existing_log_file:
            raise Exception("Unable to replicate stream({}) with binlog because log file {} does not exist."
                            .format(catalog_entry.stream, log_file))

        current_log_pos = existing_log_file[0][1]

        if log_pos > current_log_pos:
            raise Exception("Unable to replicate stream({}) with binlog because requested position ({}) for log file {} is greater than current position ({})."
                            .format(catalog_entry.stream, log_pos, log_file, current_log_pos))


def fetch_current_log_file_and_pos(connection):
    with connection.cursor() as cur:
        cur.execute("SHOW MASTER STATUS")

        result = cur.fetchone()

        if result is None:
            raise Exception("MySQL binary logging is not enabled.")

        current_log_file, current_log_pos = result[0:2]

        return current_log_file, current_log_pos


def fetch_server_id(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT @@server_id")
        server_id = cur.fetchone()[0]

        return server_id


def row_to_singer_record(catalog_entry, version, db_column_map, row, time_extracted):
    row_to_persist = {}

    for column_name, val in row.items():
        property_type = catalog_entry.schema.properties[column_name].type
        db_column_type = db_column_map.get(column_name)

        if isinstance(val, datetime.datetime):
            if db_column_type in mysql_timestamp_types:
                # The mysql-replication library creates datetimes from TIMESTAMP columns using fromtimestamp
                # which will use the local timezone thus we must set tzinfo accordingly
                # See: https://github.com/noplay/python-mysql-replication/blob/master/pymysqlreplication/row_event.py#L143-L145
                timezone = tzlocal.get_localzone()
                local_datetime = timezone.localize(val)
                utc_datetime = local_datetime.astimezone(pytz.UTC)
                row_to_persist[column_name] = utc_datetime.isoformat()
            else:
                row_to_persist[column_name] = val.isoformat() + '+00:00'

        elif isinstance(val, datetime.date):
            row_to_persist[column_name] = val.isoformat() + 'T00:00:00+00:00'

        elif isinstance(val, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + val
            row_to_persist[column_name] = timedelta_from_epoch.isoformat() + '+00:00'

        elif 'boolean' in property_type or property_type == 'boolean':
            if val is None:
                boolean_representation = None
            elif val == 0:
                boolean_representation = False
            elif db_column_type == FIELD_TYPE.BIT:
                boolean_representation = int(val) != 0
            else:
                boolean_representation = True
            row_to_persist[column_name] = boolean_representation

        else:
            row_to_persist[column_name] = val

    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)


def sync_table(connection, config, catalog_entry, state, columns):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    log_file = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'log_file')

    log_pos = singer.get_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'log_pos')

    verify_binlog_config(connection, catalog_entry)
    verify_log_file_exists(connection, catalog_entry, log_file, log_pos)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    server_id = fetch_server_id(connection)

    connection_wrapper = make_connection_wrapper(config)

    reader = BinLogStreamReader(
        connection_settings={},
        server_id=server_id,
        log_file=log_file,
        log_pos=log_pos,
        resume_stream=True,
        only_events=[RotateEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        pymysql_wrapper=connection_wrapper
    )

    database_name = common.get_database_name(catalog_entry)
    table_path = (database_name, catalog_entry.stream)

    time_extracted = utils.now()

    LOGGER.info("Starting binlog replication with log_file=%s, log_pos=%s", log_file, log_pos)

    rows_saved = 0

    for binlog_event in reader:
        if reader.log_file == log_file and reader.log_pos == log_pos:
            LOGGER.info("Skipping event for log_file=%s and log_pos=%s as it was processed last sync",
                        reader.log_file,
                        reader.log_pos)
            continue

        if isinstance(binlog_event, RotateEvent):
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_file',
                                          binlog_event.next_binlog)
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_pos',
                                          binlog_event.position)

        elif (binlog_event.schema, binlog_event.table) == table_path:
            db_column_types = {c.name:c.type for c in  binlog_event.columns}

            if isinstance(binlog_event, WriteRowsEvent):
                for row in binlog_event.rows:
                    filtered_vals = {k:v for k,v in row['values'].items()
                                    if k in columns}

                    record_message = row_to_singer_record(catalog_entry,
                                                          stream_version,
                                                          db_column_types,
                                                          filtered_vals,
                                                          time_extracted)

                    singer.write_message(record_message)
                    rows_saved = rows_saved + 1


            elif isinstance(binlog_event, UpdateRowsEvent):
                for row in binlog_event.rows:
                    filtered_vals = {k:v for k,v in row['after_values'].items()
                                    if k in columns}

                    record_message = row_to_singer_record(catalog_entry,
                                                          stream_version,
                                                          db_column_types,
                                                          filtered_vals,
                                                          time_extracted)

                    singer.write_message(record_message)

                    rows_saved = rows_saved + 1
            elif isinstance(binlog_event, DeleteRowsEvent):
                for row in binlog_event.rows:
                    event_ts = datetime.datetime.utcfromtimestamp(binlog_event.timestamp).replace(tzinfo=pytz.UTC)

                    vals = row['values']
                    vals[SDC_DELETED_AT] = event_ts

                    filtered_vals = {k:v for k,v in vals.items()
                                    if k in columns}

                    record_message = row_to_singer_record(catalog_entry,
                                                          stream_version,
                                                          db_column_types,
                                                          filtered_vals,
                                                          time_extracted)

                    singer.write_message(record_message)

                    rows_saved = rows_saved + 1

            state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'log_file',
                                      reader.log_file)

            state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'log_pos',
                                      reader.log_pos)

            if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
