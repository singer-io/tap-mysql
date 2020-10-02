import unittest
import pymysql
import tap_mysql
import copy
import singer
import os
import singer.metadata
from tap_mysql.connection import connect_with_backoff

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils

import tap_mysql.sync_strategies.binlog as binlog
import tap_mysql.sync_strategies.common as common

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

from singer.schema import Schema

LOGGER = singer.get_logger()

SINGER_MESSAGES = []

def accumulate_singer_messages(message):
    SINGER_MESSAGES.append(message)

singer.write_message = accumulate_singer_messages

class TestDateTypes(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()
        self.state = {}

        log_file, log_pos = binlog.fetch_current_log_file_and_pos(self.conn)

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE datetime_types (id int, datetime_col datetime, timestamp_col timestamp, time_col time, date_col date)')
                cursor.execute('INSERT INTO datetime_types (id, datetime_col, timestamp_col, time_col, date_col) VALUES (1, \'0000-00-00\', \'0000-00-00 00:00:00\', \'00:00:00\', \'0000-00-00\' )')
                cursor.execute('INSERT INTO datetime_types (id, datetime_col, timestamp_col, time_col, date_col) VALUES (2, NULL, NULL, NULL, NULL)')
            open_conn.commit()

        self.catalog = test_utils.discover_catalog(self.conn, {})

        for stream in self.catalog.streams:
            stream.stream = stream.table

            stream.metadata = [
                {'breadcrumb': (),
                 'metadata': {
                     'selected': True,
                     'database-name': 'tap_mysql_test',
                     'table-key-propertes': ['id']
                 }},
                {'breadcrumb': ('properties', 'id'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'datetime_col'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'timestamp_col'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'time_col'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'date_col'), 'metadata': {'selected': True}}
            ]

            test_utils.set_replication_method_and_key(stream, 'LOG_BASED', None)

            self.state = singer.write_bookmark(self.state,
                                               stream.tap_stream_id,
                                               'log_file',
                                               log_file)

            self.state = singer.write_bookmark(self.state,
                                               stream.tap_stream_id,
                                               'log_pos',
                                               log_pos)

            self.state = singer.write_bookmark(self.state,
                                               stream.tap_stream_id,
                                               'version',
                                               singer.utils.now())

    def test_initial_full_table(self):
        state = {}
        expected_log_file, expected_log_pos = binlog.fetch_current_log_file_and_pos(self.conn)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        message_types = [type(m) for m in SINGER_MESSAGES]

        
        self.assertEqual(message_types,
                         [singer.StateMessage,
                          singer.SchemaMessage,
                          singer.ActivateVersionMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage,
                          singer.ActivateVersionMessage,
                          singer.StateMessage])

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        # Expected from 0.7.11
        expected_records = [
            {'datetime_col': None,
             'id': 1,
             'timestamp_col': None,
             'time_col': '1970-01-01T00:00:00.000000Z',
             'date_col': None},
            {'datetime_col': None,
             'id': 2,
             'timestamp_col': None,
             'time_col': None,
             'date_col': None}
        ]

        self.assertEqual(expected_records, [x.asdict()['record'] for x in record_messages])
