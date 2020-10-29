from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from singer import utils
from decimal import Decimal
import copy
import datetime
import decimal
import os
import pytz
import re
import unittest

import db_utils

# BUG missing datetime precision | https://stitchdata.atlassian.net/browse/SRCE-4257 | Search for BUG_1 below

expected_schemas = {
    'mysql_binlog_test': {
        'type': 'object',
        'properties': {
            'id': {'type': ['null', 'integer'], 'minimum': -9223372036854775808, 'inclusion': 'automatic', 'maximum': 9223372036854775807},
            'our_char': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 1},
            'our_enum': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 5},
            'our_longtext': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 4294967295},
            'our_mediumtext': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 16777215},
            'our_text': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 65535},
            'our_varchar': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 255},

            'our_unsigned_tinyint': {'type': ['null', 'integer'], 'minimum': 0, 'inclusion': 'available', 'maximum': 255},
            'our_signed_tinyint': {'type': ['null', 'integer'], 'minimum': -128, 'inclusion': 'available', 'maximum': 127},

            'our_unsigned_smallint': {'type': ['null', 'integer'], 'minimum': 0, 'inclusion': 'available', 'maximum': 65535},
            'our_signed_smallint': {'type': ['null', 'integer'], 'minimum': -32768, 'inclusion': 'available', 'maximum': 32767},

            'our_unsigned_mediumint': {'type': ['null', 'integer'], 'minimum': 0, 'inclusion': 'available', 'maximum': 16777215},
            'our_signed_mediumint': {'type': ['null', 'integer'], 'minimum': -8388608, 'inclusion': 'available', 'maximum': 8388607},

            'our_unsigned_int': {'type': ['null', 'integer'], 'minimum': 0, 'inclusion': 'available', 'maximum': 4294967295},
            'our_signed_int': {'type': ['null', 'integer'], 'minimum': -2147483648, 'inclusion': 'available', 'maximum': 2147483647},

            'our_unsigned_bigint': {'type': ['null', 'integer'], 'minimum': 0, 'inclusion': 'available', 'maximum': 18446744073709551615},
            'our_signed_bigint': {'type': ['null', 'integer'], 'minimum': -9223372036854775808, 'inclusion': 'available', 'maximum': 9223372036854775807},

            # NB: The runner uses simplejson to parse with decimal=True
            #     The target should NOT be writing `Decimal(0.01)`
            'our_signed_decimal_1': {'type': ['null', 'number'], 'multipleOf': Decimal("0.01"), 'inclusion': 'available'},
            'our_unsigned_decimal_1': {'type': ['null', 'number'], 'multipleOf': Decimal("0.01"), 'inclusion': 'available'},

            'our_unsigned_decimal_2': {'type': ['null', 'number'], 'multipleOf': 1, 'inclusion': 'available'},
            'our_signed_decimal_2': {'type': ['null', 'number'], 'multipleOf': 1, 'inclusion': 'available'},

            'our_unsigned_float': {'type': ['null', 'number'], 'inclusion': 'available'},
            'our_signed_float': {'type': ['null', 'number'], 'inclusion': 'available'},

            'our_unsigned_double': {'type': ['null', 'number'], 'inclusion': 'available'},
            'our_signed_double': {'type': ['null', 'number'], 'inclusion': 'available'},

            'our_bit_1': {'type': ['null', 'boolean'], 'inclusion': 'available'},

            'our_datetime': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},
            'our_timestamp': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},
            'our_time': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},
            'our_date': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},

            # this is actually implemented as a tinyint(1) in MySQL
            'our_boolean': {'type': ['null', 'boolean'], 'inclusion': 'available'},
        }
    }
}

rec_1_datetime = datetime.datetime(2000, 1, 1, 1, 1, 1, 1000, tzinfo=pytz.UTC)
rec_2_datetime = datetime.datetime(2002, 2, 2, 2, 2, 2, 2000, tzinfo=pytz.UTC)
rec_3_datetime = datetime.datetime(2004, 4, 4, 4, 4, 4, 4000, tzinfo=pytz.UTC)

rec_1 = {
    'id': 1,
    'our_char': 'A',
    'our_enum': '1',
    'our_longtext': 'so much text',
    'our_mediumtext': 'less text',
    'our_text': 'text',
    'our_varchar': 'chicken',
    'our_unsigned_tinyint': 10,
    'our_signed_tinyint': -10,
    'our_unsigned_smallint': 200,
    'our_signed_smallint': -200,
    'our_unsigned_mediumint': 2000,
    'our_signed_mediumint': -2000,
    'our_unsigned_int': 123041234,
    'our_signed_int': -123041234,
    'our_unsigned_bigint': 1293832904823423,
    'our_signed_bigint': -1293832904823423,
    'our_unsigned_decimal_1': decimal.Decimal('12345.67'),
    'our_signed_decimal_1': decimal.Decimal('-12345.67'),
    'our_unsigned_decimal_2': decimal.Decimal('1234567'),
    'our_signed_decimal_2': decimal.Decimal('-1234567'),
    'our_unsigned_float': Decimal("1.2345"),
    'our_signed_float': -Decimal("1.2345"),
    'our_unsigned_double': Decimal("5.6789"),
    'our_signed_double': -Decimal("5.65789"),
    'our_bit_1': 1,
    'our_datetime': rec_1_datetime,
    'our_timestamp': rec_1_datetime,
    'our_date': rec_1_datetime.date(),
    'our_time': rec_1_datetime.time(),
    'our_boolean': True,
}

rec_2 = {
    'id': 2,
    'our_char': 'B',
    'our_enum': '2',
    'our_longtext': 'hippopotamus',
    'our_mediumtext': 'ostrich',
    'our_text': 'emu',
    'our_varchar': 'turkey',
    'our_unsigned_tinyint': 20,
    'our_signed_tinyint': -20,
    'our_unsigned_smallint': 400,
    'our_signed_smallint': -400,
    'our_unsigned_mediumint': 4000,
    'our_signed_mediumint': -4000,
    'our_unsigned_int': 246082468,
    'our_signed_int': -246082468,
    'our_unsigned_bigint': 2587665809646846,
    'our_signed_bigint': -2587665809646846,
    'our_unsigned_decimal_1': decimal.Decimal('24691.34'),
    'our_signed_decimal_1': decimal.Decimal('-24691.34'),
    'our_unsigned_decimal_2': decimal.Decimal('2469134'),
    'our_signed_decimal_2': decimal.Decimal('-2469134'),
    'our_unsigned_float': Decimal("2.4690"),
    'our_signed_float': -Decimal("2.4690"),
    'our_unsigned_double': Decimal("11.3578"),
    'our_signed_double': -Decimal("11.3578"),
    'our_bit_1': 0,
    'our_datetime': rec_2_datetime,
    'our_timestamp': rec_2_datetime,
    'our_date': rec_2_datetime.date(),
    'our_time': rec_2_datetime.time(),
    'our_boolean': False,
}

rec_3 = {
    'id': 3,
    'our_char': 'C',
    'our_enum': '3',
    'our_longtext': 'orange',
    'our_mediumtext': 'blue',
    'our_text': 'red',
    'our_varchar': 'quail',
    'our_unsigned_tinyint': 30,
    'our_signed_tinyint': -30,
    'our_unsigned_smallint': 600,
    'our_signed_smallint': -600,
    'our_unsigned_mediumint': 6000,
    'our_signed_mediumint': -6000,
    'our_unsigned_int': 369123702,
    'our_signed_int': -369123702,
    'our_unsigned_bigint': 3881498714470269,
    'our_signed_bigint': -3881498714470269,
    'our_unsigned_decimal_1': decimal.Decimal('37037.01'),
    'our_signed_decimal_1': decimal.Decimal('-37037.01'),
    'our_unsigned_decimal_2': decimal.Decimal('3703701'),
    'our_signed_decimal_2': decimal.Decimal('-3703701'),
    'our_unsigned_float': Decimal("3.7035"),
    'our_signed_float': -Decimal("3.7035"),
    'our_unsigned_double': Decimal("17.0367"),
    'our_signed_double': -Decimal("17.0367"),
    'our_bit_1': 1,
    'our_datetime': rec_3_datetime,
    'our_timestamp': rec_3_datetime,
    'our_date': rec_3_datetime.date(),
    'our_time': rec_3_datetime.time(),
    'our_boolean': False,
}

expected_rec_1 = {
    'id': 1,
    'our_char': 'A',
    'our_enum': 'one',
    'our_longtext': 'so much text',
    'our_mediumtext': 'less text',
    'our_text': 'text',
    'our_varchar': 'chicken',
    'our_unsigned_tinyint': 10,
    'our_signed_tinyint': -10,
    'our_unsigned_smallint': 200,
    'our_signed_smallint': -200,
    'our_unsigned_mediumint': 2000,
    'our_signed_mediumint': -2000,
    'our_unsigned_int': 123041234,
    'our_signed_int': -123041234,
    'our_unsigned_bigint': 1293832904823423,
    'our_signed_bigint': -1293832904823423,
    'our_unsigned_decimal_1': Decimal("12345.67"),
    'our_signed_decimal_1': -Decimal("12345.67"),
    'our_unsigned_decimal_2': 1234567,
    'our_signed_decimal_2': -1234567,
    'our_unsigned_float': Decimal("1.2345"),
    'our_signed_float': -Decimal("1.2345"),
    'our_unsigned_double': Decimal("5.6789"),
    'our_signed_double': -Decimal("5.65789"),
    'our_bit_1': True,
    'our_datetime': '2000-01-01T01:01:01.000000Z',  # '2000-01-01T01:01:01.001000Z' BUG_1
    'our_timestamp': '2000-01-01T01:01:01.000000Z',  # '2000-01-01T01:01:01.001000Z' BUG_1
    'our_date': '2000-01-01T00:00:00.000000Z',
    'our_time': '1970-01-01T01:01:01.000000Z',
    'our_boolean': True,
}

expected_rec_2 = {
    'id': 2,
    'our_char': 'B',
    'our_enum': 'two',
    'our_longtext': 'hippopotamus',
    'our_mediumtext': 'ostrich',
    'our_text': 'emu',
    'our_varchar': 'turkey',
    'our_unsigned_tinyint': 20,
    'our_signed_tinyint': -20,
    'our_unsigned_smallint': 400,
    'our_signed_smallint': -400,
    'our_unsigned_mediumint': 4000,
    'our_signed_mediumint': -4000,
    'our_unsigned_int': 246082468,
    'our_signed_int': -246082468,
    'our_unsigned_bigint': 2587665809646846,
    'our_signed_bigint': -2587665809646846,
    'our_unsigned_decimal_1': Decimal("24691.34"),
    'our_signed_decimal_1': -Decimal("24691.34"),
    'our_unsigned_decimal_2': 2469134,
    'our_signed_decimal_2': -2469134,
    'our_unsigned_float': Decimal("2.469"),
    'our_signed_float': -Decimal("2.469"),
    'our_unsigned_double': Decimal("11.3578"),
    'our_signed_double': -Decimal("11.3578"),
    'our_bit_1': False,
    'our_datetime': '2002-02-02T02:02:02.000000Z',  # '2002-02-02T02:02:02.002000Z' BUG_1
    'our_timestamp': '2002-02-02T02:02:02.000000Z',  # '2002-02-02T02:02:02.002000Z' BUG_1
    'our_date': '2002-02-02T00:00:00.000000Z',
    'our_time': '1970-01-01T02:02:02.000000Z',
    'our_boolean': False,
}

expected_rec_3 = {
    'id': 3,
    'our_char': 'C',
    'our_enum': 'three',
    'our_longtext': 'orange',
    'our_mediumtext': 'blue',
    'our_text': 'red',
    'our_varchar': 'quail',
    'our_unsigned_tinyint': 30,
    'our_signed_tinyint': -30,
    'our_unsigned_smallint': 600,
    'our_signed_smallint': -600,
    'our_unsigned_mediumint': 6000,
    'our_signed_mediumint': -6000,
    'our_unsigned_int': 369123702,
    'our_signed_int': -369123702,
    'our_unsigned_bigint': 3881498714470269,
    'our_signed_bigint': -3881498714470269,
    'our_unsigned_decimal_1': Decimal("37037.01"),
    'our_signed_decimal_1': -Decimal("37037.01"),
    'our_unsigned_decimal_2': 3703701,
    'our_signed_decimal_2': -3703701,
    'our_unsigned_float': Decimal("3.7035000324249268"),
    'our_signed_float': -Decimal("3.7035000324249268"),
    'our_unsigned_double': Decimal("17.0367"),
    'our_signed_double': -Decimal("17.0367"),
    'our_bit_1': True,
    'our_datetime': '2004-04-04T04:04:04.000000Z',   # '2004-04-04T04:04:04.004000Z' BUG_1
    'our_timestamp': '2004-04-04T04:04:04.000000Z',   # '2004-04-04T04:04:04.004000Z' BUG_1
    'our_date': '2004-04-04T00:00:00.000000Z',
    'our_time': '1970-01-01T04:04:04.000000Z',
    'our_boolean': False,
}



class MySQLBinlog(unittest.TestCase):
    def tap_name(self):
        return 'tap-mysql'


    def name(self):
        return "tap_tester_mysql_binlog"


    def get_type(self):
        return "platform.mysql"


    def get_credentials(self):
        return {'password': os.getenv('TAP_MYSQL_PASSWORD')}

    def database_name(self):
        return os.getenv('TAP_MYSQL_DBNAME')


    def get_properties(self):
        return {'host' : os.getenv('TAP_MYSQL_HOST'),
                'port' : os.getenv('TAP_MYSQL_PORT'),
                'database' : self.database_name(),
                'user' : os.getenv('TAP_MYSQL_USER'),
        }


    def table_name(self):
        return "mysql_binlog_test"


    def tap_stream_id(self):
        return "{}-{}".format(self.database_name(), self.table_name())


    def insert_record(self, cursor, record):
        rec_cols, rec_vals = zip(*record.items())

        columns_sql = ", \n".join(rec_cols)
        value_sql = ",".join(["%s" for i in range(len(rec_vals))])

        insert_sql = """
        INSERT INTO {}.{}
               ( {} )
        VALUES ( {} )""".format(
            self.database_name(),
            self.table_name(),
            columns_sql,
            value_sql)

        cursor.execute(insert_sql, rec_vals)


    def get_engines(self):
        return [
            "MYISAM",
            "INNODB",
        ]


    def fetch_server_id(self):
        with db_utils.get_db_connection(self.get_properties(), self.get_credentials()).cursor() as cur:
            cur.execute("SELECT @@server_id")
            server_id = cur.fetchone()[0]

            return server_id


    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_MYSQL_HOST'),
                                    os.getenv('TAP_MYSQL_PORT'),
                                    os.getenv('TAP_MYSQL_USER'),
                                    os.getenv('TAP_MYSQL_PASSWORD'),
                                    os.getenv('TAP_MYSQL_PORT')] if x == None]
        if len(missing_envs) != 0:
            #pylint: disable=line-too-long
            raise Exception("set TAP_MYSQL_HOST, TAP_MYSQL_PORT, TAP_MYSQL_DBNAME, TAP_MYSQL_USER, TAP_MYSQL_PASSWORD")


    def initialize_db(self, engine):
        connection = db_utils.get_db_connection(self.get_properties(), self.get_credentials())

        with connection.cursor() as cur:
            create_databases_sql = """
                DROP DATABASE IF EXISTS {};
                CREATE DATABASE {};
            """.format(self.database_name(), self.database_name())

            cur.execute(create_databases_sql)
            cur.execute("""
            SELECT EXISTS (
            SELECT 1
            FROM  information_schema.tables
            WHERE  table_schema = %s
            AND  table_name =   %s);""",
                        [self.database_name(), self.table_name()])

            existing_table = cur.fetchone()[0]

            if existing_table:
                cur.execute("DROP TABLE {}.{}".format(self.database_name(), self.table_name()))

            create_table_sql = """
CREATE TABLE {}.{} (
            id                     BIGINT PRIMARY KEY,
            our_char               CHAR,
            our_enum               ENUM('one', 'two', 'three'),
            our_longtext           LONGTEXT,
            our_mediumtext         MEDIUMTEXT,
            our_text               TEXT,
            our_varchar            VARCHAR(255),
            our_unsigned_tinyint   TINYINT UNSIGNED,
            our_signed_tinyint     TINYINT,
            our_unsigned_smallint  SMALLINT UNSIGNED,
            our_signed_smallint    SMALLINT,
            our_unsigned_mediumint MEDIUMINT UNSIGNED,
            our_signed_mediumint   MEDIUMINT,
            our_unsigned_int       INT UNSIGNED,
            our_signed_int         INT,
            our_unsigned_bigint    BIGINT UNSIGNED,
            our_signed_bigint      BIGINT,
            our_unsigned_decimal_1 DECIMAL(11,2) UNSIGNED,
            our_signed_decimal_1   DECIMAL(11,2),
            our_unsigned_decimal_2 DECIMAL UNSIGNED,
            our_signed_decimal_2   DECIMAL,
            our_unsigned_float     FLOAT UNSIGNED,
            our_signed_float       FLOAT,
            our_unsigned_double    DOUBLE UNSIGNED,
            our_signed_double      DOUBLE,
            our_bit_1              BIT(1),
            our_datetime           DATETIME,
            our_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            our_date               DATE,
            our_time               TIME,
            our_boolean            BOOLEAN
)
ENGINE = {}
""".format(self.database_name(), self.table_name(), engine)

            cur.execute(create_table_sql)

            # Ensure expected engine in use
            cur.execute("""
            SELECT TABLE_NAME, ENGINE
            FROM  information_schema.tables
            where  table_schema =   %s;""",
                        [self.database_name()])
            engine_in_use = cur._result.rows[0][1]
            self.assertEqual(engine, engine_in_use.upper(),
                             msg="Unexpected engine in use: {}".format(engine_in_use))

            for record in [rec_1, rec_2]:
                self.insert_record(cur, record)

        print("\n\nMySQL DB Instantiated." + \
              "\nNAME: {}\nENGINE: {}".format(self.database_name(), engine_in_use) + \
              "\nTABLE: {}\nEVENTS: 2 records inserted\n\n".format(self.table_name()))

    def test_run(self):
        """Run the binlog replication test using multiple storage engines."""
        engines = self.get_engines()
        for engine in engines:
            self.initialize_db(engine)
            self.binlog_test()


    def binlog_test(self):
        """
        Test binlog replication
        • Verify an initial sync returns expected records of various datatypes
        • Verify no changes and a subsequent sync results in no replicated records
        • Update, Delete, and Insert records then verify the next sync captures these changes
        • Verify some log_file and log_pos state was persisted after each sync
        """
        print("RUNNING {}\n\n".format(self.name()))

        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        expected_check_streams = { self.tap_stream_id() }
        expected_sync_streams = { self.table_name() }
        expected_pks = {
            self.table_name(): { 'id' }
        }

        # verify the tap discovered the right streams
        found_catalogs = [catalog for catalog
                          in menagerie.get_catalogs(conn_id)
                          if catalog['tap_stream_id'] in expected_check_streams]

        self.assertGreaterEqual(len(found_catalogs),
                                1,
                                msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = expected_check_streams.symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        # verify that persisted streams have the correct properties
        test_catalog = found_catalogs[0]

        self.assertEqual(self.table_name(), test_catalog['stream_name'])

        print("discovered streams are correct")

        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
        selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                               test_catalog,
                                                                               menagerie.get_annotated_schema(conn_id, test_catalog['stream_id']),
                                                                               additional_md)

        # clear state
        menagerie.set_state(conn_id, {})

        # run initial full table sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()

        self.maxDiff = None
        for stream, recs in records_by_stream.items():
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   expected_sync_streams,
                                                                   expected_pks)

        self.assertEqual(record_count_by_stream, { self.table_name() : 2 })
        records_for_stream = runner.get_records_from_target_output()[self.table_name()]
        messages_for_stream = records_for_stream['messages']
        message_actions = [rec['action'] for rec in messages_for_stream]

        self.assertEqual(message_actions,
                         ['activate_version',
                          'upsert',
                          'upsert',
                          'activate_version'])

        # ensure some log_file and log_pos state was persisted
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id()]

        self.assertIsNotNone(bookmark['log_file'])
        self.assertIsNotNone(bookmark['log_pos'])

        expected_log_file = bookmark['log_file']
        expected_log_pos = bookmark['log_pos']

        # grab version, log_file and log_pos from state to check later
        expected_table_version = records_for_stream['table_version']

        self.assertEqual(expected_table_version, bookmark['version'])

        # check for expected records
        upsert_records = [m['data'] for m in messages_for_stream
                          if m['action'] == 'upsert']

        self.assertEqual([expected_rec_1, expected_rec_2], upsert_records)

        # run binlog sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # check that version
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id()]

        self.assertEqual(expected_table_version, bookmark['version'])

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()

        for stream, recs in records_by_stream.items():
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        # record count should be empty as we did not persist anything to the gate
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   expected_sync_streams,
                                                                   expected_pks)

        self.assertEqual(record_count_by_stream, {})

        # run some inserts, updates, and deletes in source
        updated_rec_1_varchar = 'THIS HAS BEEN UPDATED'

        with db_utils.get_db_connection(self.get_properties(), self.get_credentials()).cursor() as cur:
            cur.execute("UPDATE {}.{} SET our_varchar = '{}' WHERE id = {}".format(
                self.database_name(),
                self.table_name(),
                updated_rec_1_varchar,
                rec_1['id'])
            )

            delete_time = datetime.datetime.now()
            cur.execute("DELETE FROM {}.{} WHERE id = {}".format(
                self.database_name(),
                self.table_name(),
                rec_2['id'])
            )

            self.insert_record(cur, rec_3)

        # run binlog sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # check that version from state is unchanged
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id()]

        self.assertEqual(expected_table_version, bookmark['version'])

        # Either the log_file is the same but the log_pos has increased or the log_file
        # has rotated and the numeric suffix has increased
        if expected_log_file == bookmark['log_file']:
            print("PATH A")
            self.assertGreater(bookmark['log_pos'], expected_log_pos)
        else:
            expected_log_file_suffix = re.search('^.*\.(\d+)$', expected_log_file).groups()[0]
            updated_log_file_suffix = re.search('^.*\.(\d+)$', bookmark['log_file']).groups()[0]
            print("PATH B")
            self.assertGreater(int(updated_log_file_suffix), int(expected_log_file_suffix))

        expected_log_file = bookmark['log_file']
        expected_log_pos = bookmark['log_pos']

        updated_expected_rec_1 = copy.deepcopy(expected_rec_1)
        updated_expected_rec_2 = copy.deepcopy(expected_rec_2)
        updated_expected_rec_3 = copy.deepcopy(expected_rec_3)

        updated_expected_rec_1['our_varchar'] = updated_rec_1_varchar

        # Floats that come back from binlog provide more precision
        # than from SELECT based queries
        updated_expected_rec_1['our_unsigned_float'] = Decimal("1.2345000505447388")
        updated_expected_rec_1['our_signed_float'] = -Decimal("1.2345000505447388")
#        updated_expected_rec_1['_sdc_deleted_at'] = None
        updated_expected_rec_2['our_unsigned_float'] = Decimal("2.4690001010894775")
        updated_expected_rec_2['our_signed_float'] = -Decimal("2.4690001010894775")
#        updated_expected_rec_2['_sdc_deleted_at'] = None
#        updated_expected_rec_3['_sdc_deleted_at'] = None

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()

        for stream, recs in records_by_stream.items():
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        # check for expected records
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   expected_sync_streams,
                                                                   expected_pks)

        self.assertEqual(record_count_by_stream, { self.table_name() : 3 })

        records_for_stream = runner.get_records_from_target_output()[self.table_name()]
        messages_for_stream = records_for_stream['messages']
        message_actions = [rec['action'] for rec in messages_for_stream]

        self.assertEqual(message_actions,
                         ['upsert',
                          'upsert',
                          'upsert'])

        upsert_records = [m['data'] for m in messages_for_stream
                          if m['action'] == 'upsert']

        deleted_at_rec = upsert_records[1].get('_sdc_deleted_at')
        deleted_at_rec_timestamp = utils.strptime_to_utc(deleted_at_rec).timestamp()
        time_delta = delete_time.timestamp() - deleted_at_rec_timestamp
        print("Delete time vs record: difference in seconds", time_delta)
        self.assertIsNotNone(deleted_at_rec)
        assert(time_delta < 3) #i dunno

        # since we don't know exactly what the _sdc_deleted_at value will be
        # we will make the assertions we can make on that field here
        # and then remove it from all records prior to doing a full
        # record-level comparison
        self.assertIn('_sdc_deleted_at', upsert_records[0])
        self.assertIn('_sdc_deleted_at', upsert_records[1])
        self.assertIn('_sdc_deleted_at', upsert_records[2])
        self.assertIsNone(upsert_records[0].get('_sdc_deleted_at'))
        self.assertIsNotNone(upsert_records[1].get('_sdc_deleted_at'))
        self.assertIsNone(upsert_records[2].get('_sdc_deleted_at'))
        del upsert_records[0]['_sdc_deleted_at']
        del upsert_records[1]['_sdc_deleted_at']
        del upsert_records[2]['_sdc_deleted_at']
        
        self.assertEqual([updated_expected_rec_1,
                          updated_expected_rec_2,
                          updated_expected_rec_3],
                         upsert_records)

        # run binlog sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # check that version from state is unchanged
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id()]

        self.assertEqual(expected_table_version, bookmark['version'])

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()
        self.maxDiff = None
        for stream, recs in records_by_stream.items():
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        # record count should be empty as we did not persist anything to the gate
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   expected_sync_streams,
                                                                   expected_pks)


        self.assertEqual(record_count_by_stream, {})

SCENARIOS.add(MySQLBinlog)
