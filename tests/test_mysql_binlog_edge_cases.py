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
import random
import string

import db_utils


expected_schemas = {
    'mysql_binlog_test_edge_cases': {
        'type': 'object',
        'properties': {
            'id': {'type': ['null', 'integer'], 'minimum': -9223372036854775808, 'inclusion': 'automatic', 'maximum': 9223372036854775807},
            'our_timestamp_1': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},
            'our_timestamp_2': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},
            'our_varchar_1': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 255},
            'our_varchar_2': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 255},
        }
    },
    'mysql_binlog_test_edge_cases_dos': {
        'type': 'object',
        'properties': {
            'id': {'type': ['null', 'integer'], 'minimum': -9223372036854775808, 'inclusion': 'automatic', 'maximum': 9223372036854775807},
            'our_timestamp_1': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},
            'our_timestamp_2': {'type': ['null', 'string'], 'inclusion': 'available', 'format': 'date-time'},
            'our_varchar_1': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 255},
            'our_varchar_2': {'type': ['null', 'string'], 'inclusion': 'available', 'maxLength': 255},
        }
    }
}

class MySQLBinlog(unittest.TestCase):
    def tap_name(self):
        return 'tap-mysql'


    def name(self):
        return "tap_tester_mysql_binlog_edge_cases"


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


    def table_name_1(self):
        return "mysql_binlog_test_edge_cases"

    def table_name_2(self):
        return "mysql_binlog_test_edge_cases_dos"


    def tap_stream_id(self, table):
        return "{}-{}".format(self.database_name(), table)


    def generate_record_n(self, n):
        rec_1_datetime = datetime.datetime(2000, 1, 1, 1, 1, 1, 1000, tzinfo=pytz.UTC)
        rec_2_datetime = datetime.datetime(2002, 2, 2, 2, 2, 2, 2000, tzinfo=pytz.UTC)
        return {
            'id': n,
            'our_timestamp_1': rec_1_datetime,
            'our_timestamp_2': rec_2_datetime,
            'our_varchar_1': 'chicken',
            'our_varchar_2': 'pickle',
        }

    def create_n_records(self, n_records=25):
        records = []
        expected_records = []

        for n in range(n_records):
            rec_n = self.generate_record_n(n)
            expected_rec_n = copy.deepcopy(rec_n)
            expected_rec_n['our_timestamp_1'] = '2000-01-01T01:01:01.000000Z' # TODO is this correct? BUG?
            expected_rec_n['our_timestamp_2'] = '2002-02-02T02:02:02.000000Z'

            records.append(rec_n)
            expected_records.append(expected_rec_n)

        return expected_records, records


    def insert_record(self, cursor, record, table):
        rec_cols, rec_vals = zip(*record.items())

        columns_sql = ", \n".join(rec_cols)
        value_sql = ",".join(["%s" for i in range(len(rec_vals))])

        insert_sql = """
        INSERT INTO {}.{}
               ( {} )
        VALUES ( {} )""".format(
            self.database_name(),
            table,
            columns_sql,
            value_sql)

        cursor.execute(insert_sql, rec_vals)


    def fetch_server_id(self):
        with db_utils.get_db_connection(self.get_properties(), self.get_credentials()).cursor() as cur:
            cur.execute("SELECT @@server_id")
            server_id = cur.fetchone()[0]

            return server_id


    def get_engines(self):
        return [
            "INNODB",
        ]


    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_MYSQL_HOST'),
                                    os.getenv('TAP_MYSQL_PORT'),
                                    os.getenv('TAP_MYSQL_USER'),
                                    os.getenv('TAP_MYSQL_PASSWORD'),
                                    os.getenv('TAP_MYSQL_PORT')] if x == None]
        if len(missing_envs) != 0:
            #pylint: disable=line-too-long
            raise Exception("set TAP_MYSQL_HOST, TAP_MYSQL_PORT, TAP_MYSQL_DBNAME, TAP_MYSQL_USER, TAP_MYSQL_PASSWORD")

    def initialize_db(self, engine, log_file_size):
        connection = db_utils.get_db_connection(self.get_properties(), self.get_credentials())

        with connection.cursor() as cur:

            create_databases_sql = """
                DROP DATABASE IF EXISTS {};
                CREATE DATABASE {}
            """.format(self.database_name(), self.database_name())

            cur.execute(create_databases_sql)
            for table_name in [self.table_name_1(), self.table_name_2()]:
                cur.execute("""
                SELECT EXISTS (
                SELECT 1
                FROM  information_schema.tables
                WHERE  table_schema = %s
                AND  table_name =   %s);""",
                            [self.database_name(), table_name])

                existing_table = cur.fetchone()[0]

                if existing_table:
                    cur.execute("DROP TABLE {}.{}".format(self.database_name(), table_name))

                create_table_sql = """
CREATE TABLE {}.{} (
            id                     BIGINT PRIMARY KEY,
            our_timestamp_1        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            our_timestamp_2        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            our_varchar_1          VARCHAR(255),
            our_varchar_2          VARCHAR(255)
)
ENGINE = {}
""".format(self.database_name(), table_name, engine)
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

            # Ensure expected log file size in use
            if log_file_size is not None:
                cur.execute("""
                SHOW VARIABLES LIKE 'innodb_log_file_size';
                """)
                log_file_size_in_use = cur._result.rows[0][1]
                self.assertEqual(log_file_size, log_file_size_in_use,
                                 msg="Unexpected log file size in use: {}".format(log_file_size_in_use))


            # Change innodb_log_file_size = 3 MB (3145728)
            # the default page size is 16384 (16KB) which has a min file size req of 3 MB
            #      Timestamp is 4 bytes
            #      2 Timestamps + 2 varchar(255) ~= 518 bytes
            #      50331648 / 518 = 97165

            n_1 = 98000
            print("Generating {} records.".format(n_1))
            expected_records, records = self.create_n_records(n_1)

            print("Inserting {} records in table {}.".format(n_1, self.table_name_1()))
            inc = 0
            for record in records:
                self.insert_record(cur, record, self.table_name_1())

                # we are inserting many records, show some output for tester's sanity
                inc += 1
                if inc % 1000 == 0 and inc < 90001:
                    s = "{}%".format(int(100*inc/n_1)) if inc % 9000 == 0 else "."
                    print(s, sep=' ', end='', flush=True)

            print("") # breakline
            n_2 = 5 # first five records from table 1
            print("Inserting {} records in table {}.".format(n_2, self.table_name_2()))
            for i in range(n_2):
                self.insert_record(cur, records[i], self.table_name_2())

        print("\n\nMySQL DB Instantiated." + \
              "\nNAME: {}\nENGINE: {}".format(self.database_name(), engine_in_use) + \
              "\nTABLE: {}\nEVENTS: {} records inserted".format(self.table_name_1(), n_1) + \
              "\nTABLE: {}\nEVENTS: {} records inserted\n\n".format(self.table_name_2(), n_2))

        return expected_records

    def test_run(self):
        """Run the binlog replication edge case test using multiple storage engines."""
        engines = self.get_engines()
        for engine in engines:
            size = '50331648' # default
            expected_records = self.initialize_db(engine, size)
            self.binlog_edge_test(expected_records)


    def binlog_edge_test(self, expected_records=[]):
        """
        Test binlog replication edge cases
        • Verify an initial sync returns expected records of various datatypes
        • Verify we bookmark correctly when a transaction spans multiple files
        • Insert and delete a record prior to sync. Verify both events are replicated
        • Insert and update a record prior to sync. Verify both events are replicated
        • Verify a valid log_file and log_pos state are persisted after each sync
        """

        conn_id = connections.ensure_connection(self)

        # prior to first sync update a record...
        updated_timestamp = datetime.datetime.now()
        updated_id = 1
        expected_records[1]['our_timestamp_2'] = datetime.datetime.strftime(updated_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

        # insert a record and...
        inserted_record = self.generate_record_n(len(expected_records))
        expected_records += [inserted_record] # TODO need to format

        # delete a record
        deleted_id = 2

        with db_utils.get_db_connection(self.get_properties(), self.get_credentials()).cursor() as cur:
            cur.execute("UPDATE {}.{} SET our_timestamp_2 = '{}' WHERE id = {}".format(
                self.database_name(),
                self.table_name_1(),
                updated_timestamp,
                updated_id)

            )

            self.insert_record(cur, inserted_record, self.table_name_1())

            delete_time = datetime.datetime.now()
            cur.execute("DELETE FROM {}.{} WHERE id = {}".format(
                self.database_name(),
                self.table_name_1(),
                deleted_id)
            )

        print(
            "\n\nMySQL DB Actions." + \
            "\nNAME: {}\nTABLE: {}".format(self.database_name(), self.table_name_1()) + \
            "\nEVENTS: {} records updated".format(1) + \
            "\n        {} records deleted\n\n".format(1)
        )

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        t1 = self.table_name_1()
        t2 = self.table_name_2()
        expected_check_streams = {self.tap_stream_id(t1), self.tap_stream_id(t2)}
        expected_sync_streams = {t1, t2}
        expected_pks = {
            t1: { 'id' },
            t2: { 'id' }
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
        self.assertEqual(self.table_name_1(), found_catalogs[0]['stream_name'])
        self.assertEqual(self.table_name_2(), found_catalogs[1]['stream_name'])
        print("discovered streams are correct")

        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
        for catalog in found_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            _ = connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, catalog, additional_md
            )

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

        # BUG missing deleted record | https://stitchdata.atlassian.net/browse/SRCE-4258
        # self.assertEqual({self.table_name_1(): len(expected_records)}, record_count_by_stream)
        records_for_stream = runner.get_records_from_target_output()[self.table_name_1()]
        messages_for_stream = records_for_stream['messages']
        message_actions = [rec['action'] for rec in messages_for_stream]

        # verify activate version messages are present
        self.assertEqual('activate_version', message_actions[0])
        self.assertEqual('activate_version', message_actions[-1])

        # ensure some log_file and log_pos state was persisted
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id(t1)]

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
        # we need to compare record by record since there are so many.
        # a failure comparing expected_records to upsert_records would result in
        # an output message greater in length than a standard tmux buffer
        # BUG missing datetime precision | https://stitchdata.atlassian.net/browse/SRCE-4257
        # for expected_record in expected_records:
        #     upsert_record = [rec for rec in upsert_records
        #                      if rec['id'] == expected_record['id']]
        #     self.assertEqual(1, len(upsert_record),
        #                      msg="multiple upsert_recs with same pk: {}".format(upsert_record))
        #     self.assertEqual(expected_record, upsert_record.pop())

        # TODO add check for _sdc_delete_at for deleted record once bug addressed

        # run binlog sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # check that version
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id(t1)]

        self.assertEqual(expected_table_version, bookmark['version'])

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()
        for stream, recs in records_by_stream.items():
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        # record count should be empty as we did not persist anything to the gate
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, expected_sync_streams, expected_pks
        )
        self.assertEqual(record_count_by_stream, {})

        # Create 1 more record prior to 2nd sync
        new_record = self.generate_record_n(len(expected_records))
        with db_utils.get_db_connection(
                self.get_properties(), self.get_credentials()).cursor() as cur:
            self.insert_record(cur, new_record, self.table_name_1())
        print(
            "\n\nMySQL DB Actions." + \
            "\nNAME: {}\nTABLE: {}".format(self.database_name(), self.table_name_1()) + \
            "\nEVENTS: {} records inserted".format(1)
        )

        # run binlog sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # check that version from state is unchanged
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id(t1)]

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

        # Execute delete across tables using join prior to 3rd sync
        deleted_id = 4

        with db_utils.get_db_connection(
                self.get_properties(), self.get_credentials()).cursor() as cur:

            delete_time = datetime.datetime.now()
            # DELETE T1, T2
            # FROM T1
            # INNER JOIN T2 ON T1.key = T2.key
            # WHERE condition;
            db = self.database_name()
            db_t1 = db + "." + t1
            db_t2 = db + "." + t2
            t1_key = db_t1 + ".id"
            t2_key = db_t2 + ".id"
            statement = "DELETE {}, {} ".format(db_t1, db_t2) + \
                "FROM {} ".format(t1) + \
                "INNER JOIN {} ON {} = {} ".format(db_t2, t1_key, t2_key) + \
                "WHERE {} = {}".format(t1_key, deleted_id)
            cur.execute(statement)

        print(
            "\n\nMySQL DB Actions." + \
            "\nNAME: {}\nTABLE: {}".format(self.database_name(), self.table_name_2()) + \
            "\nTABLE: {}".format(self.table_name_2()) + \
            "\nEVENTS:  {} records deleted\n\n".format(1)
        )

        # run binlog sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # check that version from state is unchanged
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks'][self.tap_stream_id(t1)]

        self.assertEqual(expected_table_version, bookmark['version'])

        target_records = runner.get_records_from_target_output()
        records_stream_1 = target_records[self.table_name_1()]
        upsert_records_1 = [m['data'] for m in records_stream_1['messages']
                            if m['action'] == 'upsert']
        records_stream_2 = target_records[self.table_name_2()]
        upsert_records_2 = [m['data'] for m in records_stream_2['messages']
                            if m['action'] == 'upsert']

        # make sure the record is in the target for both tables with a delete time
        deleted_at_t1 = upsert_records_1[0].get('_sdc_deleted_at')
        deleted_at_t1_timestamp = utils.strptime_to_utc(deleted_at_t1).timestamp()
        self.assertIsNotNone(deleted_at_t1)

        deleted_at_t2 = upsert_records_2[0].get('_sdc_deleted_at')
        deleted_at_t2_timestamp = utils.strptime_to_utc(deleted_at_t2).timestamp()
        self.assertIsNotNone(deleted_at_t2)

        # the delete times should be equal since it was a single transaction
        self.assertEqual(deleted_at_t1_timestamp, deleted_at_t2_timestamp)
        time_delta = delete_time.timestamp() - deleted_at_t1_timestamp
        print("Delete time vs record: difference in seconds", time_delta)
        self.assertLess(time_delta, 3) # time delta less than 3 seconds in magnitude


SCENARIOS.add(MySQLBinlog)
