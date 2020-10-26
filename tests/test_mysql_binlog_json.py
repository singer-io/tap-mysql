from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from singer import utils
from decimal import Decimal
import copy
import datetime
import decimal
import json
import os
import pytz
import re
import unittest

import db_utils

expected_schemas = {
    'mysql_binlog_json_test': {
        'type': 'object',
        'properties': {
            'id': {'type': ['null', 'integer'], 'minimum': -9223372036854775808, 'inclusion': 'automatic', 'maximum': 9223372036854775807},
            'our_json': {'type': ['null', 'string'], 'inclusion': 'available'}
        }
    }
}


rec_1 = {
    'id': 1,
    'our_json': json.dumps({"foo": "bar"}),
}


expected_rec_1 = {
    'id': 1,
    'our_json': json.dumps({"foo": "bar"})
}

class MySQLBinlogJson(unittest.TestCase):
    def tap_name(self):
        return 'tap-mysql'


    def name(self):
        return "tap_tester_mysql_binlog_json"


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
        return "mysql_binlog_json_test"


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
            our_json               JSON
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

            for record in [rec_1]:
                self.insert_record(cur, record)

        print("\n\nMySQL DB Instantiated." + \
              "\nNAME: {}\nENGINE: {}".format(self.database_name(), engine_in_use) + \
              "\nTABLE: {}\nEVENTS: 1 record inserted\n\n".format(self.table_name()))


    def test_run(self):
        """Run the binlog replication json test using multiple storage engines."""
        engines = self.get_engines()
        for engine in engines:
            self.initialize_db(engine)
            self.binlog_json_test()


    def binlog_json_test(self):
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


        self.assertEqual(record_count_by_stream, { self.table_name() : 1 })
        records_for_stream = runner.get_records_from_target_output()[self.table_name()]
        messages_for_stream = records_for_stream['messages']
        message_actions = [rec['action'] for rec in messages_for_stream]

        self.assertEqual(message_actions,
                         ['activate_version',
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

        self.assertEqual([expected_rec_1], upsert_records)

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

        # insert a new huge row
        data = dict([('foooo%i'%i, 'baaaaar%i'%i) for i in range(2560)], literal=True)
        rec = {'id': 2, 'our_json': json.dumps(data)}

        with db_utils.get_db_connection(self.get_properties(), self.get_credentials()).cursor() as cur:
            self.insert_record(cur, rec)

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
            self.assertGreater(bookmark['log_pos'], expected_log_pos)
        else:
            expected_log_file_suffix = re.search('^.*\.(\d+)$', expected_log_file).groups()[0]
            updated_log_file_suffix = re.search('^.*\.(\d+)$', bookmark['log_file']).groups()[0]

            self.assertGreater(int(updated_log_file_suffix), int(expected_log_file_suffix))

        expected_log_file = bookmark['log_file']
        expected_log_pos = bookmark['log_pos']

        expected_rec_2 = copy.deepcopy(rec)

        # check for expected records
        records_for_stream = runner.get_records_from_target_output()[self.table_name()]
        messages_for_stream = records_for_stream['messages']
        message_actions = [rec['action'] for rec in messages_for_stream]

        self.assertEqual(message_actions,
                         ['upsert'])

        upsert_records = [m['data'] for m in messages_for_stream
                          if m['action'] == 'upsert']
        del upsert_records[0]['_sdc_deleted_at']

        expected_json = json.loads(expected_rec_2.get('our_json', {}))
        actual_json = json.loads(upsert_records[0].get('our_json', {}))

        self.assertTrue(len(actual_json.keys()) > 0)
        self.assertEqual(expected_json, actual_json)

SCENARIOS.add(MySQLBinlogJson)
