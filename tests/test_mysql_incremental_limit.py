from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import datetime
import unittest
import datetime
import pprint
import pdb
from functools import reduce
from singer import utils

import db_utils

dummy_data = {'simple_example': [[1, 'matt', '2017-01-01 00:00:00'], [2, 'kyler', '2017-01-01 00:00:01'], [3, 'jimbo', '2017-01-01 00:00:02'],
                                 [4, 'jon', '2017-01-01 00:00:03'], [5, 'tom', '2017-01-01 00:00:04']] }

expected_schemas = {'simple_example': {'properties': {'c_pk': {'inclusion': 'automatic',
                                                               'maximum': 2147483647,
                                                               'minimum': -2147483648,
                                                               'type': ['null',
                                                                        'integer']},
                                                      'c_varchar': {'inclusion': 'available',
                                                                    'maxLength': 255,
                                                                    'type': ['null',
                                                                             'string']},
                                                      'c_dt': {'format': 'date-time',
                                                               'inclusion': 'available',
                                                               'type': ['null', 'string']}},
                                       'type': 'object'}
                    }

expected_catalogs = {'simple_example': {'annotated_schema': None,
                                        'database_name': 'tap_tester_mysql_0',
                                        'is_view': False,
                                        # 'key_properties': ['c_pk'],
                                        'key_properties': [],
                                        'replication_key': None,
                                        'replication_method': None,
                                        'row_count': 1,
                                        'schema_name': None,
                                        'selected': False,
                                        'stream': 'simple_example',
                                        'stream_alias': None,
                                        'stream_id': 17559,
                                        'stream_name': 'simple_example',
                                        'table_name': 'simple_example',
                                        'tap_stream_id': 'tap_tester_mysql_0-simple_example'}
                     }

class MySQLIncrementalLimit(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        missing_envs = [x for x in [os.getenv('TAP_MYSQL_HOST'),
                                    os.getenv('TAP_MYSQL_USER'),
                                    os.getenv('TAP_MYSQL_PASSWORD'),
                                    os.getenv('TAP_MYSQL_PORT')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_MYSQL_HOST, TAP_MYSQL_USER, TAP_MYSQL_PASSWORD, TAP_MYSQL_PORT")

        print("setting up mysql databases and tables")
        connection = db_utils.get_db_connection(self.get_properties(), self.get_credentials())

        with connection.cursor() as cursor:
            flatten = lambda l: [item for sublist in l for item in sublist]
            var_string_for_table = lambda t: ', '.join(['%s'] * len(dummy_data[t][0]))
            create_databases_sql = '''
                DROP DATABASE IF EXISTS tap_tester_mysql_0;
                CREATE DATABASE tap_tester_mysql_0;
                DROP DATABASE IF EXISTS tap_tester_mysql_1;
                CREATE DATABASE tap_tester_mysql_1;
            '''
            cursor.execute(create_databases_sql)

            var_string = var_string_for_table('simple_example')
            simple_example_table_sql = '''
                CREATE TABLE tap_tester_mysql_0.simple_example (
                    c_pk INTEGER PRIMARY KEY,
                    c_varchar VARCHAR(255),
                    c_dt DATETIME);
                INSERT INTO tap_tester_mysql_0.simple_example VALUES (%s), (%s), (%s), (%s), (%s);
            ''' % (var_string, var_string, var_string, var_string, var_string)

            cursor.execute(simple_example_table_sql, flatten(dummy_data['simple_example']))

    def expected_check_streams(self):
        return {
            'tap_tester_mysql_0-simple_example'
        }

    def expected_sync_streams(self):
        return {
            'simple_example'
        }

    def expected_pks(self):
        return {
            'simple_example': {'c_pk'}
        }

    def name(self):
        return "tap_tester_mysql_incremental_limit"

    def tap_name(self):
        return "tap-mysql"

    def get_type(self):
        return "platform.mysql"

    def get_credentials(self):
        return {'password': os.getenv('TAP_MYSQL_PASSWORD')}

    def get_properties(self):
        return {'host' : os.getenv('TAP_MYSQL_HOST'),
                'port': os.getenv('TAP_MYSQL_PORT'),
                'user': os.getenv('TAP_MYSQL_USER'),
                'incremental_limit': "2"}


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)


        # verify the tap discovered the right streams
        found_catalogs = [fc for fc
                          in menagerie.get_catalogs(conn_id)
                          if fc['tap_stream_id'] in self.expected_check_streams()]
        self.assertGreater(len(found_catalogs),
                           0,
                           msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        print("discovered streams are correct")

        ##select_simple_example
        for a_catalog in found_catalogs:
            additional_md = []
            unselected_fields = []
            if a_catalog['tap_stream_id'] == 'tap_tester_mysql_0-simple_example':
                additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-key' : 'c_dt', 'replication-method' : 'INCREMENTAL'}}]

            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id, a_catalog,
                                                                                   menagerie.get_annotated_schema(conn_id, a_catalog['stream_id']),
                                                                                   additional_md,
                                                                                   unselected_fields)
        # clear state
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        # the >= boundary causes the same rows to be seen in this small sized limit.
        expected_row_count = 9 # {'simple_example': 9}
        self.assertEqual(replicated_row_count,
                         expected_row_count,
                         msg="failed to replicate correct number of rows: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        records_by_stream = runner.get_records_from_target_output()

        print("records are correct")

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)
        bookmarks = state['bookmarks']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        for k, v in bookmarks.items():
            self.assertIsNotNone(v['version'],
                                 msg="expected bookmark for stream `{}` to have a version set".format(k))
            self.assertEqual(v['replication_key_value'],
                             '2017-01-01T00:00:04.000000Z',
                             msg="incorrect replication_key_value in bookmark for stream `{}`".format(k))
            self.assertEqual(v['replication_key'],
                             'c_dt',
                             msg="incorrect replication_key specified in bookmark for stream `{}`".format(k))

        print("state and bookmarks are correct")

SCENARIOS.add(MySQLIncrementalLimit)
