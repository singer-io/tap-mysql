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
from decimal import Decimal

import db_utils

class MySQLTimezoneTest(unittest.TestCase):
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
            # Set this to -7:00 because get_db_connection() sets it to +0:00
            cursor.execute("SET @@session.time_zone='-7:00'")

            flatten = lambda l: [item for sublist in l for item in sublist]
            var_string_for_table = lambda t: ', '.join(['%s'] * len(dummy_data[t][0]))
            create_databases_sql = '''
                DROP DATABASE IF EXISTS tap_tester_mysql_0;
                CREATE DATABASE tap_tester_mysql_0;
                DROP DATABASE IF EXISTS tap_tester_mysql_1;
                CREATE DATABASE tap_tester_mysql_1;
            '''
            cursor.execute(create_databases_sql)

            simple_example_table_sql = '''
                CREATE TABLE tap_tester_mysql_0.simple_example (
                    c_pk INTEGER PRIMARY KEY,
                    c_dt DATETIME);
            '''

            cursor.execute(simple_example_table_sql)
            
            data_sql = """
                INSERT INTO tap_tester_mysql_0.simple_example
                (c_pk, c_dt)
                VALUES
                (1, '2021-01-01 01:00:00'),
                (2, '2021-01-01 02:00:00'),
                (3, '2021-01-01 03:00:00'),
                (4, '2021-01-01 04:00:00'),
                (5, '2021-01-01 05:00:00'),
                (6, '2021-01-01 06:00:00'),
                (7, '2021-01-01 07:00:00'),
                (8, '2021-01-01 08:00:00'),
                (9, '2021-01-01 09:00:00'),
                (10, '2021-01-01 10:00:00');
            """
            cursor.execute(data_sql)


    def expected_check_streams(self):
        return {
            'tap_tester_mysql_0-simple_example',
            'tap_tester_mysql_0-various_types',
            'tap_tester_mysql_0-incremental',
            'tap_tester_mysql_1-my_isam',
            'tap_tester_mysql_1-view',
        }

    def expected_sync_streams(self):
        return {
            'various_types',
            'incremental',
            'my_isam',
            'view',
        }

    def expected_pks(self):
        return {
            'various_types': {'c_pk'},
            'incremental': {'c_pk'},
            'my_isam': {'c_pk'},
            'view': {'c_pk'},
        }

    def name(self):
        return "tap_tester_mysql_full_and_incremental"

    def tap_name(self):
        return "tap-mysql"

    def get_type(self):
        return "platform.mysql"

    def get_credentials(self):
        return {'password': os.getenv('TAP_MYSQL_PASSWORD')}

    def get_properties(self):
        return {'host' : os.getenv('TAP_MYSQL_HOST'),
                'port': os.getenv('TAP_MYSQL_PORT'),
                'user': os.getenv('TAP_MYSQL_USER')}


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        catalog = [catalog_entry
                   for catalog_entry in menagerie.get_catalogs(conn_id)
                   if catalog_entry['tap_stream_id'] == 'tap_tester_mysql_0-simple_example'][0]

        schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
        metadata = schema_and_metadata['metadata']
        extra_metadata = [{'breadcrumb': [],
                           'metadata': {'replication-method': "INCREMENTAL",
                                        "replication-key": "c_dt",
                                        "selected": "true"}},]

        connections.set_non_discoverable_metadata(
            conn_id,
            catalog,
            schema_and_metadata,
            extra_metadata)

        sync_job_name = runner.run_sync_mode(self, conn_id)

        connection = db_utils.get_db_connection(self.get_properties(), self.get_credentials())
        with connection.cursor() as cursor:
            # Set this to -7:00 because get_db_connection() sets it to +0:00
            cursor.execute("SET @@session.time_zone='-7:00'")
            cursor.execute("select *, CONVERT_TZ(c_dt, 'MST', 'UTC') from tap_tester_mysql_0.simple_example;")

            for row in cursor:
                print(row)

            data_sql = """
                INSERT INTO tap_tester_mysql_0.simple_example
                (c_pk, c_dt)
                VALUES
                (11, '2021-01-01 11:00:00'),
                (12, '2021-01-01 12:00:00'),
                (13, '2021-01-01 13:00:00'),
                (14, '2021-01-01 14:00:00'),
                (15, '2021-01-01 15:00:00'),
                (16, '2021-01-01 16:00:00'),
                (17, '2021-01-01 17:00:00'),
                (18, '2021-01-01 18:00:00'),
                (19, '2021-01-01 19:00:00'),
                (20, '2021-01-01 20:00:00');
            """
            cursor.execute(data_sql)
            
        # sync_job_name = runner.run_sync_mode(self, conn_id)

