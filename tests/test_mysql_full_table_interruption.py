import os
import unittest

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from tap_tester.scenario import (SCENARIOS)

import db_utils

class MySQLFullTableInterruption(unittest.TestCase):
    def name(self):
        return "tap_tester_mysql_full_table_interruption"


    def tap_name(self):
        return "tap-mysql"


    def get_type(self):
        return "platform.mysql"


    def database_name(self):
        return "tap_tester_mysql_full_table_interruption"


    def get_credentials(self):
        return {'password': os.getenv('TAP_MYSQL_PASSWORD')}


    def get_properties(self):
        return {'host' : os.getenv('TAP_MYSQL_HOST'),
                'port' : os.getenv('TAP_MYSQL_PORT'),
                'database' : self.database_name(),
                'user' : os.getenv('TAP_MYSQL_USER'),
        }


    def table_names(self):
        return {
            "full_table",
            "full_table_composite_key"
        }


    def tap_stream_ids(self):
        return set(["{}-{}".format(self.database_name(), table_name) for table_name in self.table_names()])


    def expected_pks(self):
        return {
            "full_table": { 'a_pk' },
            "full_table_composite_key": { 'a_pk', 'a_varchar' }
        }


    def dummy_data(self):
        return [[1, 'a'],
                [2, 'b'],
                [3, 'c'],
                [4, 'd'],
                [5, 'e'],
                [6, 'f']]


    def expected_schemas(self):
        return {
            'full_table': {
                'properties': {
                    'a_pk': {
                        'inclusion': 'automatic',
                        'maximum': 2147483647,
                        'minimum': -2147483648,
                        'type': ['null',
                                 'integer']},
                    'a_varchar': {
                        'inclusion': 'available',
                        'maxLength': 10,
                        'type': ['null',
                                 'string']
                    }
                },
                 'type': 'object'
            },
            'full_table_composite_key': {
                'properties': {
                    'a_pk': {
                        'inclusion': 'automatic',
                        'maximum': 2147483647,
                        'minimum': -2147483648,
                        'type': ['null',
                                 'integer']},
                    'a_varchar': {
                        'inclusion': 'automatic',
                        'maxLength': 10,
                        'type': ['null',
                                 'string']
                    }
                },
                 'type': 'object'
            }
        }

    def get_interrupted_state(self):
        # NB: It seems that tap-mysql treats `initial_full_table_complete`
        # to mean "initial full table started" and uses it to emit or not
        # emit an activate version message
        return {
            'bookmarks': {
                '{}-full_table'.format(self.database_name()): {
                    'initial_full_table_complete': True,
                    'max_pk_values': {
                        'a_pk': 6
                    },
                    'last_pk_fetched': {
                        'a_pk': 2
                    }
                },
                '{}-full_table_composite_key'.format(self.database_name()): {
                    'initial_full_table_complete': True,
                    'max_pk_values': {
                        'a_pk': 6,
                        'a_varchar': 'f'
                    },
                    'last_pk_fetched': {
                        'a_pk': 2,
                        'a_varchar': 'b'
                    }
                }
            }
        }

    def insert_record(self, cursor, record, table_name):
        value_sql = ",".join(["%s" for i in range(len(record))])

        insert_sql = """
        INSERT INTO {}.{}
               ( `a_pk`, `a_varchar` )
        VALUES ( {} )""".format(
            self.database_name(),
            table_name,
            value_sql)

        cursor.execute(insert_sql, record)


    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_MYSQL_HOST'),
                                    os.getenv('TAP_MYSQL_USER'),
                                    os.getenv('TAP_MYSQL_PASSWORD'),
                                    os.getenv('TAP_MYSQL_PORT')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_MYSQL_HOST, TAP_MYSQL_USER, TAP_MYSQL_PASSWORD, TAP_MYSQL_PORT")

        print("setting up mysql databases and tables")
        props = self.get_properties()
        props.pop('database') # Don't connect to specific database for setup
        connection = db_utils.get_db_connection(props, self.get_credentials())

        with connection.cursor() as cur:
            create_databases_sql = """
                DROP DATABASE IF EXISTS {};
                CREATE DATABASE {};
            """.format(self.database_name(), self.database_name())

            cur.execute(create_databases_sql)

            create_table_sql = """
            CREATE TABLE {}.full_table (
                    a_pk      INTEGER AUTO_INCREMENT PRIMARY KEY,
                    a_varchar VARCHAR(10));
            """.format(self.database_name())

            cur.execute(create_table_sql)

            create_composite_key_table_sql = """
                CREATE TABLE {}.full_table_composite_key (
                    a_pk      INTEGER AUTO_INCREMENT,
                    a_varchar VARCHAR(10),
                    PRIMARY KEY (a_pk, a_varchar)
                );
            """.format(self.database_name())

            cur.execute(create_composite_key_table_sql)

            for table_name in self.table_names():
                for record in self.dummy_data():
                    self.insert_record(cur, record, table_name)

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify the tap discovered the right streams
        found_catalogs = [catalog for catalog
                          in menagerie.get_catalogs(conn_id)
                          if catalog['tap_stream_id'] in self.tap_stream_ids()]

        self.assertEqual(len(found_catalogs),
                         2,
                         msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.tap_stream_ids().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        # verify that persisted streams have the correct properties
        found_table_names = set(map(lambda c: c['stream_name'], found_catalogs))
        tables_diff = self.table_names().symmetric_difference(found_table_names)
        self.assertEqual(len(tables_diff), 0, msg="discovered schemas do not match: {}".format(tables_diff))

        print("discovered streams are correct")

        # Select all catalogs for full table sync
        for test_catalog in found_catalogs:
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   test_catalog,
                                                                                   menagerie.get_annotated_schema(conn_id, test_catalog['stream_id']),
                                                                                   additional_md)

        # Set state to mimic that the a full table sync did not complete
        menagerie.set_state(conn_id, self.get_interrupted_state())

        # run full table sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)


        # verify the target output (schema, record count, message actions)
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.table_names(),
                                                                   self.expected_pks())
        self.assertEqual(record_count_by_stream, { 'full_table' : 4, 'full_table_composite_key' : 4 })

        records_by_stream = runner.get_records_from_target_output()
        for stream, recs in records_by_stream.items():
            self.assertEqual(recs['schema'],
                             self.expected_schemas()[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

            messages_for_stream = recs['messages']
            message_actions = [rec['action'] for rec in messages_for_stream]

            self.assertEqual(message_actions,
                             ['upsert',
                              'upsert',
                              'upsert',
                              'upsert',
                              'activate_version'])

        state = menagerie.get_state(conn_id)
        for tap_stream_id in self.tap_stream_ids():
            bookmark = state['bookmarks'][tap_stream_id]

            # last_pk_fetched and max_pk_values are cleared after success
            self.assertEqual(bookmark,
                             {'initial_full_table_complete': True})

SCENARIOS.add(MySQLFullTableInterruption)
