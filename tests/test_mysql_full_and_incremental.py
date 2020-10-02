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

dummy_data = {'simple_example': [[1, 'matt']],
              'various_types': [[1, -1, 1.23, 123456789.00, 127, 32767, 8388607, 2147483647, 9223372036854775807, 1.234, 1.234, 1, '2017-09-13', '12:34:56', '1999', 1],
                                [2, 0, 0.23, 123456790.00, -128, -32768, -8388608, -2147483648, -9223372036854775808, 2.234, 2.234, 0, '2017-09-14', '12:34:57', '2000', 0],
                                [3, 0, 0.23, 123456790.00, -128, -32768, -8388608, -2147483648, -9223372036854775808, 2.234, 2.234, None, '2017-09-14', '12:34:57', '2000', None]],
              'incremental': [[1, 'a', '2017-01-01 00:00:00', 'howdy'],
                              [2, 'b', '2017-01-01 00:00:01', 'ho'],
                              [3, 'c', '2017-01-01 00:00:02', 'pardner']]}

expected_schemas = {'simple_example': {'properties': {'c_pk': {'inclusion': 'automatic',
                                                               'maximum': 2147483647,
                                                               'minimum': -2147483648,
                                                               'type': ['null',
                                                                        'integer']},
                                                      'c_varchar': {'inclusion': 'available',
                                                                    'maxLength': 255,
                                                                    'type': ['null',
                                                                             'string']}},
                                       'type': 'object'},
                    'various_types': {'properties': {'c_bigint': {'inclusion': 'available',
                                                                  'maximum': 9223372036854775807,
                                                                  'minimum': -9223372036854775808,
                                                                  'type': ['null',
                                                                           'integer']},
                                                     'c_bit': {'inclusion': 'available',
                                                               'type': ['null',
                                                                        'boolean']},
                                                     'c_date': {'format': 'date-time',
                                                                'inclusion': 'available',
                                                                'type': ['null',
                                                                         'string']},
                                                     'c_decimal': {'inclusion': 'available',
                                                                   'multipleOf': 1,
                                                                   'type': ['null',
                                                                            'number']},
                                                     # NB: The runner uses simplejson to parse with decimal=True
                                                     #     The target should NOT be writing `Decimal(0.01)`
                                                     'c_decimal_2': {'inclusion': 'available',
                                                                     'multipleOf': Decimal("0.01"),
                                                                     'type': ['null',
                                                                              'number']},
                                                     'c_decimal_2_unsigned': {'inclusion': 'available',
                                                                              'multipleOf': Decimal("0.01"),
                                                                              'type': ['null',
                                                                                       'number']},
                                                     'c_double': {'inclusion': 'available',
                                                                  'type': ['null',
                                                                           'number']},
                                                     'c_float': {'inclusion': 'available',
                                                                 'type': ['null',
                                                                          'number']},
                                                     'c_int': {'inclusion': 'available',
                                                               'maximum': 2147483647,
                                                               'minimum': -2147483648,
                                                               'type': ['null',
                                                                        'integer']},
                                                     'c_mediumint': {'inclusion': 'available',
                                                                     'maximum': 8388607,
                                                                     'minimum': -8388608,
                                                                     'type': ['null',
                                                                              'integer']},
                                                     'c_pk': {'inclusion': 'automatic',
                                                              'maximum': 2147483647,
                                                              'minimum': -2147483648,
                                                              'type': ['null',
                                                                       'integer']},
                                                     'c_smallint': {'inclusion': 'available',
                                                                    'maximum': 32767,
                                                                    'minimum': -32768,
                                                                    'type': ['null',
                                                                             'integer']},
                                                     'c_time': {'format': 'date-time',
                                                                'inclusion': 'available',
                                                                'type': ['null',
                                                                         'string']},
                                                     'c_tinyint': {'inclusion': 'available',
                                                                   'maximum': 127,
                                                                   'minimum': -128,
                                                                   'type': ['null',
                                                                            'integer']},
                                                     'c_tinyint_1': {'inclusion': 'available',
                                                                     'type': ['null',
                                                                              'boolean']}},
                                      'type': 'object'},
                    'incremental': {'properties': {'c_dt': {'format': 'date-time',
                                                            'inclusion': 'available',
                                                            'type': ['null', 'string']},
                                                   'c_pk': {'inclusion': 'automatic',
                                                            'maximum': 2147483647,
                                                            'minimum': -2147483648,
                                                            'type': ['null',
                                                                     'integer']},
                                                   'c_varchar': {'inclusion': 'available',
                                                                 'maxLength': 255,
                                                                 'type': ['null',
                                                                          'string']}},
                                    'type': 'object'},
                    'my_isam': {'properties': {'c_pk': {'inclusion': 'automatic',
                                                        'maximum': 2147483647,
                                                        'minimum': -2147483648,
                                                        'type': ['null',
                                                                 'integer']},
                                               'c_varchar': {'inclusion': 'available',
                                                             'maxLength': 255,
                                                             'type': ['null',
                                                                      'string']}},
                                'type': 'object'},
                    'view': {'properties': {'c_pk': {'inclusion': 'available',
                                                     'maximum': 2147483647,
                                                     'minimum': -2147483648,
                                                     'type': ['null',
                                                              'integer']},
                                            'c_varchar': {'inclusion': 'available',
                                                          'maxLength': 255,
                                                          'type': ['null',
                                                                   'string']}},
                             'type': 'object'}}

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
                                        'tap_stream_id': 'tap_tester_mysql_0-simple_example'},
                     'various_types': {'annotated_schema': None,
                                       'is_view': False,
                                       # 'key_properties': ['c_pk'],
                                       'key_properties': [],
                                       'row_count': len(dummy_data['various_types']),
                                       'schema_name': None,
                                       'selected': False,
                                       'stream': 'various_types',
                                       'stream_alias': None,
                                       'stream_id': 17560,
                                       'stream_name': 'various_types',
                                       'table_name': 'various_types',
                                       'tap_stream_id': 'tap_tester_mysql_0-various_types'},
                     'incremental': {'annotated_schema': None,
                                     'schema_name': None,
                                     'stream': 'incremental',
                                     'stream_alias': None,
                                     'stream_id': 19254,
                                     'stream_name': 'incremental',
                                     'table_name': 'incremental',
                                     'tap_stream_id': 'tap_tester_mysql_0-incremental'},
                     'my_isam': {'annotated_schema': None,
                                 'database_name': 'tap_tester_mysql_1',
                                 'is_view': False,
                                 'key_properties': [],
                                 # 'key_properties': ['c_pk'],
                                 'replication_key': None,
                                 'replication_method': None,
                                 'row_count': 1,
                                 'schema_name': None,
                                 'selected': False,
                                 'stream': 'my_isam',
                                 'stream_alias': None,
                                 'stream_id': 20716,
                                 'stream_name': 'my_isam',
                                 'table_name': 'my_isam',
                                 'tap_stream_id': 'tap_tester_mysql_1-my_isam'},
                     'view': {'annotated_schema': None,
                              'schema_name': None,
                              'stream': 'view',
                              'stream_alias': None,
                              'stream_id': 17559,
                              'stream_name': 'view',
                              'table_name': 'view',
                              'tap_stream_id': 'tap_tester_mysql_1-view'}}

class MySQLFullAndIncremental(unittest.TestCase):
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

            simple_example_table_sql = '''
                CREATE TABLE tap_tester_mysql_0.simple_example (
                    c_pk INTEGER PRIMARY KEY,
                    c_varchar VARCHAR(255));
                INSERT INTO tap_tester_mysql_0.simple_example VALUES (%s);
            ''' % var_string_for_table('simple_example')

            cursor.execute(simple_example_table_sql, flatten(dummy_data['simple_example']))

            var_string = var_string_for_table('various_types')
            various_types_table_sql = '''
                CREATE TABLE tap_tester_mysql_0.various_types (
                    c_pk INTEGER PRIMARY KEY,
                    c_decimal DECIMAL,
                    c_decimal_2_unsigned DECIMAL(5, 2) UNSIGNED,
                    c_decimal_2 DECIMAL(11, 2),
                    c_tinyint TINYINT,
                    c_smallint SMALLINT,
                    c_mediumint MEDIUMINT,
                    c_int INT,
                    c_bigint BIGINT,
                    c_float FLOAT,
                    c_double DOUBLE,
                    c_bit BIT(4),
                    c_date DATE,
                    c_time TIME,
                    c_year YEAR,
                    c_tinyint_1 TINYINT(1));
                INSERT INTO tap_tester_mysql_0.various_types
                    VALUES (%s), (%s), (%s);
            ''' % (var_string, var_string, var_string)

            cursor.execute(various_types_table_sql, flatten(dummy_data['various_types']))

            var_string = var_string_for_table('incremental')
            incremental_table_sql = '''
                CREATE TABLE tap_tester_mysql_0.incremental (
                    c_pk INTEGER PRIMARY KEY,
                    c_varchar VARCHAR(255),
                    c_dt DATETIME,
                    c_varchar_to_deselect VARCHAR(255));
                INSERT INTO tap_tester_mysql_0.incremental
                    VALUES (%s), (%s), (%s);
            ''' % (var_string, var_string, var_string)

            cursor.execute(incremental_table_sql, flatten(dummy_data['incremental']))

            var_string = var_string_for_table('simple_example')
            isam_table_sql = '''
                CREATE TABLE tap_tester_mysql_1.my_isam (
                    c_pk INTEGER PRIMARY KEY,
                    c_varchar VARCHAR(255))
                ENGINE = MYISAM;
                INSERT INTO tap_tester_mysql_1.my_isam VALUES (%s);
            ''' % var_string_for_table('simple_example')

            cursor.execute(isam_table_sql, flatten(dummy_data['simple_example']))

            view_sql = '''
                CREATE VIEW tap_tester_mysql_1.view AS SELECT * FROM tap_tester_mysql_0.simple_example;
            '''

            cursor.execute(view_sql)

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

        # verify that persisted streams have the correct properties
        for c in found_catalogs:
            catalog_props_to_check = ['stream_name',
                                      'tap_stream_id']
            stream = c['stream_name']

            for prop in catalog_props_to_check:
                self.assertEqual(c[prop],
                                 expected_catalogs[stream][prop],
                                 msg="unexpected stream catalog property `{}` for stream `{}`: `{}` != `{}`".format(prop, stream, expected_catalogs[stream][prop], c[prop]))

        print("discovered streams are correct")

        print('checking discoverd metadata for tap_tester_mysql_0-incremental')
        incremental_catalog = [c for c in found_catalogs if c['tap_stream_id'] == 'tap_tester_mysql_0-incremental'][0]
        md = menagerie.get_annotated_schema(conn_id, incremental_catalog['stream_id'])['metadata']

        incremental_stream_metadata = {
            'database-name': 'tap_tester_mysql_0',
            'row-count': 3,
            'is-view': False,
            'selected-by-default': False,
            'table-key-properties' : ['c_pk']
        }

        self.assertEqual(sorted(md, key=lambda x: x['breadcrumb']),
                         [{'breadcrumb': [],                                      'metadata': incremental_stream_metadata},
                          {'breadcrumb': ['properties', 'c_dt'],                  'metadata': {'selected-by-default': True, 'sql-datatype': 'datetime'}},
                          {'breadcrumb': ['properties', 'c_pk'],                  'metadata': {'selected-by-default': True, 'sql-datatype': 'int(11)'}},
                          {'breadcrumb': ['properties', 'c_varchar'],             'metadata': {'selected-by-default': True, 'sql-datatype': 'varchar(255)'}},
                          {'breadcrumb': ['properties', 'c_varchar_to_deselect'], 'metadata': {'selected-by-default': True, 'sql-datatype': 'varchar(255)'}}
                         ])


        print('checking discovered metadata for tap_tester_mysql_1-view')
        view_catalog = [c for c in found_catalogs if c['tap_stream_id'] == 'tap_tester_mysql_1-view'][0]
        view_catalog_key_properties_md = [{'breadcrumb': [], 'metadata': {'view-key-properties': ['c_pk']}}]

        connections.set_non_discoverable_metadata(conn_id, view_catalog, menagerie.get_annotated_schema(conn_id, view_catalog['stream_id']), view_catalog_key_properties_md)
        md = menagerie.get_annotated_schema(conn_id, view_catalog['stream_id'])['metadata']

        view_stream_metadata = {
            'database-name': 'tap_tester_mysql_1',
            'is-view': True,
            'selected-by-default': False,
            'view-key-properties' : ['c_pk']
        }

        self.assertEqual(sorted(md, key=lambda x: x['breadcrumb']),
                         [{'breadcrumb': [], 'metadata': view_stream_metadata},
                          {'breadcrumb': ['properties', 'c_pk'], 'metadata': {'selected-by-default': True, 'sql-datatype': 'int(11)'}},
                          {'breadcrumb': ['properties', 'c_varchar'], 'metadata': {'selected-by-default': True, 'sql-datatype': 'varchar(255)'}}])


        #No selected-by-default MD for c_year because it is an unsupported type
        various_types_catalog = [c for c in found_catalogs if c['tap_stream_id'] == 'tap_tester_mysql_0-various_types'][0]
        md = menagerie.get_annotated_schema(conn_id, various_types_catalog['stream_id'])['metadata']
        c_year_md = [x for x in md if x['breadcrumb'] == ['properties', 'c_year']]
        self.assertEqual(c_year_md,  [{'breadcrumb': ['properties', 'c_year'],
                                       'metadata': {'selected-by-default': False,
                                                    'sql-datatype': 'year(4)'}}])

        ##select_simple_example
        catalogs_to_select = [c for c in found_catalogs if c['tap_stream_id'] != 'tap_tester_mysql_0-simple_example']

        for a_catalog in catalogs_to_select:
            additional_md = []
            unselected_fields = []
            if a_catalog['tap_stream_id'] == 'tap_tester_mysql_0-incremental':
                additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-key' : 'c_dt', 'replication-method' : 'INCREMENTAL'}}]
                unselected_fields = ['c_varchar_to_deselect']

            elif a_catalog['tap_stream_id'] == 'tap_tester_mysql_1-view':
                additional_md = [{ "breadcrumb" : [], "metadata" : {'view-key-properties' : ['c_pk'], 'replication-method' : 'FULL_TABLE'}}]
            else:
                additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]

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
        expected_row_count = 8 # {'my_isam': 1, 'various_types': 3, 'incremental': 3, 'view': 1}
        self.assertEqual(replicated_row_count,
                         expected_row_count,
                         msg="failed to replicate correct number of rows: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        records_by_stream = runner.get_records_from_target_output()

        # verifications about individual records
        for stream, recs in records_by_stream.items():
            # verify that activate version messages were sent in the proper position
            self.assertEqual(recs['messages'][0]['action'],
                             'activate_version',
                             msg="Expected first message sent for stream `{}` to have action `activate_version`".format(stream))

            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        # verify that the target output the proper numeric and date representations
        expected_various_types_records = [{'c_time': '1970-01-01T12:34:56.000000Z',
                                           'c_mediumint': 8388607,
                                           'c_smallint': 32767,
                                           'c_tinyint': 127,
                                           'c_date': '2017-09-13T00:00:00.000000Z',
                                           'c_bigint': 9223372036854775807,
                                           'c_decimal': -1,
                                           'c_int': 2147483647,
                                           'c_bit': True,
                                           'c_decimal_2': Decimal('123456789.0'),
                                           'c_pk': 1,
                                           'c_double': Decimal("1.234"),
                                           'c_float': Decimal("1.234"),
                                           'c_decimal_2_unsigned': Decimal("1.23"),
                                           'c_tinyint_1': True},
                                          {'c_time': '1970-01-01T12:34:57.000000Z',
                                           'c_mediumint': -8388608,
                                           'c_smallint': -32768,
                                           'c_tinyint': -128,
                                           'c_date': '2017-09-14T00:00:00.000000Z',
                                           'c_bigint': -9223372036854775808,
                                           'c_decimal': 0,
                                           'c_int': -2147483648,
                                           'c_bit': False,
                                           'c_decimal_2': Decimal("123456790.0"),
                                           'c_pk': 2,
                                           'c_double': Decimal("2.234"),
                                           'c_float': Decimal("2.234"),
                                           'c_decimal_2_unsigned': Decimal("0.23"),
                                           'c_tinyint_1': False},
                                          {'c_time': '1970-01-01T12:34:57.000000Z',
                                           'c_mediumint': -8388608,
                                           'c_smallint': -32768,
                                           'c_tinyint': -128,
                                           'c_date': '2017-09-14T00:00:00.000000Z',
                                           'c_bigint': -9223372036854775808,
                                           'c_decimal': 0,
                                           'c_int': -2147483648,
                                           'c_bit': None,
                                           'c_decimal_2': Decimal("123456790.0"),
                                           'c_pk': 3,
                                           'c_double': Decimal("2.234"),
                                           'c_float': Decimal("2.234"),
                                           'c_decimal_2_unsigned': Decimal("0.23"),
                                           'c_tinyint_1': None}]

        actual_various_types_records = [r['data'] for r in records_by_stream['various_types']['messages'][1:4]]

        self.assertEqual(actual_various_types_records,
                         expected_various_types_records,
                         msg="Expected `various_types` upsert record data to be {}, but target output {}".format(expected_various_types_records,
                                                                                                                 actual_various_types_records))

        # verify that deselected property was not output
        expected_incremental_record = {'c_pk': 1,
                                       'c_dt': '2017-01-01T00:00:00.000000Z',
                                       'c_varchar': 'a'}

        actual_incremental_record = records_by_stream['incremental']['messages'][1]['data']

        self.assertEqual(actual_incremental_record,
                         expected_incremental_record,
                         msg="Expected first `incremental` upsert record data to be {}, but target output {}".format(expected_incremental_record,
                                                                                                                     actual_incremental_record))

        print("records are correct")

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)
        bookmarks = state['bookmarks']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        for k, v in bookmarks.items():
            if k == 'tap_tester_mysql_0-incremental':
                self.assertIsNotNone(v['version'],
                                     msg="expected bookmark for stream `{}` to have a version set".format(k))
                self.assertEqual(v['replication_key_value'],
                                 '2017-01-01T00:00:02.000000Z',
                                 msg="incorrect replication_key_value in bookmark for stream `{}`".format(k))
                self.assertEqual(v['replication_key'],
                                 'c_dt',
                                 msg="incorrect replication_key specified in bookmark for stream `{}`".format(k))
            else:
                self.assertFalse('version' in v, msg="expected bookmark for stream `{}` to not have a version key".format(k))
                self.assertTrue('initial_full_table_complete' in v, msg="expected bookmark for stream `{}` to have a true initial_full_table_complete key".format(k))
        print("state and bookmarks are correct")

        incremental_table_initial_table_version = bookmarks['tap_tester_mysql_0-incremental']['version']

        #----------------------------------------------------------------------
        # invoke the sync job again after some modifications
        #----------------------------------------------------------------------

        print("adding a column to an existing table in the source db")
        connection = db_utils.get_db_connection(self.get_properties(), self.get_credentials())

        with connection.cursor() as cursor:
            add_column_sql = '''
                ALTER TABLE tap_tester_mysql_0.incremental
                  ADD COLUMN favorite_number INTEGER;
                INSERT INTO tap_tester_mysql_0.incremental VALUES (4, '4', '2017-01-01 00:00:03', 'yeehaw', 999);
            '''
            cursor.execute(add_column_sql)


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

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        expected_row_count = 7 # {'my_isam': 1, 'various_types': 3, 'incremental': 2, 'view': 1}
        self.assertEqual(replicated_row_count,
                         expected_row_count,
                         msg="failed to replicate correct number of rows: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        records_by_stream = runner.get_records_from_target_output()

        expected_schema_of_new_column = {'maximum': 2147483647,
                                         'selected': True,
                                         'inclusion': 'available',
                                         'type': ['null', 'integer'],
                                         'minimum': -2147483648}

        # verifications about individual records
        for stream, recs in records_by_stream.items():
            # verify that a activate version messages were sent in the proper position
            if stream == 'incremental':
                self.assertEqual(records_by_stream[stream]['messages'][0]['action'],
                                'activate_version',
                                msg="Expected first message sent for stream `{}` not to have action `activate_version`".format(stream))
                expected_schema_of_new_column = {'maximum': 2147483647,
                                                 'inclusion': 'available',
                                                 'type': ['null', 'integer'],
                                                 'minimum': -2147483648}
                self.assertEqual(records_by_stream[stream]['schema']['properties']['favorite_number'],
                                 expected_schema_of_new_column,
                                 msg="Expected newly-added column to be present in schema for stream `{}`, but it was not.".format(stream))
            else:
                self.assertEqual(records_by_stream[stream]['messages'][0]['action'],
                                'upsert',
                                msg="Expected first message sent for stream `{}` to have action `upsert`".format(stream))
                self.assertEqual(records_by_stream[stream]['messages'][-1]['action'],
                                'activate_version',
                                msg="Expected last message sent for stream `{}` to have action `activate_version`".format(stream))

        state = menagerie.get_state(conn_id)
        bookmarks = state['bookmarks']
        self.assertIsNone(state['currently_syncing'],
                          msg="expected state's currently_syncing to be None")
        for k, v in bookmarks.items():
            if k == 'tap_tester_mysql_0-incremental':
                self.assertIsNotNone(v['version'],
                                     msg="expected bookmark for stream `{}` to have a version set".format(k))
                self.assertEqual(v['replication_key_value'],
                                 '2017-01-01T00:00:03.000000Z',
                                 msg="incorrect replication_key_value in bookmark for stream `{}`".format(k))
                self.assertEqual(v['replication_key'],
                                 'c_dt',
                                 msg="incorrect replication_key specified in bookmark for stream `{}`".format(k))
            else:
                self.assertFalse('version' in v, msg="expected bookmark for stream `{}` to not have a version key".format(k))
                self.assertTrue('initial_full_table_complete' in v, msg="expected bookmark for stream `{}` to have a true initial_full_table_complete key".format(k))

        print("state and bookmarks are correct")

        # verify incremental table_version didn't change
        incremental_table_new_table_version = bookmarks['tap_tester_mysql_0-incremental']['version']

        self.assertEqual(incremental_table_initial_table_version,
                         incremental_table_new_table_version,
                         msg="Expected incrementally-replicated table's table_version to remain unchanged over multiple invocations.")

SCENARIOS.add(MySQLFullAndIncremental)
