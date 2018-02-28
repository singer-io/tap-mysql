import unittest
import pymysql
import tap_mysql
import copy
import singer
import os

from singer.schema import Schema

DB_NAME='tap_mysql_test'


def set_replication_method_and_key(stream, r_method, r_key):
    new_md = singer.metadata.to_map(stream.metadata)
    old_md = new_md.get(())
    if r_method:
        old_md.update({'replication-method': r_method})

    if r_key:
        old_md.update({'replication-key': r_key})

    stream.metadata = singer.metadata.to_list(new_md)
    return stream

def get_test_connection():
    creds = {}
    creds['host'] = os.environ.get('SINGER_TAP_MYSQL_TEST_DB_HOST')
    creds['user'] = os.environ.get('SINGER_TAP_MYSQL_TEST_DB_USER')
    creds['password'] = os.environ.get('SINGER_TAP_MYSQL_TEST_DB_PASSWORD')
    creds['charset'] = 'utf8'
    if not creds['password']:
        del creds['password']

    con = pymysql.connect(**creds)

    try:
        with con.cursor() as cur:
            try:
                cur.execute('DROP DATABASE {}'.format(DB_NAME))
            except:
                pass
            cur.execute('CREATE DATABASE {}'.format(DB_NAME))
    finally:
        con.close()

    creds['database'] = DB_NAME

    return pymysql.connect(**creds)

def discover_catalog(connection):
    catalog = tap_mysql.discover_catalog(connection)
    catalog.streams = [s for s in catalog.streams if s.database == DB_NAME]
    return catalog

class TestTypeMapping(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        con = get_test_connection()

        with con.cursor() as cur:
            cur.execute('''
            CREATE TABLE test_type_mapping (
            c_pk INTEGER PRIMARY KEY,
            c_decimal DECIMAL,
            c_decimal_2_unsigned DECIMAL(5, 2) UNSIGNED,
            c_decimal_2 DECIMAL(11, 2),
            c_tinyint TINYINT,
            c_tinyint_1 TINYINT(1),
            c_tinyint_1_unsigned TINYINT(1) UNSIGNED,
            c_smallint SMALLINT,
            c_mediumint MEDIUMINT,
            c_int INT,
            c_bigint BIGINT,
            c_bigint_unsigned BIGINT(20) UNSIGNED,
            c_float FLOAT,
            c_double DOUBLE,
            c_bit BIT(4),
            c_date DATE,
            c_time TIME,
            c_year YEAR
            )''')

            catalog = discover_catalog(con)
            cls.schema = catalog.streams[0].schema
            cls.metadata = catalog.streams[0].metadata

    def get_metadata_for_column(self, colName):
        return next(md for md in self.metadata if md['breadcrumb'] == ('properties', colName))['metadata']

    def test_decimal(self):
        self.assertEqual(self.schema.properties['c_decimal'],
                         Schema(['null', 'number'],
                                inclusion='available',
                                maximum=10000000000,
                                exclusiveMaximum=True,
                                minimum=-10000000000,
                                exclusiveMinimum=True,
                                multipleOf=1))
        self.assertEqual(self.get_metadata_for_column('c_decimal'),
                         {'selected-by-default': True,
                          'sql-datatype': 'decimal(10,0)'})

    def test_decimal_unsigned(self):
        self.assertEqual(self.schema.properties['c_decimal_2_unsigned'],
                         Schema(['null', 'number'],
                                inclusion='available',
                                maximum=1000,
                                exclusiveMaximum=True,
                                minimum=0,
                                multipleOf=0.01))
        self.assertEqual(self.get_metadata_for_column('c_decimal_2_unsigned'),
                         {'selected-by-default': True,
                          'sql-datatype': 'decimal(5,2) unsigned'})

    def test_decimal_with_defined_scale_and_precision(self):
        self.assertEqual(self.schema.properties['c_decimal_2'],
                         Schema(['null', 'number'],
                                inclusion='available',
                                maximum=1000000000,
                                exclusiveMaximum=True,
                                minimum=-1000000000,
                                exclusiveMinimum=True,
                                multipleOf=0.01))
        self.assertEqual(self.get_metadata_for_column('c_decimal_2'),
                         {'selected-by-default': True,
                          'sql-datatype': 'decimal(11,2)'})

    def test_tinyint(self):
        self.assertEqual(self.schema.properties['c_tinyint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-128,
                                maximum=127))
        self.assertEqual(self.get_metadata_for_column('c_tinyint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'tinyint(4)'})

    def test_tinyint_1(self):
        self.assertEqual(self.schema.properties['c_tinyint_1'],
                         Schema(['null', 'boolean'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_tinyint_1'),
                         {'selected-by-default': True,
                          'sql-datatype': 'tinyint(1)'})

    def test_tinyint_1_unsigned(self):
        self.assertEqual(self.schema.properties['c_tinyint_1_unsigned'],
                         Schema(['null', 'boolean'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_tinyint_1_unsigned'),
                         {'selected-by-default': True,
                          'sql-datatype': 'tinyint(1) unsigned'})

    def test_smallint(self):
        self.assertEqual(self.schema.properties['c_smallint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-32768,
                                maximum=32767))
        self.assertEqual(self.get_metadata_for_column('c_smallint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'smallint(6)'})

    def test_mediumint(self):
        self.assertEqual(self.schema.properties['c_mediumint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-8388608,
                                maximum=8388607))
        self.assertEqual(self.get_metadata_for_column('c_mediumint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'mediumint(9)'})

    def test_int(self):
        self.assertEqual(self.schema.properties['c_int'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-2147483648,
                                maximum=2147483647))
        self.assertEqual(self.get_metadata_for_column('c_int'),
                         {'selected-by-default': True,
                          'sql-datatype': 'int(11)'})

    def test_bigint(self):
        self.assertEqual(self.schema.properties['c_bigint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-9223372036854775808,
                                maximum=9223372036854775807))
        self.assertEqual(self.get_metadata_for_column('c_bigint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'bigint(20)'})

    def test_bigint_unsigned(self):
        self.assertEqual(self.schema.properties['c_bigint_unsigned'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=0,
                                maximum=18446744073709551615))
        self.assertEqual(self.get_metadata_for_column('c_bigint_unsigned'),
                         {'selected-by-default': True,
                          'sql-datatype': 'bigint(20) unsigned'})

    def test_float(self):
        self.assertEqual(self.schema.properties['c_float'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_float'),
                         {'selected-by-default': True,
                          'sql-datatype': 'float'})

    def test_double(self):
        self.assertEqual(self.schema.properties['c_double'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_double'),
                         {'selected-by-default': True,
                          'sql-datatype': 'double'})

    def test_bit(self):
        self.assertEqual(self.schema.properties['c_bit'],
                         Schema(['null', 'boolean'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_bit'),
                         {'selected-by-default': True,
                          'sql-datatype': 'bit(4)'})

    def test_date(self):
        self.assertEqual(self.schema.properties['c_date'],
                         Schema(['null', 'string'],
                                format='date-time',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_date'),
                         {'selected-by-default': True,
                          'sql-datatype': 'date'})

    def test_time(self):
        self.assertEqual(self.schema.properties['c_time'],
                         Schema(['null', 'string'],
                                format='date-time',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_time'),
                         {'selected-by-default': True,
                          'sql-datatype': 'time'})

    def test_year(self):
        self.assertEqual(self.schema.properties['c_year'].inclusion,
                         'unsupported')
        self.assertEqual(self.get_metadata_for_column('c_year'),
                         {'selected-by-default': False,
                          'sql-datatype': 'year(4)'})

    def test_pk(self):
        self.assertEqual(
            self.schema.properties['c_pk'].inclusion,
            'automatic')


class TestSelectsAppropriateColumns(unittest.TestCase):

    def runTest(self):
        selected_cols = set(['a', 'b', 'd'])
        table_schema = Schema(type='object',
                              properties={
                                  'a': Schema(None, inclusion='available'),
                                  'b': Schema(None, inclusion='unsupported'),
                                  'c': Schema(None, inclusion='automatic')})

        got_cols = tap_mysql.desired_columns(selected_cols, table_schema)

        self.assertEqual(got_cols,
                         set(['a', 'c']),
                         'Keep automatic as well as selected, available columns.')

class TestSchemaMessages(unittest.TestCase):

    def runTest(self):
        con = get_test_connection()
        try:
            with con.cursor() as cur:
                cur.execute('''
                    CREATE TABLE tab (
                      id INTEGER PRIMARY KEY,
                      a INTEGER,
                      b INTEGER)
                ''')

            catalog = discover_catalog(con)
            catalog.streams[0].stream = 'tab'
            catalog.streams[0].schema.selected = True
            catalog.streams[0].schema.properties['a'].selected = True
            messages = list(tap_mysql.generate_messages(con, catalog, tap_mysql.build_state({}, catalog)))
            schema_message = list(filter(lambda m: isinstance(m, singer.SchemaMessage), messages))[0]
            self.assertTrue(isinstance(schema_message, singer.SchemaMessage))
            expectedKeys = ['id', 'a']

            self.assertEqual(schema_message.schema['properties'].keys(), set(expectedKeys))

        finally:
            con.close()

def currently_syncing_seq(messages):
    return ''.join(
        [(m.value.get('currently_syncing', '_') or '_')[-1]
         for m in messages
         if isinstance(m, singer.StateMessage)]
    )

class TestCurrentStream(unittest.TestCase):

    def setUp(self):
        self.con = get_test_connection()
        with self.con.cursor() as cursor:
            cursor.execute('CREATE TABLE a (val int)')
            cursor.execute('CREATE TABLE b (val int)')
            cursor.execute('INSERT INTO a (val) VALUES (1)')
            cursor.execute('INSERT INTO b (val) VALUES (1)')

        self.catalog = discover_catalog(self.con)

        for stream in self.catalog.streams:
            stream.schema.selected = True
            stream.key_properties = []
            stream.schema.properties['val'].selected = True
            stream.stream = stream.table

    def tearDown(self):
        if self.con:
            self.con.close()
    def test_emit_currently_syncing(self):
        state = tap_mysql.build_state({}, self.catalog)
        messages = list(tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertRegexpMatches(currently_syncing_seq(messages), '^a+b+_+')

    def test_start_at_currently_syncing(self):
        state = tap_mysql.build_state({'currently_syncing': 'tap_mysql_test-b'}, self.catalog)
        messages = list(tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertRegexpMatches(currently_syncing_seq(messages), '^b+_+')

def message_types_and_versions(messages):
    message_types = []
    versions = []
    for message in messages:
        t = type(message)
        if t in set([singer.RecordMessage, singer.ActivateVersionMessage]):
            message_types.append(t.__name__)
            versions.append(message.version)
    return (message_types, versions)


class TestStreamVersionFullTable(unittest.TestCase):

    def setUp(self):
        self.con = get_test_connection()
        with self.con.cursor() as cursor:
            cursor.execute('CREATE TABLE full_table (val int)')
            cursor.execute('INSERT INTO full_table (val) VALUES (1)')

        self.catalog = discover_catalog(self.con)
        for stream in self.catalog.streams:
            stream.schema.selected = True
            stream.key_properties = []
            stream.schema.properties['val'].selected = True
            stream.stream = stream.table

    def tearDown(self):
        if self.con:
            self.con.close()

    def test_with_no_state(self):
        state = tap_mysql.build_state({}, self.catalog)
        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertEqual(['ActivateVersionMessage', 'RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])

    def test_with_no_version_in_state(self):
        state = tap_mysql.build_state({
            'bookmarks': {
                'tap_mysql_test-full_table': {
                    'version': None,
                }
            }
        }, self.catalog)
        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertEqual(['RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])

    def test_with_version_in_state(self):
        state = tap_mysql.build_state({
            'bookmarks': {
                'tap_mysql_test-full_table': {
                    'version': 1,
                }
            }
        }, self.catalog)

        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))

        self.assertEqual(['RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertEqual(versions, [1, 1])

    def test_version_cleared_from_state_after_full_table_success(self):
        state = tap_mysql.build_state({
            'bookmarks': {
                'tap_mysql_test-full_table': {
                    'version': 1,
                }
            }
        }, self.catalog)

        list(tap_mysql.generate_messages(self.con, self.catalog, state))

        self.assertEqual(state['bookmarks']['tap_mysql_test-full_table']['version'], None)


class TestIncrementalReplication(unittest.TestCase):

    def setUp(self):
        self.con = get_test_connection()
        with self.con.cursor() as cursor:
            cursor.execute('CREATE TABLE incremental (val int, updated datetime)')
            cursor.execute('INSERT INTO incremental (val, updated) VALUES (1, \'2017-06-01\')')
            cursor.execute('INSERT INTO incremental (val, updated) VALUES (2, \'2017-06-20\')')
            cursor.execute('INSERT INTO incremental (val, updated) VALUES (3, \'2017-09-22\')')
            cursor.execute('CREATE TABLE integer_incremental (val int, updated int)')
            cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (1, 1)')
            cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (2, 2)')
            cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (3, 3)')

        self.catalog = discover_catalog(self.con)
        for stream in self.catalog.streams:
            stream.schema.selected = True
            stream.key_properties = []
            stream.schema.properties['val'].selected = True
            stream.stream = stream.table
            set_replication_method_and_key(stream, None, 'updated')

    def tearDown(self):
        if self.con:
            self.con.close()


    def test_with_no_state(self):
        state = tap_mysql.build_state({}, self.catalog)
        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertEqual(
            ['ActivateVersionMessage',
             'RecordMessage',
             'RecordMessage',
             'RecordMessage',
             'ActivateVersionMessage',
             'RecordMessage',
             'RecordMessage',
             'RecordMessage'],
            message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])


    def test_with_state(self):
        state = tap_mysql.build_state({
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                    'replication_key_value': '2017-06-20',
                    'replication_key': 'updated'
                },
                'tap_mysql_test-integer_incremental': {
                    'version': 1,
                    'replication_key_value': 3,
                    'replication_key': 'updated'
                }
            }
        }, self.catalog)
        import pdb
        pdb.set_trace()

        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertEqual(
            ['ActivateVersionMessage',
             'RecordMessage',
             'RecordMessage',
             'ActivateVersionMessage',
             'RecordMessage'],
            message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])
        self.assertEqual(versions[1], 1)

    def test_version_not_cleared_from_state_after_incremental_success(self):
        state = tap_mysql.build_state({
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                }
            }
        }, self.catalog)

        list(tap_mysql.generate_messages(self.con, self.catalog, state))

        self.assertEqual(state['bookmarks']['tap_mysql_test-incremental']['version'], 1)

class TestViews(unittest.TestCase):
    def setUp(self):
        self.con = get_test_connection()
        with self.con.cursor() as cursor:
            cursor.execute(
                '''
                CREATE TABLE a_table (
                  id int primary key,
                  a int,
                  b int)
                ''')

            cursor.execute(
                '''
                CREATE VIEW a_view AS SELECT id, a FROM a_table
                ''')

    def tearDown(self):
        if self.con:
            self.con.close()

    def test_discovery_sets_is_view(self):
        catalog = discover_catalog(self.con)

        is_view = {s.table: s.is_view for s in catalog.streams}
        self.assertEqual(
            is_view,
            {'a_table': False,
             'a_view': True})

    def test_do_not_discover_key_properties_for_view(self):
        catalog = discover_catalog(self.con)
        discovered_key_properties = {
            s.table: s.key_properties
            for s in catalog.streams
        }
        self.assertEqual(
            discovered_key_properties,
            {'a_table': ['id'],
             'a_view': None})

    def test_can_set_key_properties_for_view(self):
        catalog = discover_catalog(self.con)
        for stream in catalog.streams:
            stream.stream = stream.table

            if stream.table == 'a_view':
                stream.key_properties = ['id']
                stream.schema.selected = True
                stream.schema.properties['a'].selected = True

        messages = list(tap_mysql.generate_messages(self.con, catalog, tap_mysql.build_state({}, catalog)))
        schema_message = list(filter(lambda m: isinstance(m, singer.SchemaMessage), messages))[0]
        self.assertTrue(isinstance(schema_message, singer.SchemaMessage))
        self.assertEqual(schema_message.key_properties, ['id'])



class TestEscaping(unittest.TestCase):

    def setUp(self):
        self.con = get_test_connection()
        with self.con.cursor() as cursor:
            cursor.execute('CREATE TABLE a (`b c` int)')
            cursor.execute('INSERT INTO a (`b c`) VALUES (1)')

    def tearDown(self):
        if self.con:
            self.con.close()

    def runTest(self):
        catalog = discover_catalog(self.con)
        catalog.streams[0].stream = 'some_stream_name'
        catalog.streams[0].schema.selected = True
        catalog.streams[0].key_properties = []
        catalog.streams[0].schema.properties['b c'].selected = True
        messages = tap_mysql.generate_messages(self.con, catalog, tap_mysql.build_state({}, catalog))
        record_message = list(filter(lambda m: isinstance(m, singer.RecordMessage), messages))[0]
        self.assertTrue(isinstance(record_message, singer.RecordMessage))
        self.assertEqual(record_message.record, {'b c': 1})

class TestUnsupportedPK(unittest.TestCase):

    def setUp(self):
        self.con = get_test_connection()
        with self.con.cursor() as cursor:
            cursor.execute('CREATE TABLE bad_pk_tab (bad_pk BINARY, age INT, PRIMARY KEY (bad_pk))') # BINARY not presently supported
            cursor.execute('CREATE TABLE good_pk_tab (good_pk INT, age INT, PRIMARY KEY (good_pk))')
            cursor.execute("INSERT INTO bad_pk_tab (bad_pk, age) VALUES ('a', 100)")
            cursor.execute("INSERT INTO good_pk_tab (good_pk, age) VALUES (1, 100)")

    def tearDown(self):
        if self.con:
            self.con.close()

    def runTest(self):
        catalog = discover_catalog(self.con)
        self.assertIsNone(catalog.streams[0].key_properties, None)
        self.assertEqual(catalog.streams[1].key_properties, ['good_pk'])



if __name__== "__main__":
    test1 = TestIncrementalReplication()
    test1.setUp()
    test1.test_with_state()
