import unittest
import pymysql
import tap_mysql
import copy
import singer
import os

from singer.schema import Schema

DB_NAME='tap_mysql_test'

def get_test_connection():

    creds = {}
    creds['host'] = os.environ.get('SINGER_TAP_MYSQL_TEST_DB_HOST')
    creds['user'] = os.environ.get('SINGER_TAP_MYSQL_TEST_DB_USER')
    creds['password'] = os.environ.get('SINGER_TAP_MYSQL_TEST_DB_PASSWORD')
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
            c_smallint SMALLINT,
            c_mediumint MEDIUMINT,
            c_int INT,
            c_bigint BIGINT,
            c_float FLOAT,
            c_double DOUBLE,
            c_bit BIT(4)
            )''')

            catalog = tap_mysql.discover_catalog(con)
            cls.schema = catalog.streams[0].schema


    def test_decimal(self):
        self.assertEqual(self.schema.properties['c_decimal'],
                         Schema(['null', 'number'],
                                selected=False,
                                sqlDatatype='decimal(10,0)',
                                inclusion='available',
                                maximum=10000000000,
                                exclusiveMaximum=True,
                                minimum=-10000000000,
                                exclusiveMinimum=True,
                                multipleOf=1))

    def test_decimal_unsigned(self):
        self.assertEqual(self.schema.properties['c_decimal_2_unsigned'],
                         Schema(['null', 'number'],
                                selected=False,
                                sqlDatatype='decimal(5,2) unsigned',
                                inclusion='available',
                                maximum=1000,
                                exclusiveMaximum=True,
                                minimum=0,
                                multipleOf=0.01))

    def test_decimal_with_defined_scale_and_precision(self):
        self.assertEqual(self.schema.properties['c_decimal_2'],
                         Schema(['null', 'number'],
                                selected=False,
                                sqlDatatype='decimal(11,2)',
                                inclusion='available',
                                maximum=1000000000,
                                exclusiveMaximum=True,
                                minimum=-1000000000,
                                exclusiveMinimum=True,
                                multipleOf=0.01))

    def test_tinyint(self):
        self.assertEqual(self.schema.properties['c_tinyint'],
                         Schema(['null', 'integer'],
                                selected=False,
                                sqlDatatype='tinyint(4)',
                                inclusion='available', minimum=-128,
                                maximum=127))

    def test_smallint(self):
        self.assertEqual(self.schema.properties['c_smallint'],
                         Schema(['null', 'integer'],
                                selected=False,
                                sqlDatatype='smallint(6)',
                                inclusion='available',
                                minimum=-32768,
                                maximum=32767))

    def test_mediumint(self):
        self.assertEqual(self.schema.properties['c_mediumint'],
                         Schema(['null', 'integer'],
                                selected=False,
                                sqlDatatype='mediumint(9)',
                                inclusion='available',
                                minimum=-8388608,
                                maximum=8388607))


    def test_int(self):
        self.assertEqual(self.schema.properties['c_int'],
                         Schema(['null', 'integer'],
                                selected=False,
                                sqlDatatype='int(11)',
                                inclusion='available',
                                minimum=-2147483648,
                                maximum=2147483647))

    def test_bigint(self):
        self.assertEqual(self.schema.properties['c_bigint'],
                         Schema(['null', 'integer'],
                                selected=False,
                                sqlDatatype='bigint(20)',
                                inclusion='available',
                                minimum=-9223372036854775808,
                                maximum=9223372036854775807))

    def test_float(self):
        self.assertEqual(self.schema.properties['c_float'],
                         Schema(['null', 'number'],
                                selected=False,
                                inclusion='available',
                                sqlDatatype='float'))


    def test_double(self):
        self.assertEqual(self.schema.properties['c_double'],
                         Schema(['null', 'number'],
                                selected=False,
                                inclusion='available',
                                sqlDatatype='double'))

    def test_bit(self):
        self.assertEqual(self.schema.properties['c_bit'].inclusion,
                         'unsupported')

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

            catalog = tap_mysql.discover_catalog(con)
            catalog.streams[0].stream = 'tab'
            catalog.streams[0].schema.selected = True
            catalog.streams[0].schema.properties['a'].selected = True
            messages = list(tap_mysql.generate_messages(con, catalog, {}))
            schema_message = list(filter(lambda m: isinstance(m, singer.SchemaMessage), messages))[0]
            self.assertTrue(isinstance(schema_message, singer.SchemaMessage))
            self.assertEqual(schema_message.schema['properties'].keys(), set(['id', 'a']))

        finally:
            con.close()

def current_stream_seq(messages):
    return ''.join(
        [m.value.get('current_stream', '_')[-1]
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

        self.catalog = tap_mysql.discover_catalog(self.con)
        for stream in self.catalog.streams:
            stream.schema.selected = True
            stream.key_properties = []
            stream.schema.properties['val'].selected = True
            stream.stream = stream.table

    def tearDown(self):
        if self.con:
            self.con.close()
    def test_emit_current_stream(self):
        state = {}
        messages = list(tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertRegexpMatches(current_stream_seq(messages), '^a+b+_+')

    def test_start_at_current_stream(self):
        state = {'current_stream': 'tap_mysql_test-b'}
        messages = list(tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertRegexpMatches(current_stream_seq(messages), '^b+_+')

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

        self.catalog = tap_mysql.discover_catalog(self.con)
        for stream in self.catalog.streams:
            stream.schema.selected = True
            stream.key_properties = []
            stream.schema.properties['val'].selected = True
            stream.stream = stream.table

    def tearDown(self):
        if self.con:
            self.con.close()

    def test_with_no_state(self):
        state = {}
        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertEqual(['RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])


    def test_with_state(self):
        state = {
            'streams': [{
                'tap_stream_id': 'tap_mysql_test-full_table',
                'version': 1}]
        }
        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))

        self.assertEqual(['RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertEqual(versions, [1, 1])


class TestStreamVersionIncremental(unittest.TestCase):

    def setUp(self):
        self.con = get_test_connection()
        with self.con.cursor() as cursor:
            cursor.execute('CREATE TABLE incremental (val int, updated datetime)')
            cursor.execute('INSERT INTO incremental (val, updated) VALUES (1, \'2017-06-01\')')
            cursor.execute('INSERT INTO incremental (val, updated) VALUES (2, \'2017-06-20\')')

        self.catalog = tap_mysql.discover_catalog(self.con)
        for stream in self.catalog.streams:
            stream.schema.selected = True
            stream.key_properties = []
            stream.schema.properties['val'].selected = True
            stream.stream = stream.table
            stream.replication_key = 'updated'

    def tearDown(self):
        if self.con:
            self.con.close()


    def test_with_no_state(self):
        state = {}
        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertEqual(
            ['ActivateVersionMessage', 'RecordMessage', 'RecordMessage'],
            message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])


    def test_with_state(self):
        state = {
            'streams': [{
                'tap_stream_id': 'tap_mysql_test-incremental',
                'version': 1}]
        }
        (message_types, versions) = message_types_and_versions(
            tap_mysql.generate_messages(self.con, self.catalog, state))
        self.assertEqual(
            ['ActivateVersionMessage', 'RecordMessage', 'RecordMessage'],
            message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])

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
        catalog = tap_mysql.discover_catalog(self.con)

        is_view = {s.table: s.is_view for s in catalog.streams}
        self.assertEqual(
            is_view,
            {'a_table': False,
             'a_view': True})

    def test_do_not_discover_key_properties_for_view(self):
        catalog = tap_mysql.discover_catalog(self.con)
        discovered_key_properties = {
            s.table: s.key_properties
            for s in catalog.streams
        }
        self.assertEqual(
            discovered_key_properties,
            {'a_table': ['id'],
             'a_view': None})

    def test_can_set_key_properties_for_view(self):
        catalog = tap_mysql.discover_catalog(self.con)
        for stream in catalog.streams:
            stream.stream = stream.table

            if stream.table == 'a_view':
                stream.key_properties = ['id']
                stream.schema.selected = True
                stream.schema.properties['a'].selected = True

        messages = list(tap_mysql.generate_messages(self.con, catalog, {}))
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

    def test_escape_succeeds(self):
        catalog = tap_mysql.discover_catalog(self.con)
        catalog.streams[0].stream = 'some_stream_name'
        catalog.streams[0].schema.selected = True
        catalog.streams[0].key_properties = []
        catalog.streams[0].schema.properties['b c'].selected = True
        messages = tap_mysql.generate_messages(self.con, catalog, {})
        record_message = list(filter(lambda m: isinstance(m, singer.RecordMessage), messages))[0]
        self.assertTrue(isinstance(record_message, singer.RecordMessage))
        self.assertEqual(record_message.record, {'b c': 1})
