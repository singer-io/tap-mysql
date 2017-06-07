import unittest
import pymysql
import tap_mysql
import copy
import singer
import os

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

            streams = tap_mysql.discover_schemas(con)
            cls.schema = streams[0].schema


    def test_decimal(self):
        self.assertEqual(self.schema.properties['c_decimal'],
                         tap_mysql.Schema('number',
                                          inclusion='available',
                                          exclusiveMaximum=10000000000,
                                          exclusiveMinimum=-10000000000,            
                                          multipleOf=1))

    def test_decimal_unsigned(self):
        self.assertEqual(self.schema.properties['c_decimal_2_unsigned'],
                         tap_mysql.Schema('number',
                                          inclusion='available',
                                          exclusiveMaximum=1000,         
                                          minimum=0,
                                          multipleOf=0.01))

    def test_decimal_with_defined_scale_and_precision(self):
        self.assertEqual(self.schema.properties['c_decimal_2'],
                         tap_mysql.Schema('number',
                                          inclusion='available',
                                          exclusiveMaximum=1000000000,
                                          exclusiveMinimum=-1000000000,
                                          multipleOf=0.01))

    def test_tinyint(self):
        self.assertEqual(self.schema.properties['c_tinyint'],
                         tap_mysql.Schema('integer',
                                          inclusion='available', minimum=-128,
                                          maximum=127))

    def test_smallint(self):
        self.assertEqual(self.schema.properties['c_smallint'],
                         tap_mysql.Schema('integer',
                                          inclusion='available',
                                          minimum=-32768,
                                          maximum=32767))

    def test_mediumint(self):
        self.assertEqual(self.schema.properties['c_mediumint'],
                         tap_mysql.Schema('integer',
                                          inclusion='available',
                                          minimum=-8388608,
                                          maximum=8388607))


    def test_int(self):
        self.assertEqual(self.schema.properties['c_int'],
                         tap_mysql.Schema('integer',
                                          inclusion='available',
                                          minimum=-2147483648,
                                          maximum=2147483647))

    def test_bigint(self):
        self.assertEqual(self.schema.properties['c_bigint'],
                         tap_mysql.Schema('integer',
                                          inclusion='available',
                                          minimum=-9223372036854775808,
                                          maximum=9223372036854775807))

    def test_float(self):
        self.assertEqual(self.schema.properties['c_float'],
                         tap_mysql.Schema('number', inclusion='available'))


    def test_double(self):
        self.assertEqual(self.schema.properties['c_double'],
                         tap_mysql.Schema('number', inclusion='available'))

    def test_bit(self):
        self.assertEqual(self.schema.properties['c_bit'].inclusion,
                         'unsupported')

    def test_pk(self):
        self.assertEqual(
            self.schema.properties['c_pk'].inclusion,
            'automatic')


class TestIndexDiscoveredSchema(unittest.TestCase):

    def setUp(self):
        con = get_test_connection()

        try:
            with con.cursor() as cur:
                cur.execute('''
                    CREATE TABLE tab (
                      a INTEGER,
                      b INTEGER)
                ''')

                self.streams = tap_mysql.discover_schemas(con)
        finally:
            con.close()

    def runTest(self):
        streams = copy.deepcopy(self.streams)
        print(tap_mysql.index_schema(streams))
        self.assertEqual(
            tap_mysql.index_schema(streams),
            {
                "tap_mysql_test": {
                    "tab": {
                        "b": tap_mysql.Schema(
                            'integer',
                            inclusion="available",
                            maximum=2147483647,
                            minimum=-2147483648),
                        "a": tap_mysql.Schema(
                            'integer',
                            inclusion="available",
                            maximum=2147483647,
                            minimum=-2147483648),
                    }
                }
            },
            'makes nested structure from flat discovered schemas')


class TestSelectsAppropriateColumns(unittest.TestCase):

    def runTest(self):
        selected_cols = ['a', 'b', 'd']
        indexed_schema = {'some_db':
                          {'some_table':
                           {'a': tap_mysql.Schema(None, inclusion='available'),
                            'b': tap_mysql.Schema(None, inclusion='unsupported'),
                            'c': tap_mysql.Schema(None, inclusion='automatic')}}}

        expected_pruned_schema = {'some_db':
                                  {'some_table':
                                   {'a': tap_mysql.Schema(None, inclusion='available'),
                                    'c': tap_mysql.Schema(None, inclusion='automatic'),}}}

        tap_mysql.remove_unwanted_columns(selected_cols,
                                          indexed_schema,
                                          'some_db',
                                          'some_table')

        self.assertEqual(indexed_schema,
                         expected_pruned_schema,
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

            selections = tap_mysql.discover_schemas(con)
            selections[0].stream = 'tab'
            selections[0].schema.selected = True
            selections[0].schema.properties['a'].selected = True
            messages = list(tap_mysql.generate_messages(con, selections, {}))
            schema_message = list(filter(lambda m: isinstance(m, singer.SchemaMessage), messages))[0]
            self.assertTrue(isinstance(schema_message, singer.SchemaMessage))
            self.assertEqual(schema_message.schema['properties'].keys(), set(['id', 'a']))

        finally:
            con.close()

def current_stream_seq(messages):
    return ''.join(
        [m.value.get('current_stream', '_')
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

        streams = tap_mysql.discover_schemas(self.con)
        for stream in streams:
            stream.schema.selected = True
            stream.key_properties = []
            stream.schema.properties['val'].selected = True
            stream.stream = stream.table
        self.selections = streams
    def tearDown(self):
        if self.con:
            self.con.close()
    def test_emit_current_stream(self):
        state = {}
        messages = list(tap_mysql.generate_messages(self.con, self.selections, state))
        self.assertRegexpMatches(current_stream_seq(messages), '^a+b+_+')

    def test_start_at_current_stream(self):
        state = {'current_stream': 'b'}
        messages = list(tap_mysql.generate_messages(self.con, self.selections, state))
        self.assertRegexpMatches(current_stream_seq(messages), '^b+_+')

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
        streams = tap_mysql.discover_schemas(self.con)

        is_view = {s.table: s.is_view for s in streams}
        self.assertEqual(
            is_view,
            {'a_table': False,
             'a_view': True})

    def test_do_not_discover_key_properties_for_view(self):
        streams = tap_mysql.discover_schemas(self.con)
        discovered_key_properties = {
            s.table: s.key_properties
            for s in streams
        }
        self.assertEqual(
            discovered_key_properties,
            {'a_table': ['id'],
             'a_view': None})

    def test_can_set_key_properties_for_view(self):
        streams = tap_mysql.discover_schemas(self.con)
        for stream in streams:
            stream.stream = stream.table

            if stream.table == 'a_view':
                stream.key_properties = ['id']
                stream.schema.selected = True
                stream.schema.properties['a'].selected = True

        messages = list(tap_mysql.generate_messages(self.con, streams, {}))
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
        selections = tap_mysql.discover_schemas(self.con)
        selections[0].stream = 'some_stream_name'
        selections[0].schema.selected = True
        selections[0].key_properties = []
        selections[0].schema.properties['b c'].selected = True
        messages = tap_mysql.generate_messages(self.con, selections, {})
        record_message = list(filter(lambda m: isinstance(m, singer.RecordMessage), messages))[0]
        self.assertTrue(isinstance(record_message, singer.RecordMessage))
        self.assertEqual(record_message.record, {'b c': 1})
