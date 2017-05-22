import unittest
import pymysql
import tap_mysql


class TestTypeMapping(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='password',
            database='test_tap_mysql')

        
        with connection.cursor() as cursor:
            try:
                cursor.execute('DROP TABLE column_test')
            except:
                pass

        with connection.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE column_test (
                  c_decimal DECIMAL,
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

        discovered = tap_mysql.discover_schemas(connection)
        
        cls.schema = discovered['column_test']['schema']
            
            
    def test_decimal(self):
        self.assertEqual(self.schema['properties']['c_decimal'], {
            'type': 'number',
            'exclusiveMaximum': 10000000000,
            'multipleOf': 1
        })

    def test_decimal_with_defined_scale_and_precision(self):        
        self.assertEqual(self.schema['properties']['c_decimal_2'], {
            'type': 'number',
            'exclusiveMaximum': 1000000000,
            'multipleOf': 0.01})

    def test_tinyint(self):
        self.assertEqual(self.schema['properties']['c_tinyint'], {
            'type': 'integer',
            'minimum': -128,
            'maximum': 127
        })

    def test_smallint(self):
        self.assertEqual(self.schema['properties']['c_smallint'], {
            'type': 'integer',
            'minimum': -32768,
            'maximum':  32767
        })

    def test_mediumint(self):
        self.assertEqual(self.schema['properties']['c_mediumint'], {
            'type': 'integer',
            'minimum': -8388608,
            'maximum':  8388607
        })

    def test_int(self):
        self.assertEqual(self.schema['properties']['c_int'], {
            'type': 'integer',
            'minimum': -2147483648,
            'maximum': 2147483647
        })
        
    def test_bigint(self):
        self.assertEqual(self.schema['properties']['c_bigint'], {
            'type': 'integer',
            'minimum': -9223372036854775808,
            'maximum':  9223372036854775807
        })

    def test_float(self):
        self.assertEqual(self.schema['properties']['c_float'], {
            'type': 'number',
        })                        
    

    def test_double(self):
        self.assertEqual(self.schema['properties']['c_double'], {
            'type': 'number',
        })                        
    
        

    def test_bit(self):
        self.assertEqual(self.schema['properties']['c_bit'], {
            'inclusion': 'unsupported',
            'description': 'Unsupported column type bit(4)',
        })                        
    
        
