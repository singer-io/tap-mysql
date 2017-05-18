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
            
        cls.schema = tap_mysql.schema_for_table(connection, 'column_test')
            
            
    def test_decimal(self):
        self.assertEqual(self.schema['properties']['c_decimal'], {
            "type": "number",
            "exclusiveMaximum": 100000000000,
            "multipleOf": 1
        })

    def test_decimal_with_defined_scale_and_precision(self):        
        self.assertEqual(self.schema['properties']['c_decimal_2'], {
            "type": "number",
            "exclusiveMaximum": 100000000000,
            'multipleOf': 0.01})

    def test_tinyint(self):
        self.assertEqual(self.schema['properties']['c_tinyint'], {
            "type": "integer",
            })

    def test_smallint(self):
        self.assertEqual(self.schema['properties']['c_smallint'], {
            "type": "integer",
            })

    def test_mediumint(self):
        self.assertEqual(self.schema['properties']['c_mediumint'], {
            "type": "integer",
            })

    def test_int(self):
        self.assertEqual(self.schema['properties']['c_int'], {
            "type": "integer",
            })
        
    def test_bigint(self):
        self.assertEqual(self.schema['properties']['c_bigint'], {
            "type": "integer",
            })

    def test_float(self):
        self.assertEqual(self.schema['properties']['c_float'], {
            "type": "number",
            })                        
    

    def test_double(self):
        self.assertEqual(self.schema['properties']['c_double'], {
            "type": "number",
        })                        
    
        

    def test_bit(self):
        self.assertEqual(self.schema['properties']['c_bit'], {
            "inclusion": "unsupported",
            "description": "Unsupported column type bit(4)",
        })                        
    
        
