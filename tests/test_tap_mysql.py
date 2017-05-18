import unittest
import pymysql
import tap_mysql


class TestTypeMapping(unittest.TestCase):

    def setUp(self):
        self.connection = pymysql.connect(
            host='localhost',
            user='root',
            password='password',
            database='test_tap_mysql')

        with self.connection.cursor() as cursor:
            cursor.execute('DROP TABLE column_test')
            
        with self.connection.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE column_test (
                  c_decimal DECIMAL)''')
    
    def test_(self):
        schema = tap_mysql.schema_for_table(self.connection, 'column_test')
        props = schema['properties']
        self.assertEqual(props['c_decimal'], {"type": "number", "maximum": 99999999999})
            

    
