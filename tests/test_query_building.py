import unittest
import singer
from singer import Schema
from singer.catalog import CatalogEntry

from tap_mysql.sync_strategies.full_table import generate_pk_bookmark_clause


class TestFullTableWhereClause(unittest.TestCase):


    def test_fails_with_null_bookmark(self):
        catalog_entry = CatalogEntry(schema=Schema.from_dict({'properties':{}}))
        key_properties = []
        last_pk_fetched = None

        with self.assertRaises(AssertionError):
            generate_pk_bookmark_clause(key_properties, last_pk_fetched, catalog_entry)

    def test_empty_pk(self):
        catalog_entry = CatalogEntry(schema=Schema.from_dict({'properties':{}}))
        key_properties = []
        last_pk_fetched = {}

        expected = ''
        actual = generate_pk_bookmark_clause(key_properties, last_pk_fetched, catalog_entry)

        self.assertEqual(expected, actual)

    def test_one_pk(self):
        catalog_entry = CatalogEntry(schema=Schema.from_dict({'properties':{'id1':{'type': ['integer']}}}))
        key_properties = ['id1']
        last_pk_fetched = {'id1': 4}

        expected = '(`id1` > 4)'
        actual = generate_pk_bookmark_clause(key_properties, last_pk_fetched, catalog_entry)

        self.assertEqual(expected, actual)

    def test_two_pks(self):
        catalog_entry = CatalogEntry(schema=Schema.from_dict({'properties':{'id1':{'type': ['integer']}, 'str': {'type': ['string']}}}))
        key_properties = ['id1', 'str']
        last_pk_fetched = {'id1': 4, 'str': 'apples'}

        expected = '(`id1` > 4) OR (`id1` = 4 AND `str` > \'apples\')'
        actual = generate_pk_bookmark_clause(key_properties, last_pk_fetched, catalog_entry)

        self.assertEqual(expected, actual)

    def test_three_pks(self):
        catalog_entry = CatalogEntry(schema=Schema.from_dict({'properties':{'id1':{'type': ['integer']}, 'str': {'type': ['string']}, 'number': {'type': ['number']}}}))
        key_properties = ['id1', 'str', 'number']
        last_pk_fetched = {'id1': 4, 'str': 'apples', 'number': 5.43}

        expected = '(`id1` > 4) OR (`id1` = 4 AND `str` > \'apples\') OR (`id1` = 4 AND `str` = \'apples\' AND `number` > 5.43)'
        actual = generate_pk_bookmark_clause(key_properties, last_pk_fetched, catalog_entry)

        self.assertEqual(expected, actual)
