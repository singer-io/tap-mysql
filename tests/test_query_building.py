import unittest
import singer
from singer import Schema
from singer.catalog import CatalogEntry

from tap_mysql.sync_strategies.full_table import generate_pk_bookmark_clause, generate_pk_clause


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

class TestGeneratePkClause(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None


    def test_no_pk_values(self):
        catalog_entry = CatalogEntry(schema=Schema.from_dict({'properties':{}}), metadata=[])
        state = {}

        expected = ''
        actual = generate_pk_clause(catalog_entry, state)

        self.assertEqual(expected, actual)

    def test_one_pk_value_with_bookmark(self):
        catalog_entry = CatalogEntry(tap_stream_id='foo',
                                     schema=Schema.from_dict({
                                         'properties':{
                                             'id': {'type': ['integer']}
                                         }
                                     }),
                                     metadata=[{'breadcrumb': (),
                                                'metadata': {'table-key-properties': ['id']}}])
        state = {
            'bookmarks':
                 {
                     'foo': {
                         'last_pk_fetched': {'id': 4},
                         'max_pk_values': {'id':10}
                     }
                 }
        }

        expected = ' WHERE ((`id` > 4)) AND `id` <= 10 ORDER BY `id` ASC'
        actual = generate_pk_clause(catalog_entry, state)

        self.assertEqual(expected, actual)

    def test_three_pk_values_with_bookmark(self):
        catalog_entry = CatalogEntry(tap_stream_id='foo',
                                     schema=Schema.from_dict({
                                         'properties':{
                                             'id1': {'type': ['integer']},
                                             'id2': {'type': ['string']},
                                             'id3': {'type': ['integer']}
                                         }
                                     }),
                                     metadata=[{'breadcrumb': (),
                                                'metadata': {'table-key-properties': ['id1', 'id2', 'id3']}}])
        state = {
            'bookmarks':
                 {
                     'foo': {
                         'last_pk_fetched': {'id1': 4, 'id2': 6, 'id3': 2 },
                         'max_pk_values': {'id1': 10, 'id2': 8, 'id3': 3}
                     }
                 }
        }

        expected = ' WHERE ((`id1` > 4) OR (`id1` = 4 AND `id2` > \'6\') OR (`id1` = 4 AND `id2` = \'6\' AND `id3` > 2)) AND `id1` <= 10 AND `id2` <= \'8\' AND `id3` <= 3 ORDER BY `id1`, `id2`, `id3` ASC'
        actual = generate_pk_clause(catalog_entry, state)

        self.assertEqual(expected, actual)

    def test_one_pk_values_without_bookmark(self):
        catalog_entry = CatalogEntry(tap_stream_id='foo',
                                     schema=Schema.from_dict({
                                         'properties':{
                                             'id': {'type': ['integer']}
                                         }
                                     }),
                                     metadata=[{'breadcrumb': (),
                                                'metadata': {'table-key-properties': ['id']}}])
        state = {
            'bookmarks':
                 {
                     'foo': {
                         'max_pk_values': {'id':10}
                     }
                 }
        }

        expected = ' WHERE `id` <= 10 ORDER BY `id` ASC'
        actual = generate_pk_clause(catalog_entry, state)

        self.assertEqual(expected, actual)

    def test_three_pk_values_without_bookmark(self):
        catalog_entry = CatalogEntry(tap_stream_id='foo',
                                     schema=Schema.from_dict({
                                         'properties':{
                                             'id1': {'type': ['integer']},
                                             'id2': {'type': ['string']},
                                             'id3': {'type': ['integer']}
                                         }
                                     }),
                                     metadata=[{'breadcrumb': (),
                                                'metadata': {'table-key-properties': ['id1', 'id2', 'id3']}}])
        state = {
            'bookmarks':
                 {
                     'foo': {
                         'max_pk_values': {'id1': 10, 'id2': 8, 'id3': 3}
                     }
                 }
        }

        expected = ' WHERE `id1` <= 10 AND `id2` <= \'8\' AND `id3` <= 3 ORDER BY `id1`, `id2`, `id3` ASC'
        actual = generate_pk_clause(catalog_entry, state)

        self.assertEqual(expected, actual)


# Cases:
# max_pk_values is Falsy X
# last_pk_fetched is truthy X
#   - Key_properties have something in it (1X and 3 pksX)
# last_pk_fetched is falsy X
#   - key_properties can be empty X (covered by first case)
#   - key_properties has some (1 and 3 pks)
# Try to get coverage reading
# Revert and ensure they still pass and have same coverage
