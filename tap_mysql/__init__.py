
#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time
import collections

import attr
import pendulum

import pymysql
import singer
import singer.stats
from singer import utils

import pymysql.constants.FIELD_TYPE as FIELD_TYPE

Column = collections.namedtuple('Column', [
    "table_schema", 
    "table_name", 
    "column_name", 
    "data_type", 
    "character_maximum_length", 
    "numeric_precision", 
    "numeric_scale", 
    "datetime_precision", 
    "column_type", 
    "column_key"]
)

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password',
    'database'
]

LOGGER = singer.get_logger()


def open_connection(config):
    return pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
    )

STRING_TYPES = set([
    'char',
    'enum',
    'longtext',
    'mediumtext',
    'text',
    'varchar'
])

MAX_VAL_FOR_INTEGER_TYPE = {
    
}


def schema_for_column(c):

 # varchar    
 # longtext   
 # datetime   
 # timestamp  
 # text       
 # bit        
 # char       
 # set        
 # enum       
 # longblob   
 # mediumtext 
 # blob       
 # time       
 # date       
 # binary     

    t = c.data_type

    if t == 'tinyint':
        return {
            'type': 'integer',
        }
    elif t == 'smallint':
        return {
            'type': 'integer',
        }
    elif t == 'int':
        return {
            'type': 'integer',
        }
    elif t == 'float':
        return {
            'type': 'number'
        }
    elif t == 'double':
        return {
            'type': 'number'
        }
    elif t == 'bigint':
        return {
            'type': 'integer',
            }    
    elif t == 'mediumint':
        return {
            'type': 'integer',
        }    
    elif t == 'decimal':
        return {
            'type': 'number',
            'exclusiveMaximum': 10 ** (c.numeric_precision - c.numeric_scale),
            'multipleOf': 10 ** (0 - c.numeric_scale),
        }
    elif t in STRING_TYPES:
        return {
            'type': 'string',
            'maxLength': c.character_maximum_length
        }
    else:
        return {
            'inclusion': 'unsupported',
            'description': 'Unsupported column type {}'.format(c.sql_datatype)
        }

    
def schema_for_table(connection, table):
    schema = {
        'type': 'object',
        'properties': {}
    }

    columns = {}
    with connection.cursor() as cursor:
        cursor.execute('SHOW columns FROM {}'.format(table))
        
        for c in cursor.fetchall():
            (name, datatype, _, _, _, _) = c
            column = Column()
            column.name = name
            column.sql_datatype = datatype
            columns[name] = column

    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM {} LIMIT 1'.format(table))
        for description in cursor.description:

            # Destructure column desc
            (name, type_code, display_size, internal_size,
             precision, scale, null_ok) = description
            col = columns[name]
            col.type_code = type_code
            col.display_size = display_size
            col.internal_size = internal_size
            col.precision = precision
            col.scale = scale
            col.null_ok = null_ok
            
            schema['properties'][col.name] = schema_for_column(col)

        return schema


def do_discover(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
          SELECT table_schema, 
                 table_name, 
                 column_name, 
                 data_type, 
                 character_maximum_length, 
                 numeric_precision, 
                 numeric_scale, 
                 datetime_precision, 
                 column_type, 
                 column_key from information_schema.columns
            WHERE table_schema NOT IN (
              'information_schema',
              'performance_schema',
              'mysql')
""")

        rec = cursor.fetchone()
        while rec is not None:
            col = Column(*rec)
            LOGGER.info('%s', col)

            rec = cursor.fetchone()
            

    # LOGGER.info("Databases are %s", dbs)
    # for db in dbs:
    #     with connection.cursor() as cursor:
    #         # TODO: Cleanse the db name
    #         cursor.execute('USE {}'.format(db))
    #         cursor.execute('SHOW TABLES')
    #         tables = [row[0] for row in cursor.fetchall()]
    #         LOGGER.info('DB %s has tables %s', db, tables)
    #     for table in tables:
    #         schema = schema_for_table(connection, table)


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    connection = open_connection(args.config)
    if args.discover:
        do_discover(connection)
    elif args.properties:
        print("I would do sync")
    else:
        LOGGER.info("No properties were selected")
