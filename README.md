# tap-mysql

[![PyPI version](https://badge.fury.io/py/tap-mysql.svg)](https://badge.fury.io/py/tap-mysql)
[![CircleCI Build Status](https://circleci.com/gh/singer-io/tap-mysql.png)](https://circleci.com/gh/singer-io/tap-mysql)

[Singer](https://www.singer.io/) tap that extracts data from a [MySQL](https://www.mysql.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

```bash
$ mkvirtualenv -p python3 tap-mysql
$ pip install tap-mysql
$ tap-mysql --config config.json --discover
$ tap-mysql --config config.json --properties properties.json --state state.json
```

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Usage](#usage)
  - [Installation](#installation)
  - [Have a source database](#have-a-source-database)
  - [Create the configuration file](#create-the-configuration-file)
  - [Discovery mode](#discovery-mode)
  - [Field selection](#field-selection)
  - [Sync mode](#sync-mode)
- [Replication methods and state file](#replication-methods-and-state-file)
  - [Full Table](#full-table)
  - [Incremental](#incremental)
    - [Example](#example)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Usage

This section dives into basic usage of `tap-mysql` by walking through extracting
data from a table. It assumes that you can connect to and read from a MySQL
database.

### Install

```bash
$ mkvirtualenv -p python3 tap-mysql
$ pip install tap-mysql
```

or

```bash
$ git clone git@github.com:singer-io/tap-mysql.git
$ cd tap-mysql
$ mkvirtualenv -p python3 tap-mysql
$ python install .
```

### Have a source database

There's some important business data siloed in this MySQL database -- we need to
extract it. Here's the table we'd like to sync:

```
mysql> select * from example_db.animals;
+----|----------|----------------------+
| id | name     | likes_getting_petted |
+----|----------|----------------------+
|  1 | aardvark |                    0 |
|  2 | bear     |                    0 |
|  3 | cow      |                    1 |
+----|----------|----------------------+
3 rows in set (0.00 sec)
```

### Create the configuration file

Create a config file containing the database connection credentials, e.g.:

```json
{
  "host": "localhost",
  "port": "3306",
  "user": "root",
  "password": "password"
}
```

These are the same basic configuration properties used by the MySQL command-line
client (`mysql`).

### Discovery mode

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-mysql --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

```json
{
  "streams": [
    {
      "key_properties": [
        "id"
      ],
      "tap_stream_id": "example_db-animals",
      "schema": {
        "properties": {
          "likes_getting_petted": {
            "type": [
              "null",
              "boolean"
            ],
            "inclusion": "available"
          },
          "name": {
            "maxLength": 255,
            "type": [
              "null",
              "string"
            ],
            "inclusion": "available"
          },
          "id": {
            "type": [
              "null",
              "integer"
            ],
            "maximum": 2147483647,
            "minimum": -2147483648,
            "inclusion": "automatic"
          }
        },
        "type": "object"
      },
      "table_name": "animals",
      "metadata": [
        {
          "metadata": {
            "selected-by-default": true,
            "sql-datatype": "int(11)"
          },
          "breadcrumb": [
            "properties",
            "id"
          ]
        },
        {
          "metadata": {
            "database-name": "example_db",
            "selected-by-default": false,
            "is-view": false,
            "row-count": 3
          },
          "breadcrumb": []
        },
        {
          "metadata": {
            "selected-by-default": true,
            "sql-datatype": "varchar(255)"
          },
          "breadcrumb": [
            "properties",
            "name"
          ]
        },
        {
          "metadata": {
            "selected-by-default": true,
            "sql-datatype": "tinyint(1)"
          },
          "breadcrumb": [
            "properties",
            "likes_getting_petted"
          ]
        }
      ],
      "stream": "animals"
    }
  ]
}

```

### Field selection

In sync mode, `tap-mysql` consumes a modified version of the catalog where
tables and fields have been marked as _selected_.

Redirect output from the tap's discovery mode to a file so that it can be
modified:

```bash
$ tap-mysql -c config.json --discover > properties.json
```

Then edit `properties.json` to make selections. In this example we want the
`animals` table. The stream's schema gets a top-level `selected` flag, as does
its columns' schemas:

```json
{
  "selected": "true",
  "properties": {
    "likes_getting_petted": {
      "selected": "true",
      "inclusion": "available",
      "type": [
        "null",
        "boolean"
      ]
    },
    "name": {
      "selected": "true",
      "maxLength": 255,
      "inclusion": "available",
      "type": [
        "null",
        "string"
      ]
    },
    "id": {
      "selected": "true",
      "minimum": -2147483648,
      "inclusion": "automatic",
      "maximum": 2147483647,
      "type": [
        "null",
        "integer"
      ]
    }
  },
  "type": "object"
}
```

### Sync mode

With an annotated properties catalog, the tap can be invoked in sync mode:

```bash
$ tap-mysql -c config.json --properties properties.json
```

Messages are written to standard output following the Singer specification. The
resultant stream of JSON data can be consumed by a Singer target.

```json
{"value": {"currently_syncing": "example_db-animals"}, "type": "STATE"}

{"key_properties": ["id"], "stream": "animals", "schema": {"properties": {"name": {"inclusion": "available", "maxLength": 255, "type": ["null", "string"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}, "id": {"inclusion": "automatic", "minimum": -2147483648, "type": ["null", "integer"], "maximum": 2147483647}}, "type": "object"}, "type": "SCHEMA"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"record": {"name": "aardvark", "likes_getting_petted": false, "id": 1}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "bear", "likes_getting_petted": false, "id": 2}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "cow", "likes_getting_petted": true, "id": 3}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"value": {"currently_syncing": "example_db-animals", "bookmarks": {"example_db-animals": {"version": null}}}, "type": "STATE"}

{"value": {"currently_syncing": null, "bookmarks": {"example_db-animals": {"version": null}}}, "type": "STATE"}
```

## Replication methods and state file

In the above example, we invoked `tap-mysql` without providing a _state_ file
and without specifying a replication method. The two ways to replicate a given
table are `FULL_TABLE` and `INCREMENTAL`. `FULL_TABLE` replication is used by
default.

### Full Table

Full-table replication extracts all data from the source table each time the tap
is invoked.

### Incremental

Incremental replication works in conjunction with a state file to only extract
new records each time the tap is invoked.

#### Example

Let's sync the `animals` table again, but this time using incremental
replication. The replication method and replication key are set in the
properties file:

```json
{
  "streams": [
    {
      "replication_method": "INCREMENTAL",
      "replication_key": "id",
      "key_properties": [
        "id"
      ],
      "tap_stream_id": "example_db-animals",
      "schema": {
        "selected": "true",
        "properties": {
          "likes_getting_petted": {
            "selected": "true",
            "type": [
              "null",
              "boolean"
            ],
            "inclusion": "available"
          },
          "name": {
            "selected": "true",
            "maxLength": 255,
            "type": [
              "null",
              "string"
            ],
            "inclusion": "available"
          },
          "id": {
            "selected": "true",
            "type": [
              "null",
              "integer"
            ],
            "maximum": 2147483647,
            "minimum": -2147483648,
            "inclusion": "automatic"
          }
        },
        "type": "object"
      },
      "table_name": "animals",
      "metadata": [...],
      "stream": "animals"
    }
  ]
}
```

We have no meaningful state so far, so just invoke the tap in sync mode again
without a state file:

```bash
$ tap-mysql -c config.json --properties properties.json
```

The output messages look very similar to when the table was replicated using the
default `FULL_TABLE` replication method. One important difference is that the
`STATE` messages now contain a `replication_key_value` -- a bookmark or
high-water mark -- for data that was extracted:

```
{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id"}}, "currently_syncing": "example_db-animals"}}

{"stream": "animals", "type": "SCHEMA", "schema": {"type": "object", "properties": {"id": {"type": ["null", "integer"], "minimum": -2147483648, "maximum": 2147483647, "inclusion": "automatic"}, "name": {"type": ["null", "string"], "inclusion": "available", "maxLength": 255}, "likes_getting_petted": {"type": ["null", "boolean"], "inclusion": "available"}}}, "key_properties": ["id"]}

{"stream": "animals", "type": "ACTIVATE_VERSION", "version": 1509135204169}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 1, "name": "aardvark", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 2, "name": "bear", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 3, "name": "cow", "likes_getting_petted": true}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": null}}
```

Note that the final `STATE` message has a `replication_key_value` of `3`,
reflecting that the extraction ended on a record that had an `id` of `3`.
Subsequent invocations of the tap will pick up from this bookmark.

Normally, the target will echo the last `STATE` after it's finished processing
data. For this example, let's manually write a `state.json` file using the
`STATE` message:

```json
{
  "bookmarks": {
    "example_db-animals": {
      "version": 1509135204169,
      "replication_key_value": 3,
      "replication_key": "id"
    }
  },
  "currently_syncing": null
}
```

Let's add some more animals to our farm:

```
mysql> insert into animals (name, likes_getting_petted) values ('dog', true), ('elephant', true), ('frog', false);
```

```bash
$ tap-mysql -c config.json --properties properties.json --state state.json
```

This invocation extracts any data since (and including) the
`replication_key_value`:

```json
{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 3}}, "currently_syncing": "example_db-animals"}}

{"key_properties": ["id"], "schema": {"properties": {"name": {"maxLength": 255, "inclusion": "available", "type": ["null", "string"]}, "id": {"maximum": 2147483647, "minimum": -2147483648, "inclusion": "automatic", "type": ["null", "integer"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}}, "type": "object"}, "type": "SCHEMA", "stream": "animals"}

{"type": "ACTIVATE_VERSION", "version": 1509135204169, "stream": "animals"}

{"record": {"name": "cow", "id": 3, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "dog", "id": 4, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "elephant", "id": 5, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "frog", "id": 6, "likes_getting_petted": false}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": null}}
```

---

Copyright &copy; 2017 Stitch
