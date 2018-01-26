============
tap-redshift
============


`Singer <https://singer.io>`_ tap that extracts data from a `Redshift <https://aws.amazon.com/documentation/redshift/>`_ database and produces JSON-formatted data following the Singer spec.


Usage
=====
tap-redshift assumes you have connection to redshift.

Create a configuration file
---------------------------
When you install tap-redshift, you need to create a ``config.json`` file for the database connection.

The json file requires the following attributes;

* ``host``
* ``port``
* ``dbname``
* ``user``
* ``password``

And an optional attribute;

* ``schema``

Example:

.. code-block:: json

    {
        "host": "REDSHIFT_HOSTT",
        "port": "REDSHIFT_PORT",
        "dbname": "REDSHIFT_DBNAME",
        "user": "REDSHIFT_USER",
        "password": "REDSHIFT_PASSWORD",
        "schema": "REDSHIFT_SCHEMA"
    }


Discovery mode
==============
The tap can be invoked in discovery mode to get the available tables and columns in the database. It points to the config file created to connect to redshift:

.. code-block:: shell

    $ tap-redshift --config config.json -d

A full catalog tap is writtem to stdout, with a JSON-schema description of each table. A source table directly corresponds to a Singer stream.

Redirect output from the tap's discovery mode to a file so that it can be modified when the tap is to be invoked in sync mode.

.. code-block:: shell

    $ tap-redshift -c config.json -d > properties.json

This runs the tap in discovery mode and copies the output into a ``properties.json`` file.


Tables and property selection
=============================
In sync mode, tap-redshift consumes a modified version of the catalog where tables and fields have been marked as selected.

Edit the metadata list in your ``properties.json`` file to make property selections.

The first element in the metadata list with the empty breadcrumb property is the table/schema, once ``selected: true`` is added to its metadata dict, each properties that has ``selected-by-default`` to be ``true`` or has ``selected: true`` added in its metadata dict will be synced.

Example:


.. code-block:: json

    {
        "tap_stream_id": "sample-stream-id",
        "table_name": "sample-name",
        "stream": "sample-stream",
        "is_view": false,
        "database_name": "sample-dbname"
        "schema": {
            "properties": {
                "name": {
                    "maxLength": 255,
                    "inclusion": "available",
                    "type": [
                        "null",
                        "string"
                    ]
                },
                "id": {
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
        },
        "metadata": [
            {
                "metadata": {
                    "selected-by-default": false,
                    "selected": true
                },
                "breadcrumb": [],
            },
            {
                "metadata": {
                    "selected": true,
                    "selected-by-default": true,
                    "sql-datatype": "int2"
                },
                "breadcrumb": [
                    "properties",
                    "id"
                ]
            },
            {
                "metadata": {
                    "selected-by-default": true,
                    "sql-datatype": "varchar"
                },
                "breadcrumb": [
                    "properties",
                    "catname"
                ]
            },
        ]
    }

The tap can then be invoked in sync mode with the properties catalog argument:

.. code-block:: shell

    $ tap-redshift -c config.json --properties properties.json


Replication methods and state file
==================================
There are two ways to replicate a given table. FULL_TABLE and INCREMENTAL. FULL_TABLE replication is used by default.

Full Table
----------
Full-table replication extracts all data from the source table each time the tap is invoked without a state file.

Incremental
-----------
Incremental replication works in conjunction with a state file to only extract new records each time the tap is invoked i.e continue from the last synced data.

To use incremental replication, we need to add the ``replication_method`` and ``replication_key`` to the top level of the ``properties.json file``.

.. code-block:: json

    {
        "streams": [
            {
                "replication_method": "INCREMENTAL",
                "replication_key": "id",
                "tap_stream_id": "tap-sample",
                "schema": {
                    "properties": {
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
                    }
                    "type": "object"
                }
            }
        ]
    }

We can then invoke the tap again in sync mode. This time the output will have ``STATE`` messages that contains a ``replication_key_value`` and ``bookmark`` for data that were extracted. 

Redirect the output to a ``state.json`` file. Normally, the target will echo the last STATE after it has finished processing data.

Run the code below to pass the state into a ``state.json`` file and then grab the last synced state data.

.. code-block:: shell

    $ tap-redshift -c config.json --properties properties.json > state.json

    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json

The ``state.json`` file should look like;

.. code-block:: json

    {
        "currently_syncing": "dbname-tablename",
        "bookmarks": {
            "dev-category": {
                "replication_key": "id",
                "version": 1516304171710,
                "replication_key_value": 3
            }
        }
    }

We can then always invoke the incremental replication with the ``state.json`` file to only sync new data created after the last synced data.

.. code-block:: shell

    $ tap-redshift -c config.json --properties properties.json --state state.json