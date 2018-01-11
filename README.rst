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

* ``table_schema``

Example:

.. code-block:: json

    {
        "host": "REDSHIFT_HOSTT",
        "port": "REDSHIFT_PORT",
        "dbname": "REDSHIFT_DBNAME",
        "user": "REDSHIFT_USER",
        "password": "REDSHIFT_PASSWORD",
        "table_schema": "REDSHIFT_SCHEMA"
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

Edit ``properties.json`` file to make selections by adding key-value of ``"selected": "true"`` to the top level schema and properties that should be synced.

Example:


.. code-block:: json

    {
        "selected": "true",
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
        },
        "type": "object"
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

To use incremental replication, we need to add the ``replication_method`` and ``replication_key`` to the ``properties.json file``.

.. code-block:: json

    {
        "replication_method": "INCREMENTAL",
        "replication_key": "id",
        "selected": "true",
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
        },
        "type": "object"
    }

We can then invoke the tap again in sync mode. This time the output will have ``STATE`` messages that contains a ``replication_key_value`` and ``bookmark`` for data that were extracted. 

Redirect the output to a ``state.json`` file. Normally, the target will echo the last STATE after it has finished processing data.

Run the code below to pass the state into a ``stae.json`` file and then grab the last synced state data. 

.. code-block:: shell

    $ tap-redshift -c config.json --properties properties.json > state.json

    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json

We can then always invoke the incremental replication with the ``state.json`` file to only sync new data created after the last synced data.

.. code-block:: shell

    $ tap-redshift -c config.json --properties properties.json --state state.json