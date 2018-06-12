# tap-redshift
# Copyright 2018 data.world, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# data.world, Inc.(http://data.world/).
import pytest
from doublex import Mock, ANY_ARG, Stub
from singer.catalog import Catalog

# TODO Smarter and less verbose fixtures
# TODO Possibly move from conftest.py if not shared


@pytest.fixture()
def discovery_conn(table_spec_cursor, column_specs_cursor, pk_specs_cursor):
    with Stub() as conn:
        cursors_iter = iter(
            [table_spec_cursor, column_specs_cursor, pk_specs_cursor])

        conn.cursor = lambda: next(cursors_iter)
        conn.get_dsn_parameters().returns({'dbname': 'test-db'})

    return conn


@pytest.fixture()
def table_spec_cursor():
    with Mock() as cursor:
        result = [('table1', 'BASE TABLE'), ('table2', 'BASE TABLE'),
                  ('view1', 'VIEW')]
        cursor.execute(ANY_ARG)
        cursor.fetchall().returns(result)
        cursor.close()

    return cursor


@pytest.fixture()
def pk_specs_cursor():
    with Mock() as cursor:
        result = [('table1', 'col1'), ('table2', 'col1'),
                  ('table2', 'col2')]
        cursor.execute(ANY_ARG)
        cursor.fetchall().returns(result)
        cursor.close()

    return cursor


@pytest.fixture()
def column_specs_cursor():
    with Mock() as cursor:
        result = [('table1', 1, 'col1', 'int2', 'NO'),
                  ('table1', 2, 'col2', 'float8', 'YES'),
                  ('table1', 3, 'col3', 'timestamptz', 'NO'),
                  ('table1', 4, 'col4', 'timestamp', 'NO'),
                  ('table1', 5, 'col5', 'timestamp with time zone', 'NO'),
                  ('table2', 1, 'col1', 'int4', 'NO'),
                  ('table2', 2, 'col2', 'bool', 'YES'),
                  ('view1', 1, 'col1', 'varchar', 'NO'),
                  ('view1', 2, 'col2', 'unknown', 'NO')]
        cursor.execute(ANY_ARG)
        cursor.fetchall().returns(result)
        cursor.close()

    return cursor


@pytest.fixture()
def expected_catalog_from_db():
    return Catalog.from_dict({'streams': [
        {'tap_stream_id': 'test-db.public.table1',
         'table_name': 'public.table1',
         'schema': {
             'properties': {
                 'col1': {
                     'inclusion': 'available',
                     'minimum': -32768,
                     'maximum': 32767,
                     'type': 'integer'},
                 'col2': {
                     'inclusion': 'available',
                     'type': ['null', 'number']},
                 'col3': {
                     'inclusion': 'available',
                     'format': 'date-time',
                     'type': 'string'},
                 'col4': {
                     'inclusion': 'available',
                     'format': 'date-time',
                     'type': 'string'},
                 'col5': {
                     'inclusion': 'available',
                     'format': 'date-time',
                     'type': 'string'}},
             'type': 'object'},
         'stream': 'table1',
         'metadata': [
             {'breadcrumb': (),
              'metadata': {'selected-by-default': False,
                           'valid-replication-keys': [
                               'col3', 'col4', 'col5'],
                           'table-key-properties': ['col1'],
                           'is-view': False,
                           'schema-name': 'table1',
                           'database-name': 'test-db'}},
             {'breadcrumb': ('properties', 'col1'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'int2',
                           'inclusion': 'available'}},
             {'breadcrumb': ('properties', 'col2'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'float8',
                           'inclusion': 'available'}},
             {'breadcrumb': ('properties', 'col3'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'timestamptz',
                           'inclusion': 'available'}},
             {'breadcrumb': ('properties', 'col4'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'timestamp',
                           'inclusion': 'available'}},
             {'breadcrumb': ('properties', 'col5'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'timestamp with time zone',
                           'inclusion': 'available'}}
         ]},
        {'tap_stream_id': 'test-db.public.table2',
         'table_name': 'public.table2',
         'schema': {
             'properties': {
                 'col1': {
                     'inclusion': 'available',
                     'minimum': -2147483648,
                     'maximum': 2147483647,
                     'type': 'integer'},
                 'col2': {
                     'inclusion': 'available',
                     'type': ['null', 'boolean']}},
             'type': 'object'
         },
         'stream': 'table2',
         'metadata': [
             {'breadcrumb': (),
              'metadata': {'selected-by-default': False,
                           'forced-replication-method': {
                            'replication-method': 'FULL_TABLE',
                            'reason': 'No replication keys found from table'
                           },
                           'table-key-properties': ['col1', 'col2'],
                           'is-view': False,
                           'schema-name': 'table2',
                           'database-name': 'test-db'}},
             {'breadcrumb': ('properties', 'col1'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'int4',
                           'inclusion': 'available'}},
             {'breadcrumb': ('properties', 'col2'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'bool',
                           'inclusion': 'available'}}]},
        {'tap_stream_id': 'test-db.public.view1',
         'table_name': 'public.view1',
         'schema': {
             'properties': {
                 'col1': {
                     'inclusion': 'available',
                     'type': 'string'},
                 'col2': {
                     'inclusion': 'unsupported',
                     'description': 'Unsupported column type unknown'}},
             'type': 'object'},
         'stream': 'view1',
         'metadata': [
             {'breadcrumb': (),
              'metadata': {'selected-by-default': False,
                           'forced-replication-method': {
                            'replication-method': 'FULL_TABLE',
                            'reason': 'No replication keys found from table'
                           },
                           'view-key-properties': [],
                           'is-view': True,
                           'schema-name': 'view1',
                           'database-name': 'test-db'}},
             {'breadcrumb': ('properties', 'col1'),
              'metadata': {'selected-by-default': True,
                           'sql-datatype': 'varchar',
                           'inclusion': 'available'}},
             {'breadcrumb': ('properties', 'col2'),
              'metadata': {'selected-by-default': False,
                           'sql-datatype': 'unknown',
                           'inclusion': 'unsupported'}}
         ]}
    ]})


@pytest.fixture()
def expected_catalog_discovered():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected-by-default': False
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_catalog_selected_table():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_catalog_selected_table_legacy():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected-by-default': False
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_catalog_unselected_table():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': False
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2',
                        'selected': False
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_catalog_selected_default_col():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_catalog_selected_col():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'sql-datatype': 'int2',
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_catalog_selected_col_legacy():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'selected': True,
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'selected': True,
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'sql-datatype': 'int2'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_catalog_unselected_col():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2',
                        'selected': False
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_superset_catalog_selected_default_col():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'name': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'address': {
                        'type': 'string',
                        'inclusion': 'unsupported'
                    },
                    'phone': {
                        'type': 'string',
                        'inclusion': 'available'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'name'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'address'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'phone'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'varchar'
                    }
                }
            ]
        }]
    })


@pytest.fixture()
def expected_subset_catalog_selected_default_col():
    return Catalog.from_dict({
        'streams': [{
            'database_name': 'FakeDB',
            'table_name': 'category',
            'tap_stream_id': 'dev-category',
            'is_view': False,
            'stream': 'category',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    }
                }
            },
            'metadata': [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'selected': True
                    }
                },
                {
                    'breadcrumb': (
                        'properties',
                        'id'
                    ),
                    'metadata': {
                        'selected-by-default': True,
                        'sql-datatype': 'int2'
                    }
                }
            ]
        }]
    })


@pytest.fixture(params=[
    (expected_catalog_discovered().streams[0], False),
    (expected_catalog_selected_table().streams[0], True),
    (expected_catalog_selected_table_legacy().streams[0], True),
    (expected_catalog_unselected_table().streams[0], False)])
def selectable_entry_param(request):
    return request.param


@pytest.fixture(params=[
    (expected_catalog_discovered().streams[0], {'id', 'name', 'address'}),
    (expected_catalog_selected_col().streams[0], {'id', 'name', 'address'}),
    (expected_catalog_selected_col_legacy().streams[0], {'id', 'name'}),
    (expected_catalog_unselected_col().streams[0], {'name', 'address'})])
def selectable_properties_param(request):
    return request.param


@pytest.fixture(params=[
    (expected_catalog_selected_table(), {
        'category': {'id', 'name'}}),
    (expected_catalog_selected_table_legacy(), {
        'category': {'id', 'name'}}),
    (expected_catalog_unselected_table(), {}),
    (expected_catalog_selected_col(), {
        'category': {'id', 'name'}}),
    (expected_catalog_selected_col_legacy(), {
        'category': {'id', 'name'}}),
    (expected_catalog_unselected_col(), {
        'category': {'name'}}),
    (expected_subset_catalog_selected_default_col(), {
        'category': {'id'}}),
    (expected_superset_catalog_selected_default_col(), {
        'category': {'id', 'name'}})])
def resolvable_catalog_param(request):
    return request.param
