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

from doublex import assert_that
from hamcrest import equal_to
from singer import metadata

import tap_redshift

# TODO Set up as proper fixtures

sample_db_data = {
    'name': 'account_address',
    'columns': [
        {
            'type': 'int4',
            'pos': 1,
            'name': 'id',
            'nullable': 'NO'
        },
        {
            'type': 'bool',
            'pos': 2,
            'name': 'verified',
            'nullable': 'YES'
        },
        {
            'type': 'float',
            'pos': 3,
            'name': 'coord',
            'nullable': 'YES'
        },
        {
            'type': 'numeric',
            'pos': 4,
            'name': 'cost',
            'nullable': 'YES'
        },
        {
            'type': 'varchar',
            'pos': 5,
            'name': 'email',
            'nullable': 'NO'
        },
        {
            'type': 'date',
            'pos': 6,
            'name': 'date_expired',
            'nullable': 'YES'
        },
        {
            'type': 'timestamp',
            'pos': 7,
            'name': 'date_created',
            'nullable': 'YES'
        }
    ]
}

expected_result = {
    'streams': [
        {
            'database_name': 'FakeDB',
            'is_view': False,
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {
                        'minimum': -2147483648,
                        'type': 'integer',
                        'maximum': 2147483647,
                        'inclusion': 'available'
                    },
                    'is_bool': {
                        'type': [
                            'null',
                            'boolean'
                        ],
                        'inclusion': 'available'
                    },
                    'float': {
                        'type': [
                            'null',
                            'number'
                        ],
                        'inclusion': 'available'
                    },
                    'decimal': {
                        'type': [
                            'null',
                            'number'
                        ],
                        'inclusion': 'available'
                    },
                    'varchar': {
                        'type': 'string',
                        'inclusion': 'available'
                    },
                    'expires_at': {
                        'type': [
                            'null',
                            'string'
                        ],
                        'format': 'date',
                        'inclusion': 'available'
                    },
                    'created_at': {
                        'type': [
                            'null',
                            'string'
                        ],
                        'format': 'date-time',
                        'inclusion': 'available'
                    }
                },
            },
            'metadata': [
                {
                    'metadata': {
                        'selected-by-default': False,
                        'valid-replication-keys': ['created_at']
                    },
                    'breadcrumb': ()
                },
                {
                    'metadata': {
                        'sql-datatype': 'int4',
                        'selected-by-default': True
                    },
                    'breadcrumb': ['properties', 'id']
                },
                {
                    'metadata': {
                        'sql-datatype': 'bool',
                        'selected-by-default': True
                    },
                    'breadcrumb': ['properties', 'is_bool']
                },
                {
                    'metadata': {
                        'sql-datatype': 'int4',
                        'selected-by-default': True
                    },
                    'breadcrumb': ['properties', 'float']
                },
                {
                    'metadata': {
                        'sql-datatype': 'int4',
                        'selected-by-default': True
                    },
                    'breadcrumb': ['properties', 'decimal']
                },
                {
                    'metadata': {
                        'sql-datatype': 'varchar',
                        'selected-by-default': True
                    },
                    'breadcrumb': ['properties', 'varchar']
                },
                {
                    'metadata': {
                        'sql-datatype': 'date',
                        'selected-by-default': True
                    },
                    'breadcrumb': ['properties', 'expires_at']
                },
                {
                    'metadata': {
                        'sql-datatype': 'timestamptz',
                        'selected-by-default': True
                    },
                    'breadcrumb': ['properties', 'created_at']
                },
            ],
            'key_properties': [
                'id'
            ],
            'table_name': 'fake name',
            'stream': 'fake stream',
            'tap_stream_id': 'FakeDB-fake name'
        }
    ]
}


class TestRedShiftTap(object):
    def test_discover_catalog(self, discovery_conn, expected_catalog_from_db):
        actual_catalog = tap_redshift.discover_catalog(discovery_conn,
                                                       'public')
        for i, actual_entry in enumerate(actual_catalog.streams):

            expected_entry = expected_catalog_from_db.streams[i]

            actual_schema = actual_entry.schema
            expected_schema = expected_entry.schema

            for p, actual_property in actual_schema.properties.items():
                expected_property = expected_schema.properties[p]
                assert_that(actual_property, equal_to(expected_property))

            assert_that(actual_schema, equal_to(expected_schema))

            actual_metadata = metadata.to_map(actual_entry.metadata)
            expected_metadata = metadata.to_map(expected_entry.metadata)
            for bcrumb, actual_mdata in actual_metadata.items():
                for mdata_key, actual_value in actual_mdata.items():
                    assert_that(
                        actual_value,
                        equal_to(metadata.get(
                            expected_metadata, bcrumb, mdata_key)))

            assert_that(actual_entry, equal_to(expected_entry))

    def test_create_column_metadata(self):
        cols = [{'pos': 1, 'name': 'col1', 'type': 'int2', 'nullable': 'NO'},
                {'pos': 2, 'name': 'col2', 'type': 'float8',
                 'nullable': 'YES'},
                {'pos': 3, 'name': 'col3', 'type': 'timestamptz',
                 'nullable': 'NO'}]
        expected_mdata = metadata.new()
        metadata.write(expected_mdata, (), 'selected-by-default', False)
        for col in cols:
            metadata.write(expected_mdata, (),
                           'valid-replication-keys',
                           ['col3'])
            metadata.write(expected_mdata, (
                'properties', col['name']), 'selected-by-default', True)
            metadata.write(expected_mdata, (
                'properties', col['name']), 'sql-datatype', col['type'])

        actual_mdata = tap_redshift.create_column_metadata(cols)
        assert_that(actual_mdata, equal_to(metadata.to_list(expected_mdata)))

    def test_type_int4(self):
        col = sample_db_data['columns'][0]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        ppt = expected_result['streams'][0]['schema']['properties']['id']
        expected_schema = ppt
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_bool(self):
        col = sample_db_data['columns'][1]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        stream_schema = expected_result['streams'][0]
        expected_schema = stream_schema['schema']['properties']['is_bool']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_float(self):
        col = sample_db_data['columns'][2]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        stream_schema = expected_result['streams'][0]
        expected_schema = stream_schema['schema']['properties']['float']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_decimal(self):
        col = sample_db_data['columns'][3]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        stream_schema = expected_result['streams'][0]
        expected_schema = stream_schema['schema']['properties']['decimal']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_varchar(self):
        col = sample_db_data['columns'][4]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        stream_schema = expected_result['streams'][0]
        expected_schema = stream_schema['schema']['properties']['varchar']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_date(self):
        col = sample_db_data['columns'][5]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        stream_schema = expected_result['streams'][0]
        expected_schema = stream_schema['schema']['properties']['expires_at']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_date_time(self):
        col = sample_db_data['columns'][6]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        stream_schema = expected_result['streams'][0]
        expected_schema = stream_schema['schema']['properties']['created_at']
        assert_that(column_schema, equal_to(expected_schema))

    def test_valid_rep_keys(self, discovery_conn, expected_catalog_from_db):
        actual_catalog = tap_redshift.discover_catalog(discovery_conn,
                                                       'public')
        for i, actual_entry in enumerate(actual_catalog.streams):
            expected_entry = expected_catalog_from_db.streams[i]
            actual_metadata = metadata.to_map(actual_entry.metadata)
            expected_metadata = metadata.to_map(expected_entry.metadata)
            actual_valid_rep_keys = metadata.get(
                actual_metadata, (), 'valid-replication-keys')
            expected_valid_rep_keys = metadata.get(
                expected_metadata, (), 'valid-replication-keys')
            assert_that(actual_valid_rep_keys,
                        equal_to(expected_valid_rep_keys))

        # TODO write tests for full and incremental sync
