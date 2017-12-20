import tap_redshift
import mock
from mock import patch

import pytest
from doublex import assert_that, called
from hamcrest import has_key, equal_to

import singer
from singer.schema import Schema

sample_db_data = {
    "name": "account_address",
    "columns": [
        {
            "type": "int4",
            "pos": 1,
            "name": "id"
        },
        {
            "type": "bool",
            "pos": 2,
            "name": "verified"
        },
        {
            "type": "float",
            "pos": 3,
            "name": "coord"
        },
        {
            "type": "decimal",
            "pos": 4,
            "name": "cost"
        },
        {
            "type": "varchar",
            "pos": 5,
            "name": "email"
        },
        {
            'type': 'date',
            'pos': 6,
            'name': 'date_created'
        }
    ]
}

expected_result = {
    'streams': [{
        'database_name': 'FakeDB',
        'schema': {
            'type': 'object',
            'properties': {
                'id': {
                    'minimum': -2147483648,
                    'type': [
                        'null',
                        'integer'
                    ],
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
                    'exclusiveMaximum': True,
                    'inclusion': 'available'
                },
                'varchar': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'inclusion': 'available'
                },
                "expires_at": {
                    "type": [
                        "null",
                        "string"
                    ],
                    'format': 'date-time',
                    'inclusion': 'available'
                }
            },
            'table_name': 'fake name',
            'stream': 'fake stream',
            'tap_stream_id': 'FakeDB-fake name'
        }
    }]}

@pytest.fixture()
def db_config():
    config = {
        'host':'host',
        'port':'',
        'dbname':'FakeDB',
        'user':'user',
        'password':'password'
    }
    return config

def message_types_and_versions(messages):
    message_types = []
    versions = []
    for message in messages:
        t = type(message)
        if t in set([singer.RecordMessage, singer.ActivateVersionMessage]):
            message_types.append(t.__name__)
            versions.append(message.version)
    return (message_types, versions)

class TestRedShiftTap(object):
    @mock.patch("psycopg2.connect")
    def test_discover_catalog(self, mock_connect, db_config):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected_result
        result = tap_redshift.discover_catalog(mock=db_config).to_dict()
        assert_that(result, has_key(equal_to('streams')))
        streams = result['streams'][0]
        assert_that(streams, has_key(equal_to('schema')))

    def test_type_int4(self):
        col = sample_db_data['columns'][0]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        expected_schema = expected_result['streams'][0]['schema']['properties']['id']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_bool(self):
        col = sample_db_data['columns'][1]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        expected_schema = expected_result['streams'][0]['schema']['properties']['is_bool']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_float(self):
        col = sample_db_data['columns'][2]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        expected_schema = expected_result['streams'][0]['schema']['properties']['float']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_decimal(self):
        col = sample_db_data['columns'][3]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        expected_schema = expected_result['streams'][0]['schema']['properties']['decimal']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_varchar(self):
        col = sample_db_data['columns'][4]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        expected_schema = expected_result['streams'][0]['schema']['properties']['varchar']
        assert_that(column_schema, equal_to(expected_schema))

    def test_type_date(self):
        col = sample_db_data['columns'][5]
        column_schema = tap_redshift.schema_for_column(col).to_dict()
        expected_schema = expected_result['streams'][0]['schema']['properties']['expires_at']
        assert_that(column_schema, equal_to(expected_schema))
