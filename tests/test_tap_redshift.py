import tap_redshift
import mock
import singer

import pytest
from doublex import assert_that, called
from hamcrest import has_key, equal_to, instance_of


sample_db_data = {
    "name": "account_address",
    "columns": [
        {
            "type": "int4",
            "pos": 1,
            "name": "id",
            "nullable": "NO"
        },
        {
            "type": "bool",
            "pos": 2,
            "name": "verified",
            "nullable": "YES"
        },
        {
            "type": "float",
            "pos": 3,
            "name": "coord",
            "nullable": "YES"
        },
        {
            "type": "numeric",
            "pos": 4,
            "name": "cost",
            "nullable": "YES"
        },
        {
            "type": "varchar",
            "pos": 5,
            "name": "email",
            "nullable": "NO"
        },
        {
            'type': 'date',
            'pos': 6,
            'name': 'date_created',
            "nullable": "YES"
        }
    ]
}


expected_result = {
    "streams": [
        {
            "database_name": "FakeDB",
            "is_view": False,
            "schema": {
                "type": "object",
                "properties": {
                    "id": {
                        "minimum": -2147483648,
                        "type": "integer",
                        "maximum": 2147483647,
                        "inclusion": "available"
                    },
                    "is_bool": {
                        "type": [
                            "null",
                            "boolean"
                        ],
                        "inclusion": "available"
                    },
                    "float": {
                        "type": [
                            "null",
                            "number"
                        ],
                        "inclusion": "available"
                    },
                    "decimal": {
                        "type": [
                            "null",
                            "number"
                        ],
                        "exclusiveMaximum": True,
                        "inclusion": "available"
                    },
                    "varchar": {
                        "type": "string",
                        "inclusion": "available"
                    },
                    "expires_at": {
                        "type": [
                            "null",
                            "string"
                        ],
                        "format": "date-time",
                        "inclusion": "available"
                    }
                },
            },
            "metadata": [
                {
                    "metadata": {
                        "sql-datatype": "int4",
                        "selected-by-default": True
                    },
                    "breadcrumb": ["properties", "id"]
                },
                {
                    "metadata": {
                        "sql-datatype": "bool",
                        "selected-by-default": True
                    },
                    "breadcrumb": ["properties", "is_bool" ]
                },
                {
                    "metadata": {
                        "sql-datatype": "int4",
                        "selected-by-default": True
                    },
                    "breadcrumb": ["properties", "float" ]
                },
                {
                    "metadata": {
                        "sql-datatype": "int4",
                        "selected-by-default": True
                    },
                    "breadcrumb": ["properties", "decimal" ]
                },
                {
                    "metadata": {
                        "sql-datatype": "varchar",
                        "selected-by-default": True
                    },
                    "breadcrumb": ["properties", "varchar"]
                },
                {
                    "metadata": {
                        "sql-datatype": "timestamptz",
                        "selected-by-default": True
                    },
                    "breadcrumb": ["properties", "expires_at"]
                },
            ],
            "key_properties": [
                "id",
                "is_bool",
                "float",
                "decimal",
                "varchar",
                "expires_at"
            ],
            "is_view": False,
            "table_name": "fake name",
            "stream": "fake stream",
            "tap_stream_id": "FakeDB-fake name"
        }
    ]
}


@pytest.fixture()
def db_config():
    config = {
        'host': 'host',
        'port': '',
        'dbname': 'FakeDB',
        'user': 'user',
        'password': 'password'
    }
    return config


def message_types_and_versions(messages):
    message_types = []
    for message in messages:
        t = type(message)
        if t in set([singer.StateMessage, singer.SchemaMessage]):
            message_types.append(t.__name__)
    return message_types


class TestRedShiftTap(object):
    @mock.patch("psycopg2.connect")
    def test_discover_catalog(self, mock_connect, db_config):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected_result
        result = tap_redshift.discover_catalog(mock=db_config).to_dict()
        assert_that(result, has_key(equal_to('streams')))
        result_schema = result['streams'][0]
        assert_that(result_schema, has_key(equal_to('schema')))

    @mock.patch("psycopg2.connect")
    def test_unsupported_col(self, mock_connect, db_config):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected_result
        result = tap_redshift.discover_catalog(mock=db_config)
        assert_that(result.streams[0].key_properties, equal_to(None))

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

    @mock.patch("psycopg2.connect")
    def test_no_col_selected(self, mock_connect, db_config):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected_result
        catalog = tap_redshift.discover_catalog(mock=db_config)
        state = tap_redshift.build_state({}, catalog)
        message_types = message_types_and_versions(
            tap_redshift.generate_messages(catalog, state, mock=db_config))
        assert_that((message_types, equal_to(['StateMessage'])))

    @mock.patch("psycopg2.connect")
    def test_build_state(self, mock_connect, db_config):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected_result
        catalog = tap_redshift.discover_catalog(mock=db_config)
        for stream in catalog.streams:
            stream.replication_key = 'id'
        state = tap_redshift.build_state({}, catalog)
        assert_that(state, has_key(equal_to('bookmarks')))

    @mock.patch("psycopg2.connect")
    def test_stream_version(self, mock_connect, db_config):
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected_result
        catalog = tap_redshift.discover_catalog(mock=db_config)
        stream_id = catalog.to_dict()['streams'][0]['tap_stream_id']
        state = tap_redshift.build_state({}, catalog)
        stream_version = tap_redshift.get_stream_version(stream_id, state)
        assert_that(stream_version, instance_of(int))
