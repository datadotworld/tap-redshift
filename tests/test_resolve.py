import tap_redshift
import mock
import singer

import pytest
from doublex import assert_that
from hamcrest import equal_to, calling, raises, has_key, instance_of

from singer.schema import Schema


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


expected_catalog = {
    'streams': [{
        'database_name': 'FakeDB',
        'schema': {
            'type': 'object',
            'selected': "true",
            'properties': {
                'id': {
                    "selected": "true",
                    'minimum': -2147483648,
                    'type': 'integer',
                    'maximum': 2147483647,
                    'inclusion': 'available'
                }
            },
            'table_name': 'fake name',
            'stream': 'fake stream',
            'tap_stream_id': 'FakeDB-fake name'
        }
    }]
}


class TestResolve(object):
    def test_select_desired_column(self):
        desired_cols = set(['col1', 'col2', 'col3'])
        table_schema = Schema(type='object',
                              properties={
                                  'col1': Schema(None, inclusion='available'),
                                  'col2': Schema(None,
                                                 inclusion='unsupported')})
        selected_col = tap_redshift.resolve.desired_columns(desired_cols,
                                                            table_schema)
        assert_that(selected_col, equal_to(set(['col1'])))

    def test_unknown_inclusion(self):
        selected_cols = set(['col1'])
        table_schema = Schema(type='object',
                              properties={
                                  'col1': Schema(None, inclusion='unknown'),
                                  'col2': Schema(None, inclusion='unsupported')
                              })
        assert_that(calling(
            tap_redshift.resolve.desired_columns).with_args(
                selected_cols, table_schema), raises(Exception))

    @mock.patch("psycopg2.connect")
    def test_resolve_catalog(self, db_config):
        catalog = tap_redshift.discover_catalog(mock=db_config)
        state = tap_redshift.build_state({}, catalog)
        resolved_cat = tap_redshift.resolve.resolve_catalog(catalog, state)
        assert_that(resolved_cat, instance_of(singer.catalog.Catalog))
        assert_that(resolved_cat.to_dict(), has_key(equal_to('streams')))
