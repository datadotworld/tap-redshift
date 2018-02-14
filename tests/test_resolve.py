# tap-redshift
# Copyright 2017 data.world, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# data.world, Inc.(http://data.world/).


import tap_redshift
import mock

import pytest
from doublex import assert_that
from hamcrest import equal_to, calling, raises, has_key, instance_of

from singer.schema import Schema
from singer.catalog import Catalog


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
        resolved_cat = tap_redshift.resolve.resolve_catalog(
                                                    Catalog.to_dict(catalog),
                                                    state)
        assert_that(resolved_cat, instance_of(Catalog))
        assert_that(resolved_cat.to_dict(), has_key(equal_to('streams')))
