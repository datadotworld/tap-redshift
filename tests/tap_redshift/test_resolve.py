# tap-redshift
# Copyright 2018 data.world, Inc.
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

from doublex import assert_that
from hamcrest import (equal_to, calling, raises, contains_inanyorder)
from singer.schema import Schema

import tap_redshift
from tap_redshift.resolve import (entry_is_selected, get_selected_properties,
                                  resolve_catalog)


class TestResolve(object):
    def test_select_desired_column(self):
        selected_cols = {'col1', 'col2', 'col3'}
        table_schema = Schema(type='object',
                              properties={
                                  'col1': Schema(None, inclusion='available'),
                                  'col2': Schema(None,
                                                 inclusion='unsupported'),
                                  'col4': Schema(None,
                                                 inclusion='automatic')})
        desired_columns = tap_redshift.resolve.desired_columns(selected_cols,
                                                               table_schema)
        assert_that(desired_columns, equal_to({'col1', 'col4'}))

    def test_unknown_inclusion(self):
        selected_cols = {'col1'}
        table_schema = Schema(type='object',
                              properties={
                                  'col1': Schema(None, inclusion='unknown'),
                                  'col2': Schema(None, inclusion='unsupported')
                              })
        assert_that(calling(
            tap_redshift.resolve.desired_columns).with_args(
            selected_cols, table_schema), raises(Exception))

    def test_entry_is_selected(self, selectable_entry_param):
        entry, expected = selectable_entry_param
        assert_that(entry_is_selected(entry), equal_to(expected))

    def test_get_selected_properties(self, selectable_properties_param):
        entry, expected = selectable_properties_param
        assert_that(get_selected_properties(entry), equal_to(expected))

    def test_resolve_catalog(
            self, expected_catalog_discovered, resolvable_catalog_param):
        catalog, streams_and_properties = resolvable_catalog_param
        state = tap_redshift.build_state({}, expected_catalog_discovered)
        resolved_catalog = resolve_catalog(
            expected_catalog_discovered, catalog, state)

        assert_that([entry.stream for entry in resolved_catalog.streams],
                    contains_inanyorder(*set(streams_and_properties.keys())))

        for entry in resolved_catalog.streams:
            assert_that(list(entry.schema.properties.keys()),
                        contains_inanyorder(
                            *streams_and_properties[entry.stream]))

            # TODO test currently_syncing scenario
