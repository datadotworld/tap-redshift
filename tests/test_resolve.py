import tap_redshift

import pytest
from doublex import assert_that
from hamcrest import equal_to

from singer.schema import Schema


class TestResolve(object):
    def test_select_desired_column(self):
        desired_cols = set(['col1', 'col2', 'col3'])
        table_schema = Schema(type='object',
                              properties={
                                  'col1': Schema(None, inclusion='available'),
                                  'col2': Schema(None, inclusion='unsupported')})
        selected_col = tap_redshift.resolve.desired_columns(desired_cols, table_schema)
        assert_that(selected_col, equal_to(set(['col1'])))