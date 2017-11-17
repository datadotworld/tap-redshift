import os
import psycopg2
import singer
import tap_redshift
from singer.schema import Schema
from pytest_postgresql import factories

import pytest
from doublex import assert_that, called, Stub
from hamcrest import has_entries, equal_to, not_none, only_contains, none

DB_NAME = 'tap_redshift_test'


@pytest.fixture
def get_test_db_connection():
    credentials = {}
    credentials['host'] = os.environ.get('SINGER_TAP_REDSHIFT_TEST_DB_HOST')
    credentials['user'] = os.environ.get('SINGER_TAP_REDSHIFT_TEST_DB_USER')
    credentials['password'] = os.environ.get('SINGER_TAP_REDSHIFT_TEST_DB_PASSWORD')
    credentials['port'] = os.environ.get('SINGER_TAP_REDSHIFT_TEST_DB_PORT')
    con = psycopg2.connect(**credentials)

    try:
        with con.cursor() as cur:
            try:
                cur.execute('DROP DATABASE {}'.format(DB_NAME))
            except:
                pass
            cur.execute('CREATE DATABASE {}'.format(DB_NAME))
    finally:
        con.close()

    creds['dbname'] = DB_NAME

    return psycopg2.connect(**creds)

@pytest.fixture
def discover_catalog(connection):
    catalog = tap_redshift.discover_catalog(connection)
    catalog.streams = [s for s in catalog.streams if s.database == DB_NAME]
    return catalog


class TestTypeMapping(object):

    @pytest.fixture
    def connection(self, get_test_db_connection, discover_catalog):
        con = get_test_db_connection()

        with con.cursor() as cur:
            cur.execute('''
            CREATE TABLE test_type_mapping (
            c_int4 INT,
            c_int2 INT,
            c_date DATE,
            )''')

            catalog = discover_catalog(con)
            schema = catalog.streams[0].schema


    def test_smallint(self, connection):
        assert_that(schema.properties['c_int2'],
                         has_entries(Schema(['null', 'integer'],
                                minimum=-32768,
                                maximum=32767)))




