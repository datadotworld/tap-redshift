import datetime
import copy
import time
import pendulum

import psycopg2
from itertools import groupby

import singer
import singer.metrics as metrics
from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from singer import metadata

from tap_redshift import resolve


__version__ = '1.0.0'

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'dbname',
    'user',
    'password'
]

STRING_TYPES = set([
    'char',
    'character',
    'nchar',
    'bpchar',
    'text',
    'varchar',
    'character varying',
    'nvarchar'
])

BYTES_FOR_INTEGER_TYPE = {
    'int2': 2,
    'int': 4,
    'int4': 4,
    'int8': 8
}

FLOAT_TYPES = set(['float', 'float4', 'float8'])

DATETIME_TYPES = set(['timestamp', 'timestamptz', 'date'])


def discover_catalog(**kwargs):
    '''Returns a Catalog describing the structure of the database.'''

    # For testing purpose
    if kwargs:
        args = kwargs.get('mock')
        dbname = args['dbname']
    else:
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        dbname = args.config['dbname']
        table_schema = args.config['table_schema'] or 'public'

    table_spec = select_all("""
        SELECT table_type, table_name
        FROM INFORMATION_SCHEMA.Tables
        WHERE table_schema = '{}'
        """.format(table_schema), **kwargs)

    column_specs = select_all("""
        SELECT c.table_name, c.ordinal_position, c.column_name, c.udt_name,
        c.is_nullable
        FROM INFORMATION_SCHEMA.Tables t
        JOIN INFORMATION_SCHEMA.Columns c ON c.table_name = t.table_name
        WHERE t.table_schema = '{}'
        ORDER BY c.table_name, c.ordinal_position""".format(table_schema),
        **kwargs)

    entries = []
    column = [{'name': k, 'columns': [
                {'pos': t[1], 'name': t[2], 'type': t[3], 'nullable': t[4]} for t in v]}
              for k, v in groupby(column_specs, key=lambda t: t[0])]

    for items in column:
        table_name = items['name']
        cols = items['columns']
        schema = Schema(type='object',
                        properties={
                            c['name']: schema_for_column(c) for c in cols})
        metadata = create_column_metadata(cols)
        tap_stream_id = '{}-{}'.format(dbname, table_name)
        entry = CatalogEntry(
                    database=dbname,
                    tap_stream_id=tap_stream_id,
                    stream=table_name,
                    schema=schema,
                    table=table_name,
                    metadata=metadata)
        column_is_key_prop = lambda c, s: (
            s.properties[c['name']].inclusion != 'unsupported'
        )
        key_properties = [c['name'] for c in cols if column_is_key_prop(c, schema)]
        if key_properties:
            entry.key_properties = key_properties

        table_type = [t for (t) in table_spec]
        entry.is_view = table_type == 'VIEW'
        entries.append(entry)

    return Catalog(entries)


def do_discover():
    discover_catalog().dump()


def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    column_type = c['type'].lower()
    column_nullable = c['nullable'].lower()
    inclusion = 'available'
    result = Schema(inclusion=inclusion)

    if column_type == 'bool':
        result.type = 'boolean'

    elif column_type in BYTES_FOR_INTEGER_TYPE:
        result.type = 'integer'
        bits = BYTES_FOR_INTEGER_TYPE[column_type] * 8
        result.minimum = 0 - 2 ** (bits - 1)
        result.maximum = 2 ** (bits - 1) - 1

    elif column_type in FLOAT_TYPES:
        result.type = 'number'

    elif column_type == 'numeric':
        result.type = 'number'
        result.exclusiveMaximum = True

    elif column_type in STRING_TYPES:
        result.type = 'string'

    elif column_type in DATETIME_TYPES:
        result.type = 'string'
        result.format = 'date-time'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported column type {}'
                        .format(column_type))

    if column_nullable == 'yes':
        result.type = ['null', result.type]

    return result


def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c['name']),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c['name']),
                               'sql-datatype',
                               c['type'].lower())

    return metadata.to_list(mdata)


def open_connection(**kwargs):

    # For testing purpose
    if kwargs:
        args = kwargs.get('mock')
        host = args['host'],
        port = args['port'],
        dbname = args['dbname'],
        user = args['user'],
        password = args['password']
    else:
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        config = args.config
        host = config['host'],
        port = config['port'],
        dbname = config['dbname'],
        user = config['user'],
        password = config['password']

    connection = psycopg2.connect(
            host=host[0],
            port=port[0],
            dbname=dbname[0],
            user=user[0],
            password=password)
    return connection


def select_all(query, **kwargs):
    conn = open_connection(**kwargs)
    cur = conn.cursor()
    cur.execute(query)
    column_specs = cur.fetchall()
    cur.close()
    return column_specs


def get_stream_version(tap_stream_id, state):
    return singer.get_bookmark(state,
                               tap_stream_id,
                               "version") or int(time.time() * 1000)


def row_to_record(catalog_entry, version, row, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        row_to_persist += (elem,)
    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=dict(zip(columns, row_to_persist)),
        version=version,
        time_extracted=time_extracted)


def sync_table(connection, catalog_entry, state):
    columns = list(catalog_entry.schema.properties.keys())

    if not columns:
        LOGGER.warning(
            'There are no columns selected for table {}, skipping it'
            .format(catalog_entry.table))
        return

    tap_stream_id = catalog_entry.tap_stream_id
    with connection.cursor() as cursor:
        columns = [c for c in columns]
        select = 'SELECT {} FROM {}'.format(
            ','.join(columns),
            catalog_entry.table)
        params = {}

        replication_key_value = singer.get_bookmark(state,
                                                    tap_stream_id,
                                                    'replication_key_value')
        replication_key = singer.get_bookmark(state,
                                              tap_stream_id,
                                              'replication_key')

        bookmark_is_empty = state.get('bookmarks', {}).get(tap_stream_id) is None

        stream_version = get_stream_version(tap_stream_id, state)
        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'version',
                                      stream_version)
        activate_version_message = singer.ActivateVersionMessage(
            stream=catalog_entry.stream,
            version=stream_version
        )

        # If there's a replication key, we want to emit an ACTIVATE_VERSION
        # message at the beginning so the records show up right away. If
        # there's no bookmark at all for this stream, assume it's the very
        # first replication. That is, clients have never seen rows for this
        # stream before, so they can immediately acknowledge the present
        # version.
        if replication_key or bookmark_is_empty:
            yield activate_version_message

        if replication_key_value is not None:
            entry_schema = catalog_entry.schema

            if entry_schema.properties[replication_key].format == 'date-time':
                replication_key_value = pendulum.parse(replication_key_value)

            select += ' WHERE {} >= %(replication_key_value)s ORDER BY {} ' \
                'ASC'.format(replication_key, replication_key)
            params['replication_key_value'] = replication_key_value

        elif replication_key is not None:
            select += ' ORDER BY {} ASC'.format(replication_key)

        time_extracted = utils.now()
        query_string = cursor.mogrify(select, params)
        LOGGER.info('Running {}'.format(query_string))
        cursor.execute(select, params)
        row = cursor.fetchone()
        rows_saved = 0

        with metrics.record_counter(None) as counter:
            counter.tags['database'] = catalog_entry.database
            counter.tags['table'] = catalog_entry.table
            while row:
                counter.increment()
                rows_saved += 1
                record_message = row_to_record(catalog_entry,
                                               stream_version,
                                               row,
                                               columns,
                                               time_extracted)
                yield record_message

                if replication_key is not None:
                    state = singer.write_bookmark(state,
                                                  tap_stream_id,
                                                  'replication_key_value',
                                                  record_message.record[
                                                    replication_key])
                if rows_saved % 1000 == 0:
                    yield singer.StateMessage(value=copy.deepcopy(state))
                row = cursor.fetchone()

        if not replication_key:
            yield activate_version_message
            state = singer.write_bookmark(state, catalog_entry.tap_stream_id,
                                          'version', None)

        yield singer.StateMessage(value=copy.deepcopy(state))


def generate_messages(catalog, state):
    discovered = discover_catalog()
    con = open_connection()
    catalog = resolve.resolve_catalog(con, discovered, catalog, state)

    for catalog_entry in catalog.streams:
        state = singer.set_currently_syncing(state,
                                             catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        yield singer.StateMessage(value=copy.deepcopy(state))

        # Emit a SCHEMA message before we sync any records
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=catalog_entry.key_properties)

        # Emit a RECORD message for each record in the result set
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = catalog_entry.database
            timer.tags['table'] = catalog_entry.table
            for message in sync_table(con, catalog_entry, state):
                yield message

    # If we get here, we've finished processing all the streams, so clear
    # currently_syncing from the state and emit a state message.
    state = singer.set_currently_syncing(state, None)
    yield singer.StateMessage(value=copy.deepcopy(state))


def do_sync(catalog, state):
    for message in generate_messages(catalog, state):
        singer.write_message(message)


def build_state(raw_state, catalog):
    LOGGER.info('Building State from raw state {} and catalog {}'
                .format(raw_state, catalog.to_dict()))

    state = {}

    currently_syncing = singer.get_currently_syncing(raw_state)
    if currently_syncing:
        state = singer.set_currently_syncing(state, currently_syncing)

    for catalog_entry in catalog.streams:
        tap_stream_id = catalog_entry.tap_stream_id
        if catalog_entry.replication_key:
            state = singer.write_bookmark(state,
                                          tap_stream_id,
                                          'replication_key',
                                          catalog_entry.replication_key)

            # Only keep the existing replication_key_value if the
            # replication_key hasn't changed.
            raw_replication_key = singer.get_bookmark(raw_state,
                                                      tap_stream_id,
                                                      'replication_key')
            if raw_replication_key == catalog_entry.replication_key:
                rep_key_val = singer.get_bookmark(raw_state,
                                                  tap_stream_id,
                                                  'replication_key_value')
                raw_replication_key_value = rep_key_val
                state = singer.write_bookmark(state,
                                              tap_stream_id,
                                              'replication_key_value',
                                              raw_replication_key_value)

        # Persist any existing version, even if it's None
        if raw_state.get('bookmarks', {}).get(tap_stream_id):
            raw_stream_version = singer.get_bookmark(raw_state,
                                                     tap_stream_id,
                                                     'version')

            state = singer.write_bookmark(state,
                                          tap_stream_id,
                                          'version',
                                          raw_stream_version)

    return state


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()
    elif args.catalog:
        state = build_state(args.state, args.catalog)
        do_sync(args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = build_state(args.state, catalog)
        do_sync(catalog, state)
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
