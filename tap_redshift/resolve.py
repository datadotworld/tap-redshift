from itertools import dropwhile

import singer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


LOGGER = singer.get_logger()


def desired_columns(selected, table_schema):

    '''Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    '''
    all_columns = set()
    available = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    return selected.intersection(available)


def resolve_catalog(con, discovered, catalog, state):
    # Filter catalog to include only selected streams
    streams = list(filter(
                    lambda stream: stream.is_selected(), catalog.streams))

    currently_syncing = singer.get_currently_syncing(state)
    if currently_syncing:
        streams = dropwhile(
                    lambda s: s.tap_stream_id != currently_syncing, streams)

    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams:

        discovered_table = discovered.get_stream(catalog_entry.tap_stream_id)
        if not discovered_table:
            LOGGER.warning('Database {} table {} selected but does not exist'
                           .format(catalog_entry.database,
                                   catalog_entry.table))
            continue
        selected = set([k for k, v in catalog_entry.schema.properties.items()
                        if v.selected or k == catalog_entry.replication_key])

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        schema = Schema(
            type='object',
            properties={col: discovered_table.schema.properties[col]
                        for col in columns}
        )

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            stream=catalog_entry.stream,
            database=catalog_entry.database,
            table=catalog_entry.table,
            is_view=catalog_entry.is_view,
            schema=schema
        ))

    return result
