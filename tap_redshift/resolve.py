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

from itertools import dropwhile

import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

LOGGER = singer.get_logger()


def desired_columns(selected, table_schema):
    """Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        elif inclusion == 'automatic':
            automatic.add(column)
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

    return selected.intersection(available).union(automatic)


def entry_is_selected(catalog_entry):
    mdata = metadata.new()
    if catalog_entry.metadata is not None:
        mdata = metadata.to_map(catalog_entry.metadata)
    return bool(catalog_entry.is_selected()
                or metadata.get(mdata, (), 'selected'))


def get_selected_properties(catalog_entry):
    mdata = metadata.to_map(catalog_entry.metadata)
    properties = catalog_entry.schema.properties

    return {
        k for k, v in properties.items()
        if (metadata.get(mdata, ('properties', k), 'selected')
            or (metadata.get(mdata, ('properties', k), 'selected-by-default')
                and metadata.get(mdata, ('properties', k), 'selected') is None)
            or properties[k].selected)}


def resolve_catalog(discovered, catalog, state):
    streams = list(filter(entry_is_selected, catalog.streams))

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
        selected = get_selected_properties(catalog_entry)

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
            schema=schema,
            replication_key=catalog_entry.replication_key,
            key_properties=catalog_entry.key_properties
        ))

    return result
