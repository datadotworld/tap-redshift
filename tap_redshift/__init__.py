import psycopg2
from itertools import groupby

from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry


__version__ = '1.0.0'

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'dbname',
    'user',
    'password'
]

STRING_TYPES = set([
    'char',
    'enum',
    'longtext',
    'mediumtext',
    'text',
    'varchar'
])

BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'int2': 2,
    'mediumint': 3,
    'int4': 4,
    'int8': 8
}

FLOAT_TYPES = set(['float', 'double'])

DATETIME_TYPES = set(['datetime', 'timestamptz', 'date', 'time'])


def discover_catalog(**kwargs):
    '''Returns a Catalog describing the structure of the database.'''

    # For testing purpose
    if kwargs:
        args = kwargs.get('mock')
        dbname = args['dbname']
    else:
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        dbname = args.config['dbname']

    column_specs = select_all("""
        SELECT c.table_name, c.ordinal_position, c.column_name, c.udt_name
        FROM INFORMATION_SCHEMA.Tables t
        JOIN INFORMATION_SCHEMA.Columns c ON c.table_name = t.table_name
        WHERE t.table_schema = 'public'
        ORDER BY c.table_name, c.ordinal_position
    """, **kwargs)

    entries = []
    table = [{'name': k, 'columns': [
                {'pos': t[1], 'name': t[2], 'type': t[3]} for t in v]}
             for k, v in groupby(column_specs, key=lambda t: t[0])]

    for items in table:
        table_name = items['name']
        cols = items['columns']
        schema = Schema(type='object',
                        properties={
                            c['name']: schema_for_column(c) for c in cols})
        tap_stream_id = '{}-{}'.format(dbname, table_name)
        entry = CatalogEntry(
                    database=dbname,
                    tap_stream_id=tap_stream_id,
                    stream=table_name,
                    schema=schema,
                    table=table_name)
    entries.append(entry)

    return Catalog(entries)


def do_discover():
    discover_catalog().dump()


def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    column_type = c['type'].lower()

    result = Schema()

    if column_type == 'bool':
        result.type = ['null', 'boolean']

    elif column_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[column_type] * 8
        result.minimum = 0 - 2 ** (bits - 1)
        result.maximum = 2 ** (bits - 1) - 1

    elif column_type in FLOAT_TYPES:
        result.type = ['null', 'number']

    elif column_type == 'decimal':
        result.type = ['null', 'number']
        result.exclusiveMaximum = True

    elif column_type in STRING_TYPES:
        result.type = ['null', 'string']

    elif column_type in DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    else:
        result = Schema(None,
                        description='Unsupported column type {}'
                        .format(column_type))

    return result


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


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()


if __name__ == '__main__':
    main()
