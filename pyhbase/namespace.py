#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

from .table import Table

DEFAULT_COLUMN_FAMILIES = {'cf': dict()}


class Namespace(object):

    def __init__(self, conn, name):
        """

        :param Connection conn:
        :param str name:
        """
        self._conn = conn
        self._name = name

        self._prefix = name + ':'
        self._client = conn.client
        self._tables = dict()

    @property
    def name(self):
        return self._name

    @property
    def prefix(self):
        return self._prefix

    @property
    def client(self):
        return self._client

    def tables(self):
        """List tables.

        :return list: List of table names.
        """
        return self._client.tables(self._name)

    def create_table(self, name, families=DEFAULT_COLUMN_FAMILIES):
        """Create a table.

        :param str name: The table name
        :param dict families: The name and options for each column family

        The `families` argument is a dictionary mapping column family
        names to a dictionary containing the options for this column
        family, e.g.
            families = {
                'cf1': dict(max_versions=10),
                'cf2': dict(max_versions=1, block_cache_enabled=False),
                'cf3': dict(),  # use defaults
            }
            connection.create_table('mytable', families)

        These options correspond to the ColumnDescriptor structure in
        the Thrift API, but note that the names should be provided in
        Python style, not in camel case notation, e.g. `time_to_live`,
        not `timeToLive`. The following options are supported:
        * ``max_versions`` (`int`)
        * ``compression`` (`str`)
        * ``in_memory`` (`bool`)
        * ``bloom_filter_type`` (`str`)
        * ``bloom_filter_vector_size`` (`int`)
        * ``bloom_filter_nb_hashes`` (`int`)
        * ``block_cache_enabled`` (`bool`)
        * ``time_to_live`` (`int`)
        """
        full_name = self._prefix + name
        return self._client.create_table(full_name, families)

    def table(self, name, create_if_not_exists=True):
        """Get a table object.

        :param str name: Name of the table.
        :param bool create_if_not_exists: Create a new table if the required table does not exist.
        :return Table: Table object.

        Note that if the table is automatically created, the default column family is "cf".
        """
        full_name = self._prefix + name
        if name in self._tables:
            return self._tables[name]
        if self._client.table(full_name) is None:
            if create_if_not_exists:
                if not self._client.create_table(full_name):
                    raise RuntimeError('Failed to create table %s.' % full_name)
            else:
                raise RuntimeError('Table %s does not exist.' % full_name)
        table = Table(self, name)
        self._tables[name] = table
        return table

    def __getitem__(self, name):
        """Get a table object.

        :param str name: Name of the table.
        :return Table: Table object.
        """
        return self.table(name)

    def delete_table(self, name):
        full_name = self._prefix + name
        return self._client.delete_table(full_name)
