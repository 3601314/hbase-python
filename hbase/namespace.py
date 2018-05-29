#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

from . import client
from .table import Table


class Namespace(object):

    def __init__(self, conn, name):
        """Namespace object.

        Args:
            conn (hbase.connection.Connection): Connection object.
            name (str): Name of the namespace.
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
        """Client object.

        Returns:
            client.Client: Client object.

        """
        return self._client

    def tables(self):
        """List tables of the namespace.

        Returns:
            list[str]: List of table names.

        """
        return self._client.tables(self._name)

    def create_table(self, name, families=None):
        """Create a table.

        Args:
            name (str): Table name.
            families (list[ColumnFamilyAttributes]|tuple[ColumnFamilyAttributes]):
                The name and options for each column family.

        """
        full_name = self._prefix + name
        self._client.create_table(full_name, families)

    def table(self,
              name,
              write_batch_size=500,
              read_batch_size=500,
              create_if_not_exists=True):
        """Get a table object.
        Note that if the table is automatically created, the default column family is "cf".

        Args:
            name (str): Table name.
            write_batch_size (int): Batch size for "Table.batch_put()".
            read_batch_size (int): Batch size for "Table.scan()".
            create_if_not_exists (bool): Create a new table if the required table does not exist.

        Returns:
            Table: Table object.

        Raises:
            RuntimeError: Table does not exist or failed to create a one.

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
        table = Table(self, name, write_batch_size, read_batch_size)
        self._tables[name] = table
        return table

    def __getitem__(self, name):
        """Get a table object.
        If the table does not exist, always create a new one.
        Note that if the table is automatically created, the default column family is "cf".

        Args:
            name (str): Table name.

        Returns:
            Table: Table object.

        Raises:
            RuntimeError: Table does not exist or failed to create a one.

        """
        return self.table(name)

    def delete_table(self, name):
        """Delete a table.

        Args:
            name (str): Table name.

        Returns:
            True: Success.
            False: The table dose not exist.

        """
        full_name = self._prefix + name
        return self._client.delete_table(full_name)
