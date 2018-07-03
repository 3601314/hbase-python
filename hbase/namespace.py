#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

from . import client
from .table import Table
from .exceptions import *


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
    def connection(self):
        return self._conn

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

        Raises:
            NamespaceNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        return self._client.tables(self._name)

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
            TableNotFoundError
            NamespaceNotFoundError
            TableExistsError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        full_name = self._prefix + name
        if name in self._tables:
            return self._tables[name]

        try:
            self._client.table(full_name)
        except TableNotFoundError as e:
            if create_if_not_exists:
                self._client.create_table(full_name)
            else:
                raise e

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
            TableNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        return self.table(name)

    def create_table(self, name, families=None):
        """Create a table.

        Args:
            name (str): Table name.
            families (list[ColumnFamilyAttributes]|tuple[ColumnFamilyAttributes]):
                The name and options for each column family.

        Raises:
            NamespaceNotFoundError
            TableExistsError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        full_name = self._prefix + name
        self._client.create_table(full_name, families)

    def delete_table(self, name):
        """Delete a table.

        Args:
            name (str): Table name.

        Raises:
            TableNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        full_name = self._prefix + name
        self._client.delete_table(full_name)
