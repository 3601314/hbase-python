#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

from collections import deque

from pyhbase import rest
from pyhbase.stream_io import StreamWriter, StreamReader


class Table(object):

    def __init__(self,
                 namespace,
                 name,
                 batch_size=1000):
        """Table object.

        Args:
            namespace (pyhbase.namespace.Namespace): Namespace object.
            name (str): Table name.
            batch_size (int): Batch size for batch put operation.

        """
        self._namespace = namespace
        self._name = name
        self._batch_size = batch_size

        self._full_name = namespace.prefix + name
        self._client = namespace.client
        self._batch = deque()

    @property
    def name(self):
        return self._name

    @property
    def full_name(self):
        return self._full_name

    @property
    def client(self):
        """Client object.

        Returns:
            rest.Client: Client object.

        """
        return self._client

    def row(self, key):
        """Get a row with the row key.

        Args:
            key (str): Row key.

        Returns:
            rest.Row: The row object.
            None: The row does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        return self._client.row(self._full_name, key)

    def scan(self,
             start_row=None,
             end_row=None,
             columns=None,
             start_time=None,
             end_time=None,
             batch_size=16):
        """Scan the table.

        Args:
            start_row (str): Start rwo key.
            end_row (str): End row key.
            columns (list[str]): Columns.
            start_time (int): Start timestamp.
            end_time (int): End timestamp.
            batch_size (int): Batch size.

        Returns:
            Cursor: Cursor object if success.

        Raises:
            RESTError: REST server returns other errors.
            RuntimeError: The table does not exist.

        """
        scanner_url = self._client.create_scanner(
            self._full_name,
            start_row, end_row,
            columns,
            start_time, end_time,
            batch_size
        )
        if scanner_url is None:
            raise RuntimeError('Table %s does not exist.' % self._full_name)
        return Cursor(self, scanner_url)

    def put(self, row):
        """Put one row into the table.

        Args:
            row (pyhbase.rest.Row): Row object.

        Returns:
            True: Success.

        Raises:
            RESTError: REST server returns other errors.

        """
        self.flush()
        return self._client.put(self._full_name, row)

    def put_many(self, rows):
        """Put multiple rows to table.

        :param list[pyhbase.rest.Row] rows: List of rows.
        :return: True if success.
        """
        self.flush()
        return self._client.put_many(self._full_name, rows)

    def batch_put(self, row):
        """Put for batch.
        The put operation will not perform immediately, and the row will be put into a buffer.

        :param pyhbase.rest.Row row:
        :return:
        """
        self._batch.append(row)
        if len(self._batch) >= self._batch_size:
            return self._client.put_many(self._full_name, self._batch)
        return True

    def flush(self):
        if len(self._batch) != 0:
            return self._client.put_many(self._full_name, self._batch)
        return True

    def check_and_put(self,
                      row,
                      check_column=None,
                      check_value=None):
        """Put one row to table.

        :param pyhbase.rest.Row row: Row to put.
        :param str check_column: Column to check.
        :param bytes check_value: Value to check.
        :return: True if success, False if not modified.

        Atomically checks if a row/family/qualifier value matches the expected value.
        If it does, it adds the put.
        If the passed value is None(or b''), the check is for the lack of column (ie: non-existance)
        """
        return self._client.check_and_put(
            self._full_name,
            row,
            check_column,
            check_value
        )

    def delete(self, key):
        """Delete a row.

        :param str key: Row key.
        :return: True if success, False if the table does not exist.
        """
        return self._client.delete(self._full_name, key)

    def stream_writer(self,
                      filename,
                      column='cf:chunk',
                      chunk_size=8388608):
        """

        :param str filename:
        :param str column:
        :param int chunk_size:
        :return:
        """
        meta_row = rest.Row(filename, {column: b''})
        if not self.check_and_put(meta_row, check_column=column):
            raise IOError('File %s exists in table %s.' % (filename, self._full_name))
        return StreamWriter(self, filename, column, chunk_size)

    def write_bytes(self,
                    key,
                    data,
                    column='cf:chunk',
                    chunk_size=8388608):
        """

        :param str key:
        :param bytes data:
        :param str column:
        :param int chunk_size:
        """
        with self.stream_writer(key, column, chunk_size) as f:
            f.write(data)

    def stream_reader(self, key, column='cf:chunk'):
        """
        :param str key:
        :param str column:
        :return pyhbase.stream_io.StreamReader:
        """
        if self.row(key) is None:
            raise IOError('File %s not found in table %s.' % (key, self._full_name))
        return StreamReader(self, key, column)

    def read_bytes(self, key, column='cf:chunk'):
        """
        :param str key:
        :param str column:
        :return bytes:
        """
        with self.stream_reader(key, column) as f:
            return f.read()


class Cursor(object):

    def __init__(self, table, scanner_url):
        """Cursor object.

        Args:
            table (Table): Table object.
            scanner_url (str): Scanner URL.
        """
        self._table = table
        self._scanner_url = scanner_url

        self._client = table.client
        self._full_name = table.full_name
        self._buffer = deque()

    def __del__(self):
        self.close()

    def close(self):
        if self._scanner_url is not None:
            self._client.delete_scanner(self._scanner_url)
            self._scanner_url = None

    def __iter__(self):
        return self

    def __next__(self):
        """Get next row.

        Returns:
           pyhbase.rest.Row: Row object.

        Raises:
            RESTError: REST server returns other errors.
            StopIteration: If there are no more rows.

        """
        if len(self._buffer) == 0:
            batch = self._client.iter_scanner(self._scanner_url)
            if batch is None or len(batch) == 0:
                self.close()
                raise StopIteration()
            self._buffer.extend(batch)
        return self._buffer.popleft()
