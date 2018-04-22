#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

from collections import deque

from pyhbase.rest import Row
from pyhbase.stream_io import StreamWriter, StreamReader


class Table(object):

    def __init__(self,
                 namespace,
                 name):
        """HBase table object.

        :param namespace:
        :param name:
        """
        self._namespace = namespace
        self._name = name

        self._full_name = namespace.prefix + name
        self._client = namespace.client

    @property
    def name(self):
        return self._name

    @property
    def full_name(self):
        return self._full_name

    @property
    def client(self):
        return self._client

    def row(self, key):
        """Get a row with the row key.

        :param str key: Row key.
        :return Row: The row object, or None if the row does not exist.
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

        :param str start_row: Start row key.
        :param str end_row: End row key.
        :param list columns: Columns.
        :param int start_time: Start time.
        :param int end_time: End time.
        :param int batch_size: Batch size.
        :return: A cursor object if success.
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
        """Put one row to table.

        :param Row row: Row to put.
        :return: True if success.
        """
        return self._client.put(self._full_name, row)

    def check_and_put(self,
                      row,
                      check_column=None,
                      check_value=None):
        """Put one row to table.

        :param Row row: Row to put.
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

    def put_many(self, rows):
        """Put multiple rows to table.

        :param list rows: List of rows.
        :return: True if success.
        """
        return self._client.put_many(self._full_name, rows)

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
        meta_row = Row(filename, {column: b''})
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
        :return StreamReader:
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
        """
        :param Table table:
        :param str scanner_url:
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
        """
        :return Row:
        """
        if len(self._buffer) == 0:
            batch = self._client.iter_scanner(self._scanner_url)
            if batch is None or len(batch) == 0:
                self.close()
                raise StopIteration()
            self._buffer.extend(batch)
        return self._buffer.popleft()
