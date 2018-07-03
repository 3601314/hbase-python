#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

import os
from collections import deque

from . import client
from . import stream_io
from .client import filters


class Table(object):

    def __init__(self,
                 namespace,
                 name,
                 write_batch_size,
                 read_batch_size):
        """Table object.

        Args:
            namespace (hbase.namespace.Namespace): Namespace object.
            name (str): Table name.
            write_batch_size (int): Batch size for batch_put().
            read_batch_size (int): Batch size for scan().

        """
        self._namespace = namespace
        self._name = name
        self._write_batch_size = write_batch_size
        self._read_batch_size = read_batch_size

        self._full_name = namespace.prefix + name
        self._conn = namespace.connection
        self._client = namespace.client

    @property
    def connection(self):
        return self._conn

    @property
    def name(self):
        return self._name

    @property
    def write_batch_size(self):
        return self._write_batch_size

    @write_batch_size.setter
    def write_batch_size(self, value):
        """Setter of write_batch_size.

        Args:
            value (int): New value.

        """
        if value <= 0:
            raise ValueError('write_batch_size should be positive value.')
        self._write_batch_size = value

    @property
    def read_batch_size(self):
        return self._read_batch_size

    @read_batch_size.setter
    def read_batch_size(self, value):
        """Setter of read_batch_size.

        Args:
            value (int): New value.

        """
        if value <= 0:
            raise ValueError('read_batch_size should be positive value.')
        self._read_batch_size = value

    @property
    def full_name(self):
        return self._full_name

    @property
    def client(self):
        """Client object.

        Returns:
            client.Client: Client object.

        """
        return self._client

    def get(self, key, columns=None, filter_=None):
        """Get a row with the row key.

        Args:
            key (str): Row key.
            columns (tuple[str]|list[str]): Columns to get.
            filter_ (client.filters.Filter): Filter object.

        Returns:
            client.Row: The row object.
            None: The row does not exist.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        return self._client.get(self._full_name, key, columns, filter_)

    def get_one(self, key_only=False):
        """Get the first rows sample from the table.

        Args:
            key_only (bool): Only return column keys. Contents are replaced with b''.

        Returns:
            Row: The first row in the table.
            None: The row does not exist.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        filter_ = filters.KeyOnlyFilter() if key_only else None
        return self._client.get_one(self._full_name, filter_=filter_)

    def scan(self,
             start_row=None,
             end_row=None,
             columns=None,
             filter_=None,
             batch_size=None):
        """Scan the table.

        Args:
            start_row (str): Start rwo key.
            end_row (str): End row key.
            columns (tuple[str]|list[str]): Columns.
            filter_ (hbase.filters.Filter): Filter.
            batch_size (int): Max number of rows in each request.
                None means use the table's read_batch_size.

        Returns:
            Cursor: Cursor object if success.

        """
        scanner = self._client.create_scanner(
            self._full_name,
            start_key=start_row,
            end_key=end_row,
            columns=columns,
            filter_=filter_,
            num_rows=batch_size if batch_size is not None else self._read_batch_size
        )
        return Cursor(self, scanner)

    def count(self,
              start_row=None,
              end_row=None,
              verbose=None,
              verbose_interval=1000):
        """Count the number of the rows in the table.

        Args:
            start_row (str): Start rwo key.
            end_row (str): End row key.
            verbose ((int, hbase.client.Row) -> T): Callback to notify the counting progress.
            verbose_interval (int): Interval counts between verbose calls.

        Returns:
            int: The number of rows.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        count = 0
        if verbose is None:
            for _ in self.scan(
                    start_row=start_row,
                    end_row=end_row,
                    filter_=filters.KeyOnlyFilter(),
                    batch_size=500
            ):
                count += 1
        else:
            for row in self.scan(
                    start_row=start_row,
                    end_row=end_row,
                    filter_=filters.KeyOnlyFilter(),
                    batch_size=500
            ):
                count += 1
                if count % verbose_interval == 0:
                    verbose(count, row)
        return count

    def put(self, row, callback=None):
        """Put one row into the table.

        Args:
            row (hbase.client.Row): Row object.
            callback (callable|None): Callback when the put operation complete.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        self._conn.threads.put_task(
            self._client.put,
            (self._full_name, row),
            callback
        )

    def check_and_put(self,
                      row,
                      check_column=None,
                      check_value=None):
        """Put one row to the table.
        Atomically checks if a row/family/qualifier value matches the expected value.
        If it does, it adds the put.
        If the passed value is None(or b''), the check is for the lack of column (ie: non-existance)

        Args:
            row (hbase.client.Row): Row to put.
            check_column (str): Column to check.
            check_value (bytes): Valur to check.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        return self._client.check_and_put(
            self._full_name,
            row,
            check_column,
            check_value
        )

    def delete(self, key):
        """Delete a row by key.

        Args:
            key (str): Row key.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        self._client.delete(self._full_name, key)

    def stream_writer(self,
                      filename,
                      column='cf:chunk',
                      chunk_size=8388608):
        """Create a stream writer.

        Args:
            filename (str): Filename(identifier) the writer will write data to.
            column (str): Column that the writer store the data.
            chunk_size (int): Chunk size.

        Returns:
            stream_io.StreamWriter: The stream writer if success.

        """
        return stream_io.StreamWriter(self, filename, column, chunk_size)

    def stream_reader(self, filename, column='cf:chunk'):
        """Create a stream reader.

        Args:
            filename (str): Filename(identifier) in the table.
            column (str): Column that the reader reads data from.

        Returns:
            stream_io.StreamReader: The stream reader if success.

        """
        return stream_io.StreamReader(self, filename, column)

    def write_bytes(self,
                    data,
                    filename,
                    column='cf:chunk',
                    chunk_size=8388608):
        """Write bytes as a file to the table.

        Args:
            data (bytes): Data to write.
            filename (str): Filename(identifier) the writer will write data to.
            column (str): Column that the writer store the data.
            chunk_size (int): Chunk size.

        """
        with self.stream_writer(filename, column, chunk_size) as f:
            f.write(data)

    def read_bytes(self, filename, column='cf:chunk'):
        """Read bytes from the table.

        Args:
            filename (str): Filename(identifier) the reader will read from.
            column (str): Column that the reader reads data.

        Returns:
            bytes: All bytes of the file if success.

        """
        with self.stream_reader(filename, column) as f:
            return f.read()

    def write_file(self,
                   file_path,
                   filename=None,
                   column='cf:chunk',
                   chunk_size=8388608):
        """Write file to table.

        Args:
            file_path (str): File path.
            filename (str): Filename(identifier) the reader will read from.
            column (str): Column that the reader reads data.
            chunk_size (int): Chunk size.

        Raises:
            IOError: Failed to open the file.

        """
        if filename is None:
            filename = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            with self.stream_writer(filename, column, chunk_size) as f1:
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    f1.write(data)

    def read_file(self,
                  file_path,
                  filename,
                  column='cf:chunk',
                  buffer_size=8388608):
        """Read from the table and store the data to the file.

        Args:
            file_path (str): File path.
            filename (str): Filename(identifier) the reader will read from.
            column (str): Column that the reader reads data.
            buffer_size (int): Buffer size.

        Raises:
            IOError: Failed to open the file.

        """
        with self.stream_reader(filename, column) as reader:
            with open(file_path, 'wb') as f:
                while True:
                    data = reader.read(buffer_size)
                    if not data:
                        break
                    f.write(data)


class Cursor(object):

    def __init__(self, table, scanner):
        """Cursor object.

        Args:
            table (Table): Table object.
            scanner (client.Scanner): Scanner object.

        """
        self._table = table
        self.scanner = scanner

        self._client = table.client
        self._full_name = table.full_name
        self._buffer = deque()

    def __del__(self):
        self.close()

    def close(self):
        if self.scanner is not None:
            self._client.delete_scanner(self.scanner)
            self.scanner = None

    def next(self):
        """Get next row.

        Returns:
           hbase.client.Row: Row object.
           None: If all rows have been iterated.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        while len(self._buffer) == 0:
            batch = self._client.iter_scanner(self.scanner)
            if batch is None:
                self.close()
                return None
            self._buffer.extend(batch)
        return self._buffer.popleft()

    def __iter__(self):
        return self

    def __next__(self):
        """Get next row.

        Returns:
           client.Row: Row object.

        Raises:
            StopIteration: If all rows have been iterated.

            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        while len(self._buffer) == 0:
            batch = self._client.iter_scanner(self.scanner)
            if batch is None:
                self.close()
                raise StopIteration()
            self._buffer.extend(batch)
        return self._buffer.popleft()
