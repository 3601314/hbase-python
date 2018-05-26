#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

import os
from collections import deque

from hbase import rest
from hbase import stream_io
from hbase import filters


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
        self._client = namespace.client
        self._batch = deque()

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
            rest.Client: Client object.

        """
        return self._client

    def get(self, key, columns=None):
        """Get a row with the row key.

        Args:
            key (str): Row key.
            columns (tuple[str]|list[str]): Columns to get.

        Returns:
            rest.Row: The row object.
            None: The row does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        return self._client.get(self._full_name, key, columns)

    def get_one(self, key_only=False):
        """Get the first rows sample from the table.

        Args:
            key_only (bool): Only return column keys. Contents are replaced with b''.

        Returns:
            Row: The first row in the table.
            None: The table does not exist or there is no more rows.

        Raises:
            RESTError: REST server returns other errors.

        """
        return self._client.get_one(self._full_name, key_only)

    def scan(self,
             start_row=None,
             end_row=None,
             columns=None,
             batch=None,
             start_time=None,
             end_time=None,
             filter_=None,
             caching=1000,
             batch_size=None):
        """Scan the table.

        Args:
            start_row (str): Start rwo key.
            end_row (str): End row key.
            columns (tuple[str]|list[str]): Columns.
            batch (int): The maximum number of cells to return for each iteration.
                Do not use this except you have super wide rows, e.g., 250 columns.
            start_time (int): Start timestamp.
            end_time (int): End timestamp.
            filter_ (hbase.filters.Filter): Filter.
            caching (int): REST scanner caching.
            batch_size (int): Max number of rows in each REST request.
                None means use the table's read_batch_size.

        Returns:
            Cursor: Cursor object if success.

        Raises:
            RESTError: REST server returns other errors.
            RuntimeError: The table does not exist.

        """
        scanner_url = self._client.create_scanner(
            self._full_name,
            start_row=start_row,
            end_row=end_row,
            columns=columns,
            batch=batch,
            start_time=start_time,
            end_time=end_time,
            filter_=filter_,
            caching=caching
        )
        if scanner_url is None:
            raise RuntimeError('Table %s does not exist.' % self._full_name)
        return Cursor(
            self,
            scanner_url,
            batch_size if batch_size is not None else self._read_batch_size
        )

    def count(self,
              start_row=None,
              end_row=None,
              start_time=None,
              end_time=None,
              verbose=None,
              verbose_interval=1000):
        """Count the number of the rows in the table.

        Args:
            start_row (str): Start rwo key.
            end_row (str): End row key.
            start_time (int): Start timestamp.
            end_time (int): End timestamp.
            verbose ((int, hbase.rest.Row) -> T): Callback to notify the counting progress.
            verbose_interval (int): Interval counts between verbose calls.

        Returns:
            int: The number of rows.

        Raises:
            RESTError: REST server returns other errors.
            RuntimeError: The table does not exist.

        """
        count = 0
        if verbose is None:
            for _ in self.scan(
                    start_row=start_row,
                    end_row=end_row,
                    start_time=start_time,
                    end_time=end_time,
                    batch_size=500,
                    caching=1000,
                    filter_=filters.KeyOnlyFilter()
            ):
                count += 1
        else:
            for row in self.scan(
                    start_row=start_row,
                    end_row=end_row,
                    start_time=start_time,
                    end_time=end_time,
                    batch_size=500,
                    caching=1000,
                    filter_=filters.KeyOnlyFilter()
            ):
                count += 1
                if count % verbose_interval == 0:
                    verbose(count, row)
        return count

    def put(self, row):
        """Put one row into the table.

        Args:
            row (hbase.rest.Row): Row object.

        Returns:
            True: Success.

        Raises:
            RESTError: REST server returns other errors.

        """
        self.flush()
        return self._client.put(self._full_name, row)

    def put_many(self, rows):
        """Put multiple rows to table.

        Args:
            rows (list[hbase.rest.Row]):List of rows.

        Returns:
            True: Success.

        Raises:
            RESTError: REST server returns other errors.

        """
        self.flush()
        return self._client.put_many(self._full_name, rows)

    def batch_put(self, row):
        """Put row for batch.
        The actual put operation will not perform immediately, and the row will be put into a buffer.

        Args:
            row (hbase.rest.Row): Row to put.

        Returns:
            True: Success.

        Raises:
            RESTError: REST server returns other errors.

        """
        self._batch.append(row)
        if len(self._batch) >= self._write_batch_size:
            ret = self._client.put_many(self._full_name, self._batch)
            self._batch.clear()
            return ret
        return True

    def flush(self):
        if len(self._batch) != 0:
            ret = self._client.put_many(self._full_name, self._batch)
            self._batch.clear()
            return ret
        return True

    def check_and_put(self,
                      row,
                      check_column=None,
                      check_value=None):
        """Put one row to the table.
        Atomically checks if a row/family/qualifier value matches the expected value.
        If it does, it adds the put.
        If the passed value is None(or b''), the check is for the lack of column (ie: non-existance)

        Args:
            row (hbase.rest.Row): Row to put.
            check_column (str): Column to check.
            check_value (bytes): Valur to check.

        Returns:
            True: Success.
            False: Not modified.

        Raises:
            RESTError: REST server returns other errors.

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

        Returns:
            True: Success.
            False: The table does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        return self._client.delete(self._full_name, key)

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

        Raises:
            RESTError: REST server returns other errors.

        """
        return stream_io.StreamWriter(self, filename, column, chunk_size)

    def stream_reader(self, filename, column='cf:chunk'):
        """Create a stream reader.

        Args:
            filename (str): Filename(identifier) in the table.
            column (str): Column that the reader reads data from.

        Returns:
            stream_io.StreamReader: The stream reader if success.

        Raises:
            RESTError: REST server returns other errors.

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

        Raises:
            RESTError: REST server returns other errors.

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

        Raises:
            RESTError: REST server returns other errors.

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
            RESTError: REST server returns other errors.

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
            RESTError: REST server returns other errors.

        """
        with self.stream_reader(filename, column) as reader:
            with open(file_path, 'wb') as f:
                while True:
                    data = reader.read(buffer_size)
                    if not data:
                        break
                    f.write(data)


class Cursor(object):

    def __init__(self, table, scanner_url, batch_size=None):
        """Cursor object.

        Args:
            table (Table): Table object.
            scanner_url (str): Scanner URL.
            batch_size (int): Max number of rows in each REST request.
                None means return all rows.

        """
        self._table = table
        self._scanner_url = scanner_url
        self._batch_size = batch_size

        self._client = table.client
        self._full_name = table.full_name
        self._buffer = deque()

    def __del__(self):
        self.close()

    def close(self):
        if self._scanner_url is not None:
            self._client.delete_scanner(self._scanner_url)
            self._scanner_url = None

    def next(self):
        """Get next row.

        Returns:
           hbase.rest.Row: Row object.
           None: If all rows have been iterated.

        Raises:
            RESTError: REST server returns other errors.

        """
        if len(self._buffer) == 0:
            batch = self._client.iter_scanner(self._scanner_url, self._batch_size)
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
           hbase.rest.Row: Row object.

        Raises:
            RESTError: REST server returns other errors.
            StopIteration: If all rows have been iterated.

        """
        if len(self._buffer) == 0:
            batch = self._client.iter_scanner(self._scanner_url, self._batch_size)
            if batch is None or len(batch) == 0:
                self.close()
                raise StopIteration()
            self._buffer.extend(batch)
        return self._buffer.popleft()
