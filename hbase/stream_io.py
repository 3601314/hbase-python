#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-21
"""

import io
import json

from hbase import rest


class StreamWriter(object):

    def __init__(self,
                 table,
                 filename,
                 column,
                 chunk_size):
        """Stream writer.

        Args:
            table (pyhbase.table.Table): Table object.
            filename (str): Filename(identifier) in the table.
            column (str): Column to store the data.
            chunk_size (int): Chunk size.

        """
        meta_row = rest.Row(filename, {column: b''})
        if not table.check_and_put(meta_row, check_column=column):
            raise IOError('File %s exists in table %s.' % (filename, table.full_name))

        self._table = table
        self._filename = filename
        self._column = column
        self._chunk_size = chunk_size

        self._buffer = io.BytesIO()
        self._num_chunks = 0
        self._size = 0

    def write(self, data):
        """Write data.

        Args:
            data (bytes): Data to write.

        Raises:
            RESTError: REST server returns other errors.
            RuntimeError: If the writer has been closed.

        """
        if self._buffer is None:
            raise RuntimeError('Failed to write. The writer has been closed.')
        self._buffer.write(data)
        self._size += len(data)
        self._flush_chunks()

    def _write_meta_chunk(self, meta):
        data = json.dumps(meta).encode()
        self._table.put(rest.Row(self._filename, {self._column: data}))

    def _write_data_chunk(self, data):
        key = '%s_%06d' % (self._filename, self._num_chunks)
        self._table.put(rest.Row(key, {self._column: data}))

    def _flush_chunks(self):
        buffer_size = self._buffer.tell()
        if buffer_size < self._chunk_size:
            return
        self._buffer.seek(0)
        for _ in range(buffer_size // self._chunk_size):
            chunk_data = self._buffer.read(self._chunk_size)
            self._write_data_chunk(chunk_data)
            self._num_chunks += 1
        tmp_data = self._buffer.read()
        self._buffer = io.BytesIO()
        self._buffer.write(tmp_data)

    def flush(self):
        if self._buffer is None:
            return
        size = self._buffer.tell()
        if size == 0:
            return
        self._buffer.seek(0)
        chunk_data = self._buffer.read()
        self._write_data_chunk(chunk_data)
        self._num_chunks += 1
        self._buffer = io.BytesIO()

    def close(self):
        if self._buffer is None:
            return
        self.flush()
        self._write_meta_chunk({
            'chunk_size': self._chunk_size,
            'num_chunks': self._num_chunks,
            'size': self._size
        })
        self._buffer = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class StreamReader(object):

    def __init__(self, table, filename, column):
        """Stream reader.

        Args:
            table (pyhbase.table.Table): Table object.
            filename (str): Filename(identifier) to read from.
            column (str): Column that stores the data.

        """
        meta_row = table.row(filename)
        if meta_row is None:
            raise IOError('File %s not found in table %s.' % (filename, table.full_name))

        self._table = table
        self._filename = filename
        self._column = column
        self.meta = json.loads(meta_row[column].decode())

        self._chunk_size = self.meta['chunk_size']
        self._num_chunks = self.meta['num_chunks']

        self._buffer = io.BytesIO()
        self._cursor = table.scan(
            start_row=filename,
            end_row=filename + '_1',
            batch_size=1
        )

    def read(self, n=-1):
        """Read bytes from the stream.

        Args:
            n (int): Number of bytes. If n == -1, then read all bytes and close the stream.

        Returns:
            bytes: The bytes data.

        Raises:
            RESTError: REST server returns other errors.
            RuntimeError: If the writer has been closed.

        """
        if self._buffer is None:
            raise RuntimeError('Failed to write. The writer has been closed.')
        while True:
            if n != -1 and self._buffer.tell() >= n:
                break
            chunk = self._cursor.next()
            if chunk is None:
                break
            data = chunk[self._column]
            self._buffer.write(data)

        self._buffer.seek(0)
        data = self._buffer.read(n)
        self._buffer = io.BytesIO(self._buffer.read())
        return data

    def close(self):
        if self._buffer is None:
            return
        self._buffer = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
