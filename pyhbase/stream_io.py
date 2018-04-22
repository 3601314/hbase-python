#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-21
"""

import io
import json

from pyhbase.rest import Row


class StreamWriter(object):

    def __init__(self,
                 table,
                 key,
                 column,
                 chunk_size):
        """
        :param Table table:
        :param str key:
        """
        self._table = table
        self._key = key
        self._column = column
        self._chunk_size = chunk_size

        self._buffer = io.BytesIO()
        self._num_chunks = 0
        self._size = 0

    def write(self, data):
        """
        :param bytes data:
        """
        if self._buffer is None:
            raise RuntimeError('Failed to write. The writer has been closed.')
        self._buffer.write(data)
        self._size += len(data)
        self._flush_chunks()

    def _write_meta_chunk(self, meta):
        """
        :param dict meta:
        """
        data = json.dumps(meta).encode()
        self._table.put(Row(self._key, {self._column: data}))

    def _write_data_chunk(self, data):
        """
        :param bytes data:
        """
        key = '%s_%06d' % (self._key, self._num_chunks)
        self._table.put(Row(key, {self._column: data}))

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
            'size': self._size
        })
        self._buffer = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class StreamReader(object):

    def __init__(self, table, key, column):
        """
        :param Table table:
        :param str key:
        :param str column:
        """
        self._table = table
        self._key = key
        self._column = column

    def read(self, n=-1):
        pass

    def close(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
