#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-18
"""

import io
import struct
import threading
import time

from hbase import protobuf
from hbase import services, exceptions
from hbase.client import rbtree


class Region(object):

    def __init__(self,
                 name,
                 table,
                 start_key,
                 end_key,
                 host,
                 port):
        """Region information.

        Args:
            name (str): Region name.
            table (str): Table name.
            start_key (str): Start key.
            end_key (str): End key.
            host (str): Hostname or IP address.
            port (int): Port number.

        """
        self._name = name
        self._table = table
        self._start_key = start_key
        self._end_key = end_key
        self._host = host
        self._port = port

        self._server_info = host + ':' + str(port)
        self._start_value = table + ',' + start_key
        self._end_value = table + ',' + (end_key if len(end_key) != 0 else '\xff')

    def __str__(self):
        buffer = io.StringIO()
        buffer.write(self._start_key)
        buffer.write(' ~ ')
        buffer.write(self._end_key)
        return buffer.getvalue()

    def __repr__(self):
        buffer = io.StringIO()
        buffer.write('Region: ')
        buffer.write(self._name)
        buffer.write('\n')
        buffer.write('Table: ')
        buffer.write(self._table)
        buffer.write('\n')
        buffer.write('Range: ')
        buffer.write(self._start_key)
        buffer.write(' ~ ')
        buffer.write(self._end_key)
        buffer.write('\n')
        buffer.write('Server: ')
        buffer.write(self._host)
        buffer.write(':')
        buffer.write(str(self._port))
        return buffer.getvalue()

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table

    @property
    def start_key(self):
        return self._start_key

    @property
    def end_key(self):
        return self._end_key

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def server_info(self):
        return self._server_info

    @property
    def start_value(self):
        return self._start_value

    @property
    def end_value(self):
        return self._end_value

    def __lt__(self, other):
        # print('DEBUG: __lt__')
        if isinstance(other, str):
            return self._end_value <= other
        elif isinstance(other, Region):
            return self._end_value <= other._start_value
        else:
            raise TypeError(
                'Region or str expected, got %s.' % str(type(other))
            )

    def __gt__(self, other):
        # print('DEBUG: __gt__')
        if isinstance(other, str):
            return self._start_value > other
        elif isinstance(other, Region):
            return self._start_value >= other._end_value
        else:
            raise TypeError(
                'Region or str expected, got %s.' % str(type(other))
            )

    def __eq__(self, other):
        if isinstance(other, str):
            return self._start_value <= other < self._end_value
        elif isinstance(other, Region):
            return self._start_value == other._start_value and self._end_value == other._end_value
        else:
            raise TypeError(
                'Region or str expected, got %s.' % str(type(other))
            )


class RegionManager(object):

    def __init__(self, zkquorum, zkpath=None):
        """Region manager.

        A region manager is used to:
            1) Search for a region given a 'table name' and a 'row key'.
            2) Maintain the region cache to perform fast region retrieval.
            3) Maintain a series region service (connection) mappings.

        Args:
            zkquorum (str): Zookeeper quorum. Comma-separated list of hosts to connect to.
                e.g., '127.0.0.1:2181,127.0.0.1:2182,[::1]:2183'

        Raises:
            exceptions.TransportError: Failed to connect.
            exceptions.NoSuchZookeeperNodeError: The required node not found.
            exceptions.ZookeeperProtocolError: Invalid response.

        """
        self._lock = threading.Semaphore(1)
        self._tree = rbtree.RBTree()
        self._meta_service = services.MetaService(zkquorum, zkpath)
        self._region_services = dict()

    def close(self):
        if not self._region_services:
            return
        for service in self._region_services.values():
            service.close()
        self._region_services = dict()

    def get_region(self, table, key, use_cache=True):
        """Get region information.

        Args:
            table (str): Table name.
            key (str): Row key.
            use_cache (bool): If set to True, the manager will always try to search the cache first.
                If set to False, it never uses the cache and always query the meta region server.

        Returns:
            Region: The region matches.

        Raises:
            exceptions.TransportError: Failed to connect.
            exceptions.ProtocolError: Invalid response.
            exceptions.RequestError: Failed to get a region.

        """
        with self._lock:
            meta_key = self._make_meta_key(table, key)
            if use_cache:
                node = self._tree.find(meta_key[:-2])
                if node is None:
                    region = self._region_lookup(meta_key)
                    if region is None:
                        raise exceptions.RequestError(
                            'Failed to get region.'
                        )
                    self._add_to_cache(region)
                    return region
                else:
                    return node.value
            else:
                self._remove_from_cache(meta_key[:-2])
                region = self._region_lookup(meta_key)
                if region is None:
                    raise exceptions.RequestError(
                        'Failed to get region.'
                    )
                self._add_to_cache(region)
                return region

    @staticmethod
    def _make_meta_key(table, key):
        buffer = io.StringIO()
        buffer.write(table)
        buffer.write(',')
        buffer.write(key)
        buffer.write(',:')
        return buffer.getvalue()

    def _add_to_cache(self, region):
        """

        Args:
            region (Region):

        """
        self._remove_from_cache(region)
        self._tree.insert(region)

    def _remove_from_cache(self, region_or_meta_key):
        """

        Args:
            region_or_meta_key (Region|str):

        """
        while self._tree.delete(region_or_meta_key) is not None:
            pass

    def _region_lookup(self, meta_key):
        #Fix from https://github.com/3601314/hbase-python/issues/3
        column = protobuf.Column()
        column.family = b'info'
        req = protobuf.ScanRequest()
        req.scan.column.extend([column])
        req.scan.start_row = meta_key.encode()
        req.scan.reversed = True
        req.region.type = 1
        req.region.value = b'hbase:meta,,1'
        req.number_of_rows = 1
        try:
            resp = self._meta_service.request(req)
        except exceptions.RegionError:
            while True:
                time.sleep(3)
                try:
                    resp = self._meta_service.request(req)
                    break
                except exceptions.RegionError:
                    continue
        cells = []
        for result in resp.results:
            cells = result.cell
            break
        if len(cells) == 0:
            return None

        region_name = cells[0].row.decode()
        server_info = None
        region_info = None
        for cell in cells:
            qualifier = cell.qualifier.decode()
            if qualifier == 'server':
                server_info = cell.value.decode()
            elif qualifier == 'regioninfo':
                region_info_bytes = cell.value
                magic = struct.unpack(">4s", region_info_bytes[:4])[0]
                if magic != b'PBUF':
                    raise exceptions.ProtocolError(
                        'Meta region server returned an invalid response. b\'PBUF\' expected, got %s.' % magic
                    )
                region_info = protobuf.RegionInfo()
                region_info.ParseFromString(region_info_bytes[4:-4])

        if server_info is None:
            raise exceptions.ProtocolError(
                'Server host information not found.'
            )
        if region_info is None:
            raise exceptions.ProtocolError(
                'Region information not found.'
            )

        host, port = server_info.split(':')
        port = int(port)
        table = region_info.table_name.namespace.decode() + ':' + region_info.table_name.qualifier.decode()
        start_key = region_info.start_key.decode()
        end_key = region_info.end_key.decode()
        return Region(region_name, table, start_key, end_key, host, port)

    def get_service(self, region):
        """Get a region service given a region.

        Args:
            region (Region): Region information.

        Returns:
            services.RegionService: The region service.

        Raises:
            exceptions.TransportError: Failed to connect.

        """
        with self._lock:
            host, port = region.host, region.port
            try:
                service = self._region_services[(host, port)]
            except KeyError:
                service = services.RegionService(host, port)
                self._region_services[(host, port)] = service
            return service
