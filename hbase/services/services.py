#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-18
"""
import time

from hbase import exceptions
from hbase.services import request
from hbase.services import zookeeper
import threading


class Service(object):

    def __init__(self, host, port):
        """Master service.

        Args:
            host (str|None): Hostname or IP address.
            port (int|None): Port number.

        """

        self._host = host
        self._port = port

        self._request = None
        self._lock = threading.Semaphore(1)

        self._rebuild_request()

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def close(self):
        if self._request:
            self._request.close()
            self._request = None

    def _rebuild_request(self):
        raise NotImplementedError()

    def request(self, pb_req):
        """Send a request to the service.

        Args:
            pb_req: Request object.

        Returns:
            Response object.

        """
        try:
            return self._request.call(pb_req)
        except exceptions.TransportError or exceptions.ProtocolError:
            for _ in range(3):
                time.sleep(3)
                with self._lock:
                    self._rebuild_request()
                return self._request.call(pb_req)


class MasterService(Service):

    def __init__(self, zkquorum):
        """Master service.

        Args:
            zkquorum (str): Zookeeper quorum. Comma-separated list of hosts to connect to.
                e.g., '127.0.0.1:2181,127.0.0.1:2182,[::1]:2183'

        Raises:
            exceptions.TransportError: Failed to connect.
            exceptions.NoSuchZookeeperNodeError: The required node not found.
            exceptions.ZookeeperProtocolError: Invalid response.

        """
        self._zkquorum = zkquorum
        super(MasterService, self).__init__(None, None)

    def _rebuild_request(self):
        self._host, self._port = zookeeper.get_master(self._zkquorum)
        self._request = request.Request(self._host, self._port, 'MasterService')


class MetaService(Service):

    def __init__(self, zkquorum):
        """Meta region service.

        Args:
            zkquorum (str): Zookeeper quorum. Comma-separated list of hosts to connect to.
                e.g., '127.0.0.1:2181,127.0.0.1:2182,[::1]:2183'

        Raises:
            exceptions.TransportError: Failed to connect.
            exceptions.NoSuchZookeeperNodeError: The required node not found.
            exceptions.ZookeeperProtocolError: Invalid response.

        """
        self._zkquorum = zkquorum
        super(MetaService, self).__init__(None, None)

    def _rebuild_request(self):
        self._host, self._port = zookeeper.get_region(self._zkquorum)
        self._request = request.Request(self._host, self._port, 'ClientService')


class RegionService(Service):

    def __init__(self, host, port):
        """Region service.

        Args:
            host (str): Hostname or IP address.
            port (int): Port number.

        Raises:
            exceptions.TransportError: Failed to connect.

        """
        super(RegionService, self).__init__(host, port)

    def _rebuild_request(self):
        self._request = request.Request(self._host, self._port, 'ClientService')
