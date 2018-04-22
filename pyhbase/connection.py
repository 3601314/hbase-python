#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-13
"""

import collections

from pyhbase import rest
from .namespace import Namespace

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8080


class Connection(object):

    def __init__(self,
                 on_close,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT):
        """Connection.

        :param on_close: Callback when connection close.
        :param str host: Hostname or address.
        :param int port: Port number.
        """
        self._on_close = on_close
        self._host = host or DEFAULT_HOST
        self._port = port or DEFAULT_PORT

        self._client = rest.Client(host, port)
        self._namespaces = dict()

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def client(self):
        return self._client

    def __del__(self):
        self._client.close()

    def close(self):
        self._on_close(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def namespaces(self):
        """List namespaces.

        :return list: List of namespaces' names.
        """
        return self._client.namespaces()

    def create_namespace(self, name, props=None):
        """Create a namespace.

        :param str name: Name of the namespace.
        :param dict props: Custom properties.
        :return: True if success, False if the namespace exists.
        """
        return self._client.create_namespace(name, props)

    def namespace(self, name, create_if_not_exists=True):
        """Get a namespace object.

        :param str name: Name of the namespace.
        :param bool create_if_not_exists: Create a new namespace if the required namespace does not exist.
        :return Namespace: Namespace object.
        """
        if name in self._namespaces:
            return self._namespaces[name]
        if self._client.namespace(name) is None:
            if create_if_not_exists:
                if not self._client.create_namespace(name):
                    raise RuntimeError('Failed to create namespace %s.' % name)
            else:
                raise RuntimeError('Namespace %s does not exist.' % name)
        ns = Namespace(self, name)
        self._namespaces[name] = ns
        return ns

    def __getitem__(self, name):
        """Get a namespace object.

        :param str name: Name of the namespace.
        :return Namespace: Namespace object.
        """
        return self.namespace(name)


class ConnectionPool(object):

    def __init__(self,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 max_size=10):
        """Connection pool.

        :param str host: Hostname or address.
        :param int port: Port number.
        :param int max_size: Max pool size.
        """
        self._host = host
        self._port = port
        self._max_size = max_size
        self._conns = collections.deque()

    def connect(self):
        """Get a DB connection.

        :return Connection: A connection.
        """
        if len(self._conns) > 0:
            return self._conns.pop(0)
        else:
            return Connection(self._on_conn_close, self._host, self._port)

    def _on_conn_close(self, conn):
        """Callback when connection close.

        :param Connection conn: Connection to close.
        """
        if len(self._conns) < self._max_size:
            self._conns.append(conn)
        else:
            del conn
