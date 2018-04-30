#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-13
"""

import collections

from hbase import rest
from .namespace import Namespace

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8080


class Connection(object):

    def __init__(self,
                 on_close,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT):
        """Connection.

        Args:
            on_close: Callback when connection close.
            host (str): Hostname or address.
            port (int): Port number.

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
        """Client object.

        Returns:
            rest.Client: Client object.

        """
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

        Returns:
            list[str]: List of namespace names.

        Raises:
            RESTError: REST server returns other errors.

        """
        return self._client.namespaces()

    def create_namespace(self, name, props=None):
        """Create a namespace.

        Args:
            name (str): Name of the namespace.
            props (dict[str, str]): Custom properties.

        Returns:
            True: Success.
            False: The namespace exists.

        Raises:
            RESTError: REST server returns other errors.

        """
        return self._client.create_namespace(name, props)

    def namespace(self, name, create_if_not_exists=True):
        """Get a namespace object.

        Args:
            name (str): Name of the namespace.
            create_if_not_exists (bool): Create a new namespace if the required namespace does not exist.

        Returns:
            Namespace: Namespace object.

        Raises:
            RESTError: REST server returns other errors.
            RuntimeError: Namespace does not exist or failed to create a new one.

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
        If the namespace does not exist, always create a new one.

        Args:
            name (str): Name of the namespace.

        Returns:
            Namespace: Namespace object.

        Raises:
            RESTError: REST server returns other errors.
            RuntimeError: Namespace does not exist or failed to create a new one.

        """
        return self.namespace(name)


class ConnectionPool(object):

    def __init__(self,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 max_size=10):
        """Connection pool.

        Args:
            host (str): Hostname or address.
            port (int): Port number.
            max_size (int): Max pool size.

        """
        self._host = host
        self._port = port
        self._max_size = max_size
        self._conns = collections.deque()

    def connect(self):
        """Get a database connection.

        Returns:
            Connection: A connection object.

        """
        if len(self._conns) > 0:
            return self._conns.pop(0)
        else:
            return Connection(self._on_conn_close, self._host, self._port)

    def _on_conn_close(self, conn):
        """Callback when connection close.
        Not really close(delete) a connection, just put it back to the pool.

        Args:
            conn (Connection): Connection object to close.

        """
        if len(self._conns) < self._max_size:
            self._conns.append(conn)
        else:
            del conn
