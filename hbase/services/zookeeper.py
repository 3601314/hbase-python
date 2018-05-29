#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-13
"""

import struct
import time

from google.protobuf.message import DecodeError
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.handlers.threading import KazooTimeoutError

from hbase import exceptions
from hbase import protobuf as pb

PATH_MASTER = '/hbase/master'
PATH_META_REGION = '/hbase/meta-region-server'


def get_master(zkquorum, timeout=9, retries=3):
    """Get master server address.

    Args:
        zkquorum (str):
        timeout (int):
        retries (int):

    Returns:
        tuple: (hostname, port)

    Raises:
        exceptions.TransportError: Failed to connect.
        exceptions.NoSuchZookeeperNodeError: The required node not found.
        exceptions.ZookeeperProtocolError: Invalid response.

    """
    return _get_address(zkquorum, PATH_MASTER, timeout, retries)


def get_region(zkquorum, timeout=9, retries=3):
    """Get meta region server address.

    Args:
        zkquorum (str):
        timeout (int):
        retries (int):

    Returns:
        tuple: (hostname, port)

    Raises:
        exceptions.TransportError: Failed to connect.
        exceptions.NoSuchZookeeperNodeError: The required node not found.
        exceptions.ZookeeperProtocolError: Invalid response.

    """
    return _get_address(zkquorum, PATH_META_REGION, timeout, retries)


def _get_address(zkquorum, path, timeout=9, retries=3):
    """Get specific server address.

    Args:
        zkquorum (str):
        path (str):
        timeout (int):
        retries (int):

    Returns:
        tuple: (hostname, port)

    Raises:
        exceptions.TransportError: Failed to connect.
        exceptions.NoSuchZookeeperNodeError: The required node not found.
        exceptions.ZookeeperProtocolError: Invalid response.

    """
    zk_client = KazooClient(hosts=zkquorum)
    try:
        zk_client.start(timeout=timeout)
    except KazooTimeoutError:
        raise exceptions.TransportError(
            'Failed to connect to zookeeper at %s.' % zkquorum
        )
    response, znodestat = None, None
    for _ in range(retries + 1):
        try:
            response, znodestat = zk_client.get(path)
        except NoNodeError:
            time.sleep(3.0)
            continue
        else:
            break
    if response is None:
        raise exceptions.NoSuchZookeeperNodeError(
            'ZooKeeper does not contain a %s node.' % path
        )
    zk_client.stop()

    # the message contains at least 5 bytes with the following structure:
    # (1B)(4B)... => (b'\xff')(meta_size)...
    if len(response) < 5:
        raise exceptions.ZookeeperProtocolError(
            'ZooKeeper returned too few response. Response size: %d.' % len(response)
        )
    tag, meta_size = struct.unpack('>cI', response[:5])
    if tag != b'\xff':
        raise exceptions.ZookeeperProtocolError(
            'ZooKeeper returned an invalid response. b\'\\xff\' expected, got %s' % tag
        )
    if meta_size <= 0 or meta_size > 65000:
        raise exceptions.ZookeeperProtocolError(
            'ZooKeeper returned an invalid meta size %d.' % meta_size
        )

    # (meta_size B)(4B)... => (meta)(b'PBUF')...
    magic = struct.unpack('>4s', response[meta_size + 5:meta_size + 9])[0]
    if magic != b'PBUF':
        raise exceptions.ZookeeperProtocolError(
            'ZooKeeper returned an invalid response. b\'PBUF\' expected, got %s.' % magic
        )

    meta = pb.MetaRegionServer()
    try:
        meta.ParseFromString(response[meta_size + 9:])
    except DecodeError:
        raise exceptions.ZookeeperProtocolError(
            'Failed to parse MetaRegionServer from response.'
        )
    return meta.server.host_name, meta.server.port
