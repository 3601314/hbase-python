#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-14
"""


class TransportError(IOError):
    pass


class ProtocolError(IOError):
    pass


class ZookeeperProtocolError(ProtocolError):
    pass


class ServiceProtocolError(ProtocolError):
    pass


class RequestError(IOError):
    pass


class NoSuchZookeeperNodeError(RequestError):
    pass


class RegionError(RequestError):
    pass


class RegionMovedError(RegionError):
    pass


class NotServingRegionError(RegionError):
    pass


class RegionServerStoppedError(RegionError):
    pass


class RegionOpeningError(RegionError):
    pass


class RegionTooBusyError(RegionError):
    pass


class ServerIOError(RequestError):
    """Server side IO error.

    This error can be caused by:
        java.io.IOException
    """
    pass


class NamespaceNotFoundError(RequestError):
    """The required namespace doesn't exist.

    This error can be caused by:
        org.apache.hadoop.hbase.NamespaceNotFoundException
    """
    pass


class NamespaceExistError(RequestError):
    """The namespace has already exist.

    This error can be caused by:
        org.apache.hadoop.hbase.NamespaceExistException
    """
    pass


class TableNotFoundError(RequestError):
    """The required table doesn't exist.

    This error can be caused by:
        org.apache.hadoop.hbase.TableNotFoundException
    """
    pass


class TableExistsError(RequestError):
    """The table has already exist.

    This error can be caused by:
        org.apache.hadoop.hbase.TableExistsException
    """
    pass
