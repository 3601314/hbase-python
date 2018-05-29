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


class MasterError(RequestError):
    pass


class ClientError(RequestError):
    pass
