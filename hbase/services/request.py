#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-13
"""

import io
import socket
import struct
import sys
import threading

from hbase import exceptions
from hbase import protobuf as pb


def encode_varint(value):
    tmp = []
    bits = value & 0x7f
    value >>= 7
    while value:
        tmp.append(bits | 0x80)
        bits = value & 0x7f
        value >>= 7
    tmp.append(bits)
    return bytes(tmp)


def decode_varint(buffer, pos, mask=(1 << 64) - 1):
    result = 0
    shift = 0
    while 1:
        if pos > len(buffer) - 1:
            raise exceptions.ServiceProtocolError("Not enough data to decode varint")
        b = buffer[pos]
        result |= ((b & 0x7f) << shift)
        pos += 1
        if not (b & 0x80):
            result &= mask
            return result, pos
        shift += 7
        if shift >= 64:
            raise exceptions.ServiceProtocolError('Too many bytes when decoding varint.')


class Request(object):

    def __init__(self, host, port, service_name):
        """Request object.

        Args:
            host (str): Hostname or IP address.
            port (int): Port number.
            service_name (str): Service name.
                It can be one of {'MasterService', 'ClientService'}.

        Raises:
            exceptions.TransportError: Failed to connect or send messages.

        """
        self._host = host
        self._port = port
        self._service_name = service_name

        self._call_lock = threading.Semaphore(1)
        self._call_lock_ = threading.Semaphore(1)
        self._call_id = 0
        self._lock_dict = dict()  # call_id => semaphore

        self._resp_lock = threading.Semaphore(1)
        self._resp_dict = dict()  # call_id => (header, data)

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connect()

    def _next_call_id(self):
        with self._call_lock:
            call_id = self._call_id
            self._call_id = (call_id + 1) % sys.maxsize
        return call_id

    def _wait_call(self, call_id):
        with self._call_lock:
            if call_id not in self._lock_dict:
                self._lock_dict[call_id] = threading.Semaphore(0)
            lock = self._lock_dict[call_id]
        lock.acquire()
        with self._call_lock:
            del self._lock_dict[call_id]

    def _notify_call(self, call_id):
        with self._call_lock:
            if call_id not in self._lock_dict:
                self._lock_dict[call_id] = threading.Semaphore(0)
            lock = self._lock_dict[call_id]
        lock.release()

    def _put_response(self, call_id, header, data, error):
        with self._resp_lock:
            if call_id in self._resp_dict:
                raise exceptions.TransportError('FATAL ERROR: Response for call [%d] exists.' % call_id)
            self._resp_dict[call_id] = (header, data, error)

    def _get_response(self, call_id):
        with self._resp_lock:
            if call_id not in self._resp_dict:
                raise exceptions.TransportError('FATAL ERROR: Response for call [%d] does not exist.' % call_id)
            resp = self._resp_dict[call_id]
            del self._resp_dict[call_id]
            return resp

    def _connect(self):
        """Connect to the server and send "Hello" message.

        # Message consists of three components:
        # 1) b'HBas\x00\x50'.
        # 2) Big-endian uint32 indicating length of serialized ConnectionHeader.
        # 3) Serialized ConnectionHeader.

        Raises:
            exceptions.TransportError: Failed to connect or send messages.

        """
        try:
            self._sock.connect((self._host, self._port))
        except socket.error:
            raise exceptions.TransportError(
                'Failed to connect to server %s:%d.' % (self._host, self._port)
            )

        header = pb.ConnectionHeader()
        header.user_info.effective_user = 'hbase-python'
        header.service_name = self._service_name
        header_bytes = header.SerializeToString()

        message = b'HBas\x00\x50' + struct.pack('>I', len(header_bytes)) + header_bytes
        self._sock.settimeout(60)
        self._sock_send(message)

    def _sock_send(self, data):
        target_size = len(data)
        sent_size = 0
        while sent_size < target_size:
            try:
                pack_size = self._sock.send(data[sent_size:])
            except socket.error:
                raise exceptions.TransportError(
                    'Failed to send request to server %s:%d.' % (self._host, self._port)
                )
            if pack_size == 0:
                raise exceptions.TransportError(
                    'Failed to send request to server %s:%d.' % (self._host, self._port)
                )
            sent_size += pack_size

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None
            # print('DEBUG: connection to %s:%d closed.' % (self._host, self._port))

    def call(self, pb_req):
        # send request
        call_id = self._next_call_id()
        with self._call_lock:
            method_name = pb.get_request_name(pb_req)
        self._send(call_id, method_name, pb_req)

        # receive response
        header, data, error = self._receive()
        call_id_ = header.call_id
        if call_id_ != call_id:
            self._put_response(call_id_, header, data, error)
            self._notify_call(call_id_)
            self._wait_call(call_id)
            header, data, error = self._get_response(call_id)
        if error:
            if error == 'org.apache.hadoop.hbase.exceptions.RegionMovedException':
                raise exceptions.RegionMovedError(error)
            elif error == 'org.apache.hadoop.hbase.NotServingRegionException':
                raise exceptions.NotServingRegionError()
            elif error == 'org.apache.hadoop.hbase.regionserver.RegionServerStoppedException':
                raise exceptions.RegionServerStoppedError(error)
            elif error == 'org.apache.hadoop.hbase.exceptions.RegionOpeningException':
                raise exceptions.RegionOpeningError(error)
            elif error == 'org.apache.hadoop.hbase.RegionTooBusyException':
                raise exceptions.RegionTooBusyError(error)
            else:
                raise exceptions.RequestError(error)
        pb_resp_size, resp_obj_start = decode_varint(data, 0)
        pb_resp = pb.get_response_object(method_name)
        pb_resp.ParseFromString(data[resp_obj_start: resp_obj_start + pb_resp_size])
        return pb_resp

    def _send(self, call_id, method_name, pb_req):
        pb_header = pb.RequestHeader()
        pb_header.call_id = call_id
        pb_header.method_name = method_name
        pb_header.request_param = True
        header_bytes = pb_header.SerializeToString()
        header_size = len(header_bytes)

        req_bytes = pb_req.SerializeToString()
        req_size_bytes = encode_varint(len(req_bytes))

        total_size = 1 + len(header_bytes) + len(req_size_bytes) + len(req_bytes)
        # Total length doesn't include the initial 4 bytes (for the total_length uint32)
        to_send = struct.pack(">IB", total_size, header_size)
        to_send += header_bytes + req_size_bytes + req_bytes

        with self._call_lock:
            self._sock_send(to_send)

    def _receive(self):
        with self._call_lock_:
            data = self._sock_recv(4)
            total_size = struct.unpack(">I", data)[0]
            data = self._sock_recv(total_size)

        header_size, header_start = decode_varint(data, 0)
        header_end = header_start + header_size
        pb_header = pb.ResponseHeader()
        pb_header.ParseFromString(data[header_start: header_end])

        error = pb_header.exception.exception_class_name
        if error:
            return pb_header, None, error
        return pb_header, data[header_end:], None

    def _sock_recv(self, n):
        buffer = io.BytesIO()
        received = 0
        while received < n:
            try:
                data = self._sock.recv(n - received)
            except socket.error:
                raise exceptions.TransportError(
                    'Failed to receive response to server %s:%d.' % (self._host, self._port)
                )
            if not data:
                raise exceptions.TransportError(
                    'Failed to receive response to server %s:%d.' % (self._host, self._port)
                )
            received += len(data)
            buffer.write(data)
        return buffer.getvalue()
