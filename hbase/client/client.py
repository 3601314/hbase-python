#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-13
"""

import collections
import time
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from . import filters
from . import region as _region
from .. import protobuf
from .. import services
from ..exceptions import *
from ..conf import thread_pool_size, fail_task_retry

DEFAULT_FAMILY = 'cf'


class Row(dict):

    def __init__(self, key=None, cells=None):
        """Row object.

        Args:
            key (str): Row key.
            cells (dict[str, bytes]): Cells, e.g., {'family:qualifier': b'data'}

        """
        super(Row, self).__init__(cells)
        self.key = key

    def __str__(self):
        return '%s\t%s' % (self.key, super(Row, self).__repr__())

    def __repr__(self):
        return '%s\t%s' % (self.key, super(Row, self).__repr__())


class ColumnFamilyAttributes(dict):

    def __init__(self,
                 name,
                 versions=b'1',
                 min_versions=b'0',
                 compression=b'NONE',
                 keep_deleted_cells=b'FALSE',
                 blockcache=b'true',
                 blocksize=b'65536',
                 in_memory=b'false'):
        super(ColumnFamilyAttributes, self).__init__()

        self._name = name

        self.versions = versions
        self.min_versions = min_versions
        self.compression = compression
        self.keep_deleted_cells = keep_deleted_cells
        self.blockcache = blockcache
        self.blocksize = blocksize
        self.in_memory = in_memory

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def versions(self):
        return self[b'VERSIONS']

    @versions.setter
    def versions(self, value):
        self[b'VERSIONS'] = value

    @property
    def min_versions(self):
        return self[b'MIN_VERSIONS']

    @min_versions.setter
    def min_versions(self, value):
        self[b'MIN_VERSIONS'] = value

    @property
    def compression(self):
        return self[b'COMPRESSION']

    @compression.setter
    def compression(self, value):
        self[b'COMPRESSION'] = value

    @property
    def keep_deleted_cells(self):
        return self[b'KEEP_DELETED_CELLS']

    @keep_deleted_cells.setter
    def keep_deleted_cells(self, value):
        self[b'KEEP_DELETED_CELLS'] = value

    @property
    def blockcache(self):
        return self[b'BLOCKCACHE']

    @blockcache.setter
    def blockcache(self, value):
        self[b'BLOCKCACHE'] = value

    @property
    def blocksize(self):
        return self[b'BLOCKSIZE']

    @blocksize.setter
    def blocksize(self, value):
        self[b'BLOCKSIZE'] = value

    @property
    def in_memory(self):
        return self[b'IN_MEMORY']

    @in_memory.setter
    def in_memory(self, value):
        self[b'IN_MEMORY'] = value


class Client(object):

    def __init__(self, zkquorum, zk_master_path=None, zk_region_path=None):
        """HBase client.

        Args:
            zkquorum (str): Zookeeper quorum. Comma-separated list of hosts to connect to.
                e.g., '127.0.0.1:2181,127.0.0.1:2182,[::1]:2183'

        Raises:
            TransportError: Failed to connect.
            NoSuchZookeeperNodeError: The required node not found.
            ZookeeperProtocolError: Invalid response.

        """
        self._zkquorum = zkquorum

        self._master_service = services.MasterService(zkquorum, zk_master_path)
        self._region_manager = _region.RegionManager(zkquorum, zk_region_path)
        self.fail_retrys = fail_task_retry
        self.pool = ThreadPoolExecutor(max_workers=thread_pool_size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if hasattr(self, '_master_service') and self._master_service:
            self._master_service.close()
            self._master_service = None
        if hasattr(self, '_region_manager') and self._region_manager:
            self._region_manager.close()
            self._region_manager = None

        self.pool.shutdown()

    def namespaces(self):
        """List all namespaces.

        Returns:
            list[str]: List of namespace names.

        Raises:
            TransportError: Connection failed.
            ZookeeperProtocolError: Invalid zookeeper protocol.
            ServiceProtocolError: Invalid service protocol.
            NoSuchZookeeperNodeError: Failed to find the zookeeper node.
            MasterError: Master service request error.

        """
        #
        # message ListNamespaceDescriptorsRequest {
        # }
        pb_req = protobuf.ListNamespaceDescriptorsRequest()

        #
        # message ListNamespaceDescriptorsResponse {
        #   repeated NamespaceDescriptor namespaceDescriptor = 1;
        # }
        pb_resp = self._master_service.request(pb_req)
        return [
            pb_desc.name.decode()
            for pb_desc in pb_resp.namespaceDescriptor
        ]

    def namespace(self, namespace):
        """Get descriptions of the namespace.

        Args:
            namespace (str): Name of the namespace.

        Returns:
            dict[str, str]: Descriptions in dict, e.g., {'property': 'value'}.

        Raises:
            NamespaceNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message GetNamespaceDescriptorRequest {
        #   required string namespaceName = 1;
        # }
        pb_req = protobuf.GetNamespaceDescriptorRequest()
        pb_req.namespaceName = namespace

        #
        # message GetNamespaceDescriptorResponse {
        #   required NamespaceDescriptor namespaceDescriptor = 1;
        # }
        try:
            pb_resp = self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.NamespaceNotFoundException':
                raise NamespaceNotFoundError()
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad namespace name.')
            else:
                raise e
        return {
            pb_conf.name: pb_conf.value
            for pb_conf in pb_resp.namespaceDescriptor.configuration
        }

    def create_namespace(self, namespace, confs=None):
        """Create a namespace.

        Args:
            namespace (str): Namespace name.
            confs (dict[str, str]): Custom properties.

        Raises:
            NamespaceExistError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message CreateNamespaceRequest {
        #   required NamespaceDescriptor namespaceDescriptor = 1;
        # }
        # message NamespaceDescriptor {
        #   required bytes name = 1;
        #   repeated NameStringPair configuration = 2;
        # }
        # message NameStringPair {
        #   required string name = 1;
        #   required string value = 2;
        # }
        pb_req = protobuf.CreateNamespaceRequest()
        pb_desc = pb_req.namespaceDescriptor
        pb_desc.name = namespace.encode()
        if confs is not None:
            if not isinstance(confs, dict):
                raise ValueError('"confs" should be a dict.')
            for name, value in confs.items():
                pb_conf = pb_desc.configuration.add()
                pb_conf.name = name
                pb_conf.value = value

        #
        # message CreateNamespaceResponse {
        # }
        try:
            self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.NamespaceExistException':
                raise NamespaceExistError()
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad namespace name.')
            else:
                raise e

    def delete_namespace(self, namespace):
        """Delete a namespace.

        Args:
            namespace (str): Namespace name.

        Raises:
            NamespaceNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message DeleteNamespaceRequest {
        #   required string namespaceName = 1;
        # }
        pb_req = protobuf.DeleteNamespaceRequest()
        pb_req.namespaceName = namespace.encode()

        #
        # message DeleteNamespaceResponse {
        # }
        try:
            self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.NamespaceNotFoundException':
                raise NamespaceNotFoundError()
            elif err == 'org.apache.hadoop.hbase.constraint.ConstraintException':
                raise RequestError('Failed to delete namespace due to the constraint exception.')
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad namespace name.')
            else:
                raise e

    def tables(self, namespace):
        """List all table of the given namespace.

        Args:
            namespace (str): Name of the namespace.

        Returns:
            list[str]: List of table names.

        Raises:
            NamespaceNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message ListTableNamesByNamespaceRequest {
        #   required string namespaceName = 1;
        # }
        pb_req = protobuf.ListTableNamesByNamespaceRequest()
        pb_req.namespaceName = namespace

        #
        # message ListTableNamesByNamespaceResponse {
        #   repeated TableName tableName = 1;
        # }
        # message TableName {
        #   required bytes namespace = 1;
        #   required bytes qualifier = 2;
        # }
        try:
            pb_resp = self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.NamespaceNotFoundException':
                raise NamespaceNotFoundError()
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad namespace name.')
            else:
                raise e
        tables = [
            pb_table_name.qualifier.decode()
            for pb_table_name in pb_resp.tableName
        ]
        return tables

    def table(self, table):
        """Get table schema.

        Args:
            table (str): Table name.

        Returns:
            dict[str, T]: Description of the table.

        Raises:
            TableNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message GetTableDescriptorsRequest {
        #   repeated TableName table_names = 1;
        #   optional string regex = 2;
        #   optional bool include_sys_tables = 3 [default=false];
        #   optional string namespace = 4;
        # }
        # message TableName {
        #   required bytes namespace = 1;
        #   required bytes qualifier = 2;
        # }
        pb_req = protobuf.GetTableDescriptorsRequest()

        if not isinstance(table, str):
            raise TypeError('Invalid table name. str expected, got %s.' % str(type(table)))
        namespace, qualifier = self._split_name(table)
        pb_table_name = pb_req.table_names.add()
        pb_table_name.namespace = namespace.encode()
        pb_table_name.qualifier = qualifier.encode()

        #
        # message GetTableDescriptorsResponse {
        #   repeated TableSchema table_schema = 1;
        # }
        # message TableSchema {
        #   optional TableName table_name = 1;
        #   repeated BytesBytesPair attributes = 2;
        #   repeated ColumnFamilySchema column_families = 3;
        #   repeated NameStringPair configuration = 4;
        # }
        # message ColumnFamilySchema {
        #   required bytes name = 1;
        #   repeated BytesBytesPair attributes = 2;
        #   repeated NameStringPair configuration = 3;
        # }
        try:
            pb_resp = self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'java.io.IOException':
                raise ServerIOError('Bad table name.')
            else:
                raise e
        if len(pb_resp.table_schema) == 0:
            raise TableNotFoundError()
        pb_schema = pb_resp.table_schema[0]
        return {
            pb_column_family.name.decode(): (
                {pb_conf.name: pb_conf.value for pb_conf in pb_column_family.configuration},
                {pb_attr.first: pb_attr.second for pb_attr in pb_column_family.attributes}
            )
            for pb_column_family in pb_schema.column_families
        }

    def create_table(self, table, families=None):
        """Create a table.

        Args:
            table (str): Table name.
            families (list[ColumnFamilyAttributes]|tuple[ColumnFamilyAttributes]):
                Column families.

        Raises:
            NamespaceNotFoundError
            TableExistsError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message CreateTableRequest {
        #   required TableSchema table_schema = 1;
        #   repeated bytes split_keys = 2;
        #   optional uint64 nonce_group = 3 [default = 0];
        #   optional uint64 nonce = 4 [default = 0];
        # }
        # message TableSchema {
        #   optional TableName table_name = 1;
        #   repeated BytesBytesPair attributes = 2;
        #   repeated ColumnFamilySchema column_families = 3;
        #   repeated NameStringPair configuration = 4;
        # }
        pb_req = protobuf.CreateTableRequest()
        pb_schema = pb_req.table_schema

        namespace, qualifier = self._split_name(table)
        pb_table_name = pb_schema.table_name
        pb_table_name.namespace = namespace.encode()
        pb_table_name.qualifier = qualifier.encode()

        if families is None:
            families = (ColumnFamilyAttributes(DEFAULT_FAMILY),)
        for attrs in families:
            pb_column_family = pb_schema.column_families.add()
            pb_column_family.name = attrs.name.encode()
            for attr_name, attr_value in attrs.items():
                pb_attribute = pb_column_family.attributes.add()
                pb_attribute.first = attr_name
                pb_attribute.second = attr_value

        #
        # message CreateTableResponse {
        #   optional uint64 proc_id = 1;
        # }
        try:
            pb_resp = self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.NamespaceNotFoundException':
                raise NamespaceNotFoundError()
            elif err == 'org.apache.hadoop.hbase.TableExistsException':
                raise TableExistsError()
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad table name.')
            else:
                raise e
        self._wait_for_proc(pb_resp.proc_id, 1)

    def enable_table(self, table):
        """Enable a table.

        Args:
            table (str): Table name.

        Raises:
            TableNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message EnableTableRequest {
        #   required TableName table_name = 1;
        #   optional uint64 nonce_group = 2 [default = 0];
        #   optional uint64 nonce = 3 [default = 0];
        # }
        pb_req = protobuf.EnableTableRequest()

        namespace, qualifier = self._split_name(table)
        pb_table_name = pb_req.table_name
        pb_table_name.namespace = namespace.encode()
        pb_table_name.qualifier = qualifier.encode()

        #
        # message EnableTableResponse {
        #   optional uint64 proc_id = 1;
        # }
        try:
            pb_resp = self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.TableNotFoundException':
                raise TableNotFoundError()
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad table name.')
            else:
                raise e
        self._wait_for_proc(pb_resp.proc_id, 1)

    def disable_table(self, table):
        """Disable a table.

        Args:
            table (str): Table name.

        Raises:
            TableNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message DisableTableRequest {
        #   required TableName table_name = 1;
        #   optional uint64 nonce_group = 2 [default = 0];
        #   optional uint64 nonce = 3 [default = 0];
        # }
        pb_req = protobuf.DisableTableRequest()

        namespace, qualifier = self._split_name(table)
        pb_table_name = pb_req.table_name
        pb_table_name.namespace = namespace.encode()
        pb_table_name.qualifier = qualifier.encode()

        #
        # message DisableTableResponse {
        #   optional uint64 proc_id = 1;
        # }
        try:
            pb_resp = self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.TableNotFoundException':
                raise TableNotFoundError()
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad table name.')
            else:
                raise e
        self._wait_for_proc(pb_resp.proc_id, 1)

    def delete_table(self, table, need_disable=True):
        """Delete a table.

        Args:
            table (str): Table name.
            need_disable (bool): Whether it need to disable the table
                before perform the delete operation.

        Raises:
            TableNotFoundError
            ServerIOError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        if need_disable:
            self.disable_table(table)
        #
        # message DeleteTableRequest {
        #   required TableName table_name = 1;
        #   optional uint64 nonce_group = 2 [default = 0];
        #   optional uint64 nonce = 3 [default = 0];
        # }
        pb_req = protobuf.DeleteTableRequest()

        namespace, qualifier = self._split_name(table)
        pb_table_name = pb_req.table_name
        pb_table_name.namespace = namespace.encode()
        pb_table_name.qualifier = qualifier.encode()

        #
        # message DeleteTableResponse {
        #   optional uint64 proc_id = 1;
        # }
        try:
            pb_resp = self._master_service.request(pb_req)
        except RequestError as e:
            err = str(e)
            if err == 'org.apache.hadoop.hbase.TableNotFoundException':
                raise TableNotFoundError()
            elif err == 'java.io.IOException':
                raise ServerIOError('Bad table name.')
            else:
                raise e
        self._wait_for_proc(pb_resp.proc_id, 1)

    def _wait_for_proc(self, proc_id, sleep):
        """Wait for the master procedure to complete.

        This method is mainly used to wait for operations such as "create table",
        "delete table", "enable table" and "disable table".

        Args:
            proc_id (int): Procedure ID.
            sleep (float): Seconds that sleep between check loops.

        Returns:
            The response object.

        Raises:
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        while True:
            time.sleep(sleep)
            #
            # message GetProcedureResultRequest {
            #   required uint64 proc_id = 1;
            # }
            pb_req = protobuf.GetProcedureResultRequest()
            pb_req.proc_id = proc_id

            #
            # message GetProcedureResultResponse {
            #   enum State {
            #     NOT_FOUND = 0;
            #     RUNNING = 1;
            #     FINISHED = 2;
            #   }
            #
            #   required State state = 1;
            #   optional uint64 start_time = 2;
            #   optional uint64 last_update = 3;
            #   optional bytes result = 4;
            #   optional ForeignExceptionMessage exception = 5;
            # }
            pb_resp = self._master_service.request(pb_req)
            state = pb_resp.state
            if state == 0:
                raise RequestError('Procedure %d not found.' % proc_id)
            elif state == 1:
                sleep = min(sleep * 2, 10)
                continue
            else:
                break

    def mget(self, table, keys, columns=None, filter_=None):
        """Query to get a row object with multiple row keys.

        Args:
            table (str): Table name.
            key (tuple[str]|list[str]): Multi row keys.
            columns (tuple[str]|list[str]): Columns to fetch.
            filter_ (filters.Filter): Filter object.

        Returns:
            Row: The list row object.
            None: The row does not exist.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        pb_reqs = {}
        for key in keys:
            region = self._region_manager.get_region(table, key)
            region_service = self._region_manager.get_service(region)
            print(f'{region_service.host}:{region_service.port}')
            pb_req = protobuf.GetRequest()

            pb_req.region.type = 1
            pb_req.region.value = region.name.encode()

            pb_get = pb_req.get
            pb_get.row = key.encode()

            if columns is not None:
                qualifier_dict = collections.defaultdict(list)
                for column in columns:
                    try:
                        family, qualifier = column.split(':')
                    except ValueError or AttributeError:
                        raise RequestError(
                            'Invalid column name. {family}:{qualifier} expected, got %s.' % column
                        )
                    qualifier_dict[family.encode()].append(qualifier.encode())
                for family, qualifiers in qualifier_dict.items():
                    pb_column = pb_get.column.add()
                    pb_column.family = family
                    pb_column.qualifier.extend(qualifiers)

            if filter_ is not None:
                pb_filter = pb_get.filter
                pb_filter.name = filter_.name
                pb_filter.serialized_filter = filter_.serialize()
            pb_reqs.setdefault(key, (region_service, pb_req))

        def _asyncrun(pb_reqs):
            results = {}
            fails = []

            def __request(region_service, pb_req):
                try:
                    pb_resp = region_service.request(pb_req)
                except RegionError:
                    raise Exception(f"request {region_service} failure.")

                return self._cells_to_row(pb_resp.result.cell)

            tasks = {self.pool.submit(__request, pb_reqs.get(key)[0], pb_reqs.get(key)[1]): key for key in pb_reqs}

            for f in futures.as_completed(tasks):
                key = tasks[f]
                try:
                    results.setdefault(key, f.result())
                except Exception as err:
                    # print(str(err))
                    fails.append(key)
            return results, fails

        results, fails = _asyncrun(pb_reqs)

        count = 0
        # Retry fails
        while count < self.fail_retrys and len(fails) != 0:
            new_pb_reqs = {}
            for k in pb_reqs:
                if k in fails:
                    new_pb_reqs[k] = pb_reqs[k]
            r, fails = _asyncrun(new_pb_reqs)
            # Merge re-fetch data from rpc request.
            results.update(r)
            time.sleep(3)

        # message GetResponse {
        #   optional Result result = 1;
        # }
        # region_service, pb_req, key, table, region
        # try:
        #     pb_resp = region_service.request(pb_req)
        # except RegionError:
        #     while True:
        #         time.sleep(3)
        #         # print('DEBUG: put() RegionError')
        #         # print(repr(region))
        #         # refresh the region information and retry the operation
        #         region = self._region_manager.get_region(table, key, use_cache=False)
        #         region_service = self._region_manager.get_service(region)
        #         pb_req.region.value = region.name.encode()
        #         # if the new region still doesn't work, it is a fatal error
        #         # print(repr(region))
        #         try:
        #             pb_resp = region_service.request(pb_req)
        #             break
        #         except RegionError:
        #             continue
        return results

    def get(self,
            table,
            key,
            columns=None,
            filter_=None):
        """Query to get a row object with a row key.

        Args:
            table (str): Table name.
            key (str): Row key.
            columns (tuple[str]|list[str]): Columns to fetch.
            filter_ (filters.Filter): Filter object.

        Returns:
            Row: The row object.
            None: The row does not exist.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        region = self._region_manager.get_region(table, key)
        region_service = self._region_manager.get_service(region)

        #
        # message GetRequest {
        #   required RegionSpecifier region = 1;
        #   required Get get = 2;
        # }
        # message Get {
        #   required bytes row = 1;
        #   repeated Column column = 2;
        #   repeated NameBytesPair attribute = 3;
        #   optional Filter filter = 4;
        #   optional TimeRange time_range = 5;
        #   optional uint32 max_versions = 6 [default = 1];
        #   optional bool cache_blocks = 7 [default = true];
        #   optional uint32 store_limit = 8;
        #   optional uint32 store_offset = 9;
        #
        #   // The result isn't asked for, just check for
        #   // the existence.
        #   optional bool existence_only = 10 [default = false];
        #
        #   // If the row to get doesn't exist, return the
        #   // closest row before.
        #   optional bool closest_row_before = 11 [default = false];
        #
        #   optional Consistency consistency = 12 [default = STRONG];
        #   repeated ColumnFamilyTimeRange cf_time_range = 13;
        # }
        # message Column {
        #   required bytes family = 1;
        #   repeated bytes qualifier = 2;
        # }
        pb_req = protobuf.GetRequest()

        pb_req.region.type = 1
        pb_req.region.value = region.name.encode()

        pb_get = pb_req.get
        pb_get.row = key.encode()

        if columns is not None:
            qualifier_dict = collections.defaultdict(list)
            for column in columns:
                try:
                    family, qualifier = column.split(':')
                except ValueError or AttributeError:
                    raise RequestError(
                        'Invalid column name. {family}:{qualifier} expected, got %s.' % column
                    )
                qualifier_dict[family.encode()].append(qualifier.encode())
            for family, qualifiers in qualifier_dict.items():
                pb_column = pb_get.column.add()
                pb_column.family = family
                pb_column.qualifier.extend(qualifiers)

        if filter_ is not None:
            pb_filter = pb_get.filter
            pb_filter.name = filter_.name
            pb_filter.serialized_filter = filter_.serialize()

        #
        # message GetResponse {
        #   optional Result result = 1;
        # }
        try:
            pb_resp = region_service.request(pb_req)
        except RegionError:
            while True:
                time.sleep(3)
                # print('DEBUG: put() RegionError')
                # print(repr(region))
                # refresh the region information and retry the operation
                region = self._region_manager.get_region(table, key, use_cache=False)
                region_service = self._region_manager.get_service(region)
                pb_req.region.value = region.name.encode()
                # if the new region still doesn't work, it is a fatal error
                # print(repr(region))
                try:
                    pb_resp = region_service.request(pb_req)
                    break
                except RegionError:
                    continue
        return self._cells_to_row(pb_resp.result.cell)

    def get_one(self,
                table,
                key=None,
                columns=None,
                filter_=None):
        """Query to get a row object with a row key.

        Args:
            table (str): Table name.
            key (str): Row key.
            columns (tuple[str]|list[str]): Columns to fetch.
            filter_ (filters.Filter): Filter object.

        Returns:
            Row: The row object.
            None: The row does not exist.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        if key is None:
            # TODO: Here we should use a randomly generated key.
            key = ''
        region = self._region_manager.get_region(table, key)
        region_service = self._region_manager.get_service(region)
        pb_resp = self._create_region_scanner(
            region,
            region_service,
            table,
            start_key=key,
            end_key=None,
            columns=columns,
            filter_=filter_,
            num_rows=1,
            reversed=True
        )
        scanner_id = pb_resp.scanner_id
        self._close_region_scanner(region, region_service, scanner_id)
        if len(pb_resp.results) < 1:
            return None
        else:
            return self._cells_to_row(pb_resp.results[0].cell)

    def put(self, table, row):
        """Insert a row into a table.

        Args:
            table (str): Table name.
            row (Row): Row object to insert.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        key = row.key
        region = self._region_manager.get_region(table, key)
        region_service = self._region_manager.get_service(region)
        # print('DEBUG: Get region\n%s' % repr(region))

        #
        # message MutateRequest {
        #   required RegionSpecifier region = 1;
        #   required MutationProto mutation = 2;
        #   optional Condition condition = 3;
        #   optional uint64 nonce_group = 4;
        # }
        # message MutationProto {
        #   optional bytes row = 1;
        #   optional MutationType mutate_type = 2;
        #   repeated ColumnValue column_value = 3;
        #   optional uint64 timestamp = 4;
        #   repeated NameBytesPair attribute = 5;
        #   optional Durability durability = 6 [default = USE_DEFAULT];
        #
        #   // For some mutations, a result may be returned, in which case,
        #   // time range can be specified for potential performance gain
        #   optional TimeRange time_range = 7;
        #   // The below count is set when the associated cells are NOT
        #   // part of this protobuf message; they are passed alongside
        #   // and then this Message is a placeholder with metadata.  The
        #   // count is needed to know how many to peel off the block of Cells as
        #   // ours.  NOTE: This is different from the pb managed cell_count of the
        #   // 'cell' field above which is non-null when the cells are pb'd.
        #   optional int32 associated_cell_count = 8;
        #
        #   optional uint64 nonce = 9;
        #
        #   enum Durability {
        #     USE_DEFAULT  = 0;
        #     SKIP_WAL     = 1;
        #     ASYNC_WAL    = 2;
        #     SYNC_WAL     = 3;
        #     FSYNC_WAL    = 4;
        #   }
        #
        #   enum MutationType {
        #     APPEND = 0;
        #     INCREMENT = 1;
        #     PUT = 2;
        #     DELETE = 3;
        #   }
        #
        #   enum DeleteType {
        #     DELETE_ONE_VERSION = 0;
        #     DELETE_MULTIPLE_VERSIONS = 1;
        #     DELETE_FAMILY = 2;
        #     DELETE_FAMILY_VERSION = 3;
        #   }
        #
        #   message ColumnValue {
        #     required bytes family = 1;
        #     repeated QualifierValue qualifier_value = 2;
        #
        #     message QualifierValue {
        #       optional bytes qualifier = 1;
        #       optional bytes value = 2;
        #       optional uint64 timestamp = 3;
        #       optional DeleteType delete_type = 4;
        #       optional bytes tags = 5;
        #     }
        #   }
        # }
        # message Condition {
        #   required bytes row = 1;
        #   required bytes family = 2;
        #   required bytes qualifier = 3;
        #   required CompareType compare_type = 4;
        #   required Comparator comparator = 5;
        # }
        pb_req = protobuf.MutateRequest()

        pb_req.region.type = 1
        pb_req.region.value = region.name.encode()

        pb_mutation = pb_req.mutation
        pb_mutation.row = key.encode()
        pb_mutation.mutate_type = 2
        pb_mutation.column_value.extend(self._row_to_column_values(row))

        #
        # message MutateResponse {
        #   optional Result result = 1;
        #
        #   // used for mutate to indicate processed only
        #   optional bool processed = 2;
        # }
        try:
            pb_resp = region_service.request(pb_req)
        except RegionError:
            while True:
                time.sleep(3)
                # print('DEBUG: put() RegionError')
                # print(repr(region))
                # refresh the region information and retry the operation
                region = self._region_manager.get_region(table, key, use_cache=False)
                region_service = self._region_manager.get_service(region)
                pb_req.region.value = region.name.encode()
                # if the new region still doesn't work, it is a fatal error
                # print(repr(region))
                try:
                    pb_resp = region_service.request(pb_req)
                    break
                except RegionError:
                    continue
        return pb_resp.processed

    def check_and_put(self,
                      table,
                      row,
                      check_column,
                      check_value=None,
                      comparator_type=filters.EQUAL):
        """Check and put.

        The put operation will be performed only if the condition is meet.

        Args:
            table (str): Table name.
            row (Row): Row object.
            check_column (str): The name of the column to check.
            check_value (bytes): The value of the condition.
            comparator_type (int): The comparator type. Should be one of:
                LESS = 0,
                LESS_OR_EQUAL = 1,
                EQUAL = 2,
                NOT_EQUAL = 3,
                GREATER_OR_EQUAL = 4,
                GREATER = 5,
                NO_OP = 6,
                which are defined in filters.py.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        key = row.key
        region = self._region_manager.get_region(table, key)
        region_service = self._region_manager.get_service(region)

        pb_req = protobuf.MutateRequest()

        pb_req.region.type = 1
        pb_req.region.value = region.name.encode()

        pb_mutation = pb_req.mutation
        pb_mutation.row = key.encode()
        pb_mutation.mutate_type = 2
        pb_mutation.column_value.extend(self._row_to_column_values(row))

        if check_column is not None:
            pb_condition = pb_req.condition
            pb_condition.row = key.encode()
            family, qualifier = self._split_name(check_column)
            pb_condition.family = family.encode()
            pb_condition.qualifier = qualifier.encode()
            pb_condition.compare_type = comparator_type
            if check_value is None:
                check_value = b''
            comp = filters.BinaryComparator(check_value)
            pb_comp = pb_condition.comparator
            pb_comp.name = comp.name
            pb_comp.serialized_comparator = comp.serialize()

        try:
            pb_resp = region_service.request(pb_req)
        except RegionError:
            while True:
                time.sleep(3)
                # print('DEBUG: put() RegionError')
                # print(repr(region))
                # refresh the region information and retry the operation
                region = self._region_manager.get_region(table, key, use_cache=False)
                region_service = self._region_manager.get_service(region)
                pb_req.region.value = region.name.encode()
                # if the new region still doesn't work, it is a fatal error
                # print(repr(region))
                try:
                    pb_resp = region_service.request(pb_req)
                    break
                except RegionError:
                    continue
        return pb_resp.processed

    @staticmethod
    def _row_to_column_values(row):
        """Convert a row to protobuf.MutationProto.ColumnValue objects.

        Args:
            row (Row): The row object.

        Returns:
            list[protobuf.MutationProto.ColumnValue]: List of pb_objects.

        """
        qv_dict = collections.defaultdict(list)
        for column, value in row.items():
            family, qualifier = Client._split_name(column)
            pb_qualifier_value = protobuf.MutationProto.ColumnValue.QualifierValue()
            qv_dict[family].append(pb_qualifier_value)
            pb_qualifier_value.qualifier = qualifier.encode()
            pb_qualifier_value.value = value
        cv_list = list()
        for family, qv_list in qv_dict.items():
            pb_column_value = protobuf.MutationProto.ColumnValue()
            cv_list.append(pb_column_value)
            pb_column_value.family = family.encode()
            pb_column_value.qualifier_value.extend(qv_list)
        return cv_list

    def create_scanner(self,
                       table,
                       start_key=None,
                       end_key=None,
                       columns=None,
                       filter_=None,
                       num_rows=100):
        """Create a scanner for a table.

        Args:
            table (str): Table name.
            start_key (str): Start key.
            end_key (str): End key.
            columns (list[str]|tuple[str]): Name of the columns to query.
                This is similar to the projection operation in SQL.
            filter_ (filters.Filter): The filter object.
            num_rows (int): Number of rows returned in every iteration.

        Returns:
            Scanner: A scanner object.
                Note that a scanner is only an object used to store scanning information.

        """
        return Scanner(
            self,
            table,
            start_key if start_key is not None else '',
            end_key,
            columns if columns is not None else [],
            filter_,
            num_rows
        )

    def iter_scanner(self, scanner):
        """Iterate the scanner to get a batch of rows.

        The number of rows returned is determined by the scanner.__num_rows__
        which is set during creation of the scanner.

        Args:
            scanner (Scanner):

        Returns:
            list[Row]: List of rows.
            None: There is no more rows.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        #
        # message ScanRequest {
        #   optional RegionSpecifier region = 1;
        #   optional Scan scan = 2;
        #   optional uint64 scanner_id = 3;
        #   optional uint32 number_of_rows = 4;
        #   optional bool close_scanner = 5;
        #   optional uint64 next_call_seq = 6;
        #   optional bool client_handles_partials = 7;
        #   optional bool client_handles_heartbeats = 8;
        #   optional bool track_scan_metrics = 9;
        #   optional bool renew = 10 [default = false];
        # }
        # message Scan {
        #   repeated Column column = 1;
        #   repeated NameBytesPair attribute = 2;
        #   optional bytes start_row = 3;
        #   optional bytes stop_row = 4;
        #   optional Filter filter = 5;
        #   optional TimeRange time_range = 6;
        #   optional uint32 max_versions = 7 [default = 1];
        #   optional bool cache_blocks = 8 [default = true];
        #   optional uint32 batch_size = 9;
        #   optional uint64 max_result_size = 10;
        #   optional uint32 store_limit = 11;
        #   optional uint32 store_offset = 12;
        #   optional bool load_column_families_on_demand = 13;
        #   optional bool small = 14;
        #   optional bool reversed = 15 [default = false];
        #   optional Consistency consistency = 16 [default = STRONG];
        #   optional uint32 caching = 17;
        #   optional bool allow_partial_results = 18;
        #   repeated ColumnFamilyTimeRange cf_time_range = 19;
        # }
        # message Column {
        #   required bytes family = 1;
        #   repeated bytes qualifier = 2;
        # }
        # message Filter {
        #   required string name = 1;
        #   optional bytes serialized_filter = 2;
        # }
        if scanner.__client__ != self:
            raise ValueError('Invalid scanner.')
        if scanner.__scanner_id__ is not None:
            region = scanner.__region__
            assert region is not None
            region_service = self._region_manager.get_service(region)
            pb_resp = self._scan_region_scanner(
                region,
                region_service,
                scanner.__scanner_id__,
                scanner.__num_rows__
            )

            if not pb_resp.more_results_in_region:
                self._close_region_scanner(region, region_service, scanner.__scanner_id__)
                scanner.__scanner_id__ = None
                scanner.__region__ = None
                next_start_key = region.end_key
                end_key = scanner.__end_key__
                scanner.__current_start_key__ = (
                    next_start_key
                    if next_start_key != '' and (end_key is None or next_start_key < end_key)
                    else None
                )

            return [
                self._cells_to_row(result.cell)
                for result in pb_resp.results
            ]
        else:
            start_key = scanner.__current_start_key__
            if start_key is None:
                return None

            region = self._region_manager.get_region(scanner.__table__, start_key)
            region_service = self._region_manager.get_service(region)
            scanner.__region__ = region

            pb_resp = self._create_region_scanner(
                region,
                region_service,
                scanner.__table__,
                start_key,
                scanner.__end_key__,
                scanner.__columns__,
                scanner.__filter___,
                scanner.__num_rows__
            )
            scanner.__scanner_id__ = pb_resp.scanner_id

            return [
                self._cells_to_row(result.cell)
                for result in pb_resp.results
            ]

    def delete_scanner(self, scanner):
        """Delete the scanner.

        Args:
            scanner (Scanner): The scanner object.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        if scanner.__client__ != self:
            raise ValueError('Invalid scanner.')
        scanner_id = scanner.__scanner_id__
        if scanner_id is not None:
            region = scanner.__region__
            region_service = self._region_manager.get_service(region)
            self._close_region_scanner(
                region,
                region_service,
                scanner_id
            )

    def _create_region_scanner(self,
                               region,
                               region_service,
                               table,
                               start_key,
                               end_key,
                               columns,
                               filter_,
                               num_rows,
                               reversed=False):
        """Create a scanner on a region and return the first iteration results.

        Args:
            region (_region.Region): The region object.
            region_service (services.RegionService): The region service.
            table (str): Table name.
            start_key (str|None): Start key.
            end_key (str|None): End key.
            columns (list[str]|tuple[str]|None): Name of the columns to query.
                This is similar to the projection operation in SQL.
            filter_ (filters.Filter|None): The filter object.
            num_rows (int): Number of rows returned in every iteration.

        Returns:
            The protocol response object.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        # print('DEBUG: Create scanner on %s.' % str(region))
        pb_req = protobuf.ScanRequest()

        pb_req.region.type = 1
        pb_req.region.value = region.name.encode()

        # start_key and end_key
        pb_scan = pb_req.scan
        if start_key is not None:
            pb_scan.start_row = start_key.encode()
        if end_key is not None:
            pb_scan.stop_row = end_key.encode()

        # columns
        if columns is not None:
            pb_columns = pb_scan.column
            for column in columns:
                family, qualifier = self._split_name(column)
                pb_column = pb_columns.add()
                pb_column.family = family
                pb_column.qualifier = qualifier

        # filter
        if filter_ is not None:
            pb_filter = pb_scan.filter
            pb_filter.name = filter_.name
            pb_filter.serialized_filter = filter_.serialize()

        # number of rows
        pb_req.number_of_rows = num_rows

        # reversed
        pb_scan.reversed = reversed

        try:
            return region_service.request(pb_req)
        except RegionError:
            while True:
                time.sleep(3)
                # print('DEBUG: put() RegionError')
                # print(repr(region))
                # refresh the region information and retry the operation
                region = self._region_manager.get_region(table, start_key, use_cache=False)
                region_service = self._region_manager.get_service(region)
                pb_req.region.value = region.name.encode()
                # if the new region still doesn't work, it is a fatal error
                # print(repr(region))
                try:
                    pb_resp = region_service.request(pb_req)
                    break
                except RegionError:
                    continue
            return pb_resp

    @staticmethod
    def _scan_region_scanner(region,
                             region_service,
                             scanner_id,
                             num_rows):
        """Iterate the region scanner.

        Args:
            region (_region.Region): The region object.
            region_service (services.RegionService): The region service.
            scanner_id (int): The region scanner ID.
            num_rows (int): Number of rows returned in every iteration.

        Returns:
            The protocol response object.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        pb_req = protobuf.ScanRequest()

        pb_req.region.type = 1
        pb_req.region.value = region.name.encode()

        pb_req.number_of_rows = num_rows
        pb_req.scanner_id = scanner_id

        return region_service.request(pb_req)

    @staticmethod
    def _close_region_scanner(region,
                              region_service,
                              scanner_id):
        """Close the region scanner.

        Args:
            region (_region.Region): The region object.
            region_service (services.RegionService): The region service.
            scanner_id (int): The region scanner ID.

        Returns:
            The protocol response object.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        # print('DEBUG: Close scanner on %s.' % str(region))
        pb_req = protobuf.ScanRequest()

        pb_req.region.type = 1
        pb_req.region.value = region.name.encode()

        pb_req.scanner_id = scanner_id
        pb_req.close_scanner = True

        return region_service.request(pb_req)

    @staticmethod
    def _cells_to_row(pb_cells):
        if len(pb_cells) < 1:
            return None
        doc = Row(pb_cells[0].row.decode(), {
            pb_cell.family.decode() + ':' + pb_cell.qualifier.decode(): pb_cell.value
            for pb_cell in pb_cells
        })
        return doc

    @staticmethod
    def _cells_to_rows(_pb_cells):
        if len(_pb_cells) < 1:
            return None
        row_dict = dict()
        for pb_cell in _pb_cells:
            key = pb_cell.row.decode()
            column = pb_cell.family.decode() + ':' + pb_cell.qualifier.decode()
            value = pb_cell.value
            try:
                row = row_dict[key]
            except KeyError:
                row = Row(key)
                row_dict[key] = row
            row[column] = value
        row_list = [row for row in row_dict.values()]
        return row_list

    def delete(self, table, key):
        """Delete a row.

        Args:
            table (str): Table name.
            key (str): Row key.

        Raises:
            RegionError
            RequestError

            TransportError
            ZookeeperProtocolError
            ServiceProtocolError
            NoSuchZookeeperNodeError

        """
        region = self._region_manager.get_region(table, key)
        region_service = self._region_manager.get_service(region)

        pb_req = protobuf.MutateRequest()

        pb_req.region.type = 1
        pb_req.region.value = region.name.encode()

        pb_mutation = pb_req.mutation
        pb_mutation.row = key.encode()
        pb_mutation.mutate_type = 3

        #
        # message MutateResponse {
        #   optional Result result = 1;
        #
        #   // used for mutate to indicate processed only
        #   optional bool processed = 2;
        # }
        try:
            pb_resp = region_service.request(pb_req)
        except RegionError:
            # refresh the region information and retry the operation
            region = self._region_manager.get_region(table, key, use_cache=False)
            region_service = self._region_manager.get_service(region)
            pb_req.region.value = region.name.encode()
            # if the new region still doesn't work, it is a fatal error
            pb_resp = region_service.request(pb_req)
        return pb_resp.processed

    @staticmethod
    def _split_name(full_name):
        name_qualifier = full_name.split(':')
        if len(name_qualifier) != 2:
            raise ValueError(
                'Invalid name. {namespace}:{qualifier} expected, got %s.' % full_name
            )
        return name_qualifier


class Scanner(object):

    def __init__(self,
                 client,
                 table,
                 start_key,
                 end_key,
                 columns,
                 filter_,
                 num_rows):
        self.__client__ = client
        self.__table__ = table
        self.__start_key__ = start_key
        self.__end_key__ = end_key
        self.__columns__ = columns
        self.__filter___ = filter_
        self.__num_rows__ = num_rows

        self.__current_start_key__ = start_key
        self.__scanner_id__ = None
        self.__region__ = None
