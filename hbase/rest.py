#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-17
"""

import requests

from hbase import protobuf_schema

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8080
DEFAULT_FAMILIES = {'cf': dict()}

CODE_OK = 200
CODE_CREATED = 201
CODE_NO_CONTENT = 204
CODE_NOT_MODIFIED = 304
CODE_FORBIDDEN = 403
CODE_NOT_FOUND = 404
CODE_INTERNAL_SERVER_ERROR = 500


class RESTError(RuntimeError):

    def __init__(self, code=None, message=None):
        """REST server error.

        Args:
            code (int):Return code from the REST server.
            message (str): The corresponding error message.

        """
        text = ''
        if code is not None:
            text += 'Return Code: %d\n' % code
        if message is not None and len(message) > 0:
            text += message
        super(RESTError, self).__init__(text)


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
        return '(%s, %s)' % (self.key, super(Row, self).__str__())

    def __repr__(self):
        return '(%s, %s)' % (self.key, super(Row, self).__repr__())


class Client(object):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        """REST client object.

        Args:
            host (str): Hostname or IP address.
            port (int): Port number.

        """
        self._host = host
        self._port = port
        self._base_url = 'http://%s:%d' % (host, port)
        self._session = requests.Session()

    def __del__(self):
        if self._session is not None:
            try:
                self._session.close()
            except ReferenceError:
                pass

    def close(self):
        if self._session is not None:
            try:
                self._session.close()
            except ReferenceError:
                pass
            self._session = None

    def namespaces(self):
        """Get namespaces.

        Returns:
            list[str]: List of namespace names.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.get(
            url='/'.join((self._base_url, 'namespaces')),
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            namespaces = protobuf_schema.Namespaces()
            namespaces.ParseFromString(response.content)
            return [
                namespace
                for namespace in namespaces.namespace
            ]
        else:
            raise RESTError(code, response.text)

    def namespace(self, namespace):
        """Get descriptions of the namespace.

        Args:
            namespace (str): Name of the namespace.

        Returns:
            dict[str, str]: Descriptions in dict, e.g., {'property': 'value'}.
            None: The namespace does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.get(
            url='/'.join((self._base_url, 'namespaces', namespace)),
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            namespace_properties = protobuf_schema.NamespaceProperties()
            namespace_properties.ParseFromString(response.content)
            return {
                prop.key: prop.value
                for prop in namespace_properties.props
            }
        elif code == CODE_NOT_FOUND or CODE_INTERNAL_SERVER_ERROR:
            return None
        else:
            raise RESTError(code, response.text)

    def create_namespace(self, namespace, props=None):
        """Create a namespace.

        Args:
            namespace (str): Name of the namespace.
            props (dict[str, str]): Custom properties.

        Returns:
            True: Success.
            False: The namespace exists.

        Raises:
            RESTError: REST server returns other errors.

        """
        namespace_properties = protobuf_schema.NamespaceProperties()
        if props is not None:
            if not isinstance(props, dict):
                raise ValueError('"props" should be a dict.')
            for k, v in props.items():
                prop = namespace_properties.props.add()
                prop.key = k
                prop.value = v

        response = self._session.post(
            url='/'.join((self._base_url, 'namespaces', namespace)),
            headers={
                'Content-Type': 'application/x-protobuf',
                'Accept': 'application/x-protobuf'
            },
            data=namespace_properties.SerializeToString()
        )

        code = response.status_code
        if code == CODE_CREATED:
            return True
        elif code == CODE_FORBIDDEN:
            return False  # the namespace exists
        else:
            raise RESTError(code, response.text)

    def delete_namespace(self, namespace):
        """Delete namespace.

        Args:
            namespace (str): Name of the namespace.

        Returns:
            True: Success.
            False: The namespace does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.delete(
            url='/'.join((self._base_url, 'namespaces', namespace)),
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            return True
        elif code == CODE_NOT_FOUND:
            return False  # the namespace does not exist
        else:
            raise RESTError(code, response.text)

    def tables(self, namespace=None):
        """Get the table names under the given namespace.

        Args:
            namespace (str): Name of the namespace.

        Returns:
            list[str]: List of tables names
            None: The namespace does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        if namespace is None:
            response = self._session.get(
                url=self._base_url,
                headers={
                    'Accept': 'application/x-protobuf'
                }
            )
            code = response.status_code
            if code == CODE_OK:
                table_list = protobuf_schema.TableList()
                table_list.ParseFromString(response.content)
                return [
                    name
                    for name in table_list.name
                ]
            else:
                raise RESTError(code, response.text)
        else:
            response = self._session.get(
                url='/'.join((self._base_url, 'namespaces', namespace, 'tables')),
                headers={
                    'Accept': 'application/x-protobuf'
                }
            )
            code = response.status_code
            if code == CODE_OK:
                table_list = protobuf_schema.TableList()
                table_list.ParseFromString(response.content)
                return [
                    name
                    for name in table_list.name
                ]
            elif code == CODE_NOT_FOUND or code == CODE_INTERNAL_SERVER_ERROR:
                return None
            else:
                raise RESTError(code, response.text)

    def table(self, table):
        """Get table schema.

        Args:
            table (str): Table name.

        Returns:
            dict[str, T]: Description of the table.
            None: The table does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.get(
            url='/'.join((self._base_url, table, 'schema')),
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            table_schema = protobuf_schema.TableSchema()
            table_schema.ParseFromString(response.content)
            return {
                'name': table_schema.name,
                'in_memory': table_schema.inMemory,
                'read_only': table_schema.readOnly,
                'columns': [
                    {
                        'name': column.name,
                        'ttl': column.ttl,
                        'max_versions': column.maxVersions,
                        'compression': column.compression,
                        'attrs': {
                            attr.name: attr.value
                            for attr in column.attrs
                        }
                    }
                    for column in table_schema.columns
                ],
                'attrs': {
                    attr.name: attr.value
                    for attr in table_schema.attrs
                }
            }
        elif code == CODE_NOT_FOUND or code == CODE_INTERNAL_SERVER_ERROR:
            return None
        else:
            raise RESTError(code, response.text)

    def create_table(self, table, families=DEFAULT_FAMILIES):
        """Create table.

        Args:
            table (str): Table name.
            families (dict[str, dict[str, T]]): Column families, e.g., {'family_name': {'attr': value}}.

        Returns:
            True: Success.
            False: The table exists.

        Raises:
            RESTError: REST server returns other errors.

        """
        table_schema = protobuf_schema.TableSchema()
        table_schema.name = table
        for name, attrs in families.items():
            column = table_schema.columns.add()
            column.name = name
            if 'ttl' in attrs:
                column.ttl = attrs['ttl']
            if 'max_versions' in attrs:
                column.maxVersions = attrs['max_versions']
            if 'compression' in attrs:
                column.compression = attrs['compression']

        response = self._session.post(
            url='/'.join((self._base_url, table, 'schema')),
            headers={
                'Content-Type': 'application/x-protobuf',
                'Accept': 'application/x-protobuf'
            },
            data=table_schema.SerializeToString()
        )

        code = response.status_code
        if code == CODE_CREATED:
            return True
        elif code == CODE_OK:
            return False
        else:
            raise RESTError(code, response.text)

    def delete_table(self, table):
        """Delete table.

        Args:
            table (str): Table name.

        Returns:
            True: Success.
            False: The table dose not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.delete(
            url='/'.join((self._base_url, table, 'schema')),
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            return True
        elif code == CODE_NOT_FOUND:
            return False
        else:
            raise RESTError(code, response.text)

    def row(self, table, key):
        """Query to get a row object with a row key.

        Args:
            table (str): Table name.
            key (str): Row key.

        Returns:
            Row: The row object.
            None: The row does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.get(
            url='/'.join((self._base_url, table, key)),
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            cell_set = protobuf_schema.CellSet()
            cell_set.ParseFromString(response.content)
            return [
                Row(
                    key=row.key.decode(),
                    cells={
                        value.column.decode(): value.data
                        for value in row.values
                    }
                )
                for row in cell_set.rows
            ]
        elif code == CODE_NOT_FOUND:
            return None
        else:
            raise RESTError(code, response.text)

    def create_scanner(self,
                       table,
                       start_row=None,
                       end_row=None,
                       columns=None,
                       start_time=None,
                       end_time=None,
                       batch_size=16):
        """Create a scanner for a table.

        Args:
            table (str): Table name.
            start_row (str): Start row key.
            end_row (str): End row key.
            columns (list[str]): Columns to fetch.
            start_time (int): Start timestamp.
            end_time (int): End timestamp.
            batch_size (int): Batch size.

        Returns:
            str: A scanner URL which can be then used to iterate the table rows.
            None: THe table does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        scanner = protobuf_schema.Scanner()
        if start_row is not None:
            scanner.startRow = start_row.encode()
        if end_row is not None:
            scanner.endRow = end_row.encode()
        if columns is not None:
            scanner.columns.extend([
                column.encode()
                for column in columns
            ])
        if start_time is not None:
            scanner.startTime = start_time
        if end_time is not None:
            scanner.endTime = end_time
        if batch_size is not None:
            scanner.batch = batch_size

        response = self._session.post(
            url='/'.join((self._base_url, table, 'scanner')),
            headers={
                'Content-Type': 'application/x-protobuf',
                'Accept': 'application/x-protobuf'
            },
            data=scanner.SerializeToString()
        )

        code = response.status_code
        if code == CODE_CREATED:
            return response.headers['Location']
        elif code == CODE_NOT_FOUND:
            raise None
        else:
            raise RESTError(code, response.text)

    def delete_scanner(self, scanner_url):
        """Delete a scanner.

        Args:
            scanner_url (str): The scanner URL which returned from "create_scanner".

        Returns:
            True: Success.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.delete(scanner_url)

        code = response.status_code
        if code == CODE_OK:
            return True
        else:
            raise RESTError(code, response.text)

    def iter_scanner(self, scanner_url):
        """Iterate a scanner.

        Args:
            scanner_url (str): The scanner URL which returned from "create_scanner".

        Returns:
            list[Row]: List of row.
            None: There is not more content to be iterated.

        Raises:
            RESTError: REST server returns other errors.

        Note that the scanner will not be closed(deleted) when the rows exhausted.
        You should close it manually.

        """
        response = self._session.get(
            url=scanner_url,
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            cell_set = protobuf_schema.CellSet()
            cell_set.ParseFromString(response.content)
            return [
                Row(
                    key=row.key,
                    cells={
                        value.column.decode(): value.data
                        for value in row.values
                    }
                )
                for row in cell_set.rows
            ]
        elif code == CODE_NO_CONTENT:
            return None
        else:
            raise RESTError(code, response.text)

    def put(self, table, row):
        """Put(insert) one row to the table.

        Args:
            table (str): Table name.
            row (Row): Row to insert.

        Returns:
            True: Success.

        Raises:
            RESTError: REST server returns other errors.

        """
        cell_set = protobuf_schema.CellSet()
        row_ = cell_set.rows.add()
        row_.key = row.key.encode()
        for column, data in row.items():
            value = row_.values.add()
            value.column = column.encode()
            value.data = data

        response = self._session.post(
            url='/'.join((self._base_url, table, 'fakerow')),
            headers={
                'Content-Type': 'application/x-protobuf',
                'Accept': 'application/x-protobuf'
            },
            data=cell_set.SerializeToString()
        )

        code = response.status_code
        if code == CODE_OK:
            return True  # put success
        else:
            raise RESTError(code, response.text)

    def check_and_put(self,
                      table,
                      row,
                      check_column,
                      check_value=None):
        """Put(insert) one row to the table when the given condition is satisfied.

        Atomically checks if a row/family/qualifier value matches the expected value.
        If it does, it adds the put.
        If the passed value is None(or b''), the check is for the lack of column (ie: non-existance)

        Args:
            table (str): Table name.
            row (Row): Row to insert.
            check_column (str): Column to check.
            check_value (bytes): Value to check.

        Returns:
            True: Success.
            False: Not modified. (The given condition is not satisfied.)

        Raises:
            RESTError: REST server returns other errors.

        """
        cell_set = protobuf_schema.CellSet()
        row_ = cell_set.rows.add()
        row_.key = row.key.encode()
        for column, data in row.items():
            value = row_.values.add()
            value.column = column.encode()
            value.data = data
        if check_column is not None:
            value = row_.values.add()
            value.column = check_column.encode()
            value.data = check_value if check_value is not None else b''
            url = '/'.join((self._base_url, table, 'fakerow?check=put'))
        else:
            url = '/'.join((self._base_url, table, 'fakerow'))

        response = self._session.post(
            url=url,
            headers={
                'Content-Type': 'application/x-protobuf',
                'Accept': 'application/x-protobuf'
            },
            data=cell_set.SerializeToString()
        )

        code = response.status_code
        if code == CODE_OK:
            return True  # put success
        elif code == CODE_NOT_MODIFIED:
            return False  # not modified
        else:
            raise RESTError(code, response.text)

    def put_many(self, table, rows):
        """Put(insert) multiple rows to the table.

        Args:
            table (str): Table name.
            rows (list[Row] | collections.deque): Row to insert.

        Returns:
            True: Success.

        Raises:
            RESTError: REST server returns other errors.

        """
        cell_set = protobuf_schema.CellSet()
        for row in rows:
            row_ = cell_set.rows.add()
            row_.key = row.key.encode()
            for column, data in row.items():
                value = row_.values.add()
                value.column = column.encode()
                value.data = data

        response = self._session.post(
            url='/'.join((self._base_url, table, 'fakerow')),
            headers={
                'Content-Type': 'application/x-protobuf',
                'Accept': 'application/x-protobuf'
            },
            data=cell_set.SerializeToString()
        )

        code = response.status_code
        if code == CODE_OK:
            return True
        else:
            raise RESTError(code, response.text)

    def delete(self, table, key):
        """Delete a row.

        Args:
            table (str): Table name.
            key (str): Row key.

        Returns:
            True: Success.
            False: The table does not exist.

        Raises:
            RESTError: REST server returns other errors.

        """
        response = self._session.delete(
            url='/'.join((self._base_url, table, key)),
            headers={
                'Accept': 'application/x-protobuf'
            }
        )

        code = response.status_code
        if code == CODE_OK:
            return True
        elif code == CODE_NOT_FOUND:
            return False
        else:
            raise RESTError(code, response.text)
