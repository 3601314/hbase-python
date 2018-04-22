#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-17
"""

import requests

from pyhbase import protobuf_schema

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

        :param int code: Return code from the REST server.
        :param str message: The corresponding error message.
        """
        text = ''
        if code is not None:
            text += 'Return Code: %d\n' % code
        if message is not None and len(message) > 0:
            text += message
        super(RESTError, self).__init__(text)


class Row(dict):

    def __init__(self, key=None, cells=None):
        """Row Object.

        :param str key: Row key.
        :param dict cells: {'family:qualifier': b'data'}
        """
        super(Row, self).__init__(cells)
        self.key = key

    def __str__(self):
        return '(%s, %s)' % (self.key, super(Row, self).__str__())

    def __repr__(self):
        return '(%s, %s)' % (self.key, super(Row, self).__repr__())


class Client(object):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        """REST client.

        :param str host: Hostname or address.
        :param int port: Port number.
        """
        self._host = host
        self._port = port
        self._base_url = 'http://%s:%d' % (host, port)
        self._session = requests.Session()

    def __del__(self):
        if self._session is not None:
            self._session.close()

    def close(self):
        if self._session is not None:
            self._session.close()
            self._session = None

    def namespaces(self):
        """Get namespaces.

        :return list: List of namespaces' names.
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

        :param str namespace: Name of the namespace.
        :return: {'property': 'value'} or None if the namespace does not exist.
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

        :param str namespace: Name of the namespace.
        :param dict props: Custom properties.
        :return: True if success, False if the namespace exists.
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

        :param str namespace: Name of the namespace.
        :return: True if success, False if the namespace does not exist.
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

        :param str namespace: Name of the namespace.
        :return list: List of tables names, or None if the namespace does not exist.
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

        :param str table: Table name.
        :return dict: A dict that describe the table schema, or None if the table dose not exist.
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

        :param str table: Table name.
        :param dict families: Column families. {'family_name': {'attr': 'value'}}
        :return: True if success, False if the table exists.
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

        :param str table: Table name.
        :return: True if success, False if the table does not exist.
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
        """Get a row with the row key.

        :param str table: Table name.
        :param str key: Row key.
        :return Row: The row object, or None if the row does not exist.
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

        :param str table: Table name.
        :param str start_row: Start row key.
        :param str end_row: End row key.
        :param list columns: Columns.
        :param int start_time: Start time.
        :param int end_time: End time.
        :param int batch_size: Batch size.
        :return: The scanner if success, or None if the table does not exist.
        """
        scanner = protobuf_schema.Scanner()
        if start_row is not None:
            scanner.startRow = start_row
        if end_row is not None:
            scanner.endRow = end_row
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
        """Delete the scanner.

        :param str scanner_url: The url returned by "create_scanner()".
        :return: True if success.
        """
        response = self._session.delete(scanner_url)

        code = response.status_code
        if code == CODE_OK:
            return True
        else:
            raise RESTError(code, response.text)

    def iter_scanner(self, scanner_url):
        """Iterate the scanner.

        :param str scanner_url: The url returned by "create_scanner()".
        :return list: List of rows if there are rows available, or None if there is no more content.

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

    def put(self,
            table,
            row):
        """Put one row to table.

        :param str table: Table name.
        :param Row row: Row to put.
        :return: True if success.
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
        """Put one row to table.

        :param str table: Table name.
        :param Row row: Row to put.
        :param str check_column: Column to check.
        :param bytes check_value: Value to check.
        :return: True if success, False if not modified.

        Atomically checks if a row/family/qualifier value matches the expected value.
        If it does, it adds the put.
        If the passed value is None(or b''), the check is for the lack of column (ie: non-existance)
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
        """Put multiple rows to table.

        :param str table: Table name.
        :param list rows: List of rows.
        :return: True if success.
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

        :param str table: Table name.
        :param str key: Row key.
        :return: True if success, False if the table does not exist.
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


if __name__ == '__main__':
    client = Client('202.38.75.5', 2222)
    ret = client.create_scanner('maymay:second', columns=['CF:scorem', 'CF:name'])
    print(ret)
    ret = client.iter_scanner(ret)
    print(ret)
    exit()
