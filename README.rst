hbase-python
^^^^^^^^^^^^

(The development of this package has not finished.)

hbase-python is a python package used to work HBase.

It is now tested under HBase 1.2.6.

Before using HBase, we are familiar with MongoDB and pymongo.
While, when coming to HBase, we found it is not easy to access the database via python.
So, I spent some days to start this project and hope it can be helpful to our daily research work.
The thought of this package comes from "happybase" and "starbase", and I am trying to make the API behaves like
"pymongo".

Dependencies
------------

* Python 3.4+
* requests

Installation
------------

The package can be installed from PyPI repository:

.. code-block:: bash

    pip3 install hbase-python

Examples
--------

Get a row by key:

.. code-block:: python

    import hbase

    HOSTNAME = 'localhost'
    PORT = 8080

    if __name__ == '__main__':
        with hbase.ConnectionPool(HOSTNAME, PORT).connect() as conn:
            table = conn['mytest']['videos']
            row = table.get('00001')
            print(row)
        exit()

Scan a table:

.. code-block:: python

    import hbase

    HOSTNAME = 'localhost'
    PORT = 8080

    if __name__ == '__main__':
        with hbase.ConnectionPool(HOSTNAME, PORT).connect() as conn:
            table = conn['mytest']['videos']
            for row in table.scan():
                print(row)
        exit()

Put record to a tables:

.. code-block:: python

    import hbase

    HOSTNAME = 'localhost'
    PORT = 8080

    if __name__ == '__main__':
        with hbase.ConnectionPool(HOSTNAME, PORT).connect() as conn:
            table = conn['mytest']['videos']
            table.put(hbase.Row(
                '0001', {
                    'cf:name': b'Lily',
                    'cf:age': b'20'
                }
            ))
        exit()

Write a file to table:

.. code-block:: python

    import hbase

    HOSTNAME = 'localhost'
    PORT = 8080

    if __name__ == '__main__':
        video_file = './test_video.mp4'
        with hbase.ConnectionPool(HOSTNAME, PORT).connect() as conn:
            table = conn['mytest']['videos']
            table.write_file(video_file)  # default filename is "test_video.mp4"
        exit()

