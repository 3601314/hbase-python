#!/usr/bin/env python3


from setuptools import setup

if __name__ == '__main__':
    with open('README.rst') as file:
        long_description = file.read()
    setup(
        name='hbase-python',
        packages=[
            'hbase',
            'hbase.client',
            'hbase.protobuf',
            'hbase.services'
        ],
        version='0.5',
        keywords=('hbase', 'hbase-client', 'hadoop'),
        description='User friendly HBase client for Python 3. (Pure python implementation)',
        long_description=long_description,
        license='Free',
        author='dm.ustc.edu.cn',
        author_email='gylv@mail.ustc.edu.cn',
        url='https://github.com/3601314/hbase-python',
        platforms='any',
        classifiers=[
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
        ],
        include_package_data=True,
        zip_safe=True,
        install_requires=[
            'requests',
            'protobuf',
            'kazoo'
        ]
    )
