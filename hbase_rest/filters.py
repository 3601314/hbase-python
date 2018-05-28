#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-11
"""

import json


class Filter(dict):

    def __init__(self, type_):
        super(Filter, self).__init__(type=type_)

    def __str__(self):
        return json.dumps(self)

    def __repr__(self):
        return json.dumps(self)


class ColumnPrefixFilter(Filter):

    def __init__(self):
        super(ColumnPrefixFilter, self).__init__('ColumnPrefixFilter')

    @property
    def value(self):
        return self['value'] if 'value' in self else None

    @value.setter
    def value(self, value):
        self['value'] = value


class FirstKeyOnlyFilter(Filter):

    def __init__(self):
        super(FirstKeyOnlyFilter, self).__init__('FirstKeyOnlyFilter')


class KeyOnlyFilter(Filter):

    def __init__(self):
        super(KeyOnlyFilter, self).__init__('KeyOnlyFilter')
