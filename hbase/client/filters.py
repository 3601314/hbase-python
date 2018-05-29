#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-25
"""

from hbase import protobuf

FILTER_PATH = 'org.apache.hadoop.hbase.filter.'
COMPARATOR_PATH = 'org.apache.hadoop.hbase.filter.'

LESS = 0
LESS_OR_EQUAL = 1
EQUAL = 2
NOT_EQUAL = 3
GREATER_OR_EQUAL = 4
GREATER = 5
NO_OP = 6


class Filter(object):

    def __init__(self, name):
        self._name = FILTER_PATH + name

    @property
    def name(self):
        return self._name

    def serialize(self):
        raise NotImplementedError()


class KeyOnlyFilter(Filter):

    def __init__(self, len_as_val=False):
        super(KeyOnlyFilter, self).__init__('KeyOnlyFilter')
        self._len_as_val = len_as_val

    def serialize(self):
        #
        # message KeyOnlyFilter {
        #   required bool len_as_val = 1;
        # }
        pb_filter = protobuf.KeyOnlyFilter()
        pb_filter.len_as_val = self._len_as_val
        return pb_filter.SerializeToString()


class Comparator(object):

    def __init__(self, name):
        self._name = COMPARATOR_PATH + name

    @property
    def name(self):
        return self._name

    def serialize(self):
        raise NotImplementedError()


class BinaryComparator(Comparator):

    def __init__(self, comparable):
        super(BinaryComparator, self).__init__('BinaryComparator')
        self._comparable = comparable

    def serialize(self):
        #
        # message BinaryComparator {
        #   required ByteArrayComparable comparable = 1;
        # }
        pb_comp = protobuf.BinaryComparator()
        pb_comp.comparable.value = self._comparable
        return pb_comp.SerializeToString()


class BinaryPrefixComparator(Comparator):

    def __init__(self, comparable):
        super(BinaryPrefixComparator, self).__init__('BinaryPrefixComparator')
        self._comparable = comparable

    def serialize(self):
        #
        # message BinaryPrefixComparator {
        #   required ByteArrayComparable comparable = 1;
        # }
        pb_comp = protobuf.BinaryPrefixComparator()
        pb_comp.comparable.value = self._comparable
        return pb_comp.SerializeToString()


class SubstringComparator(Comparator):

    def __init__(self, substr):
        super(SubstringComparator, self).__init__('SubstringComparator')
        self._substr = substr

    def serialize(self):
        #
        # message SubstringComparator {
        #   required string substr = 1;
        # }
        pb_comp = protobuf.SubstringComparator()
        pb_comp.substr = self._substr
        return pb_comp.SerializeToString()


class RegexStringComparator(Comparator):

    def __init__(self, pattern, pattern_flag, charset='UTF-8', engine=None):
        super(RegexStringComparator, self).__init__('RegexStringComparator')
        self._pattern = pattern
        self._pattern_flag = pattern_flag
        self._charset = charset
        self._engine = engine

    def serialize(self):
        #
        # message RegexStringComparator {
        #   required string pattern = 1;
        #   required int32 pattern_flags = 2;
        #   required string charset = 3;
        #   optional string engine = 4;
        # }
        pb_comp = protobuf.RegexStringComparator()
        pb_comp.pattern = self._pattern
        pb_comp.pattern_flag = self._pattern_flag
        pb_comp.charset = self._charset
        if self._engine is not None:
            pb_comp.engine = self._engine
        return pb_comp.SerializeToString()


class NullComparator(Comparator):

    def __init__(self):
        super(NullComparator, self).__init__('NullComparator')

    def serialize(self):
        #
        # message NullComparator {
        # }
        pb_comp = protobuf.NullComparator()
        return pb_comp.SerializeToString()
