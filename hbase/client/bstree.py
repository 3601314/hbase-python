#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-16
"""

import io


class Node(object):

    def __init__(self):
        self.left = None
        self.right = None
        self.parent = None

        self.value = None

    def __str__(self):
        return '%s' % str(self.value)

    def __repr__(self, depth=0):
        buffer = io.StringIO()
        self._intend(buffer, depth)
        buffer.write(str(self))
        buffer.write('\n')
        depth += 1
        if self.left is not None:
            buffer.write(self.left.__repr__(depth))
        else:
            self._intend(buffer, depth)
            buffer.write('\n')
        if self.right is not None:
            buffer.write(self.right.__repr__(depth))
        else:
            self._intend(buffer, depth)
            buffer.write('\n')
        return buffer.getvalue()

    @staticmethod
    def _intend(buffer, depth):
        for _ in range(depth - 1):
            buffer.write('    ')
        if depth > 0:
            buffer.write('....')


class BSTree(object):

    def __init__(self):
        self._root = None

    @property
    def root(self):
        return self._root

    def find(self, value):
        x = self._root
        while x is not None:
            if x.value > value:
                x = x.left
            elif x.value < value:
                x = x.right
            else:
                return x
        return None

    def insert(self, value):
        if self._root is None:
            x = Node()
            x.value = value
            self._root = x
            return x

        y = self._root
        while True:
            if value < y.value:
                if y.left is None:
                    # add new node to y's left
                    x = Node()
                    x.value = value
                    x.parent = y
                    y.left = x
                    return x
                else:
                    y = y.left
            elif value > y.value:
                if y.right is None:
                    # add new node to y's right
                    x = Node()
                    x.value = value
                    x.parent = y
                    y.right = x
                    return x
                else:
                    y = y.right
            else:
                # return the existing node
                return y

    def delete(self, value):
        ret = x = self.find(value)
        if x is None:
            return None

        # x: target to remove
        # y: replacement of x, can be "None"
        if x.left is None:
            y = x.right
        elif x.right is None:
            y = x.left
        else:
            t = x
            x = self.minimum(x.right)
            t.value = x.value
            # since x is now the left most node, it can only have right child
            y = x.right

        # p: parent of x, can be "None" (when x is the root)
        # after deletion, p is the parent of y
        p = x.parent
        if p is None:
            self._root = y
        else:
            if x == p.left:
                p.left = y
            else:
                p.right = y
        if y is not None:
            y.parent = p

        return ret

    @staticmethod
    def minimum(x):
        while x.left is not None:
            x = x.left
        return x

    @staticmethod
    def maximum(x):
        while x.right is not None:
            x = x.right
        return x

    @staticmethod
    def successor(x):
        if x.right is not None:
            return BSTree.minimum(x.right)
        y = x.parent
        while y is not None and x == y.right:
            x = y
            y = y.parent
        return y

    def _rotate_left(self, x):
        #
        # x: the pivot
        # p: parent of x
        # y: right node of x
        # b: left node of y
        #
        p = x.parent
        y = x.right
        assert y is not None
        b = y.left

        x.right = b
        if b is not None:
            b.parent = x

        y.left = x
        x.parent = y

        if p is None:
            self._root = y
        else:
            if p.left == x:
                p.left = y
            else:
                p.right = y
        y.parent = p
        return y

    def _rotate_right(self, x):
        #
        # x: the pivot
        # p: parent of x
        # y: left node of x
        # b: right node of y
        #
        p = x.parent
        y = x.left
        assert y is not None
        b = y.right

        x.left = b
        if b is not None:
            b.parent = x

        y.right = x
        x.parent = y

        if p is None:
            self._root = y
        else:
            if p.left == x:
                p.left = y
            else:
                p.right = y
        y.parent = p
        return y
