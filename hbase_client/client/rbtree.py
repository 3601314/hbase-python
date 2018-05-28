#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-15
"""

from . import bstree

RED = True
BLACK = False


class Node(bstree.Node):

    def __init__(self):
        super(Node, self).__init__()
        self.color = RED

    def __str__(self):
        return ('[%s]' if self.color == BLACK else ' %s ') % str(self.value)


class RBTree(bstree.BSTree):

    def __str__(self):
        if self._root is not None:
            return repr(self._root)

    def insert(self, value):
        if self._root is None:
            x = Node()
            x.value = value
            x.color = BLACK
            self._root = x
            return x

        y = self._root
        while True:
            if value < y.value:
                if y.left is None:
                    # add new node to y's left
                    x = Node()
                    x.value = value
                    x.color = RED
                    x.parent = y
                    y.left = x
                    self._insert_fixup(x)
                    return x
                else:
                    y = y.left
            elif value > y.value:
                if y.right is None:
                    # add new node to y's right
                    x = Node()
                    x.value = value
                    x.color = RED
                    x.parent = y
                    y.right = x
                    self._insert_fixup(x)
                    return x
                else:
                    y = y.right
            else:
                # return the existing node
                return y

    def _insert_fixup(self, x):
        """Fix up for insert.

        "x": New node.
        "p": Parent of "x".
        "g": Grand parent of "x".
        "u": Uncle of "x".

        Args:
            x (Node): New Node.

        """
        while True:
            p = x.parent
            if p is None or p.color == BLACK:
                break
            g = p.parent
            if p == g.left:
                u = g.right
                if u is not None and u.color == RED:
                    # case 1
                    p.color = BLACK
                    u.color = BLACK
                    g.color = RED
                    x = g
                    # continue
                else:
                    if x == p.right:
                        # case 2
                        self._rotate_left(p)
                        x, p = p, x
                    # case 3
                    p.color = BLACK
                    g.color = RED
                    self._rotate_right(g)
                    # continue
            else:
                u = g.left
                if u is not None and u.color == RED:
                    # case 1
                    p.color = BLACK
                    u.color = BLACK
                    g.color = RED
                    x = g
                    # continue
                else:
                    if x == p.left:
                        # case 2
                        self._rotate_right(p)
                        x, p = p, x
                    # case 3
                    p.color = BLACK
                    g.color = RED
                    self._rotate_left(g)
                    # continue
        self._root.color = BLACK

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

        if x.color == BLACK:
            self._delete_fixup(y, p)
        return ret

    def _delete_fixup(self, y, p):
        while p is not None and (y is None or y.color == BLACK):
            if y == p.left:
                w = p.right
                assert w is not None
                if w.color == RED:
                    # Case 1
                    w.color = BLACK
                    p.color = RED
                    self._rotate_left(p)
                    w = p.right
                if ((w.left is None or w.left.color == BLACK) and
                        (w.right is None or w.right.color == BLACK)):
                    # Case 2
                    w.color = RED
                    y = p
                    p = y.parent
                else:
                    if w.right is None or w.right.color == BLACK:
                        # Case 3
                        w.left.color = BLACK
                        w.color = RED
                        self._rotate_right(w)
                        assert p.right == w.parent
                        w = w.parent
                    # Case 4
                    w.color = p.color
                    p.color = BLACK
                    w.right.color = BLACK
                    self._rotate_left(p)
                    y = self._root
                    break
            else:
                w = p.left
                assert w is not None
                if w.color == RED:
                    # Case 1
                    w.color = BLACK
                    p.color = RED
                    self._rotate_right(p)
                    w = p.left
                if ((w.right is None or w.right.color == BLACK) and
                        (w.left is None or w.left.color == BLACK)):
                    # Case 2
                    w.color = RED
                    y = p
                    p = y.parent
                else:
                    if w.left is None or w.left.color == BLACK:
                        # Case 3
                        w.right.color = BLACK
                        w.color = RED
                        self._rotate_left(w)
                        assert p.left == w.parent
                        w = w.parent
                    # Case 4
                    w.color = p.color
                    p.color = BLACK
                    w.left.color = BLACK
                    self._rotate_right(p)
                    y = self._root
                    break
        if y is not None:
            y.color = BLACK


if __name__ == '__main__':
    tree = RBTree()
    tree.insert(1)
    tree.insert(4)
    tree.insert(2)
    tree.insert(3)
    tree.insert(8)
    tree.insert(10)
    tree.insert(11)
    tree.insert(12)
    tree.delete(8)
    tree.delete(12)
    tree.delete(11)
    print(tree)
    exit()
