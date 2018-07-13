#!/usr/bin/env python3

"""
@author: xi
@since: 2018-04-14
"""

import argparse
import cmd
import shlex
import time

import hbase


class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        raise RuntimeError(message)


class HBaseShell(cmd.Cmd):

    def __init__(self, args):
        self._args = args

        super(HBaseShell, self).__init__()
        self.prompt = '?> '

        self._zk = args.zk

        self._parsers = dict()
        self._conn = None  # type: hbase.Connection
        self._ns = None  # type: hbase.Namespace

    def __enter__(self):
        self._conn = hbase.ConnectionPool(self._zk).connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def onecmd(self, line):
        try:
            super(HBaseShell, self).onecmd(line)
        except Exception as e:
            print(str(e))
        finally:
            if line:
                print()

    def do_ls(self, line):
        parser_name = 'ls'
        if parser_name not in self._parsers:
            parser = ArgumentParser(prog=parser_name)
            self._parsers[parser_name] = parser
            parser.add_argument('--namespaces', '-n', action='store_true')
            parser.add_argument('--tables', '-t', action='store_true')
        args = self._parsers[parser_name].parse_args(shlex.split(line))

        if args.namespaces or (not args.namespaces and not args.tables):
            for ns in self._conn.namespaces():
                print(ns)
            return

        if args.tables:
            if self._ns is None:
                raise RuntimeError('No namespace selected. Please use "use [namespace]".')
            for tbl in self._ns.tables():
                print(tbl)

    def do_use(self, line):
        parser_name = 'use'
        if parser_name not in self._parsers:
            parser = ArgumentParser(prog=parser_name)
            self._parsers[parser_name] = parser
            parser.add_argument('name', default=None, nargs='?')
        args = self._parsers[parser_name].parse_args(shlex.split(line))

        if args.name is None:
            self._ns = None
            self.prompt = '?> '
            return

        self._ns = self._conn.namespace(args.name, create_if_not_exists=False)
        self.prompt = self._ns.name + '> '

    def do_get(self, line):
        parser_name = 'get'
        if parser_name not in self._parsers:
            parser = ArgumentParser(prog=parser_name)
            self._parsers[parser_name] = parser
            parser.add_argument('table')
            parser.add_argument('key', nargs='?')
            parser.add_argument('--full', '-f', action='store_true', default=False)
        args = self._parsers[parser_name].parse_args(shlex.split(line))

        if self._ns is None:
            raise RuntimeError('No namespace selected. Please use "use [namespace]".')
        tbl = self._ns.table(args.table, create_if_not_exists=False)
        row = tbl.get(args.key) if args.key else tbl.get_one(not args.full)
        print(row)

    def do_count(self, line):
        parser_name = 'count'
        if parser_name not in self._parsers:
            parser = ArgumentParser(prog=parser_name)
            self._parsers[parser_name] = parser
            parser.add_argument('table')
        args = self._parsers[parser_name].parse_args(shlex.split(line))

        tbl = self._ns.table(args.table, create_if_not_exists=False)
        t = time.time()
        count = tbl.count(
            verbose=lambda c, r: print('%d: %s' % (c, r.key))
        )
        t = time.time() - t
        print('\nTotal count: %d.\n%d seconds used.' % (count, t))

    def do_drop_namespace(self, line):
        parser_name = 'drop_namespace'
        if parser_name not in self._parsers:
            parser = ArgumentParser(prog=parser_name)
            self._parsers[parser_name] = parser
            parser.add_argument('namespace')
        args = self._parsers[parser_name].parse_args(shlex.split(line))

        self._conn.delete_namespace(args.namespace)
        if self._ns.name == args.namespace:
            self._ns = None
            self.prompt = '?> '
        print('Namespace %s dropped.' % args.namespace)

    def do_drop_table(self, line):
        parser_name = 'drop_table'
        if parser_name not in self._parsers:
            parser = ArgumentParser(prog=parser_name)
            self._parsers[parser_name] = parser
            parser.add_argument('table')
        args = self._parsers[parser_name].parse_args(shlex.split(line))

        self._ns.delete_table(args.table)
        print('Table %s dropped.' % args.table)

    def do_EOF(self, line):
        return True


def main(args):
    if args.zk is None:
        args.zk = input('Please input zookeeper quorum: ')
    with HBaseShell(args) as shell:
        shell.cmdloop()
    return 0


if __name__ == '__main__':
    _parser = argparse.ArgumentParser()
    _parser.add_argument('zk', nargs='?')
    _args = _parser.parse_args()
    exit(main(_args))
