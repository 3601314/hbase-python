#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-07
"""

import argparse
import hbase
import shlex


class Shell(object):

    def __init__(self, args):
        self._args = args

        self._host = args.host
        self._port = args.port

        self._parsers = dict()
        self._conn = None
        self._conn = hbase.ConnectionPool(self._host, self._port).connect()
        self._ns = None
        self._tbl = None

    def __enter__(self):
        self._conn = hbase.ConnectionPool(self._host, self._port).connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def main(self):
        while True:
            prompt = '%s:%s> ' % (
                self._ns.name if self._ns is not None else '?',
                self._tbl.name if self._tbl is not None else '?'
            )
            try:
                cmd = input(prompt).strip()
            except EOFError:
                break
            if len(cmd) == 0:
                continue
            if cmd == 'exit':
                break
            args = shlex.split(cmd)
            self._run_cmd(args[0], args[1:])
        print('Bye bye.')

    def _run_cmd(self, cmd, args):
        try:
            exec('self._%s(args)' % cmd)
        except AttributeError:
            print('%s: command not found' % cmd)
        except RuntimeError as e:
            print(e)
        except Exception as e:
            print(e)

    def _ls(self, args):
        parser_name = 'ls'
        if parser_name not in self._parsers:
            parser = argparse.ArgumentParser()
            self._parsers[parser_name] = parser
            parser.add_argument('--namespaces', '-n', action='store_true')
            parser.add_argument('--tables', '-t', action='store_true')
        args = self._parsers[parser_name].parse_args(args)

        if args.namespaces:
            for ns in self._conn.namespaces():
                print(ns)
        elif args.tables:
            if self._ns is None:
                raise RuntimeError('No namespace selected. Please use "use [namespace]".')
            for tbl in self._ns.tables():
                print(tbl)

    def _use(self, args):
        parser_name = 'use'
        if parser_name not in self._parsers:
            parser = argparse.ArgumentParser()
            self._parsers[parser_name] = parser
            parser.add_argument('name', default=None)
            parser.add_argument('--tables', '-t', action='store_true')
        args = self._parsers[parser_name].parse_args(args)

        if args.name is None:
            self._ns = None
            self._tbl = None
            return

        if args.tables:
            return

        self._ns = self._conn.namespace(args.name, False)


def main(args):
    with Shell(args) as shell:
        shell.main()
    return 0


if __name__ == '__main__':
    _parser = argparse.ArgumentParser()
    _parser.add_argument('--host', default='localhost')
    _parser.add_argument('--port', type=int, default=8080)
    _args = _parser.parse_args()
    exit(main(_args))
