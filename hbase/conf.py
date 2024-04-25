#!/usr/bin/env python3

"""
@author: xi
@since: 2018-07-03
"""
import os

num_threads_per_conn = 5
num_tasks_per_conn = 100


# Modify This Configuration on use zk path
class Conf:
    EFFECTIVE_USER = os.environ.get('EFFECTIVE_USER', 'hbase-python')
    PATH_MASTER = os.environ.get('PATH_MASTER', '/hbase/master')
    PATH_META_REGION = os.environ.get('PATH_META_REGION', '/hbase/meta-region-server')

    def effect_user(self, effect_user='hbase-python'):
        self.EFFECTIVE_USER = effect_user
        return self

    def master_path(self, master_path='/hbase/master'):
        self.PATH_MASTER = master_path
        return self

    def meta_region_path(self, meta_region_path='/hbase/meta-region-server'):
        self.PATH_META_REGION = meta_region_path
        return self
