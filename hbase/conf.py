#!/usr/bin/env python3

"""
@author: xi
@since: 2018-07-03
"""
import os

num_threads_per_conn = 5
num_tasks_per_conn = 100

thread_pool_size = 10
fail_task_retry = 3

# Modify This Configuration on use zk path
class Conf:
    EFFECTIVE_USER = os.environ.get('EFFECTIVE_USER', 'hbase-python')
    PATH_MASTER = os.environ.get('PATH_MASTER', '/hbase/master')
    PATH_META_REGION = os.environ.get('PATH_META_REGION', '/hbase/meta-region-server')

    @classmethod
    def effect_user(cls, effect_user='hbase-python'):
        cls.EFFECTIVE_USER = effect_user
        return cls

    @classmethod
    def master_path(cls, master_path='/hbase/master'):
        cls.PATH_MASTER = master_path
        return cls

    @classmethod
    def meta_region_path(cls, meta_region_path='/hbase/meta-region-server'):
        cls.PATH_META_REGION = meta_region_path
        return cls