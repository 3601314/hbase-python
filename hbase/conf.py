#!/usr/bin/env python3

"""
@author: xi
@since: 2018-07-03
"""

num_threads_per_conn = 5
num_tasks_per_conn = 100

# Mofify This Configuration on use zk path
EFFECTIVE_USER = "hbase-python"
PATH_MASTER = '/hbase/master'
PATH_META_REGION = '/hbase/meta-region-server'