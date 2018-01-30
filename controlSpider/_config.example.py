# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:        _config
   Description:
   Author:           w
   Date：            2018/1/24 14:52
   Version:          python3.6
-------------------------------------------------
"""
DEBUG = False

# 任务队列缓存数
QUEUE_NUM = 20

if DEBUG:
    DATABASE = {
        'host': '',
        'port': '',
        'user': 'xxx',
        'passwd': 'xxx',
        'database': 'xxx',
        'table': 'xxx',
        'database2': 'xxx',
        'table2': 'xxx',
    }
    HOST_PORT = {
        'host': 'xxx.xxx.xxx.xxx',
        'port': xxxxx,
        'authkey': 'xxxx',
    }

else:
    DATABASE = {
        'host': '',
        'port': '',
        'user': 'xxxx',
        'passwd': 'xxxx',
        'database': 'xxxx',
        'table': 'xxxx',
        'database2': 'xxx',
        'table2': 'xxx',
    }
    HOST_PORT = {
        'host': 'xxx.xxx.xxx.xxx',
        'port': xxxx,
        'authkey': 'xxxx',
    }