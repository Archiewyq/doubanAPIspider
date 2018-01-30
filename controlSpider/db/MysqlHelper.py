# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:        mysql
   Description:
   Author:           w
   Date：            2018/1/28 11:34
   Version:          python3.6
-------------------------------------------------
"""
import pymysql
from controlSpider import _config

HOST = _config.DATABASE['host']
USER = _config.DATABASE['user']
PASSWD = _config.DATABASE['passwd']

class MysqlHelper(object):

    def __init__(self, DB):
        self.conn = pymysql.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, charset='utf8')
        self.cursor = self.conn.cursor()

    def select(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            datas = self.cursor.fetchall()
        except Exception as e:
            datas = None
            print('[！db]  Select error--->', e)
        return datas

    def update(self, sql):
        # print(sql)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return 0
        except Exception as e:
            print('[！db]  Update error--->', e)
            return 1

    def delete(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print('[！db]  Delete error--->', e)
            pass
