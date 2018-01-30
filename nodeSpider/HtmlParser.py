# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:        HtmlParser
   Description:
   Author:           w
   Date：            2018/1/18 22:25
   Version:          python3.6
-------------------------------------------------
"""
import re
from urllib.parse import urlparse, urljoin
import pymysql

# DATABASE_NAME = 'kindle_test0'
# TABLE_NAME = 'books_book'

class HtmlParser(object):
    '''
    HTML内容解析器:
        一个对外接口:
            内容解析器: parser(url,content)
    '''

    # def __init__(self):
    #     self.conn = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db=DATABASE_NAME, charset='utf8')
    #     self.cursor = self.conn.cursor()
    #     self.sql = 'UPDATE ' + TABLE_NAME + ' SET bimg=%s,btag=%s,bauthor=%s,bstars=%s,bdoubanlink=%s WHERE id=%s'

    def parser(self, id_, content):
        '''
        用于解析网页内容抽取URL和数据
        :param url: 下载页面的URL
        :param content: 下载的网页内容
        :return:返回URL和数据
        '''
        if content is None:
            return None
        data = {}
        data['id'] = id_
        try:
            book = content['books'][0]
            data['img'] = book['image']
            data['tags'] = ' '.join([tag['name'] for tag in book['tags']]).strip()
            data['author'] = ' '.join([a for a in book['author']]).strip()
            data['stars'] = float(book['rating']['average'])
            data['doubanlink'] = book['alt']
            data['summary'] = book['summary'].replace('"', '`').replace("'", '`')
            data['error_code'] = '0'
        except Exception as e:
            print('网页解析错误！', e)
            raise Exception('PARSE_ERROR')

        return data
