# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:        UrlManager
   Description:
   Author:           w
   Date：            2018/1/18 21:23
   Version:          python3.6
-------------------------------------------------
"""
try:
    import cPickle as pickle
except:
    import pickle
import hashlib

class UrlManager(object):
    '''
    url管理器：
        两个url集合（集合可以自动去重）：
            未爬取url: new_urls
            已爬取url: old_urls
        七个接口：
            判断是否有待爬取url:    has_new_url()
            添加新url到集合:        add_new_url(url)、add_new_urls(urls)
            取出一个url:            get_new_url()
            获取未爬取url集合大小:  new_url_size()
            获取已爬取url集合大小:  old_url_size()
            进度保存:               save_procese(path, data)
            进度加载:               load_process(path)
    '''

    def __init__(self):
        self.new_urls = self.load_process('new_urls.txt')  # 未爬取URL集合
        self.old_urls = self.load_process('old_urls.txt')  # 已爬取URL集合

    def has_new_url(self):
        '''
        判断是否有待爬取的url
        :return: True or False
        '''
        return self.new_url_size() != 0

    def get_new_url(self):
        '''
        获取一个未爬取的url
        :return: 一个未爬取的url
        '''
        new_url = self.new_urls.pop()
        # m = hashlib.md5()
        # m.update(new_url.encode('utf-8'))
        # # 为了减少存储量，存储已爬取的url的md5而不是url
        # self.old_urls.add(m.hexdigest())

        return new_url

    def add_new_url(self, url):
        '''
        添加一个新url到未爬取集合
        :param url: 单个url
        :return:    无
        '''
        if url is None:
            return
        m = hashlib.md5()
        m.update(url.encode('utf-8'))
        url_md5 = m.hexdigest()
        if url not in self.new_urls and url_md5 not in self.old_urls:
            self.new_urls.add(url)

    def add_old_url(self, url):
        if url is None:
            return
        m = hashlib.md5()
        m.update(url.encode('utf-8'))
        url_md5 = m.hexdigest()
        if url_md5 not in self.old_urls:
            self.old_urls.add(url)

    def add_new_urls(self, urls):
        '''
        添加多个新url到未爬取的url集合
        :param urls:多个url
        :return:    无
        '''
        if urls is None or len(urls)==0:
            return
        for url in urls:
            self.add_new_url(url)

    def add_old_urls(self, urls):
        '''
        添加多个新url到未爬取的url集合
        :param urls:多个url
        :return:    无
        '''
        if urls is None or len(urls)==0:
            return
        for url in urls:
            self.add_old_url(url)

    def new_url_size(self):
        '''
        获取未爬取url集合大小
        :return: 未爬取url集合大小
        '''
        return len(self.new_urls)

    def old_url_size(self):
        '''
        获取已爬取url集合大小
        :return: 已爬取url集合大小
        '''
        return len(self.old_urls)

    def save_process(self, path, data):
        '''
        进度保存
        :param path:    保存文件路径
        :param data:    数据
        :return:
        '''
        print('[!]保存进度：%s' % path)
        with open(path, 'wb') as f:
            pickle.dump(data, f)    # 使用数据持久存储模块pickle

        exit(0)

    def load_process(self, path):
        '''
        从本地文件加载爬虫进度
        :param path:    本地文件路径
        :return:
        '''
        print('[+] 从文件加载进度：%s' % path)
        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
                return data
        except:
            print('[!]无进度文件，创建：%s' % path)
        return set()
