# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:        MainSpider
   Description:
   Author:           w
   Date：            2018/1/18 22:24
   Version:          python3.6
-------------------------------------------------
"""
from multiprocessing.managers import BaseManager
from nodeSpider.HtmlDownLoader import HtmlDownloader
from nodeSpider.HtmlParser import HtmlParser
from nodeSpider import _config
import time, re

pattern1 = re.compile(r'\(.*\)')
pattern2 = re.compile(r'\（.*\）')
pattern3 = re.compile(r'\[|\]|【|】|\)|（|\(|）')

class SpiderWork(object):
    '''
    爬虫节点调度器:
        初始化:
            从主节点注册任务
            初始化下载器和解析器
        开始爬取:
    '''
    def __init__(self):
        # 初始化分布式进程中的工作节点的连接工作
        # 实现第一步：使用BaseManager注册获取Queue的方法名称
        BaseManager.register('get_task_queue')
        BaseManager.register('get_proxy_queue')
        BaseManager.register('get_result_queue')
        # 实现第二步：连接到服务器:
        server_addr = _config.HOST_PORT['host']
        server_port = _config.HOST_PORT['port']
        authkey = _config.HOST_PORT['authkey']
        print('Connect to server %s...' % server_addr)
        # 端口和验证口令注意保持与服务进程设置的完全一致:
        self.m = BaseManager(address=(server_addr, server_port), authkey=authkey.encode('utf-8'))
        try:
            self.m.connect()
            # 实现第三步：获取Queue的对象:
            self.task = self.m.get_task_queue()
            self.proxy = self.m.get_proxy_queue()
            self.result = self.m.get_result_queue()
        except ConnectionRefusedError:
            print('[!]  Server refuse to connect.')
            exit(-1)
        except TimeoutError:
            print('[!]  Connect timeout.')
            exit(-1)

        # 初始化网页下载器和解析器
        self.downloader = HtmlDownloader()
        self.parser = HtmlParser()
        print('*********NodeSpider init finished!*********')


    def crawl(self, counter):
        print('---------- NodeSpider start work ----------')
        while True:
            try:
                if not self.task.empty():
                    task = self.task.get()
                    if task == 'end':
                        print('[·]  Node received "end" command...')
                        #接着通知其它节点停止工作
                        self.result.put({'old_id': 'end', 'data': 'end'})
                        exit(0)

                    data = task.split('$$')
                    id_ = data[0]
                    # 书名不规则，所以需要清洗
                    word = data[1].split('-')[0].split('：')[0].split('@')[0]
                    word = re.sub(pattern1, '', word)
                    word = re.sub(pattern2, '', word)
                    word = re.sub(pattern3, '', word)[:20].split(' ')[0].strip()
                    print('>>>  Crawling：%s-%s' % (id_, word))
                    if not self.proxy.empty():
                        proxy = self.proxy.get()
                        if proxy == 'null':
                            proxy = None
                    else:
                        proxy = None

                    results = {}
                    results['word'] = data[1]
                    if proxy:
                        results['ip'] = proxy.split(':')[0]
                    else:
                        results['ip'] = ''

                    try:
                        content = self.downloader.download(word, proxy)
                        results = self.parser.parser(id_, content)
                    except Exception as e:
                        results['id'] = id_
                        if 'PROXY_ERROR' in e.args:
                            print('[x]  Bad proxy.')
                            results['error_code'] = '1'
                        elif 'CONNECT_TIMEOUT_ERROR' in e.args:
                            print('[x]  Connect timeout.')
                            results['error_code'] = '2'
                        elif 'IP_LIMITED_ERROR' in e.args:
                            print('[x]  Ip limmited.')
                            results['error_code'] = '3'
                        elif 'NO_RESULT_ERROR' in e.args:
                            print('[x]  No result.')
                            results['error_code'] = '4'
                        elif 'PARSE_ERROR' in e.args:
                            print('[x]  Parse error.')
                            results['error_code'] = '-1'
                        elif 'UNKNOWN_ERROR' in e.args:
                            print('[x]  Unknown error.')
                            results['error_code'] = '-2'
                        else:
                            print('其他待捕获异常--->', e)
                            results['error_code'] = '-3'
                    if results['error_code'] == '0':
                        print('[√]  Succeeded！')
                    self.result.put({'old_id': id_, 'data': results})
                else:
                    print('[·]  No tasks.')
                    time.sleep(1)
            except EOFError as e:
                print("[!]  Connect error！")
                return
            # except Exception as e:
            #     print(e)
            #     print('[!]  Crawled fail.')
        print('Over!')
        return


if __name__ == "__main__":
    counter = 0
    spider = SpiderWork()
    spider.crawl(counter)
