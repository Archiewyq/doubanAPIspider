# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:        NodeManager
   Description:
   Author:           w
   Date：            2018/1/18 21:21
   Version:          python3.6
-------------------------------------------------
"""
from multiprocessing.managers import BaseManager
from multiprocessing import Process, Queue
try:
    from UrlManager import UrlManager
except:
    from controlSpider.UrlManager import UrlManager
try:
    import _config
except:
    from controlSpider import _config
try:
    from db.MysqlHelper import MysqlHelper
except:
    from controlSpider.db.MysqlHelper import MysqlHelper
import time, re, pymysql

pattern1 = re.compile(r'\(.*\)')
pattern2 = re.compile(r'\（.*\）')
pattern3 = re.compile(r'\[|\]|【|】|\)|（|\(|）')

HOST = _config.DATABASE['host']
USER = _config.DATABASE['user']
PASSWD = _config.DATABASE['passwd']
DATABASE_NAME = _config.DATABASE['database']
TABLE_NAME = _config.DATABASE['table']
DATABASE_NAME2 = _config.DATABASE['database2']
TABLE_NAME2 = _config.DATABASE['table2']

class NodeManager(object):

    # def __init__(self):
    #     self.db_data = MysqlHelper(DATABASE_NAME)
    #     self.db_proxy = MysqlHelper(DATABASE_NAME2)

    def start_Manager(self, task_queue, proxy_queue, result_queue):
        '''
        创建分布式爬虫管理器
        :param url_queue:   url队列
        :param result_queue:result队列
        :return:            manager对象
        '''
        def _get_task():
            return task_queue

        def _get_proxy():
            return proxy_queue

        def _get_result():
            return result_queue
        # 在网络上注册两个管理队列
        BaseManager.register('get_task_queue', callable=_get_task)
        BaseManager.register('get_proxy_queue', callable=_get_proxy)
        BaseManager.register('get_result_queue', callable=_get_result)
        # 监听主机 端口，设置身份认证口令
        host = _config.HOST_PORT['host']
        port = _config.HOST_PORT['port']
        authkey = _config.HOST_PORT['authkey']
        manager = BaseManager(address=(host, port), authkey=authkey.encode('utf-8'))
        return manager

    def url_manager_process(self, task_queue):
        '''
        url管理器进程
        :param task_queue:   url队列
        :param conn_queue:
        :param root_url:    起始url
        :return:
        '''
        sql = 'SELECT id,bname FROM ' + TABLE_NAME + ' WHERE bdoubanlink IS NULL OR bdoubanlink=""'
        url_manager = UrlManager()
        db = MysqlHelper(DATABASE_NAME)

        while True:
            if not url_manager.has_new_url():
                datas = db.select(sql)
                if datas:
                    for data in datas:
                        task_data = str(data[0])+'$$'+data[1].strip()
                        url_manager.add_new_url(task_data)
                    print('[√]  Datas has been read from database!')
                else:
                    print('[！]  Fetch database null.')
                    exit(-1)

            # 添加爬虫结束条件
            if not url_manager.has_new_url():
                # 通知节点停止工作
                task_queue.put('end')
                print('[·]  Controler send "end" command.')
                return

            while task_queue.qsize() < _config.QUEUE_NUM and url_manager.has_new_url():
                # 从url管理器获取新的url
                new_url = url_manager.get_new_url()
                # 将url分发下去
                task_queue.put(new_url)
                print('[+]  >>> %s' % new_url)

            # try:
            #     if not conn_queue.empty():
            #         urls = conn_queue.get()
            #         url_manager.add_old_urls(urls)
            # except:
            #     pass

    def result_process(self, result_queue, store_queue):
        '''
        # 结果分析进程
        :param result_queue:
        :param conn_queue:
        :param store_queue:
        :return:
        '''
        while True:
            try:
                if not result_queue.empty():
                    content = result_queue.get(True)
                    if content['old_id'] == 'end':
                        print('结果分析进程收到结束命令')
                        store_queue.put('end')
                        return
                    # conn_queue.put(content['old_url'])
                    store_queue.put(content['data'])
                else:
                    # 休息一会儿
                    time.sleep(0.1)
            except BaseException as e:
                # 休息一会儿
                time.sleep(0.1)

    def store_process(self, store_queue, task_queue):
        '''
        # 结果存储进程
        :param store_queue:
        :return:
        '''
        db = MysqlHelper(DATABASE_NAME)
        sql = 'UPDATE '+TABLE_NAME+' SET bimg="%s", btag="%s", bauthor="%s", bstars="%s", bdoubanlink="%s", bsummary="%s" WHERE id=%s'
        sql_ = 'UPDATE '+TABLE_NAME+' SET bdoubanlink="null" WHERE id=%s'

        db2 = MysqlHelper(DATABASE_NAME2)
        sql2 = 'UPDATE ' + TABLE_NAME2 + ' SET fail=fail+1 WHERE ip="%s"'

        while True:
            if not store_queue.empty():
                data = store_queue.get(True)
                # print(data)
                if data == 'end':
                    print('存储进程接到结束命令')
                    return
                if data['error_code'] == '0':
                    c = db.update(sql % (data['img'], data['tags'], data['author'], data['stars'], data['doubanlink'], data['summary'], data['id']))
                    if c == 0:
                        print('[√]  Update success ', data['id'])
                    else:
                        print('[x]  Update error ', data['id'])
                elif data['error_code'] in ['1', '2']:
                    print('[·]  Update error %s---->Bad proxy' % data['id'])
                    c = db2.update(sql2 % (data['ip'],))
                    if c == 0:
                        print('[·]  Bad proxy.')
                    else:
                        print('[x]  Bad proxy.')
                    task_queue.put(data['id'] + '$$' + data['word'])
                elif data['error_code'] == '3':
                    print('[·]  Update error %s---->IP limited.' % data['id'])
                    task_queue.put(data['id'] + '$$' + data['word'])
                elif data['error_code'] == '4':
                    print('[√]  Not found ', data['id'])
                    db.update(sql_ % (data['id'],))

            else:
                # 休息一会儿
                time.sleep(0.1)

    def proxy_process(self, proxy_queue):
        # print('proxy_process start------------------')
        while True:
            if proxy_queue.empty():
                print('补充IP')
                db = MysqlHelper(DATABASE_NAME2)
                sql = 'SELECT ip,port,fail FROM ' + TABLE_NAME2
                sql2 = 'DELETE FROM '+TABLE_NAME2+' WHERE ip="%s"'
                datas = db.select(sql)
                if datas:
                    for data in datas:
                        if int(data[2]) >= 5:
                            db.delete(sql2 % data[0])
                            proxy_queue.put('null')
                        else:
                            proxy_queue.put(str(data[0])+':'+str(data[1]))

if __name__ == '__main__':
    # root_url = 'http://music.163.com/#/song?id=149229'
    # 初始化队列
    task_queue = Queue()
    result_queue = Queue()
    store_queue = Queue()
    conn_queue = Queue()
    proxy_queue = Queue()

    # 创建分布式管理器
    node = NodeManager()
    manager = node.start_Manager(task_queue, proxy_queue, result_queue)
    # 创建url管理进程、数据提取进程、数据存储进程
    url_manager_process = Process(target=node.url_manager_process, args=(task_queue,))
    result_process = Process(target=node.result_process, args=(result_queue, store_queue,))
    store_process = Process(target=node.store_process, args=(store_queue,task_queue,))
    proxy_process = Process(target=node.proxy_process, args=(proxy_queue,))
    # 启动各个进程和分布式管理器
    proxy_process.start()
    url_manager_process.start()
    result_process.start()
    store_process.start()
    manager.get_server().serve_forever()


