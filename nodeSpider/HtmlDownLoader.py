# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:        HtmlDownLoader
   Description:
   Author:           w
   Date：            2018/1/18 22:25
   Version:          python3.6
-------------------------------------------------
"""
import requests
from requests import exceptions as REQUESTS_ERROR
import json
try:
    from nodeSpider import _config
except:
    import _congfig
import random, time

class HtmlDownloader(object):
    '''
    HTML下载器：
        一个接口：
            网页下载接口: downloader(url)
    '''
    def __init__(self):
        self.headers = _config.get_header()
        self.base_url = 'https://api.douban.com/v2/book/search?q=%s&count=1'

    def download(self, keyword, proxy=None):
        if keyword is None:
            return None
        if proxy:
            proxy = {
                'https': 'https://'+proxy
            }
        # print(proxy)
        # proxy = None
        try:
            r = requests.get(self.base_url % keyword, headers=self.headers, proxies=proxy, timeout=4)
        except REQUESTS_ERROR.ProxyError:
            raise Exception('PROXY_ERROR')
        except REQUESTS_ERROR.ConnectTimeout:
            raise Exception('CONNECT_TIMEOUT_ERROR')
        except Exception as e:
            print(e)
            print('待捕获异常！！！！')
            raise Exception('UNKONWN_ERROR')
        if r.status_code == 200:
            text = json.loads(r.text)
            # print(text)
            # 搜索无结果
            if text['count'] == 0:
                raise Exception('NO_RESULT_ERROR')
            return text
        elif r.status_code == 400:
            raise Exception('IP_LIMITED_ERROR')


if __name__ == '__main__':
    d = HtmlDownloader()
    for i in range(5):
        d.download('https://book.douban.com/subject_search?search_text=%E8%A5%BF%E6%B8%B8%E8%AE%B0&cat=1001')
