# -*- coding: UTF-8 -*-
import requests, re, redisutil, time, random, threading
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import common
from bs4 import BeautifulSoup

# 将列表页插入redis
def parseList(url):
    #lst = re.compile(r'http:\/\/91\.91p17\.space\/view_video\.php\?viewkey\=\w+').findall(common.visit(url))
    m = common.visit(url)
    soup = BeautifulSoup(m, "lxml")
    for url  in soup.find_all(name='a',attrs={"href":re.compile(r'^http:(.*)view_video(.*)')}):
        lst = url.get('href')
        if not redisutil.exists(lst, common.KEY):
            redisutil.add(lst, common.KEY)
            print(threading.current_thread().name, " insert into redis ", lst)
        else:
            print(threading.current_thread().name, " redis 已经存在，不再访问 ", lst)

'''
    线程主方法
'''
def enter(**kwargs):
    start = kwargs["start"]
    end = kwargs["end"]
    for page in range(start, end):
        url = common.URL + "/v.php?next=watch&page=" + str(page)
        try:
            print(threading.current_thread().name, " 解析 ", page, " 页 ", url)
            parseList(url)
            time.sleep(random.randint(1, 3))
        except RuntimeError:
            print(threading.current_thread().name, " visiting page ", page, " occurs some errors ", RuntimeError.__with_traceback__)
            redisutil.add(url, "91_error")
            continue
    # current thread has finished, log it and we can easily know it
    with open(common.LOG, "a") as f:
    	f.write("线程" + threading.current_thread().name + " 已经完成抓取 \n")

# 运行方法
def start():
    thread_list = []
    total = common.getNumber()
    thread_total = 5 # 线程总数，默认为5，如果抓取页面小于5，则线程总数就是抓取的页面总数

    if total <= 5:
        page_size = 1
        thread_total = total
    else:
        page_size = total / 5 # start 5 thread to visit

    for i in range(1, thread_total + 1):
        start = (i - 1) * page_size + 1
        end = i * page_size + 1
        name = "a" + str(i)
        t = threading.Thread(target=enter, name=name, kwargs={"start":start,"end":end})
        thread_list.append(t)

    for t in thread_list:
        t.start()

    for t in thread_list:
        t.join()

    print("all thread over")
