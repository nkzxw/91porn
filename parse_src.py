# -*- coding: UTF-8 -*-
import requests, re, redis, redisutil, time, random
from pyquery import PyQuery as pq
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import threading
import common
from bs4 import BeautifulSoup

# 将列表页插入redis
def parse(url, c, ts):
    d = pq(common.visit(url))
    src = d("video").find("source").attr("src")
    if src != None:
        m = common.visit(url)
        soup = BeautifulSoup(m, "lxml")
        con = soup.find(name="div", attrs={"class": "boxPart"}).text
        con = "".join(con.split())
        t = con.split(":")
        times = int(t[1])
        ts = int(ts)
        if times >= ts:
            print( threading.current_thread().name,  " insert into redis ", src)
            redisutil.add(src, common.KEY_SRC)
            c.lrem(common.KEY, 1, url)
        else:
            print(threading.current_thread().name,  src, "Not enough time")
    else:
        print(threading.current_thread().name,  src, "解析为None, 插入 redis_error")
        redisutil.add(src, common.KEY_NONE)

def enter(**kwargs):
    start = kwargs["start"]
    end = kwargs["end"]
    ts = kwargs["ts"]
    c = redisutil.connect()
    lst = c.lrange(common.KEY, int(start), int(end))

    for a in lst:
         print(threading.current_thread().name,  " parsing url ", a)
         parse(a, c, ts)
         time.sleep(0.1)
    with open(common.PARSE_LOG, "a") as f:
        f.write(threading.current_thread().name + " 已经解析完毕.\n")

def start():
    thread_list = []
    total = redisutil.total(common.KEY   )
    ts = common.getTime()
    page_size = 0
    thread_total = 5

    if total <= 5:
        page_size = 1
        thread_total = total
    else:
        page_size = total / 5

    for t in range(1, thread_total + 1):
        start = (t - 1) * page_size + 1
        end = t * page_size + 1
        name = "a" + str(t)
        t = threading.Thread(target=enter, name=name, kwargs={"start":start, "end":end,"ts":ts})
        thread_list.append(t)

    for t in thread_list:
        t.start()

    for t in thread_list:
        t.join()

    print("all thread over")