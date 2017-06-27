# -*- coding: utf-8 -*-
"""
-------------------------------------------------
    Filename:   mobikeCrawlerMongo.py
    Author:     Helyao
    Description:
        Get Current Mobikes Position in Multi-Process with MongoDB.
-------------------------------------------------
    Change Logs:
    2017-06-25 6:40pm   create
    2017-06-27 6:53pm   re-divided to 28 pieces
-------------------------------------------------
"""
import os
import sys
import json
import redis
import pymongo
import requests
import datetime
import numpy as np
import configparser
import multiprocessing
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

CONFIG_INI = r'config.ini'  # config file
BLOCK_NUM = 28              # divided 28 pieces

class MobikeCrawler(object):

    # Init, get parameters from config file
    def __init__(self, table, index=0, mode='demo'):
        try:
            # Start Timestamp
            self.startstamp = datetime.datetime.now()
            # Get Settings
            cp = configparser.ConfigParser()
            cp.read(CONFIG_INI)
            # redis
            redis_host = cp.get('redis', 'host')
            redis_port = cp.getint('redis', 'port')
            redis_db = cp.getint('redis', 'db')
            self.proxy = cp.get('redis', 'workin')
            rconn = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)
            self.rdb = redis.Redis(connection_pool=rconn)
            # mongo
            mongo_host = cp.get('mongo', 'host')
            mongo_port = int(cp.get('mongo', 'port'))
            mongo_db = cp.get('mongo', 'database')
            self.mongo_data = table
            self.mongo_log = cp.get('mongo', 'log')
            self.mongo_error = cp.get('mongo', 'error')
            mconn = pymongo.MongoClient(host=mongo_host, port=mongo_port)
            self.mdb = mconn[mongo_db]
            # Get Task Parameters
            self.mode = mode
            self.index = index
            self.left = float(cp.get(self.mode, 'left'))
            self.right = float(cp.get(self.mode, 'right'))
            self.top = float(cp.get(self.mode, 'top'))
            self.bottom = float(cp.get(self.mode, 'bottom'))
            self.offset = float(cp.get('parameter', 'offset'))
            self.maxthread = int(cp.get('parameter', 'maxthread'))
        except Exception as ex:
            print('[MobikeCrawler-{}]: {}'.format(index, ex))

    # run task
    def run(self):
        print('The task \"{task}\" get the mobikes position from point({left}, {top}) to point({right}, {bottom})'.format(
            task=self.mode, left=self.left, top=self.top, right=self.right, bottom=self.bottom))
        results = []
        executor = ThreadPoolExecutor(max_workers=self.maxthread)
        lon_range = np.arange(self.left, self.right, self.offset)
        lat_range = np.arange(self.top, self.bottom, -self.offset)
        print('This task will be divied {} pieces'.format(len(lon_range) * len(lat_range)))
        # print(lon_range)
        # print(lat_range)
        for lon in lon_range:
            for lat in lat_range:
                results.append(executor.submit(self.__getMobikes, (lon, lat)))
        list(as_completed(results))
        self.endstamp = datetime.datetime.now()
        self.cost = (self.endstamp - self.startstamp).seconds
        self.startstamp = self.startstamp.strftime("%Y-%m-%d %H:%M:%S")
        self.endstamp = self.endstamp.strftime("%Y-%m-%d %H:%M:%S")
        message = {
            'left': self.left,
            'right': self.right,
            'top': self.top,
            'bottom': self.bottom,
            'start': self.startstamp,
            'end': self.endstamp,
            'cost': self.cost,
            'host': self.index,
            'table': self.mongo_data,
        }
        # Write log before finish
        try:
            collog = self.mdb[self.mongo_log]
            collog.insert(message)
        except:
            pass
        return message

    # Get mobike info around args=(lon, lat)
    def __getMobikes(self, args):
        try:
            url = "https://mwx.mobike.com/mobike-api/rent/nearbyBikesInfo.do"
            payload = "latitude={lat}&longitude={lon}&errMsg=getMapCenterLocation".format(lon=args[0], lat=args[1])
            headers = {
                'charset': "utf-8",
                'platform': "4",
                "referer": "https://servicewechat.com/wx40f112341ae33edb/1/",
                'content-type': "application/x-www-form-urlencoded",
                'user-agent': "MicroMessenger/6.5.4.1000 NetType/WIFI Language/zh_CN",
                'host': "mwx.mobike.com",
                'connection': "Keep-Alive",
                'accept-encoding': "gzip",
                'cache-control': "no-cache"
            }
            self.__request(headers, payload, url, args)
        except Exception as ex:
            print('[MobikeCrawler-{}.getMobikes]: {}'.format(self.index, ex))

    # Post api interface
    def __request(self, headers, payload, url, args, num_retries=5):
        try:
            proxy = self.__getProxy()
            if proxy:
                proxies = {"http": "http://{proxy}".format(proxy=proxy), "https": "https://{proxy}".format(proxy=proxy)}
                response = requests.request('POST', url, data=payload, headers=headers, proxies=proxies, timeout=10, verify=False)
            else:
                response = requests.request('POST', url, data=payload, headers=headers, timeout=10, verify=False)
            code = response.status_code
            print(code)
            if (num_retries > 0):
                if (500 <= code < 600):
                    return self.__request(headers, payload, url, args, num_retries - 1)
            else:
                # print('Cannot get the data near point=({lon}, {lat})'.format(lon=args[0], lat=args[1]))
                return
            results = json.loads(response.text)
            coldata = self.mdb[self.mongo_data]
            for x in results['object']:
                # restore data in mongodb
                x['time'] = self.__now()
                x['host'] = self.index
                coldata.insert(x)
            return
        except Exception as ex:
            # print('[MobikeCrawler-{}.getMobikes]: {}'.format(self.index, ex))
            if (num_retries > 0):
                return self.__request(headers, payload, url, args, num_retries - 1)
            else:
                # print('Cannot get the data near point=({lon}, {lat})'.format(lon=args[0], lat=args[1]))
                # write error in mongodb
                colerror = self.mdb[self.mongo_error]
                error = {
                    'time': self.__now(),
                    'table': self.mongo_data,
                    'lon': args[0],
                    'lat': args[1],
                    'host': self.index
                }
                colerror.insert(error)
                return

    # Get proxy from redis
    def __getProxy(self):
        try:
            if self.rdb.scard(self.proxy) > 0:
                item = self.rdb.srandmember(self.proxy, 1)[0].decode('utf-8')
                return item
            else:
                return
        except Exception as ex:
            print('[MobikeCrawler-{}.getProxy]: {}'.format(self.index, ex))

    # Get current timestamp with string type
    def __now(self):
        now = datetime.datetime.now()
        return now.strftime('%Y-%m-%d %H:%M:%S')

def do(tablename, index):
    mobike = MobikeCrawler(table=tablename, index=index, mode='Block{}'.format(index))
    res = mobike.run()
    return res

if __name__ == '__main__':
    # Get mysql configuration
    cp = configparser.ConfigParser()
    cp.read(CONFIG_INI)
    mysql_host = cp.get('mysql', 'host')
    mysql_port = cp.get('mysql', 'port')
    mysql_user = cp.get('mysql', 'user')
    mysql_pass = cp.get('mysql', 'passwd')
    mysql_db = cp.get('mysql', 'database')
    mysql_seed = cp.get('mysql', 'seed')
    # while True:
    # Start TimeStamp
    startstamp = datetime.datetime.now()
    # Table Name
    tablename = 'mobike_{}'.format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    # Run
    results = []
    pool = multiprocessing.Pool(processes=BLOCK_NUM)
    for num in range(1, BLOCK_NUM+1):
        results.append(pool.apply_async(func=do, args=(tablename, num)))
    pool.close()
    pool.join()
    # End TimeStamp
    endstamp = datetime.datetime.now()
    # Results
    for item in results:
        print(item.get())
    print('Project costs {}s'.format((endstamp - startstamp).seconds))
