# -*- coding: utf-8 -*-
"""
-------------------------------------------------
    Filename:   mobikeCrawlerPool.py
    Author:     Helyao
    Description:
        Get Current Mobikes Position in Multi-Process with mysql connection pool.
-------------------------------------------------
    Change Logs:
    2017-06-22 6:33pm   create
-------------------------------------------------
"""

import redis
import ujson
import pymysql
import requests
import datetime
import numpy as np
import configparser
import multiprocessing
from mysqlConnPool import MysqlPool
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

CONFIG_INI = r'config.ini'  # configuration
BLOCK_NUM = 12

class MobikeCrawler():

    def __init__(self, table, index=0, mode='demo'):
        try:
            self.startstamp = datetime.datetime.now()
            cp = configparser.ConfigParser()
            cp.read(CONFIG_INI)
            # Get Redis Configurations
            redis_host = cp.get('redis', 'host')
            redis_port = cp.getint('redis', 'port')
            redis_db = cp.getint('redis', 'db')
            self.proxy = cp.get('redis', 'workin')
            rconn = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)
            self.rdb = redis.Redis(connection_pool=rconn)
            # Get Task Parameters
            self.mode = mode
            self.index = index
            self.left = float(cp.get(self.mode, 'left'))
            self.right = float(cp.get(self.mode, 'right'))
            self.top = float(cp.get(self.mode, 'top'))
            self.bottom = float(cp.get(self.mode, 'bottom'))
            self.offset = float(cp.get('parameter', 'offset'))
            self.maxthread = int(cp.get('parameter', 'maxthread'))
            # Get Mysql Connection Poll
            self.dbPool = MysqlPool()
            # Get Mysql Table
            self.mysql_table = table
            self.mysql_seed = cp.get('mysql', 'seed')
            self.mysql_log = cp.get('mysql', 'log')
            self.__initMysqlTable()
        except Exception as ex:
            print('[MobikeCrawler-{}]: {}'.format(index, ex))

    def __initMysqlTable(self):
        conn = self.dbPool.getMysqlConn()
        cursor = conn.cursor()
        # Check whether self.mysql_table existed or not
        cursor.execute("select count(*) from information_schema.tables where table_name='{}'".format(self.mysql_table))
        item = cursor.fetchall()[0][0]
        if item < 1:
            # cannot find the table, create temp table to restore results
            print('Cannot find {} table'.format(self.mysql_table))
            self.mysql_table = 'mobike_temp_{}_{}'.format(self.index, datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            print('Create temp table {} to restore data'.format(self.mysql_table))
            cursor.execute('create table {table} like {seed}'.format(table=self.mysql_table, seed=self.mysql_seed))
        cursor.execute('commit')
        cursor.close()
        conn.close()

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

    def __request(self, headers, payload, url, args, num_retries=5):
        try:
            proxy = self.__getProxy()
            proxies = {"http": "http://{proxy}".format(proxy=proxy), "https": "https://{proxy}".format(proxy=proxy)}
            response = requests.request('POST', url, data=payload, headers=headers, proxies=proxies, timeout=10,
                                        verify=False)
            code = response.status_code
            print(code)
            if (num_retries > 0):
                if (500 <= code < 600):
                    return self.request(headers, payload, url, args, num_retries - 1)
            else:
                print('Cannot get the data near point=({lon}, {lat})'.format(lon=args[0], lat=args[
                    1]))  # NOTE: should write into db
                return
            results = ujson.decode(response.text)['object']
            conn = self.dbPool.getMysqlConn()
            cursor = conn.cursor()
            for x in results:
                sql = 'INSERT INTO {}(time, bikeid, biketype, distid, distnum, type, x, y, host) VALUES (NOW(),\'{}\',{},\'{}\',{},{},{},{},{})'.format(
                    self.mysql_table, x['bikeIds'], int(x['biketype']), int(x['distId']), x['distNum'],
                    x['type'], x['distX'], x['distY'], self.index)
                cursor.execute(sql)
            cursor.execute('commit')
            cursor.close()
            conn.close()
            return
        except Exception as ex:
            print('[MobikeCrawler-{}.getMobikes]: {}'.format(self.index, ex))
            if (num_retries > 0):
                return self.request(headers, payload, url, args, num_retries - 1)
            else:
                print('Cannot get the data near point=({lon}, {lat})'.format(lon=args[0], lat=args[1]))
                return

    def __getProxy(self):
        try:
            item = self.rdb.srandmember(self.proxy, 1)[0].decode('utf-8')
            return item
        except Exception as ex:
            print('[MobikeCrawler-{}.getProxy]: {}'.format(self.index, ex))

    def run(self):
        print('The task \"{task}\" get the mobikes position from point({left}, {top}) to point({right}, {bottom})'.format(
                task=self.mode, left=self.left, top=self.top, right=self.right, bottom=self.bottom))
        results = []
        executor = ThreadPoolExecutor(max_workers=self.maxthread)
        lon_range = np.arange(self.left, self.right, self.offset)
        lat_range = np.arange(self.top, self.bottom, -self.offset)
        print('This task will be divied {} pieces'.format(len(lon_range) * len(lat_range)))
        print(lon_range)
        print(lat_range)
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
            'index': self.index
        }
        return message

    def __writeLog(self):
        try:
            sql = 'insert into {table}(`start`, `end`, `left`, `right`, `top`, `bottom`, `cost`, `host`) values(\'{start}\', \'{end}\', {left}, {right}, {top}, {bottom}, {cost}, {host})'.format(
                table=self.mysql_log, start=self.startstamp, end=self.endstamp, left=self.left, right=self.right,
                top=self.top, bottom=self.bottom, cost=self.cost, host=self.index)
            conn = self.dbPool.getMysqlConn()
            cursor = conn.cursor()
            cursor.execute(sql)
            cursor.execute('commit')
            cursor.close()
            conn.close()
        except:
            pass

    def __del__(self):
        self.__writeLog()

def createMysqlTable(mysql_table):
    global mysql_host, mysql_port, mysql_user, mysql_pass, mysql_db, mysql_seed
    connect = pymysql.connect(host=mysql_host, port=int(mysql_port), user=mysql_user, passwd=mysql_pass, db=mysql_db)
    cursor = connect.cursor()
    cursor.execute('create table {table} like {seed}'.format(table=mysql_table, seed=mysql_seed))
    cursor.execute('commit')
    cursor.close()
    connect.close()

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
    # Start TimeStamp
    startstamp = datetime.datetime.now()
    # Table Name
    tablename = 'mobike_{}'.format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    createMysqlTable(tablename)
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
