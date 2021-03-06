# -*- coding: utf-8 -*-
"""
-------------------------------------------------
    Filename:   mobikeCrawler.py
    Author:     Helyao
    Description:
        Get Current Mobikes Position in given range
-------------------------------------------------
    Change Logs:
    2017-06-05 2:09pm   create
-------------------------------------------------
"""
import os
import time
import redis
import ujson
import pymysql
import requests
import datetime
import threading
import numpy as np
import configparser
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

CONFIG_INI = r'config.ini'

class MobikeCrawler():
    def __init__(self, mode='demo'):
        try:
            self.startstamp = datetime.datetime.now()
            cp = configparser.ConfigParser()
            cp.read(CONFIG_INI)
            # Get Redis Configurations
            redis_host = cp.get('redis', 'host')
            redis_port = cp.getint('redis', 'port')
            redis_db = cp.getint('redis', 'db')
            self.proxy = cp.get('redis', 'workin')
            print('Redis Connection: {host}:{port}/{db}?workin={workin}'.format(host=redis_host, port=redis_port, db=redis_db, workin=self.proxy))
            rconn = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)
            self.rdb = redis.Redis(connection_pool=rconn)
            # Get Mysql Configurations
            mysql_host = cp.get('mysql', 'host')
            mysql_port = cp.get('mysql', 'port')
            mysql_user = cp.get('mysql', 'user')
            mysql_pass = cp.get('mysql', 'passwd')
            mysql_db = cp.get('mysql', 'database')
            self.mysql_table = cp.get('mysql', 'table') if mode == 'demo' else 'mobike_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.mysql_seed = cp.get('mysql', 'seed')
            self.mysql_log = cp.get('mysql', 'log')
            print('Mysql Connection: {host}:{port}@{user}/{passwd} at {database}/{table}'.format(host=mysql_host, port=mysql_port, user=mysql_user,
                                                                                                 passwd=mysql_pass, database=mysql_db, table=self.mysql_table))
            self._mconn = pymysql.connect(host=mysql_host, port=int(mysql_port), user=mysql_user, passwd=mysql_pass, db=mysql_db)
            self.mdb = self._mconn.cursor()
            # Get Task Parameters
            self.mode = mode
            self.left = float(cp.get(self.mode, 'left'))
            self.right = float(cp.get(self.mode, 'right'))
            self.top = float(cp.get(self.mode, 'top'))
            self.bottom = float(cp.get(self.mode, 'bottom'))
            self.offset = float(cp.get('parameter', 'offset'))
            self.maxthread = int(cp.get('parameter', 'maxthread'))
            self.lock = threading.Lock()
            # Start Task Now by Settings in Config.ini
            # self.run()
        except Exception as ex:
            print('[MobikeCrawler]: {}'.format(ex))

    # Start Task
    def run(self):
        print('The task \"{task}\" get the mobikes position from point({left}, {top}) to point({right}, {bottom})'.format(task=self.mode, left=self.left, top=self.top, right=self.right, bottom=self.bottom))
        # init table to restore results
        self._initTable()
        # thread pool
        results = []
        executor = ThreadPoolExecutor(max_workers=self.maxthread)
        lon_range = np.arange(self.left, self.right, self.offset)
        lat_range = np.arange(self.top, self.bottom, -self.offset)
        print('This task will be divied {} pieces'.format(len(lon_range)*len(lat_range)))
        print(lon_range)
        print(lat_range)
        for lon in lon_range:
            for lat in lat_range:
                results.append(executor.submit(self.getMobikes, (lon, lat)))
        list(as_completed(results))

    def _initTable(self):
        if (self.mode == 'demo') :
            # When mode is DEMO, clear temp table
            self.mdb.execute('delete from {table}'.format(table=self.mysql_table))
        else:
            # When mode isn't DEMO, copy empty table <mobike_seed> to init
            self.mdb.execute('create table {table} like {seed}'.format(table=self.mysql_table, seed=self.mysql_seed))

    def getMobikes(self, args):
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
            self.request(headers, payload, url, args)
        except Exception as ex:
            print('[MobikeCrawler.getMobikes]: {}'.format(ex))

    def request(self, headers, payload, url, args, num_retries=5):
        try:
            proxy = self.getProxy()
            proxies = {"http": "http://{proxy}".format(proxy=proxy), "https": "https://{proxy}".format(proxy=proxy)}
            response = requests.request('POST', url, data=payload, headers=headers, proxies=proxies, timeout=10, verify=False)
            code = response.status_code
            print(code)
            if (num_retries > 0):
                if (500 <= code < 600):
                    return self.request(headers, payload, url, args, num_retries - 1)
            else:
                print('Cannot get the data near point=({lon}, {lat})'.format(lon=args[0], lat=args[1]))
                return
            results = ujson.decode(response.text)['object']
            with self.lock:
                for x in results:
                    sql = 'INSERT INTO {}(time, bikeid, biketype, distid, distnum, type, x, y, host) VALUES (NOW(),\'{}\',{},\'{}\',{},{},{},{},{})'.format(
                        self.mysql_table, x['bikeIds'], int(x['biketype']), int(x['distId']), x['distNum'],
                        x['type'], x['distX'], x['distY'], 0)
                    self.insertRecord(sql)
                self.commitRecord()
                return
        except Exception as ex:
            print('[MobikeCrawler.getMobikes]: {}'.format(ex))
            if (num_retries > 0):
                return self.request(headers, payload, url, args, num_retries - 1)
            else:
                print('Cannot get the data near point=({lon}, {lat})'.format(lon=args[0], lat=args[1]))
                return

    # Redis Functions
    def getProxy(self):
        try:
            item = self.rdb.srandmember(self.proxy, 1)[0].decode('utf-8')
            return item
        except Exception as ex:
            print('[MobikeCrawler.getProxy]: {}'.format(ex))

    # Mysql Functions
    def insertRecord(self, sql):
        try:
            self.mdb.execute(sql)
        except Exception as ex:
            print('[MobikeCrawler.insertRecord]: {}'.format(ex))

    def commitRecord(self):
        try:
            self.mdb.execute('commit')
        except Exception as ex:
            print('[MobikeCrawler.commitRecord]: {}'.format(ex))

    def _closeMysql(self):
        try:
            self.mdb.execute('commit')
            self.mdb.close()
            self._mconn.close()
        except Exception as ex:
            print('[MobikeCrawler._closeMysql]: {}'.format(ex))

    def _writeLog(self):
        try:
            self.endstamp = datetime.datetime.now()
            cost = (self.endstamp - self.startstamp).seconds
            print('This task totally costs {}s.'.format(cost))
            self.startstamp = self.startstamp.strftime("%Y-%m-%d %H:%M:%S")
            self.endstamp = self.endstamp.strftime("%Y-%m-%d %H:%M:%S")
            sql = 'insert into {table}(`start`, `end`, `left`, `right`, `top`, `bottom`, `cost`) values(\'{start}\', \'{end}\', {left}, {right}, {top}, {bottom}, {cost})'.format(
                table=self.mysql_log, start=self.startstamp, end=self.endstamp, left=self.left, right=self.right,
                top=self.top, bottom=self.bottom, cost=cost)
            self.mdb.execute(sql)
            self.mdb.execute('commit')
        except:
            pass

    def __del__(self):
        self._writeLog()
        self._closeMysql()
        # Redis need not close because it's just one connection in redis pool which managed by redis own manager.

# one task
def singlerun():
    mobike = MobikeCrawler(mode='demo3')
    mobike.run()

# run mode = every 5-minute run one task
def runWithSchedule():
    singlerun()
    scheduler = BlockingScheduler()
    scheduler.add_job(singlerun, 'interval', seconds=300)
    print('Press Ctrl+{} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

# run mode2 = run one task again and again right now
def runRightNow():
    while True:
        singlerun()

# choose run mode
def run():
    runRightNow()


###### Just for test schedule ######
def tick():
    print('Start Tick! The time is: {}'.format(datetime.datetime.now()))
    time.sleep(5)
    print('finish Tick! The time is: {}'.format(datetime.datetime.now()))

def multiTick():
    # thread pool
    print('------------------- RUN -------------------')
    results = []
    executor = ThreadPoolExecutor(max_workers=100)
    for item in range(0,10):
        results.append(executor.submit(tick))
    list(as_completed(results))
    print('------------------- END -------------------')

def scheduletest():
    scheduler = BlockingScheduler()
    scheduler.add_job(multiTick, 'interval', seconds=3)
    print('Press Ctrl+{} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
###### test end ######

if __name__ == '__main__':
    run()
    # scheduletest()