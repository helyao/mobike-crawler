# -*- coding: utf-8 -*-
"""
-------------------------------------------------
    Filename:   mysqlConnPool.py
    Author:     Helyao
    Description:
        Mysql Connection Pool.
-------------------------------------------------
    Change Logs:
    2017-06-06 11:42am   create
-------------------------------------------------
"""
import pymysql
import configparser
from DBUtils.PooledDB import PooledDB

CONFIG_INI = r'config.ini'

class ConnectionPool():
    __pool = None
    def __init__(self):
        cp = configparser.ConfigParser()
        cp.read(CONFIG_INI)
        # mysql connection info
        self.host = cp.get('mysql', 'host')
        self.port = int(cp.get('mysql', 'port'))
        self.user = cp.get('mysql', 'user')
        self.passwd = cp.get('mysql', 'passwd')
        self.database = cp.get('mysql', 'database')
        # mysql pool setting
        self.charset = cp.get('mysqlpool', 'charset')
        self.mincache = int(cp.get('mysqlpool', 'mincache'))
        self.maxcache = int(cp.get('mysqlpool', 'maxcache'))
        self.maxshare = int(cp.get('mysqlpool', 'maxshare'))
        self.maxconnection = int(cp.get('mysqlpool', 'maxconnection'))
        self.setblock = True if(int(cp.get('mysqlpool', 'setblock')) == 1) else False
        self.maxusage = int(cp.get('mysqlpool', 'maxusage'))
        self.setsession = None

    def __enter__(self):
        self.conn = self.__getConn()
        self.cursor = self.conn.cursor()
        return self

    def __getConn(self):
        if self.__pool is None:
            self.__pool = PooledDB(creator=pymysql, mincached=self.mincache, maxcached=self.maxcache, maxshared=self.maxshare,
                                   maxconnections=self.maxconnection, blocking=self.setblock, maxusage=self.maxusage,
                                   setsession=self.setsession,
                                   host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                   db=self.database, use_unicode=False, charset=self.charset)
        return self.__pool.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

def getMysqlConnection():
    return ConnectionPool()

def _test():
    with getMysqlConnection() as db:
        sql = "SELECT * FROM mobike_log"
        try:
            db.cursor.execute(sql)
            results = db.cursor.fetchall()
            for row in results:
                id = row[0]
                start= row[1]
                end = row[2]
                left = row[3]
                right = row[4]
                top = row[5]
                bottom = row[6]
                cost = row[7]
                print('{id} | {start} | {end} | {left} | {right} | {top} | {bottom} | {cost}'.format(id=id, start=start, end=end, left=left, right=right, top=top, bottom=bottom, cost=cost))
        except:
            print ("Error: unable to fecth data")

if __name__ == '__main__':
    _test()