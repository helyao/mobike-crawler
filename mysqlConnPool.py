# -*- coding: utf-8 -*-
"""
-------------------------------------------------
    Filename:   mysqlConnPool.py
    Author:     Helyao
    Description:
        Mysql Connection Pool.
-------------------------------------------------
    Change Logs:
    2017-06-06  11:42am     create
    2017-06-08  11:31pm     re-write class
-------------------------------------------------
"""
import pymysql
import configparser
from DBUtils.PooledDB import PooledDB

CONFIG_INI = r'config.ini'

class ConnectionPool():     # This class with issue, so re-write it as MysqlPool class
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


class MysqlPool():
    __pool = None
    __config = {}

    def __init__(self):
        MysqlPool.__getConfig()
        self.pool = MysqlPool.__getConnection()

    @classmethod
    def __getConfig(cls):
        if not MysqlPool.__config:
            cp = configparser.ConfigParser()
            cp.read(CONFIG_INI)
            # MySql Connection Info
            MysqlPool.__config['host'] = cp.get('mysql', 'host')
            MysqlPool.__config['port'] = int(cp.get('mysql', 'port'))
            MysqlPool.__config['user'] = cp.get('mysql', 'user')
            MysqlPool.__config['passwd'] = cp.get('mysql', 'passwd')
            MysqlPool.__config['database'] = cp.get('mysql', 'database')
            # Mysql Pool Settings
            MysqlPool.__config['charset'] = cp.get('mysqlpool', 'charset')
            MysqlPool.__config['mincache'] = int(cp.get('mysqlpool', 'mincache'))
            MysqlPool.__config['maxcache'] = int(cp.get('mysqlpool', 'maxcache'))
            MysqlPool.__config['maxshare'] = int(cp.get('mysqlpool', 'maxshare'))
            MysqlPool.__config['maxconnection'] = int(cp.get('mysqlpool', 'maxconnection'))
            MysqlPool.__config['setblock'] = True if (int(cp.get('mysqlpool', 'setblock')) == 1) else False
            MysqlPool.__config['maxusage'] = int(cp.get('mysqlpool', 'maxusage'))
            MysqlPool.__config['setsession'] = None

    @classmethod
    def __getConnection(cls):
        if MysqlPool.__pool is None:
            MysqlPool.__pool = PooledDB(creator=pymysql,
                                        mincached=MysqlPool.__config['mincache'], maxcached=MysqlPool.__config['maxcache'],
                                        maxshared=MysqlPool.__config['maxcache'], maxconnections=MysqlPool.__config['maxconnection'],
                                        blocking=MysqlPool.__config['setblock'], maxusage=MysqlPool.__config['maxusage'],
                                        setsession=MysqlPool.__config['setsession'],
                                        host=MysqlPool.__config['host'], port=MysqlPool.__config['port'],
                                        user=MysqlPool.__config['user'], passwd=MysqlPool.__config['passwd'],
                                        db=MysqlPool.__config['database'],
                                        use_unicode=False, charset=MysqlPool.__config['charset'])
        return MysqlPool.__pool

    def getMysqlConn(self):
        return self.pool.connection()

# old test function
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