import re
import os
import json
import pymysql
import pymongo
import datetime
import multiprocessing

# [MongoDB] Mobike1 = Block1 ~ Block14 = run MyLinux & store MyLinux.mobike
mobike1 = {
    "index": 1,
    "host": "192.168.1.100",
    "port": 27017,
    "database": "mobike"
}
# [MongoDB] Mobike2 = Block15 ~ Block22 = run Server & store MyLinux.mobike2
mobike2 = {
    "index": 2,
    "host": "192.168.1.100",
    "port": 27017,
    "database": "mobike2"
}
# [MongoDB] Mobike3 = Block23 ~ Block28 = run T420s & store T420s.mobike
mobike3 = {
    "index": 3,
    "host": "192.168.1.166",
    "port": 27017,
    "database": "mobike"
}

# [Mysql]
mysql = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "passwd": "welcome",
    "database": "mobike"
}

table_seed = 'mobike_unique'

# [JSON]
json_path = r"D:/open/mobike-crawler/mongo2mysql/json"

# [CSV]
csv_path = r"D:/open/mobike-crawler/mongo2mysql/csv"

# [SQL]
sql_path = r"D:/open/mobike-crawler/mongo2mysql/sql"

mysql_process_num = 12
sql_process_num = 2

# Export mongo to json
def Mongo2Json(mongo):
    connect = pymongo.MongoClient(host=mongo['host'], port=mongo['port'])
    database = connect[mongo['database']]
    col_list = database.collection_names()
    for item in col_list:
        if (re.match("mobike_[0-9]{8}_[0-9]{6}", item)):
            filename = "{collection}_{index}".format(collection=item, index=mongo['index'])
            os.system('mongoexport -h {host} -d {database} -c {collection} -o {jsonpath}/{jsonname}.json'.format(
                host=mongo['host'],
                database=mongo['database'],
                collection=item,
                jsonpath=json_path,
                jsonname=filename))
    connect.close()

# Convert Mongoexport.json to Mysql.sql
def Json2Sql(file_name):
    startstamp = datetime.datetime.now()
    try:
        strs = re.match(r'(mobike_\d{8}_\d{6}_\d).json', file_name)
        if (len(strs.groups()) == 1):
            # Get stable's name
            table_name = strs.group(1)
            print('table = {}'.format(table_name))
            # New Sql File to Output
            sql_file_path = os.path.abspath(os.path.join(sql_path, table_name+'.sql'))
            with open(sql_file_path, 'w') as fout:
                # Create table
                fout.write('create table {table} like {seed};\n'.format(table=table_name, seed=table_seed))
                json_file_path = os.path.abspath(os.path.join(json_path, file_name))
                # Read Data from Json File
                with open(json_file_path, 'r') as fin:
                    for line in fin.readlines():
                        record = json.loads(line)
                        time = record['time']
                        bikeid = record['bikeIds']
                        biketype = record['biketype']
                        distid = record['distId']
                        distnum = record['distNum']
                        type = record['type']
                        x = record['distX']
                        y = record['distY']
                        host = record['host']
                        sql = 'insert ignore into {table_name} values("{time}", "{bikeid}", {biketype}, "{distid}", {distnum}, {type}, {x}, {y}, {host});\n'.format(
                            table_name=table_name,
                            time=time,
                            bikeid=bikeid,
                            biketype=biketype,
                            distid=distid,
                            distnum=distnum,
                            type=type,
                            x=x,
                            y=y,
                            host=host
                        )
                        fout.write(sql)
    except Exception as ex:
        print(ex)
    finishstamp = datetime.datetime.now()
    cost = (finishstamp - startstamp).seconds
    print('Finish {}'.format(table_name))
    return {'filename': file_name, 'cost': cost}

def Json2SqlManager():
    costs = []
    results = os.listdir(json_path)
    try:
        pool = multiprocessing.Pool(processes=sql_process_num)
        for item in results:
            costs.append(pool.apply_async(func=Json2Sql, args=(item,)))
        pool.close()
        pool.join()
        for cost in costs:
            print(cost.get())
    except Exception as ex:
        print(ex)

# Import json to mysql - do away with no-unique data
def Json2Mysql(file_name):
    startstamp = datetime.datetime.now()
    connect = pymysql.connect(host=mysql['host'], port=mysql['port'], user=mysql['user'], passwd=mysql['passwd'], db=mysql['database'])
    cursor = connect.cursor()
    try:
        strs = re.match(r'(mobike_\d{8}_\d{6}_\d).json', file_name)
        if (len(strs.groups()) == 1):
            table_name = strs.group(1)
            cursor.execute('create table {table} like {seed}'.format(table=table_name, seed=table_seed))
            file_path = os.path.abspath(os.path.join(json_path, file_name))
            with open(file_path, 'r') as f:
                for line in f.readlines():
                    record = json.loads(line)
                    time = record['time']
                    bikeid = record['bikeIds']
                    biketype = record['biketype']
                    distid = record['distId']
                    distnum = record['distNum']
                    type = record['type']
                    x = record['distX']
                    y = record['distY']
                    host = record['host']
                    sql = 'insert ignore into {table_name} values("{time}", "{bikeid}", {biketype}, "{distid}", {distnum}, {type}, {x}, {y}, {host})'.format(
                        table_name = table_name,
                        time = time,
                        bikeid = bikeid,
                        biketype = biketype,
                        distid = distid,
                        distnum = distnum,
                        type = type,
                        x = x,
                        y = y,
                        host = host
                    )
                    # print(sql)
                    cursor.execute(sql)
            connect.commit()
            print('Finish {}'.format(table_name))
        cursor.close()
        connect.close()
    except Exception as ex:
        print(ex)
    finishstamp = datetime.datetime.now()
    cost = (finishstamp - startstamp).seconds
    return {'filename': file_name, 'cost': cost}

def Json2MysqlManager():
    costs = []
    results = os.listdir(json_path)
    try:
        pool = multiprocessing.Pool(processes=mysql_process_num)
        for item in results:
            costs.append(pool.apply_async(func=Json2Mysql, args=(item,)))
        pool.close()
        pool.join()
        for cost in costs:
            print(cost.get())
    except Exception as ex:
        print(ex)


# Export mysql to csv
def Mysql2Csv():
    pass

def DropMongoCollection(mongo, collections):
    connect = pymongo.MongoClient(host=mongo['host'], port=mongo['port'])
    database = connect[mongo['database']]
    try:
        for collection in collections:
            database.drop_collection(collection)
    except Exception as ex:
        print(ex)
    print('Drop all connections from mobike{}'.format(mongo['index']))

# Drop collections have exported to json
def FreeMongo():
    arrMobike1 = []
    arrMobike2 = []
    arrMobike3 = []
    # Get all json file
    results = os.listdir(json_path)
    print('The number of json file = {}'.format(len(results)))
    # Classify json file by name
    for item in results:
        strs = re.match(r'(mobike_\d{8}_\d{6})_(\d).json', item)
        if (len(strs.groups()) == 2):
            index = strs.group(2)
            name = strs.group(1)
            if (index == '1'):
                arrMobike1.append(name)
            elif (index == '2'):
                arrMobike2.append(name)
            elif (index == '3'):
                arrMobike3.append(name)
    print('Length of Mobike1 = {}'.format(len(arrMobike1)))
    print('Length of Mobike2 = {}'.format(len(arrMobike2)))
    print('Length of Mobike3 = {}'.format(len(arrMobike3)))
    # Delete collections from MongoDB
    DropMongoCollection(mobike1, arrMobike1)
    DropMongoCollection(mobike2, arrMobike2)
    DropMongoCollection(mobike3, arrMobike3)


# Drop tables have exported to csv
def FreeMysql():
    pass

if __name__ == '__main__':
    # Mongo2Json(mobike1)
    # Mongo2Json(mobike2)
    # Mongo2Json(mobike3)

    # FreeMongo()

    # Json2MysqlManager()

    Json2SqlManager()

