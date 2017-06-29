import re
import os
import pymysql
import pymongo

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
output = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "passwd": "welcome",
    "database": "mobike"
}

# [JSON]
json_path = r"D:/open/mobike-crawler/mongo2mysql/json"

# [CSV]
csv_path = r"D:/open/mobike-crawler/mongo2mysql/csv"

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

# Import json to mysql - do away with no-unique data
def Json2Mysql():
    pass

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
    FreeMongo()