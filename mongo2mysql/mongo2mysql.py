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

process_num = 12

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
        pool = multiprocessing.Pool(processes=process_num)
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

    Json2MysqlManager()

"""
Finish mobike_20170628_200026_3
Finish mobike_20170628_200817_3
Finish mobike_20170628_202246_3
Finish mobike_20170628_201536_3
Finish mobike_20170628_202958_3
Finish mobike_20170628_203649_3
Finish mobike_20170628_201539_2
Finish mobike_20170628_200812_2
Finish mobike_20170628_200018_2
Finish mobike_20170628_202303_2
Finish mobike_20170628_200017_1
Finish mobike_20170628_202351_1
Finish mobike_20170628_204409_3
Finish mobike_20170628_201613_1
Finish mobike_20170628_205121_3
Finish mobike_20170628_200831_1
Finish mobike_20170628_203042_2
Finish mobike_20170628_203113_1
Finish mobike_20170628_203801_2
Finish mobike_20170628_205841_3
Finish mobike_20170628_210559_3
Finish mobike_20170628_203846_1
Finish mobike_20170628_211311_3
Finish mobike_20170628_204528_2
Finish mobike_20170628_205328_1
Finish mobike_20170628_205310_2
Finish mobike_20170628_212039_3
Finish mobike_20170628_204602_1
Finish mobike_20170628_210753_2
Finish mobike_20170628_210034_2
Finish mobike_20170628_212749_3
Finish mobike_20170628_213517_3
Finish mobike_20170628_210114_1
Finish mobike_20170628_211456_2
Finish mobike_20170628_214225_3
Finish mobike_20170628_212222_2
Finish mobike_20170628_210844_1
Finish mobike_20170628_211622_1
Finish mobike_20170628_214926_3
Finish mobike_20170628_215702_3
Finish mobike_20170628_212952_2
Finish mobike_20170628_212404_1
Finish mobike_20170628_213708_2
Finish mobike_20170628_214410_2
Finish mobike_20170628_220408_3
Finish mobike_20170628_213146_1
Finish mobike_20170628_221136_3
Finish mobike_20170628_215118_2
Finish mobike_20170628_215844_2
Finish mobike_20170628_213857_1
Finish mobike_20170628_214617_1
Finish mobike_20170628_221840_3
Finish mobike_20170628_215350_1
Finish mobike_20170628_222619_3
Finish mobike_20170628_220610_2
Finish mobike_20170628_223323_3
Finish mobike_20170628_220134_1
Finish mobike_20170628_221350_2
Finish mobike_20170628_220919_1
Finish mobike_20170628_224134_3
Finish mobike_20170628_221634_1
Finish mobike_20170628_222116_2
Finish mobike_20170628_224833_3
Finish mobike_20170628_222915_2
Finish mobike_20170628_225545_3
Finish mobike_20170628_222422_1
Finish mobike_20170628_223156_1
Finish mobike_20170628_224446_2
Finish mobike_20170628_230239_3
Finish mobike_20170628_223613_2
Finish mobike_20170628_230924_3
Finish mobike_20170628_225128_2
Finish mobike_20170628_225820_2
Finish mobike_20170628_224000_1
Finish mobike_20170628_224739_1
Finish mobike_20170628_231616_3
Finish mobike_20170628_230152_1
Finish mobike_20170628_225450_1
Finish mobike_20170628_232246_3
Finish mobike_20170628_233032_3
Finish mobike_20170628_230507_2
Finish mobike_20170628_230850_1
Finish mobike_20170628_233717_3
Finish mobike_20170628_231147_2
Finish mobike_20170628_231538_1
Finish mobike_20170628_234408_3
Finish mobike_20170628_231821_2
Finish mobike_20170628_232229_1
Finish mobike_20170628_235107_3
Finish mobike_20170628_232456_2
Finish mobike_20170628_233214_2
Finish mobike_20170628_235806_3
Finish mobike_20170628_233727_1
Finish mobike_20170629_000520_3
Finish mobike_20170628_233035_1
Finish mobike_20170628_233856_2
Finish mobike_20170629_001233_3
Finish mobike_20170628_235220_2
Finish mobike_20170628_234523_2
Finish mobike_20170628_234421_1
Finish mobike_20170629_001956_3
Finish mobike_20170628_235920_2
Finish mobike_20170629_002734_3
Finish mobike_20170628_235828_1
Finish mobike_20170628_235122_1
Finish mobike_20170629_003455_3
Finish mobike_20170629_000617_2
Finish mobike_20170629_000537_1
Finish mobike_20170629_004158_3
Finish mobike_20170629_001339_2
Finish mobike_20170629_001315_1
Finish mobike_20170629_004856_3
Finish mobike_20170629_002818_2
Finish mobike_20170629_002105_2
Finish mobike_20170629_005545_3
Finish mobike_20170629_002052_1
Finish mobike_20170629_010344_3
Finish mobike_20170629_003520_2
Finish mobike_20170629_002822_1
Finish mobike_20170629_004222_2
Finish mobike_20170629_003547_1
Finish mobike_20170629_004915_2
Finish mobike_20170629_005603_2
Finish mobike_20170629_011040_3
Finish mobike_20170629_004250_1
Finish mobike_20170629_011734_3
Finish mobike_20170629_012406_3
Finish mobike_20170629_004959_1
Finish mobike_20170629_005649_1
Finish mobike_20170629_010345_2
Finish mobike_20170629_011643_2
Finish mobike_20170629_013028_3
Finish mobike_20170629_010443_1
Finish mobike_20170629_011009_2
Finish mobike_20170629_012315_2
Finish mobike_20170629_013641_3
Finish mobike_20170629_014307_3
Finish mobike_20170629_011133_1
Finish mobike_20170629_011827_1
Finish mobike_20170629_014924_3
Finish mobike_20170629_012913_2
Finish mobike_20170629_012510_1
Finish mobike_20170629_015538_3
Finish mobike_20170629_013515_2
Finish mobike_20170629_013127_1
Finish mobike_20170629_020203_3
Finish mobike_20170629_014050_2
Finish mobike_20170629_014639_2
Finish mobike_20170629_020824_3
Finish mobike_20170629_013734_1
Finish mobike_20170629_014344_1
Finish mobike_20170629_015220_2
Finish mobike_20170629_015823_2
Finish mobike_20170629_014951_1
Finish mobike_20170629_022100_3
Finish mobike_20170629_021453_3
Finish mobike_20170629_015554_1
Finish mobike_20170629_020419_2
Finish mobike_20170629_022721_3
Finish mobike_20170629_020206_1
Finish mobike_20170629_020816_1
Finish mobike_20170629_021020_2
Finish mobike_20170629_023334_3
Finish mobike_20170629_021602_2
Finish mobike_20170629_021422_1
Finish mobike_20170629_023953_3
Finish mobike_20170629_022153_2
Finish mobike_20170629_024609_3
Finish mobike_20170629_022006_1
Finish mobike_20170629_023310_2
Finish mobike_20170629_022733_2
Finish mobike_20170629_022549_1
Finish mobike_20170629_025245_3
Finish mobike_20170629_023137_1
Finish mobike_20170629_025918_3
Finish mobike_20170629_023821_2
Finish mobike_20170629_024416_2
Finish mobike_20170629_030540_3
Finish mobike_20170629_023736_1
Finish mobike_20170629_031202_3
Finish mobike_20170629_024323_1
Finish mobike_20170629_025525_2
Finish mobike_20170629_024947_2
Finish mobike_20170629_030131_2
Finish mobike_20170629_025523_1
Finish mobike_20170629_031820_3
Finish mobike_20170629_024927_1
Finish mobike_20170629_030129_1
Finish mobike_20170629_032441_3
Finish mobike_20170629_030715_2
Finish mobike_20170629_033058_3
Finish mobike_20170629_033709_3
Finish mobike_20170629_031312_2
Finish mobike_20170629_030732_1
Finish mobike_20170629_032459_2
Finish mobike_20170629_034318_3
Finish mobike_20170629_031905_2
Finish mobike_20170629_033031_2
Finish mobike_20170629_031340_1
Finish mobike_20170629_031957_1
Finish mobike_20170629_032601_1
Finish mobike_20170629_034929_3
Finish mobike_20170629_033605_2
Finish mobike_20170629_035533_3
Finish mobike_20170629_033159_1
Finish mobike_20170629_034135_2
Finish mobike_20170629_040155_3
Finish mobike_20170629_040758_3
Finish mobike_20170629_033803_1
Finish mobike_20170629_041418_3
Finish mobike_20170629_034700_2
Finish mobike_20170629_034407_1
Finish mobike_20170629_035238_2
Finish mobike_20170629_035808_2
Finish mobike_20170629_042027_3
Finish mobike_20170629_035617_1
Finish mobike_20170629_035008_1
Finish mobike_20170629_040338_2
Finish mobike_20170629_040231_1
Finish mobike_20170629_042658_3
Finish mobike_20170629_040905_2
Finish mobike_20170629_040839_1
Finish mobike_20170629_043320_3
Finish mobike_20170629_041509_2
Finish mobike_20170629_041452_1
Finish mobike_20170629_043941_3
Finish mobike_20170629_042052_1
Finish mobike_20170629_044551_3
Finish mobike_20170629_042701_2
Finish mobike_20170629_042059_2
Finish mobike_20170629_043306_2
Finish mobike_20170629_045208_3
Finish mobike_20170629_042711_1
Finish mobike_20170629_043904_2
Finish mobike_20170629_045820_3
Finish mobike_20170629_043943_1
Finish mobike_20170629_043331_1
Finish mobike_20170629_044450_2
Finish mobike_20170629_050434_3
Finish mobike_20170629_045014_2
Finish mobike_20170629_045531_2
Finish mobike_20170629_051045_3
Finish mobike_20170629_044552_1
Finish mobike_20170629_051649_3
Finish mobike_20170629_050326_1
Finish mobike_20170629_050046_2
Finish mobike_20170629_045721_1
Finish mobike_20170629_045140_1
Finish mobike_20170629_052310_3
Finish mobike_20170629_050547_2
Finish mobike_20170629_052933_3
Finish mobike_20170629_050922_1
Finish mobike_20170629_051645_2
Finish mobike_20170629_051116_2
Finish mobike_20170629_053547_3
Finish mobike_20170629_054204_3
Finish mobike_20170629_051500_1
Finish mobike_20170629_052756_2
Finish mobike_20170629_052614_1
Finish mobike_20170629_052236_2
Finish mobike_20170629_052038_1
Finish mobike_20170629_054815_3
Finish mobike_20170629_055447_3
Finish mobike_20170629_053304_2
Finish mobike_20170629_053156_1
Finish mobike_20170629_053824_2
Finish mobike_20170629_060156_3
Finish mobike_20170629_054321_1
Finish mobike_20170629_053744_1
Finish mobike_20170629_054346_2
Finish mobike_20170629_060815_3
Finish mobike_20170629_055506_2
Finish mobike_20170629_054922_2
Finish mobike_20170629_061440_3
Finish mobike_20170629_054913_1
Finish mobike_20170629_055517_1
Finish mobike_20170629_062101_3
Finish mobike_20170629_060106_2
Finish mobike_20170629_060707_2
Finish mobike_20170629_060138_1
Finish mobike_20170629_062718_3
Finish mobike_20170629_061259_2
Finish mobike_20170629_063348_3
Finish mobike_20170629_060740_1
Finish mobike_20170629_061359_1
Finish mobike_20170629_061840_2
Finish mobike_20170629_064004_3
Finish mobike_20170629_062404_2
Finish mobike_20170629_064642_3
Finish mobike_20170629_062931_2
Finish mobike_20170629_062001_1
Finish mobike_20170629_062550_1
Finish mobike_20170629_065324_3
Finish mobike_20170629_063506_2
Finish mobike_20170629_063138_1
Finish mobike_20170629_070027_3
Finish mobike_20170629_064050_2
Finish mobike_20170629_063730_1
Finish mobike_20170629_070716_3
Finish mobike_20170629_064623_2
Finish mobike_20170629_064340_1
Finish mobike_20170629_071347_3
Finish mobike_20170629_065210_2
Finish mobike_20170629_065550_1
Finish mobike_20170629_064941_1
Finish mobike_20170629_065802_2
Finish mobike_20170629_072032_3
Finish mobike_20170629_070401_2
Finish mobike_20170629_072711_3
Finish mobike_20170629_073330_3
Finish mobike_20170629_070204_1
Finish mobike_20170629_071651_2
Finish mobike_20170629_071033_2
Finish mobike_20170629_070823_1
Finish mobike_20170629_071501_1
Finish mobike_20170629_074011_3
Finish mobike_20170629_072308_2
Finish mobike_20170629_074647_3
Finish mobike_20170629_072133_1
Finish mobike_20170629_075327_3
Finish mobike_20170629_072938_2
Finish mobike_20170629_072800_1
Finish mobike_20170629_073606_2
Finish mobike_20170629_080004_3
Finish mobike_20170629_073442_1
Finish mobike_20170629_074233_2
Finish mobike_20170629_080650_3
Finish mobike_20170629_074919_2
Finish mobike_20170629_074105_1
Finish mobike_20170629_081333_3
Finish mobike_20170629_075546_2
Finish mobike_20170629_074725_1
Finish mobike_20170629_082014_3
Finish mobike_20170629_080203_2
Finish mobike_20170629_075405_1
Finish mobike_20170629_082701_3
Finish mobike_20170629_080843_2
Finish mobike_20170629_083337_3
Finish mobike_20170629_080036_1
Finish mobike_20170629_080728_1
Finish mobike_20170629_081506_2
Finish mobike_20170629_084016_3
Finish mobike_20170629_082136_2
Finish mobike_20170629_084707_3
Finish mobike_20170629_082103_1
Finish mobike_20170629_081417_1
Finish mobike_20170629_085358_3
Finish mobike_20170629_082812_2
Finish mobike_20170629_082746_1
Finish mobike_20170629_090044_3
Finish mobike_20170629_083449_2
Finish mobike_20170629_084143_2
Finish mobike_20170629_090733_3
Finish mobike_20170629_083434_1
Finish mobike_20170629_084128_1
Finish mobike_20170629_084826_2
Finish mobike_20170629_091418_3
Finish mobike_20170629_085519_2
Finish mobike_20170629_084826_1
Finish mobike_20170629_090207_2
Finish mobike_20170629_092116_3
Finish mobike_20170629_085536_1
Finish mobike_20170629_092809_3
Finish mobike_20170629_090854_2
Finish mobike_20170629_093450_3
Finish mobike_20170629_094135_3
Finish mobike_20170629_090934_1
Finish mobike_20170629_090224_1
Finish mobike_20170629_091609_2
Finish mobike_20170629_092309_2
Finish mobike_20170629_094816_3
Finish mobike_20170629_095527_3
Finish mobike_20170629_091631_1
Finish mobike_20170629_093021_2
Finish mobike_20170629_093721_2
Finish mobike_20170629_092317_1
Finish mobike_20170629_093038_1
Finish mobike_20170629_094426_2
Finish mobike_20170629_100245_3
Finish mobike_20170629_093734_1
Finish mobike_20170629_100944_3
Finish mobike_20170629_101955_3
Finish mobike_20170629_095128_2
Finish mobike_20170629_094436_1
Finish mobike_20170629_095825_2
Finish mobike_20170629_101234_2
Finish mobike_20170629_095142_1
Finish mobike_20170629_103939_3
Finish mobike_20170629_100530_2
Finish mobike_20170629_102300_1
Finish mobike_20170629_101216_1
Finish mobike_20170629_102326_2
Finish mobike_20170629_095841_1
Finish mobike_20170629_103608_2
Finish mobike_20170629_105038_3
Finish mobike_20170629_105940_3
Finish mobike_20170629_110836_3
Finish mobike_20170629_100528_1
Finish mobike_20170629_103759_1
Finish mobike_20170629_111555_3
Finish mobike_20170629_112529_3
Finish mobike_20170629_105802_1
Finish mobike_20170629_104726_2
Finish mobike_20170629_105512_2
Finish mobike_20170629_110259_2
Finish mobike_20170629_111051_2
Finish mobike_20170629_111752_2
Finish mobike_20170629_104928_1
Finish mobike_20170629_113408_3
Finish mobike_20170629_112901_2
Finish mobike_20170629_110624_1
Finish mobike_20170629_114111_3
Finish mobike_20170629_114821_3
Finish mobike_20170629_111411_1
Finish mobike_20170629_115508_3
Finish mobike_20170629_112213_1
Finish mobike_20170629_120202_3
Finish mobike_20170629_113601_2
Finish mobike_20170629_114254_2
Finish mobike_20170629_114930_2
Finish mobike_20170629_120921_3
Finish mobike_20170629_113241_1
Finish mobike_20170629_114020_1
Finish mobike_20170629_115632_2
Finish mobike_20170629_114720_1
Finish mobike_20170629_121615_3
Finish mobike_20170629_115416_1
Finish mobike_20170629_122334_3
Finish mobike_20170629_120336_2
Finish mobike_20170629_123031_3
Finish mobike_20170629_123711_3
Finish mobike_20170629_120147_1
Finish mobike_20170629_120857_1
Finish mobike_20170629_121025_2
Finish mobike_20170629_124406_3
Finish mobike_20170629_121741_2
Finish mobike_20170629_122454_2
Finish mobike_20170629_121612_1
Finish mobike_20170629_122356_1
Finish mobike_20170629_123135_2
Finish mobike_20170629_125022_3
Finish mobike_20170629_125622_3
Finish mobike_20170629_123052_1
Finish mobike_20170629_123753_2
Finish mobike_20170629_130245_3
Finish mobike_20170629_123713_1
Finish mobike_20170629_124419_2
"""