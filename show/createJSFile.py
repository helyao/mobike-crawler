import os
import sys
import pymysql

HOST = '192.168.1.100'
PORT = 3306
USER = 'helyao'
PASSWD = 'welcome'
DATABASE = 'mobike'
TABLE = 'mobike_shanghai_search'
FILENAME = r'point.js'

output = []

sql = 'select x, y from {table}'.format(table=TABLE)
connection = pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PASSWD, db=DATABASE)

try:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()
        for item in results:
            output.append([item[0], item[1], 1])
finally:
    connection.close()

print(len(output))

content = {"data": output}

with open(FILENAME, 'w') as f:
    f.write('var data = {point};'.format(point=content))



