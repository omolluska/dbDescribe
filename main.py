import json, os
from utils.dbIO import mysqlIO

config   = json.load( open('config/config.json') )

def testData():

    results = mysqlIO.getAllData('select * from BOTTLE_FEEDING limit 10;')
    print(results)

    return

if __name__ == '__main__':
    print(testData())
