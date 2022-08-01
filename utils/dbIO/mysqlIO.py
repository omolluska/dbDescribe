import sshtunnel
import pymysql, pymysql.cursors
import json

dbConfig = json.load( open('config/dbConfig.json') )

def getAllData(query, params = None, DBkey='mumDB'):

    result = None

    DB    = dbConfig[DBkey] 
    try:
        
        connection = pymysql.connect(
            host     = '127.0.0.1',
            port     = DB['port'],
            user     = DB['user'],
            password = DB['password'],
            database = DB['database'],
        )
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            result = cursor.fetchall()
    finally:
        connection.close()
        
    return result
