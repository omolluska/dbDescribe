import sshtunnel
import pymysql, pymysql.cursors
import json

dbConfig = json.load( open('config/dbConfig.json') )

def getAllData(query, params = None):

    result = None

    MUM    = dbConfig['mumDB'] 
    try:
        
        connection = pymysql.connect(
            host     = '127.0.0.1',
            port     = MUM['port'],
            user     = MUM['user'],
            password = MUM['password'],
            database = MUM['database'],
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

def getAllDataTunnel(query, params=None):

    result = None

    TUNNEL = dbConfig['sshTunnel']
    MUM    = dbConfig['mumDB'] 

    with sshtunnel.SSHTunnelForwarder(
        ( TUNNEL['host'], 22),
        ssh_username        = TUNNEL['user'],
        ssh_pkey            = TUNNEL['pem_file'],
        remote_bind_address = (MUM['host'], MUM['port']),
        local_bind_address  = ('127.0.0.1', MUM['port']),
    ) as tunnel:
        try:
            
            connection = pymysql.connect(
                host     = '127.0.0.1',
                port     = MUM['port'],
                user     = MUM['user'],
                password = MUM['password'],
                database = MUM['database'],
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