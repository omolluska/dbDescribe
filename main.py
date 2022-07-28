import json, os
import pymysql, pymysql.cursors
import sshtunnel

config   = json.load( open('config/config.json') )
dbConfig = json.load( open('config/dbConfig.json') )

def main():

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
            with connection.cursor() as cursor:
                cursor.execute('select * from BOTTLE_FEEDING limit 10;')
                result = cursor.fetchall()
                print( result )
        finally:
            connection.close()


if __name__ == '__main__':
    main()
