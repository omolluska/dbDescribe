import json
import pandas    as pd
import sshtunnel

from tqdm       import tqdm
from utils.dbIO import mysqlIO
from datetime   import datetime as dt


config   = json.load( open('config/config.json') )
dbConfig = json.load( open('config/dbConfig.json') )

def getForeignKeyInfo(tableName:str) -> pd.DataFrame:

    query = '''
        select
            table_name,
            column_name,
            referenced_table_name,
            referenced_column_name 
        from 
            information_schema.key_column_usage
        where
            table_schema    = %s and
            table_name      = %s and
            referenced_table_name is not null
    '''

    foreignKeys = mysqlIO.getAllData(
        query, ('MUM_CONSUMER', tableName)
    )

    foreignKeys = pd.DataFrame( 
        foreignKeys, 
        columns = ['table_name', 'column_name', 'referenced_table_name', 'referenced_column_name'], 
    )

    return foreignKeys

def generateTableDetails(tableName:str) -> pd.DataFrame:

    query = '''
        select 
            table_name,
            column_name,
            column_key,
            column_default,
            is_nullable,
            data_type,
            column_type,
            extra
        from 
            information_schema.columns
        where
            table_schema    = %s and
            table_name      = %s 
    '''

    tableProperties = mysqlIO.getAllData(
        query, 
        ('MUM_CONSUMER', tableName)
    )

    tableProperties = pd.DataFrame( tableProperties )
    foreignKeys     = getForeignKeyInfo( tableName )

    tableProperties = tableProperties.merge( foreignKeys, how='left' )

    return tableProperties

def getAllTableInfo(problemTables) -> pd.DataFrame:

    if problemTables is None:
        tableNames = mysqlIO.getAllData("""
            select table_name from information_schema.tables 
            where
                table_schema=%s
        """,
        ('MUM_CONSUMER',)
        )
        tableNames = [t['table_name'] for t in tableNames]
    elif len(problemTables) > 0:
        tableNames = [p for p in problemTables]
    else:
        return None, []

    allTables     = []
    problemTables = []
    for tableName in tqdm(tableNames, leave=False):
        try:
            tableDetails = generateTableDetails( tableName )
            allTables.append( tableDetails )
        except Exception as e:
            print(f'Problem generating reuslts for {tableName}: {e}')
            problemTables.append( tableName )
    
    allTables = pd.concat(allTables)
    return allTables, problemTables

if __name__ == '__main__':

    TUNNEL = dbConfig['sshTunnel']
    MUM    = dbConfig['mumDB'] 
    problemTables = None
    i = 0

    with sshtunnel.SSHTunnelForwarder(
        ( TUNNEL['host'], 22),
        ssh_username        = TUNNEL['user'],
        ssh_pkey            = TUNNEL['pem_file'],
        remote_bind_address = (MUM['host'], MUM['port']),
        local_bind_address  = ('127.0.0.1', MUM['port']),
    ) as tunnel:
        while problemTables != []:
            
            print(f'This is iteration number: {i}')
            allTables, problemTables = getAllTableInfo(problemTables)
            print('saving the results ...')
            now = dt.now().strftime('%Y%m%d%H%M%S')        
            allTables.to_csv( f'results/MUM_CONSUMER_{now}.csv', index=False )

