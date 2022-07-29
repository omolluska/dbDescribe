import json, os
import pandas as pd
from utils.dbIO import mysqlIO
from pprint import pprint

config   = json.load( open('config/config.json') )

def testData():

    tableNames = mysqlIO.getAllData("""
        select table_name from information_schema.tables 
        where
            table_schema=%s
    """,
    ('MUM_CONSUMER',)
    )
    tableNames = [t['table_name'] for t in tableNames]
    print(tableNames)

    tableName = tableNames[0]

    
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
        ('MUM_CONSUMER', 'BOTTLE_FEEDING')
    )

    tableProperties = pd.DataFrame(tableProperties)

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

    foreign_keys = mysqlIO.getAllData(
        query, ('MUM_CONSUMER', 'BOTTLE_FEEDING')
    )

    foreign_keys = pd.DataFrame( foreign_keys )
    
    tableProperties = tableProperties.merge( foreign_keys, how='left' )
    print( tableProperties )

    tableProperties.to_csv( 'results/temp.csv', index=False )




    return

if __name__ == '__main__':
    print(testData())
