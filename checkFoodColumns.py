import pandas as pd

def main():

    fileName = 'results/DIGITAL_SERVICES_20220801091017.csv'

    df = pd.read_csv(fileName)
    _df = df[ df.table_name.str.startswith('FOOD_') |  df.table_name.str.startswith('FORMULA_') ]
    _df = _df[['table_name', 'column_name']]
    uniqueTables = _df.table_name.unique()
    print(uniqueTables)

    agg = _df.groupby('column_name').agg(lambda m: len(list(m))).reset_index()

    print(agg)

    print( agg[ agg.table_name != 20 ] )

    return

if __name__ == '__main__':
    main()


