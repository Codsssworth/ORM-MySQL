import os
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine,Table, MetaData, insert,text
import pandas as pd
import psycopg2
import urllib

def map_dtype(dtype):
    if dtype == 'int64':
        return 'INTEGER'
    elif dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'object':
        return 'TEXT'
    elif dtype == 'datetime64[ns]':
        return 'TIMESTAMP'
    elif dtype == 'bool':
        return 'BOOLEAN'
    else:
        return 'TEXT'


def generate_create_table_sql(df, table_name):
    columns = []
    for column, dtype in df.dtypes.items():
        sql_type = map_dtype( str( dtype ) )
        columns.append( f'"{column}" {sql_type}' )

    columns_sql = ",".join( columns )
    create_table_sql = f"CREATE TABLE {table_name} ({columns_sql});"
    return create_table_sql


load_dotenv()
# driver = os.getenv("DRIVER")
# server = os.getenv("SERVER")
db = os.getenv("DATABASE")
user = os.getenv("USER")
pwd = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")

df = pd.read_csv('orders.csv',na_values=['Not Available','unknown'])
# df['Ship Mode'].unique()
#
# df.rename(columns={'Order Id':'order_id', 'City':'city'})
df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace(' ','_')

df['discount']=df['list_price']*df['discount_percent']*.01
df['sale_price']= df['list_price']-df['discount']
df['profit']=df['sale_price']-df['cost_price']
df['order_date']=pd.to_datetime(df['order_date'],format="%Y-%m-%d")
df.drop(columns=['list_price','cost_price','discount_percent'],inplace=True)


# params = urllib.parse.quote_plus(
#     f"DRIVER={{{driver}}};"
#     f"SERVER={server};"
#     f"DATABASE={db};"
#     "Trusted_Connection=yes;"
# )

# engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
engine = create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")
# engine = create_engine('mssql://ANKIT\SQLEXPRESS/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
conn=engine.connect()
print(conn)
table_name = input("Give a table name :")
query = generate_create_table_sql(df , table_name)
print (query)
print(df)
conn.execute(text(query))
df.to_sql(table_name, con=conn , index=False, if_exists = 'append')
print (f'{table_name} inserted succesfully ')
conn.commit()
conn.close()