import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime
import requests
from tqdm import tqdm
import psycopg2 as pg

def create_table_pgsql():
  try:
    connection = pg.connect(user="admin",
                            password="quest",
                            host="127.0.0.1",
                            port="8812",
                            database="qdb")
    cursor = connection.cursor()
    print('PostgreSql connection created')

    # text-only query
    cursor.execute(''' create table if not exists 
                        crypto_100_demo ('composite_key' string, upload_ts timestamp,
                        hourly_ts timestamp, 'symbol' symbol, open double, 
                        high double, low double, close double, 
                        volumefrom double, volumeto double,
                        conversionType string, conversionSymbol string,
                        exchange string, api_source string) timestamp(upload_ts)
                    ''')
    connection.commit()
    print('table created')
    
  except (Exception, pg.Error) as error:
    print("Error while connecting to PostgreSQL", error)
    
  finally:
    #closing database connection.
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

def insert_data_pgsql(upload_df):
  df = upload_df
  try:
    connection = pg.connect(user="admin",
                            password="quest",
                            host="127.0.0.1",
                            port="8812",
                            database="qdb")
    cursor = connection.cursor()
    print('PostgreSql connection created')

  except (Exception, pg.Error) as error:
    print("Error while connecting to PostgreSQL", error)  
  

  l = []
  m = []
  for i in tqdm(range(0,len(df))):

      try:
          cursor.execute(f"""
          INSERT INTO crypto_100_demo
          VALUES 
              ('{df['composite_key'].iloc[i]}',{np.int64(time.time())*1000000},
              {df['time'].iloc[i]*1000000}, '{df['symbol'].iloc[i]}', 
              {df['open'].iloc[i]},{df['high'].iloc[i]}, {df['low'].iloc[i]}, {df['close'].iloc[i]}, 
              {df['volumefrom'].iloc[i]}, {df['volumeto'].iloc[i]}, 
              '{df['conversionType'].iloc[i]}', '{df['conversionSymbol'].iloc[i]}',
              '{df['exchange'].iloc[i]}', '{df['api_source'].iloc[i]}')
              ;""")
          l.append(i)
          if i%1000 == 0:
            connection.commit()           

      except (Exception) as e:
          m.append([i,e])
          #print([i,e])
  connection.commit()

  if (connection):
      cursor.close()
      connection.close()
      print("PostgreSQL connection is closed")

  print("total committed rows - ",len(l))
  print("total avoided - ", len(m))#, m[-1])

raw_df = pd.read_csv('top51_100_crypto_18_22_v1.csv')
print('total data - ',len(raw_df))

create_table_pgsql()
insert_data_pgsql(raw_df[-5000:])

