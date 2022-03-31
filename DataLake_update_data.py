import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime
import requests
from tqdm import tqdm
import psycopg2 as pg

def fetch_latest_data_pgsql():
  
  try:
    connection = pg.connect(user="admin",
                            password="quest",
                            host="127.0.0.1",
                            port="8812",
                            database="qdb")
    cursor = connection.cursor()
    print('PostgreSql connection created')

    cursor.execute("SELECT * FROM crypto_100_demo latest by symbol;")
    records = cursor.fetchall()
    df = pd.DataFrame(records, columns=['composite_key','upload_ts','time', 'symbol','open','high','low','close','volumefrom', 'volumeto','conversionType','conversionSymbol','exchange','api_source'])   
    df['time'] = (pd.to_datetime(df['time'])).astype('int64')//10**9


  except (Exception, pg.Error) as error:
    print("Error while connecting to PostgreSQL", error)  

  finally:
    #closing database connection.
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
        return df

def top_coins_by_mktcap_cc(num):
  req = {'api_url':'https://min-api.cryptocompare.com/data/top/mktcapfull',
       'curr_sym':'USD',        # MANDATORY - The currency symbol to convert into
       'limit':num,             # The number of data points to return. Min = 1, Max = 2000, Default = 168
       'page':0,              #The pagination for the request. If you want to paginate by 50 for example, pass in the limit_toplist param the value 50 and increasing page_toplist integer values, 0 would return coins 0-50, 1 returns coins 50-100 [ Min - 0] [ Default - 0]
       'ascending': 1
       }

  full_url = f"{req['api_url']}?&tsym={req['curr_sym']}&limit={req['limit']}&page={req['page']}&ascending={req['ascending']}"

  resp1 = requests.get(full_url)
  #print(resp1.json())

  top_list = []
  for i in range(0,len(resp1.json()['Data'])):
    top_list.append(resp1.json()['Data'][i]['CoinInfo']['Name'])

  return top_list

def gather_data(latest_df, symbol_list):  
  df = latest_df
  ts = time.time()
  df1 = df.iloc[-3:-1] 
  while ts > 1609459200:
      req = {'api_url':'https://min-api.cryptocompare.com/data/v2/histohour',
            'crypto_sym':i,
            'currency_sym':'USD',
            'exchange':'CCCAGG',
            'aggregate':1,          # return aggregrate value over n hrs - default 1
            'limit':2000,             # The number of data points to return. Min = 1, Max = 2000, Default = 168
            'to_timestamp': ts #Returns historical data before that timestamp. If you want to get all the available historical data, you can use limit=2000 and keep going back in time using the toTs param. You can then keep requesting batches using: &limit=2000&toTs={the earliest timestamp received}
            }

      ##print(datetime.fromtimestamp(ts).date())
      full_url = f"{req['api_url']}?fsym={req['crypto_sym']}&tsym={req['currency_sym']}&e={req['exchange']}&aggregate={req['aggregate']}&limit={req['limit']}&toTs={req['to_timestamp']}"

      resp1 = requests.get(full_url)

      df2 = pd.DataFrame.from_dict(resp1.json()["Data"]["Data"])
      df2['symbol'] = req['crypto_sym']
      df2['exchange'] = 'CCCAGG'
      df2['api_source'] = 'cryptocompare'
      df2['composite_key'] = df2['time'].astype(str) + '-' + df2['symbol']      

      df1 = df1.append(df2,ignore_index=True)
      df1 = df1.drop_duplicates()  
      df1.sort_values(by='time',ignore_index=True,inplace=True)
      
      ts = df1['time'].iloc[0]

      req['to_timestamp'] = ts

  df = df.append(df1[:-2],ignore_index=True)
  df.drop_duplicates(inplace=True)
  df.sort_values(by='time', ignore_index=True, inplace=True)
  print(i,len(df))
  return df


def update_data(latest_df, symbol_list):
  try:
    x = symbol_list
    df = latest_df
    
    for i in tqdm(x):
        y = df[df['symbol'] == i]
        ts_exist = y['time'].max()
        #print("ts_exist",type(ts_exist),ts_exist)

        ts = round(time.time())
        #print("ts",type(ts),ts)
        df1 = y.iloc[:]
        while ts > ts_exist:
            req = {'api_url':'https://min-api.cryptocompare.com/data/v2/histohour',
                'crypto_sym':i,
                'curr_sym':'USD',
                'exchange':'CCCAGG', 
                'aggregate':1,          
                'limit':2000,             # The number of data points to return. Min = 1, Max = 2000, Default = 168
                'to_timestamp': ts 
                }

            full_url = f"{req['api_url']}?fsym={req['crypto_sym']}&tsym={req['curr_sym']}&e={req['exchange']}&aggregate={req['aggregate']}&limit={req['limit']}&toTs={req['to_timestamp']}"

            resp1 = requests.get(full_url)

            df2 = pd.DataFrame.from_dict(resp1.json()["Data"]["Data"])
            df2['symbol'] = req['crypto_sym']
            df2['exchange'] = 'CCCAGG'
            df2['api_source'] = 'cryptocompare'
            df2['composite_key'] = df2['time'].astype(str) + '-' + df2['symbol']

            #print(df2)
            df1 = df1.append(df2,ignore_index=True)
            df1 = df1.drop_duplicates()  
            df1.sort_values(by='time',ignore_index=True,inplace=True)
            
            ts = df2['time'].iloc[0]
            req['to_timestamp'] = ts
            #print("new ts: ", (ts))

        df1['conversionSymbol'].replace({"":np.nan},inplace=True)
        df = df.append(df1[df1['time']>ts_exist],ignore_index=True)
        df.drop_duplicates(inplace=True)
        df.sort_values(by='time', ignore_index=True, inplace=True)
        #print(i,ts_exist)
    

  except (Exception) as e:
      print("Error from CryptoCompare API: ", e)

  df = df[len(x):-2*len(x)]

  return df

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
          #print(df.iloc[i])
          if i%1000 == 0:
            connection.commit()          

      except (Exception) as e:
          m.append([i,e,df.iloc[i]])
          #print([i,e])

  connection.commit()

  if (connection):
      cursor.close()
      connection.close()
      print("PostgreSQL connection is closed")

  print("total committed rows - ",len(l))
  print("total avoided - ", len(m), m)


qdb_df = fetch_latest_data_pgsql()

sym_list = qdb_df['symbol'].unique()

top_100_list = top_coins_by_mktcap_cc(100)
#print(top_100_list)

m =[]
n=[]
for i in range(0,len(top_100_list)):
  if top_100_list[i] in sym_list:
    m.append(top_100_list[i])
  else:
    n.append(top_100_list[i])

print('Existing coins currently in top 100 - ',len(m))
print('New coins - ', len(n))
print(n)

print('total rows before updation - ',len(qdb_df))
qdb_df = update_data(qdb_df,sym_list)
print('total rows after updating existing coins - ',len(qdb_df))
if len(n)>0:
    for i in tqdm(n):
      qdb_df = gather_data(qdb_df,i)
    print(len(qdb_df['symbol'].unique()))
print('total rows after adding data of new coins - ',len(qdb_df))

insert_data_pgsql(qdb_df)
