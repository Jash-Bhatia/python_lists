from datetime import datetime
from importlib.resources import path
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from tqdm import tqdm

## CHECKING IF URL WORKS ##

req = {'api_url':'https://min-api.cryptocompare.com/data/v2/histohour',
       'crypto_sym':'BTC',
       'curr_sym':'USD',
       'exchange':'CCCAGG',
       'aggregate':1,          # return aggregrate value over n hrs - default 1
       'limit':2000,             # The number of data points to return. Min = 1, Max = 2000, Default = 168
       'to_timestamp': time.time() #Returns historical data before that timestamp. If you want to get all the available historical data, you can use limit=2000 and keep going back in time using the toTs param. You can then keep requesting batches using: &limit=2000&toTs={the earliest timestamp received}
       }

full_url = f"{req['api_url']}?fsym={req['crypto_sym']}&tsym={req['curr_sym']}&e={req['exchange']}&aggregate={req['aggregate']}&limit={req['limit']}"
print(full_url)

resp1 = requests.get(full_url)
print(resp1.json())

df = pd.DataFrame.from_dict(resp1.json()["Data"]["Data"])
df['symbol'] = req['crypto_sym']

## URL WORKS!!!! BUILDING ON THE DATA ##

string = "01/01/2019"           ## ENTER DATE STRING FOR YOUR START DATE
print(time.mktime(datetime.strptime(string,"%d/%m/%Y").timetuple()))
from_time = (time.mktime(datetime.strptime(string,"%d/%m/%Y").timetuple()))

crypto_10 = ["BTC","ETH","BNB",'USDT','SOL','ADA','USDC','XRP','LUNA','DOT']  ## LIST OF TOP 10 COINS
crypto_100 = ["BTC",'ETH','BNB','USDT','SOL','ADA','USDC','XRP','LUNA','DOT','AVAX','DOGE','SHIB',
              'MATIC','BUSD','CRO','WBTC','UNI','ALGO','LINK','LTC','UST','NEAR','DAI','ATOM','BCH',
              'TRX','FTM','XLM','HBAR','MANA','AXS','FTT','VET','ICP','SAND','FIL','BTCB','EGLD','THETA',
              'ETC','XTZ','HNT','XMR','MIOTA','ONE','LEO','KLAY','AAVE','EOS','GRT','CAKE','CRV','GALA',
              'STX','FLOW','BTT','LRC','RUNE','KSM','MKR','BSV','ENJ','AMP','XEC','QNT','AR','CELO','ZEC',
              'BAT','KDA','NEO','KCS','CHZ','OKB','WAVES','ROSE','HT','NEXO','DASH','YFI','COMP','TUSD',
              'HOT','MINA','RVN','XDC','IOTX','XEM','SUSHI','TFUEL','VLX','BORA','CEL','SCRT',
              'DCR','USDP','GNO','ANKR', 'DIVI'] ## LIST OF TOP 100 COINS

## LOOPING THROUGH ALL THE COINS IN THE LIST

for i in tqdm(crypto_10):

  ts = time.time()
  df1 = df
  #type(df1)
  while ts > from_time:
    req = {'api_url':'https://min-api.cryptocompare.com/data/v2/histohour',
          'crypto_sym':i,
          'curr_sym':'USD',
          'exchange':'CCCAGG',
          'aggregate':1,          # return aggregrate value over n hrs - default 1
          'limit':500,             # The number of data points to return. Min = 1, Max = 2000, Default = 168
          'to_timestamp': ts #Returns historical data before that timestamp. If you want to get all the available historical data, you can use limit=2000 and keep going back in time using the toTs param. You can then keep requesting batches using: &limit=2000&toTs={the earliest timestamp received}
          }

    ##print(datetime.fromtimestamp(ts).date())
    full_url = f"{req['api_url']}?fsym={req['crypto_sym']}&tsym={req['curr_sym']}&e={req['exchange']}&aggregate={req['aggregate']}&limit={req['limit']}&toTs={req['to_timestamp']}"
    ##print(full_url)

    resp1 = requests.get(full_url)
    ##print(resp1.json())

    df2 = pd.DataFrame.from_dict(resp1.json()["Data"]["Data"])
    df2['symbol'] = req['crypto_sym']
    ##print(df1['time'].iloc[-1])
    ##print("\n\n")

    df1 = df1.append(df2,ignore_index=True)
    df1 = df1.drop_duplicates()  
    df1.sort_values(by='time',ignore_index=True,inplace=True)
    
    ts = df1['time'].iloc[0]

    req['to_timestamp'] = ts
    ##print("\n\n")

  df = df.append(df1,ignore_index=True)
  df.drop_duplicates(inplace=True)
  df.sort_values(by='time', ignore_index=True, inplace=True)
  #print(datetime.now())
#print(df)

## CREATING ADDITIONAL COLUMNS + DROPING DUPLICATES

df['exchange'] = 'CCCAGG'
df['api_source'] = 'cryptocompare'
df['composite_key'] = ""
df['composite_key'] = df['time'].astype(str) + "-" + df['symbol']
df.drop_duplicates(inplace=True)

print(df.columns)

## CHECKING NO. OF ROWS FOR EACH COIN

print('current lenth of df - ',len(df))

x = df['symbol'].unique()
for i in (x):
    y = df[df['symbol'] == i]
    #if len(y) > 46100:
    print(i,len(y['time']))#,pd.to_datetime(y['time'].max(), unit='s'))

df.reset_index(drop=True, inplace = True)

## DELETING DUPLICATE/REPEATED ROWS IF ANY

for i in tqdm(x):
  y = df[df['symbol'] == i]
  l = []
  for j in range(1,len(y)):
    if y['composite_key'].iloc[j] == y['composite_key'].iloc[j-1]:
      l.append(j-1)
      #print(j,j-1)#,pd.to_datetime(y['time'].max(), unit='s'))
  df.drop(index = df[df['symbol'] == i].iloc[l].index, inplace=True)

## RECHECKING NO OF ROWS FOR EACH COIN

print('current lenth of df - ',len(df))

x = df['symbol'].unique()
for i in (x):
    y = df[df['symbol'] == i]
    #if len(y) > 46100:
    print(i,len(y['time']))#,pd.to_datetime(y['time'].max(), unit='s'))

## SAVING OUR WORK

df.to_csv("/save_data.csv")


