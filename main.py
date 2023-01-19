# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import ccxt
import time
from datetime import datetime
import pandas as pd

exchange = ccxt.binance()
exchange.load_markets()
print(exchange.markets)
markets = exchange.markets


if exchange.has['fetchOHLCV']:
    for symbol in exchange.markets:
        time.sleep (exchange.rateLimit / 1000) # time.sleep wants seconds
        print (symbol, exchange.fetch_ohlcv (symbol, '1d')) # one day

symbol = 'BTC/USDT' 
since = int(datetime.strptime('2022-09-01', '%Y-%m-%d').timestamp()) * 1000

history = exchange.fetch_ohlcv (symbol, '1m', since=since, limit=9999999)

df = pd.DataFrame(history, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume'])

df['datetime'] = pd.to_datetime(df['timestamp']*1000000)
localDf = df.copy()

while len(localDf) == 1000:
    print(since)
    since = df.tail(1)['timestamp'].values[0]
    history = exchange.fetch_ohlcv (symbol, '1m', since=since, limit=9999999)
    localDf = pd.DataFrame(history, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    localDf['datetime'] = pd.to_datetime(localDf['timestamp']*1000000)
    concat = [df, localDf]
    df = pd.concat(concat)
    df.drop_duplicates(inplace=True)

df.tail()
df.to_csv('btc_usdt_since_2022_09_01.csv', index=False)



