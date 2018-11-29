import requests
import json
import time
import pandas as pd
from datetime import datetime
import threading

def getInfo(url):
    try:
        r = requests.get(url)
        if r.status_code == 200:
            info = json.loads(r.content)
            return info
        else:
            return None
    except Exception as e:
        print(e)
        return None

def dumpCSV(info, columns, index, path):
    df = pd.DataFrame(info,index=[index], columns=columns)
    df.to_csv(path, mode='a', header=False)

def loop(execute_second, market):
    start_date = datetime.now().strftime("%Y-%m-%d")
    columns = ['product_code', 'timestamp', 'tick_id', 'best_bid', 'best_ask',
       'best_bid_size', 'best_ask_size', 'total_bid_depth', 'total_ask_depth',
       'ltp', 'volume', 'volume_by_product']
    index = 0
    path = f'./data/bitflyer_{market}_{start_date}.csv'
    df = pd.DataFrame({},index=[], columns=columns)
    df.to_csv(path, mode='w', header=True)
    while True:
        base_time = time.time()
        if datetime.now().strftime("%S") == execute_second:
            url = ticker_url + f'?product_code={market}'
            info = getInfo(url)
            if info:
                dumpCSV(info, columns, index, path)
                index += 1
            if 60-(time.time()-base_time)-1>0:
                time.sleep(60-(time.time()-base_time)-1)
        else:
            time.sleep(0.1)

if __name__ == '__main__':
    markests = {'BTC_JPY':'ビットコイン/円', 'FX_BTC_JPY':'FX ビットコイン/円', 'ETH_BTC':'イーサリアム/ビットコイン', 'BCH_BTC':'ビットキャッシュ/ビットコイン'}
    ticker_url = 'https://api.bitflyer.com/v1/ticker'
    second = 0
    for market in markests:
        thread = threading.Thread(target=loop, args=('{0:02d}'.format(second), market))
        thread.start()
        second += 10