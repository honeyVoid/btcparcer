import os

import requests
import time
from datetime import datetime as dt

import redis

from pymongo import MongoClient

import psycopg2

from dotenv import load_dotenv

load_dotenv()

password = os.getenv('postgres_pass')

client = MongoClient('mongodb://localhost:27017/')
db = client['mydatabase']
logs = db['logs']

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='price_db',
    user='postgres',
    password=f'{password}',
)
cursor = conn.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS prices (burse TEXT, price NUMERICAL, timestamp TIMESTAMP)')
conn.commit()

URL_1 = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
URL_2 = 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=BTC-USDT'
URL_3 = 'https://api.bybit.com/v2/public/tickers?symbol=BTCUSD'




# def get_binance_price(url):
#     try:
#         response = requests.get(url)
#         r.set('Binance_BTCUSD', response.json().get('price'))
#         # print(response.json().get('price'))
#     except Exception as error:
#         logger.error(error)

#     return response.json()


# def get_kucoin_price(url):
#     try:
#         response = requests.get(url)
#         r.set('Kucoin_BTCUSD', response.json().get('data')['price'])
#         # print(response.json().get('data')['price'])
#     except Exception as error:
#         logger.error(error)
#     return response.json().get('data')


# def get_bybit_price(url):
#     try:
#         response = requests.get(url)
#         r.set('Bybit_BTCUSD',
#               response.json().get('result')[0].get('last_price'))
#         # print(response.json().get('result')[0].get('last_price'))
#     except Exception as error:
#         logger.error(error)
#     return response.json().get('result')[0]


# def check_instance(response):
#     if not isinstance(response, dict):
#         logger.error('Получен не словарь')
#     return response


# def main():
#     while True:
#         responses = (get_binance_price(URL_1),
#                      get_kucoin_price(URL_2),
#                      get_bybit_price(URL_3))
#         for response in responses:
#             check_instance(response)
#         time.sleep(60)


# if __name__ == '__main__':
#     main()


class BTCprice:
    def __init__(self, url, url_2, url_3) -> None:
        self.url = url
        self.url_2 = url_2
        self.url_3 = url_3

    def get_price(self):
        try:
            response = requests.get(self.url)
            response_2 = requests.get(self.url_2)
            response_3 = requests.get(self.url_3)
        except Exception as error:
            logs.insert_one(
                {
                    'error': error,
                    'time': dt.now()
                }
            )
        prices = {
            'BINANCE': response.json().get('price'),
            'KUCOIN': response_2.json().get('data')['price'],
            'BYBIT': response_3.json().get('result')[0].get('last_price'),
        }
        for burse, price in prices.values():
            timestemp = dt.now()
            cursor.execute(
                'INSERT INTO prices (burse, price, timestamp) VALUES (%s, %s, %s)',
                (burse, price, timestemp)
            )
            conn.commit()
            cursor.close()
            conn.close()
        r.mset(prices)
        price_list = r.mget('BINANCE', 'KUCOIN', 'BYBIT')
        print(price_list[0])


def main():
    logs.insert_one(
        {
            'message': 'Start',
            'time': dt.now()
        }
    )
    p = BTCprice(URL_1, URL_2, URL_3)
    while True:
        p.get_price()
        time.sleep(5)


if __name__ == '__main__':
    main()
