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


URL_1 = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
URL_2 = 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=BTC-USDT'
URL_3 = 'https://api.bybit.com/v2/public/tickers?symbol=BTCUSD'

ERROR_MESSAGE = 'Resived invalid data.'


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
            'BINANCE:': response.json().get('price'),
            'KUCOIN:': response_2.json().get('data')['price'],
            'BYBIT:': response_3.json().get('result')[0].get('last_price'),
        }

        for price in prices.values():
            if price is None or len(price) == 0:
                logs.insert_one(
                    {
                        'message': ERROR_MESSAGE,
                        'time': dt.now()
                    }
                )
                raise TypeError(ERROR_MESSAGE)

        r.mset(prices)
        return prices


def main():
    p = BTCprice(URL_1, URL_2, URL_3)

    conn = psycopg2.connect(
        host='localhost',
        password=f'{password}',
        port=5432,
        dbname='price',
        user='postgres',
    )

    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS \
                   prices (burse TEXT, price NUMERIC, timestamp TIMESTAMP)')

    logs.insert_one(
        {
            'message': 'Start',
            'time': dt.now()
        }
    )

    while True:
        price_list = p.get_price()
        with conn.cursor() as cursor:
            for burse, price in price_list.items():
                timestemp = dt.now()
                cursor.execute(
                    'INSERT INTO prices\
                          (burse, price, timestamp) VALUES (%s, %s, %s)',
                    (burse, price, timestemp)
                )
                conn.commit()
        time.sleep(60)


if __name__ == '__main__':
    main()
