import requests
import time
import redis
import logging
from logging.handlers import RotatingFileHandler

URL_1 = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
URL_2 = 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=BTC-USDT'
URL_3 = 'https://api.bybit.com/v2/public/tickers?symbol=BTCUSD'

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def init_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s, %(levelname)s, %(message)s, %(name)s'
    )
    file_handler = RotatingFileHandler(
        'parcer.log',
        maxBytes=5000000,
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


logger = init_logger()


def get_binance_price(url):
    try:
        response = requests.get(url)
        r.set('Binance_BTCUSD', response.json().get('price'))
        # print(response.json().get('price'))
    except Exception as error:
        logger.error(error)

    return response.json()


def get_kucoin_price(url):
    try:
        response = requests.get(url)
        r.set('Kucoin_BTCUSD', response.json().get('data')['price'])
        # print(response.json().get('data')['price'])
    except Exception as error:
        logger.error(error)
    return response.json().get('data')


def get_bybit_price(url):
    try:
        response = requests.get(url)
        r.set('Bybit_BTCUSD',
              response.json().get('result')[0].get('last_price'))
        # print(response.json().get('result')[0].get('last_price'))
    except Exception as error:
        logger.error(error)
    return response.json().get('result')[0]


def check_instance(response):
    if not isinstance(response, dict):
        logger.error('Получен не словарь')
    return response


def main():
    logger.info('Start.')
    while True:
        responses = (get_binance_price(URL_1),
                     get_kucoin_price(URL_2),
                     get_bybit_price(URL_3))
        for response in responses:
            check_instance(response)
        time.sleep(60)


if __name__ == '__main__':
    main()
