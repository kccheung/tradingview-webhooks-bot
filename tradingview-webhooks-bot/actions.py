import ccxt
import ast
from copy import deepcopy


def round_off_rating(number):
    """Round a number to the closest half integer.
    >>> round_off_rating(1.3)
    1.5
    >>> round_off_rating(2.6)
    2.5
    >>> round_off_rating(3.0)
    3.0
    >>> round_off_rating(4.1)
    4.0"""

    return round(number * 2) / 2


def parse_webhook(webhook_data):
    """
    This function takes the string from tradingview and turns it into a python dict.
    :param webhook_data: POST data from tradingview, as a string.
    :return: Dictionary version of string.

    {"type": "market", "side": "buy", "amount": "500", "symbol": "XBTUSD", "price": "None", "key": "8234023409fa3242309sdfasdf903024917325"}
    {"key": "key", "exchange": "{{exchange}}", "pair": "{{ticker}}", "dt": "{{timenow}}", "interval": "{{interval}}", "mean": {{plot_0}}, "inner_sky": {{plot_1}}, "moon": {{plot_2}}, "outer_sky": {{plot_3}}, "shore": {{plot_4}}, "start_of_underworld": {{plot_5}}, "end_of_underworld": {{plot_6}}, "buoy": {{plot_7}}, "beach": {{plot_8}}, "marker": {{plot_9}}}
    """
    # print(webhook_data)
    data = ast.literal_eval(webhook_data)
    for (k, v) in deepcopy(data).items():
        if isinstance(v, float):
            data[k] = round_off_rating(v)
    # print(data)
    return data


def calc_price(given_price):
    """
    Will use this function to calculate the price for limit orders.
    :return: calculated limit price
    """

    if given_price == None:
        price = given_price
    else:
        price = given_price
    return price


def send_order(data):
    """
    This function sends the order to the exchange using ccxt.
    :param data: python dict, with keys as the API parameters.
    :return: the response from the exchange.
    """

    # Replace kraken with your exchange of choice.
    exchange = ccxt.kraken({
        # Inset your API key and secrets for exchange in question.
        'apiKey': '',
        'secret': '',
        'enableRateLimit': True,
    })

    # Send the order to the exchange, using the values from the tradingview alert.
    print('Sending:', data['symbol'], data['type'], data['side'], data['amount'], calc_price(data['price']))
    order = exchange.create_order(data['symbol'], data['type'], data['side'], data['amount'], calc_price(data['price']))
    # This is the last step, the response from the exchange will tell us if it made it and what errors pop up if not.
    print('Exchange Response:', order)
