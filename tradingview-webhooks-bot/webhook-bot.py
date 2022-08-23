"""
Tradingview-webhooks-bot is a python bot that works with tradingview's webhook alerts!
This bot is not affiliated with tradingview and was created by @robswc

You can follow development on github at: github.com/robswc/tradingview-webhook-bot

I'll include as much documentation here and on the repo's wiki!  I
expect to update this as much as possible to add features as they become available!
Until then, if you run into any bugs let me know!
"""
import re

from actions import send_order, parse_webhook
from auth import get_token
from flask import Flask, request, abort

import numpy as np
import pymarketstore as pymkts
import ciso8601

# Create Flask object called app.
app = Flask(__name__)
cli = pymkts.Client(endpoint="http://localhost:5993/rpc")


# Create root to easily let us know its on/working.
@app.route('/')
def root():
    return 'online'


OBDC_TEMPLATE = r'Alert: OBDC \((?P<depth>.*)%\) \> -99 \((?P<paira>.*)\) (?P<alert>.*)(?P<pairb>.*) on (?P<exchange>.*)Price: (?P<price>.*) • Order Book Depth Cumulative \(aggregation\) \((?P<depthb>.*)\)Ratio: (?P<ratio>.*) \> -99'


def feed_store(data):
    rec_chunks = None
    if 'trdr' in data:
        # \create BTCUSD_OBDC/1Sec/SIGNAL:Symbol/Timeframe/AttributeGroup depth,price,ratio/float32:Epoch/int64 fixed
        bucket_name = f'BTCUSD_OBDC/1Sec/SIGNAL'
        epoch = data['ts']
        msg = data['msg'].replace('\n', '')
        m3 = re.match(OBDC_TEMPLATE, msg)
        # print(msg)
        # print(m3)
        if m3:
            # pair = m3.group('paira').replace('USDT', 'USD').replace('BTC', 'XBT').replace('/', '')
            depth = float(m3.group('depth').replace('%', ''))
            price = float(m3.group('price'))
            ratio = float(m3.group('ratio'))
            print(f'{depth}, {price}, {ratio}')
            rec_chunks = np.array([(epoch, depth, price, ratio)],
                                  dtype=[('Epoch', 'i8'), ('depth', '<f4'), ('price', '<f4'), ('ratio', '<f4')])
    else:
        # tradingview alert
        period = 'NA'
        if data['interval'] == '60':
            period = '1H'
        elif data['interval'] == '240':
            period = '4H'
        # CME_BTC!_EMASAR_1H/1Min/SIGNAL
        # {'key': 'key', 'exchange': 'CME', 'ticker': 'BTC1!', 'dt': '2022-02-15T14:43:53Z', 'interval': '60', 'open': 44365, 'high': 44620, 'low': 44285, 'close': 44455, 'mean': 43009.0, 'inner_sky': 45224.5, 'moon': 46657.5, 'outer_sky': 48090.5, 'shore': 41057.0, 'start_of_underworld': 37664.0, 'end_of_underworld': 34270.5, 'buoy': 39360.5, 'beach': 42442.0, 'marker': 39054.0}
        bucket_name = f'{data["exchange"]}_{data["ticker"]}_{data["signal"]}_{period}/1Min/SIGNAL'

        # \create CME_BTC1!_EMASAR_1H/1Min/SIGNAL:Symbol/Timeframe/AttributeGroup open,high,low,close,mean,inner_sky,moon,outer_sky,shore,start_of_underworld,end_of_underworld,buoy,beach,marker/float32:Epoch/int64 fixed
        epoch = int(ciso8601.parse_datetime(data['dt']).timestamp())
        rec_chunks = np.array([(epoch, data['open'], data['high'], data['low'], data['close'], data['mean'], data['inner_sky'], data['moon'], data['outer_sky'], data['shore'], data['start_of_underworld'], data['end_of_underworld'], data['buoy'], data['beach'], data['marker'])],
                              dtype=[('Epoch', 'i8'), ('open', '<f4'), ('high', '<f4'), ('low', '<f4'), ('close', '<f4'), ('mean', '<f4'), ('inner_sky', '<f4'), ('moon', '<f4'), ('outer_sky', '<f4'), ('shore', '<f4'), ('start_of_underworld', '<f4'), ('end_of_underworld', '<f4'), ('buoy', '<f4'),
                                     ('beach', '<f4'), ('marker', '<f4')])
    rec_chunks = rec_chunks.view(np.recarray)

    response = cli.write(rec_chunks, bucket_name)
    print("sent write request to %s:\n%s\n" % (cli.endpoint, response))
    if response and 'responses' in response and not response['responses']:
        print("finished writing %d records to %s" % (rec_chunks.size, bucket_name))


@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        # Parse the string data from tradingview into a python dict
        data = parse_webhook(request.get_data(as_text=True))
        # Check that the key is correct
        if get_token() == data['key']:
            print(' [Alert Received] ')
            print('POST Received:', data)
            feed_store(data)
            # send_order(data)
            return '', 200
        abort(403)
    abort(400)


if __name__ == '__main__':
    app.run()
