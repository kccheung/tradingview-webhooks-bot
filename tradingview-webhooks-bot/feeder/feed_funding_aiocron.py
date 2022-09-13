import asyncio
from datetime import datetime, timezone

import aiocron as aiocron
import numpy as np
import pandas as pd
import pymarketstore as pymkts
import requests
from requests.structures import CaseInsensitiveDict

import logging
logging.basicConfig(level=logging.DEBUG)


cli = pymkts.Client(endpoint="http://localhost:5993/rpc")
BUCKET_NAME = "WEIGHTED_FUNDING_%s/5Min/SIGNAL"
# BUCKET_NAME = "WEIGHTED_FUNDING/1Min/SIGNAL"

# URL = "https://api.laevitas.ch/charts/futures/weighted_funding/btc?start=2022-07-01&end=2022-07-01"


def get_funding(symbol='btc'):
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json, text/plain, */*"
    headers["Referer"] = "https://app.laevitas.ch/"
    headers[
        "Authorization"] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImNodWJhbyIsImlkIjoiNTI0OTkxYzQtMWM1NS00MDZjLThmYmYtYTMwNWQ5ZThkZTA5IiwiZW1haWwiOiJrY2NoZXVuZzA2MjVAZ21haWwuY29tIiwicm9sZSI6IkNMSUVOVCIsImltYWdlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EtL0FPaDE0R2lVaHpaMmpUTXE1QW05OHNsTnA5U2VfSEkxcU9tN2ZXTFJyNmE1SFE9czk2LWMiLCJmaXJzdE5hbWUiOiJEYXZpZCBDIiwibGFzdE5hbWUiOiJDIiwiaWF0IjoxNjYxMTM1OTQ2LCJleHAiOjE2NjExNTc1NDZ9.7R06rVqlTK7lVzMGExHD1KJOouopuiTlX5S301QFr6I"
    now_utc = datetime.now(timezone.utc)
    headers["recaptcha"] = "03ANYolqtJAUAwBHnjZiu-suovE5_ZyzjDpGpIKbPZIteFh6ObwA8WcPB4Xwyh-jIkr5EmnZQI52uvOWszHlv9qUkR87OrqFpUNvUeQL8nDK1W629vZzrzPj94wNCqRfcOCQ_QC8RySG0wM5HrVIfydDpy-P95iVWTA8f84NXvdCrhXCM_A2q0UIwrAiJyTm6P899CmekRcK6TCaU47BfLSEfLamWHOtmjLdOeJFKKTAihrZ1WIvqvGfFofOdUM2OQWn6wkrE2YQWU5VadXVdXbC_HOl9-wzYmgTK4_M-UdwP1vh4OsO0hE6Od5Ev7vBRvrGHnkGxxdt2UW-RSViprDM3Ml9LHVnZIZTh0n84uRsd8VuKBDn3RTW09qc_v-Pd3dJHgf_FMZ65Owq2bk7HnpiSfG1HSj5yPzD_lQrJ1X0kSU0BaRQs0cShZuADAvzVFI0mPWa6mFeSax6s32GKpEzSF86NbM0YD92W2lPdijdEWdbxtOD_sw8UIfd59gWofBYRuz_4ti78MaRLEojrp0oJRuo4na9lvGg"
    today = now_utc.strftime("%Y-%m-%d")
    request_url = f"https://be.laevitas.ch/charts/futures/weighted_funding/{symbol}/?start={today}&end={today}"
    print(request_url)
    resp = requests.get(request_url, headers=headers, timeout=(5, 5))
    data = resp.json()

    # print(data)
    print(data[-1])
    # data = [data[-1]]  # take last record only

    # an_array = np.array(data)
    # print(an_array)
    df = pd.DataFrame(data)
    df["date"] = df["date"] / 1000  # nearest second only, 10 digits
    vals = list(df.itertuples(index=False, name=None))
    # print(vals)
    rec_chunks = np.array(vals, dtype=[('Epoch', 'i8'), ('apr', '<f4'), ('funding', '<f4'),
                                       ('next_fr', '<f4'), ('price', '<f4'),
                                       ('apr_c', '<f4'), ('funding_c', '<f4'), ('next_fr_c', '<f4'),
                                       ('apr_u', '<f4'), ('funding_u', '<f4'), ('next_fr_u', '<f4'), ])
    rec_chunks = rec_chunks.view(np.recarray)
    print(now_utc)
    # print(rec_chunks)

    # WEIGHTED_FUNDING/5Min/SIGNAL
    # \create WEIGHTED_FUNDING/5Min/SIGNAL:Symbol/Timeframe/AttributeGroup apr,funding,next_fr,price,apr_c,funding_c,next_fr_c,apr_u,funding_u,next_fr_u/float32:Epoch/int64 fixed
    # \create WEIGHTED_FUNDING_ETH/5Min/SIGNAL:Symbol/Timeframe/AttributeGroup apr,funding,next_fr,price,apr_c,funding_c,next_fr_c,apr_u,funding_u,next_fr_u/float32:Epoch/int64 fixed
    # \create WEIGHTED_FUNDING/1Min/SIGNAL:Symbol/Timeframe/AttributeGroup apr,funding,next_fr,price,apr_c,funding_c,next_fr_c,apr_u,funding_u,next_fr_u/float32:Epoch/int64 fixed
    bucket = BUCKET_NAME % symbol.upper()
    response = cli.write(rec_chunks, bucket)
    print("sent write request to %s:\n%s\n" % (cli.endpoint, response))
    if response and 'responses' in response and not response['responses']:
        print("finished writing %d records to %s" % (rec_chunks.size, bucket))


@aiocron.crontab('*/5 * * * *', start=True)
async def feed_marketstore():
    try:
        get_funding('btc')
        get_funding('eth')
    except Exception as errc:
        print(f"Error Connecting: {errc}, retry on next batch")


if __name__ == '__main__':
    asyncio.get_event_loop().run_forever()
