import asyncio
from datetime import datetime, timezone

import aiocron as aiocron
import numpy as np
import pandas as pd
import pymarketstore as pymkts
import requests
from requests.structures import CaseInsensitiveDict


cli = pymkts.Client(endpoint="http://localhost:5993/rpc")
BUCKET_NAME = "WEIGHTED_FUNDING_%s/5Min/SIGNAL"
# BUCKET_NAME = "WEIGHTED_FUNDING/1Min/SIGNAL"

# URL = "https://api.laevitas.ch/charts/futures/weighted_funding/btc?start=2022-07-01&end=2022-07-01"


def get_funding(symbol='btc'):
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json, text/plain, */*"
    headers["Referer"] = "https://app.laevitas.ch/"
    headers[
        "Authorization"] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImNodWJhbyIsImlkIjoiNTI0OTkxYzQtMWM1NS00MDZjLThmYmYtYTMwNWQ5ZThkZTA5IiwiZW1haWwiOiJrY2NoZXVuZzA2MjVAZ21haWwuY29tIiwicm9sZSI6IkNMSUVOVCIsImltYWdlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EtL0FPaDE0R2lVaHpaMmpUTXE1QW05OHNsTnA5U2VfSEkxcU9tN2ZXTFJyNmE1SFE9czk2LWMiLCJmaXJzdE5hbWUiOiJEYXZpZCBDIiwibGFzdE5hbWUiOiJDIiwiaWF0IjoxNjU2NjY2NjQ0LCJleHAiOjE2NTkyNTg2NDR9.Lnv4zTYrhJ2CpYKJqJxeFbXrqe25FnKj4VgWlEzdi9w"
    now_utc = datetime.now(timezone.utc)
    today = now_utc.strftime("%Y-%m-%d")
    request_url = f"https://api.laevitas.ch/charts/futures/weighted_funding/{symbol}?start={today}&end={today}"
    print(request_url)
    resp = requests.get(request_url, headers=headers)
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
