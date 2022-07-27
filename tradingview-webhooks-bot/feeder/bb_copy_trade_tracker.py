import asyncio
import time
from copy import deepcopy
from datetime import datetime, timezone

import aiocron as aiocron
import requests
from requests.structures import CaseInsensitiveDict
from jsondiff import diff
from pprint import pprint
from pushover import init, Client
import os


prev = {}
token = os.environ.get("PUSHOVER_API_TOKEN")
# print(token)
init(token)
leadermark = os.environ.get("LEADERMARK")
keyy = os.environ.get("PUSHOVER_USER_KEY")
# print(keyy)
po_client = Client(keyy)


def track(skip=False):
    global prev, po_client
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json, text/plain, */*"
    headers["usertoken"] = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTgzMzExODMsInVzZXJfaWQiOjU4NTYzMH0.uuQP20nlQ3F8lWvkIVCgOIWDRzNOWQ1eqTssP_Zc78PwsEJoroh4uFskwAG_M7bFsbWPn3XVTXnFtQtC9aDhKw"
    now_utc = datetime.now(timezone.utc)
    # today = now_utc.strftime("%Y-%m-%d")
    # request_url = f"https://api2.byapis.com/fapi/beehive/public/v1/common/leader-history?timeStamp={int(time.time()*1000)}&page=1&pageSize=20&leaderMark=bTTxdRatXQ6XkYDX8mHgyw%3D%3D"
    request_url = f"https://api2.byapis.com/fapi/beehive/public/v1/common/order/list-detail?timeStamp={int(time.time()*1000)}&leaderMark={leadermark}"
    # request_url = f"https://api2.byapis.com/fapi/beehive/public/v1/common/position/list?timeStamp={int(time.time()*1000)}&leaderMark=bTTxdRatXQ6XkYDX8mHgyw%3D%3D"
    # print(request_url)
    resp = requests.get(request_url, headers=headers)
    data = resp.json()
    if not data['result']['data']:
        if bool(prev):
            po_client.send_message(f"{now_utc}", title=f"closed all orders", priority=2, expire=120, retry=30, timestamp=int(time.time()))
            print(f"{now_utc}: closed orders, {data}")
            prev = {}
            return
        print(f"{now_utc}: no opening orders, {data}")
        return
    new = data['result']['data'][0]
    thediff = diff(prev, new)
    print(f"{now_utc}: {new}")

    if bool(thediff) and not skip:
        pprint(new)
        pprint(thediff)
        po_client.send_message(f"{now_utc}", title=f"{new}", priority=2, expire=120, retry=30, timestamp=int(time.time()))
    prev = deepcopy(new)


@aiocron.crontab('*/1 * * * *', start=True)
async def start_track():
    try:
        track()
    except Exception as errc:
        print(f"Error Connecting: {errc}, retry on next batch")


if __name__ == '__main__':
    track(True)
    asyncio.get_event_loop().run_forever()
