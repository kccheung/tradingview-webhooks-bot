import asyncio
import json
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
keyy = os.environ.get("PUSHOVER_USER_KEY")
# print(keyy)
po_client = Client(keyy)


def track(skip=False):
    global prev, po_client
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json, text/plain, */*"
    headers["content-type"] = "application/json;charset=UTF-8"
    headers[
        "cookie"] = "_gid=GA1.2.526308827.1659264479; BITGET_LOCAL_COOKIE={%22bitget_lang%22:%22en%22%2C%22bitget_unit%22:%22USD%22%2C%22bitget_showasset%22:false%2C%22bitget_theme%22:%22black%22%2C%22bitget_layout%22:%22right%22%2C%22bitget_valuationunit%22:1%2C%22bitgt_login%22:false}; _ga_clientid=1764596555.1659264478; _ga_sessionid=1659264478; _tt_enable_cookie=1; _ttp=72a6f985-89b7-437e-8a48-aea2e5fea53f; locale=en-US; _ym_uid=165926448658216582; _ym_d=1659264486; __utma=260101022.1764596555.1659264478.1659264486.1659264486.1; __utmc=260101022; __utmz=260101022.1659264486.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _hjFirstSeen=1; _hjIncludedInSessionSample=0; _hjSession_2898268=eyJpZCI6ImMzZTE5MTA1LTI3MGEtNDIyMi04YzMwLWRkYjk3ZDliZjliYiIsImNyZWF0ZWQiOjE2NTkyNjQ0ODYzOTksImluU2FtcGxlIjpmYWxzZX0=; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=1; _ym_isad=1; afUserId=966c9c46-c600-4822-ac41-e171b10dadae-p; AF_SYNC=1659264488772; _ym_visorc=b; _9755xjdesxxd_=32; YD00635511674906%3AWM_NI=0QhFCsK11H8kE%2Fi9AttEVYJcHpR%2FbivzIFswbZK5LGyZeTzA64QEoo7zJREo1rJxGxYSfvB2uvDFdd0G4Fq892xvXnCa2EbdLbym5876tH9c3i0tSGDN1xVihnZuV9%2F7Yms%3D; YD00635511674906%3AWM_NIKE=9ca17ae2e6ffcda170e2e6eea7c25e89b289afb1479b928ba6d14b868e8b82d8498897ab91aa67f1edbd99c82af0fea7c3b92af48fa8a7c27989efffa9bb6187f1a7dac480b89b9abac9698cb09ed0e163b6a687a7f27da28685d6cf4dbb86abb5f94bf6ec9fa6ee65f5a686b3c865f3ae8699c6749b8daa97c13ab7aa8ca7e770f499fca7f434fbeeb897db6681bffea6e17090beb78eb834f5eabca9bc69f1b70095d24d8892ff99d563acb29696d533ede9978ef237e2a3; YD00635511674906%3AWM_TID=VRRVbuGOk1xFFQAEAQORWWde9vmzn9wU; __zlcmid=1BEl7E99CNz5b8B; _hjSessionUser_2898268=eyJpZCI6Ijg4NDc5ODE0LWM2ZWEtNThlYS05MDE4LTM0MWE5YWZhOGY5NCIsImNyZWF0ZWQiOjE2NTkyNjQ0Nzk3NzgsImV4aXN0aW5nIjp0cnVlfQ==; __snaker__id=HFQ3inUWxwMGB1BI; bt_rtoken=upex:session:id:4bc645b542cbbf411fa1e2f04f5a6ad206a97152febfecc0039d1a140cc3a007; bt_sessonid=447001ce-dcae-4534-b1fa-4986d043ba61; bt_newsessionid=eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI1ZjQ4NzgwZC0xM2NlLTRmMjEtYWVlNy1kZTlkMGUzNjNhZDE0MTUwNzA4NzciLCJ1aWQiOiJPeFU2cmlLRXhLczlHaXRDblF0eXRnPT0iLCJzdWIiOiJjaHViYW8qKioqQGdtYWlsLmNvbSIsImlwIjoiM0RrN3BSeFlZdGlRdFlkTjJFZ3JLUT09IiwiZGlkIjoiU1Y4VFdveEQwMG5WWGdYRndnazZJbEhaZy80MnlDVDU2enpJbzhxNVFDOTEyUklzWlFLTTVIQmtTNHlUUmhuVyIsInN0cyI6MCwiaWF0IjoxNjU5MjY0NzQzLCJleHAiOjE2NjcwNDA3NDMsInB1c2hpZCI6ImRka1NMR1VDak9Sd1pFdU1rMFlaMWc9PSIsImlzcyI6InVwZXgifQ.ttjTHEtieO_lb4AbFcNxUPW1IBTSAyUibuOA424Z7LE; _uetsid=3e0523d010be11ed8925a1b96418563a; _uetvid=3e05919010be11edb3063942ffe34f5a; _ga_B8RNGYK5MS=GS1.1.1659264478.1.1.1659265247.0; _ga=GA1.2.1764596555.1659264478; __utmt=1; __utmb=260101022.10.10.1659264486; gdxidpyhxdE=1%2FZZywgnUbS7iph27WGeyuZWS0I44be2VfTwRrhnaqqtH%2BSjx7PIBQUNh%5C9%2FzRE8OuQzimSbNAT%2Fhc4SrjjvRUDkLd0kEAOyWkphwMP4CdAGZyzSqYPSyOwz3dOvbC%2BwA8bsT8AgZEV35WyViY%5CIWPNQpN6PNxhVtvcKt9wcIRnZknPg%3A1659266230101; _ga_Z8Q93KHR0F=GS1.1.1659264478.1.1.1659265415.0; arp_scroll_position=1845"
    now_utc = datetime.now(timezone.utc)
    # today = now_utc.strftime("%Y-%m-%d")
    request_url = f"https://www.bitget.com/v1/trigger/trace/order/currentList"
    # print(request_url)
    payload = {"traderUid": "1201450986", "pageNo": 1, "pageSize": 9999, "languageType": 0}
    resp = requests.post(request_url, data=json.dumps(payload), headers=headers)
    data = resp.json()
    if not data['data']['items']:
        if bool(prev):
            po_client.send_message(f"{now_utc}", title=f"closed all orders", priority=2, expire=120, retry=30, timestamp=int(time.time()))
            print(f"{now_utc}: closed orders, {data}")
            prev = {}
            return
        print(f"{now_utc}: no opening orders, {data}")
        return
    new = data['data']['items'][0]
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
