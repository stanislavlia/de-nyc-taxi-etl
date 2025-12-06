import httpx
from tenacity import retry, wait_fixed, stop_after_attempt
from loguru import logger
from datetime import datetime
from urllib.parse import urljoin
import asyncio

#=============SETTINGS==========
BASE_URL="https://data.cityofnewyork.us/resource/u253-aew4.json"
OFFSET=0
LIMIT=4
TIMEOUT=40


async def get_trips_records(limit: int, offset: int):
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:

        params = {
            "$limit" : LIMIT,
            "$offset" : OFFSET,
            "$order" : ":id",
            "$select" : ":*,*"
        }

        response = await client.get(BASE_URL, params=params)
        return response.json()


print(asyncio.run(get_trips_records(limit=4, offset=0)))