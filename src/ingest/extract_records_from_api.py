import httpx
from tenacity import retry, wait_fixed, stop_after_attempt
from loguru import logger
from datetime import datetime
from urllib.parse import urljoin
import asyncio
from settings import settings


async def get_trips_records(limit: int, offset: int):
    async with httpx.AsyncClient(timeout=settings.timeout) as client:

        params = {
            "$limit" : limit,
            "$offset" : offset,
            "$order" : ":id",
            "$select" : ":*,*"
        }

        response = await client.get(settings.base_url, params=params)
        return response.json()




print(asyncio.run(get_trips_records(limit=4, offset=0)))