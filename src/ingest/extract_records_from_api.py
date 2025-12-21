import httpx
from tenacity import retry, wait_fixed, stop_after_attempt
from loguru import logger
from datetime import datetime
from urllib.parse import urljoin
import asyncio
from settings import settings
from typing import List, Dict, Optional
import json
import psycopg2


async def get_trips_records(limit: int, offset: int) -> List[Dict]:
    try:
        async with httpx.AsyncClient(timeout=settings.timeout) as client:
            
            params = {
                "$limit" : limit,
                "$offset" : offset,
                "$order" : ":id",
                "$select" : ":*,*"
            }
            logger.info(f"Call paramerers: {params}")
            response = await client.get(settings.base_url, params=params)
            logger.info(f"Extracted {len(response.json())} taxi trips record")
            return response.json()
    except Exception as e:
        logger.error(f"Failed get taxi trips: {e}")


def read_current_offset():
    conn = psycopg2.connect(**settings.db_conn_params())
    cursor = conn.cursor()
    cursor.execute("SELECT current_offset FROM data_load_info ORDER BY updated_at DESC LIMIT 1")
    result = cursor.fetchone()
    offset = result[0]
    logger.info(f"Read current offset info from db: {result}")
    return offset

def update_current_offset(new_offset):
    conn = psycopg2.connect(**settings.db_conn_params())
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("UPDATE data_load_info SET current_offset=%s, updated_at=%s", (new_offset, datetime.now()))
    conn.commit()
    logger.info(f"Updated offset in DB to: {new_offset}")

def insert_bulk_trip_records(records: List[Dict]):
    
    try:
        logger.debug(f"Connecting to database: {settings.db_host}:{settings.db_port}/{settings.db_database}")
        conn = psycopg2.connect(**settings.db_conn_params())
        conn.autocommit = True
        logger.success("Database connection established")
        cursor = conn.cursor()
        logger.debug("Database cursor created")

        query = f"""
        INSERT INTO taxi_trips (
                    id, socrata_created_at, socrata_updated_at, version,
                    hvfhs_license_num, dispatching_base_num, originating_base_num,
                    request_datetime, on_scene_datetime, pickup_datetime, dropoff_datetime,
                    pulocationid, dolocationid,
                    trip_miles, trip_time,
                    base_passenger_fare, tolls, bcf, sales_tax, 
                    congestion_surcharge, airport_fee, tips, driver_pay,
                    shared_request_flag, shared_match_flag, wav_request_flag, wav_match_flag
                ) VALUES (
                    %(id)s, %(socrata_created_at)s, %(socrata_updated_at)s, %(version)s,
                    %(hvfhs_license_num)s, %(dispatching_base_num)s, %(originating_base_num)s,
                    %(request_datetime)s, %(on_scene_datetime)s, %(pickup_datetime)s, %(dropoff_datetime)s,
                    %(pulocationid)s, %(dolocationid)s,
                    %(trip_miles)s, %(trip_time)s,
                    %(base_passenger_fare)s, %(tolls)s, %(bcf)s, %(sales_tax)s,
                    %(congestion_surcharge)s, %(airport_fee)s, %(tips)s, %(driver_pay)s,
                    %(shared_request_flag)s, %(shared_match_flag)s, %(wav_request_flag)s, %(wav_match_flag)s
                )
        ON CONFLICT (id) DO NOTHING; --skip duplicates
    """
        values_to_insert = []

        for trip in records:
            values = {
                    'id': trip.get(':id'),
                    'socrata_created_at': trip.get(':created_at'),
                    'socrata_updated_at': trip.get(':updated_at'),
                    'version': trip.get(':version'),
                    'hvfhs_license_num': trip.get('hvfhs_license_num'),
                    'dispatching_base_num': trip.get('dispatching_base_num'),
                    'originating_base_num': trip.get('originating_base_num'),
                    'request_datetime': trip.get('request_datetime'),
                    'on_scene_datetime': trip.get('on_scene_datetime'),
                    'pickup_datetime': trip.get('pickup_datetime'),
                    'dropoff_datetime': trip.get('dropoff_datetime'),
                    'pulocationid': trip.get('pulocationid'),
                    'dolocationid': trip.get('dolocationid'),
                    'trip_miles': trip.get('trip_miles'),
                    'trip_time': trip.get('trip_time'),
                    'base_passenger_fare': trip.get('base_passenger_fare'),
                    'tolls': trip.get('tolls'),
                    'bcf': trip.get('bcf'),
                    'sales_tax': trip.get('sales_tax'),
                    'congestion_surcharge': trip.get('congestion_surcharge'),
                    'airport_fee': trip.get('airport_fee'),
                    'tips': trip.get('tips'),
                    'driver_pay': trip.get('driver_pay'),
                    'shared_request_flag': trip.get('shared_request_flag'),
                    'shared_match_flag': trip.get('shared_match_flag'),
                    'wav_request_flag': trip.get('wav_request_flag'),
                    'wav_match_flag': trip.get('wav_match_flag')
                }
            values_to_insert.append(values)

        cursor.executemany(query, values_to_insert)
        conn.commit()

        inserted = cursor.rowcount
        skipped = len(records) - inserted
        
        logger.success(f"Taxi trips Bulk Insert Completed | inserted: {inserted} | skipped: {skipped}")
        if inserted == 0:
            logger.warning(f"NO ROWS INSERTED!!! | ALL DUPLICATES | CHECK PIPELINE SETTINGS!")

    except Exception as e:
        logger.error(f"Failed to bulk insert trips: {e}")

    finally:
        cursor.close()
        conn.close()

def run_ingestion():
    logger.info(f"Starting Loading Taxi Trips from API to Staging DB")
    
    offset = 0
    if settings.read_offset_from_db:
        offset = read_current_offset()
    
    records = asyncio.run(get_trips_records(limit=settings.limit, offset=offset))
    insert_bulk_trip_records(records)

    if settings.autoupdate_offset:
        update_current_offset(new_offset=offset + settings.limit)
    
    

if __name__ == "__main__":
    run_ingestion()
    

    
