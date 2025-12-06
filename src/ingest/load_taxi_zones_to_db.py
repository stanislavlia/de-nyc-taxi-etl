import httpx
from tenacity import retry, wait_fixed, stop_after_attempt
from loguru import logger
from datetime import datetime
from urllib.parse import urljoin
import asyncio
import psycopg2
from settings import settings
from typing import List, Dict, Optional
import json

async def get_taxi_zones_records(limit) -> List[Dict]:
    logger.info(f"Starting to fetch taxi zones with limit={limit}")
    logger.debug(f"API URL: {settings.taxi_zones_base_url}")
    
    try:
        async with httpx.AsyncClient(timeout=settings.timeout) as client:
            logger.info(f"Making HTTP GET request to {settings.taxi_zones_base_url}")
            logger.debug(f"Request params: {{'$limit': {limit}}}")
            
            response = await client.get(settings.taxi_zones_base_url, params={"$limit": limit})
            
            logger.info(f"Response received with status code: {response.status_code}")
            response.raise_for_status()
            
            data = response.json()
            logger.success(f"Successfully fetched {len(data)} taxi zones")
            logger.debug(f"First record keys: {list(data[0].keys()) if data else 'No data'}")
            
            return data
            
    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_taxi_zones_records: {e}")
        raise


def bulk_insert_taxi_zones_to_db(taxi_zones: List[Dict]):
    logger.info(f"Starting bulk insert of {len(taxi_zones)} taxi zones")
    
    try:
        logger.debug(f"Connecting to database: {settings.db_host}:{settings.db_port}/{settings.db_database}")
        conn = psycopg2.connect(**settings.db_conn_params())
        conn.autocommit = True
        logger.success("Database connection established")
        
        cursor = conn.cursor()
        logger.debug("Database cursor created")
        
        query = """INSERT INTO taxi_zones VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"""
        logger.debug(f"Insert query: {query}")
        
        values_to_insert = []
        curr_timstamp = datetime.now()
        logger.info(f"Processing records with timestamp: {curr_timstamp}")
        
        for i, tx_zone in enumerate(taxi_zones):
            logger.debug(f"Processing zone {i+1}/{len(taxi_zones)}: {tx_zone.get('zone', 'Unknown')}")
            
            polygon_json = json.dumps(tx_zone.get("the_geom"))
            logger.trace(f"  - Polygon JSON length: {len(polygon_json)} chars")
            
            values = (
                i, 
                tx_zone.get("shape_area"),
                tx_zone.get("shape_leng"),
                tx_zone.get("zone"),
                tx_zone.get("locationid"),
                tx_zone.get("borough"),
                polygon_json,
                curr_timstamp
            )
            values_to_insert.append(values)
            logger.trace(f"  - Location ID: {tx_zone.get('locationid')}, Borough: {tx_zone.get('borough')}")
        
        logger.info(f"Prepared {len(values_to_insert)} records for insertion")
        logger.debug("Executing bulk insert...")
        
        cursor.executemany(query, values_to_insert)
        # committing changes
        conn.commit()
        
        logger.success(f"Successfully inserted {len(values_to_insert)} taxi zones into database")
        logger.info("Closing database cursor and connection")
        
        cursor.close()
        conn.close()
        logger.debug("Database resources cleaned up")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        logger.error(f"Error code: {e.pgcode}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in bulk_insert_taxi_zones_to_db: {e}")
        raise


if __name__ == "__main__":
    logger.info("Starting Taxi Zones Load Process")
    
    try:
        logger.info("Fetching taxi zones data...")
        result = asyncio.run(get_taxi_zones_records(limit=500))
        
        logger.info(f"Fetch Result len: {len(result)}")
        
        logger.info(f"Loading to DB...")
        bulk_insert_taxi_zones_to_db(result)
        logger.success("Load Process Completed Successfully")
        
    except Exception as e:
        logger.critical(f"Load Process Failed: {e}")
        logger.exception("Full traceback:")
        raise