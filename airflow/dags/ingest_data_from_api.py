from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator # type: ignore
from airflow.providers.standard.operators.bash import BashOperator # type: ignore
import psycopg2
from loguru import logger
import httpx
from airflow.models import Variable


# Default arguments applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

def get_db_config() -> dict:
    return {
        'host': Variable.get("DB_HOST"),
        'port': int(Variable.get("DB_PORT", default_var="25060")),
        'database': Variable.get("DB_DATABASE"),
        'user': Variable.get("DB_USER"),
        'password': Variable.get("DB_PASSWORD"),
        'sslmode': 'require'
    }

def get_api_config() -> dict:
    return {
        'base_url': Variable.get("API_BASE_URL"),
        'limit': int(Variable.get("BATCH_LIMIT", default_var="1000")),
        'timeout': int(Variable.get("API_TIMEOUT", default_var="40"))
    }


with DAG(
    dag_id="ingest_taxi_trips_from_api",
    schedule=timedelta(minutes=5),
    catchup=False,
    start_date=datetime(2026, 1, 1),
    description="Ingests taxi trips data from API to Postgres"
):
    
    #=============TASK IMPLEMENTATION=============

    def read_current_offset_task(**context):
        conn = psycopg2.connect(**get_db_config())
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT current_offset FROM data_load_info ORDER BY updated_at DESC LIMIT 1")
            result = cursor.fetchone()
            offset = result[0]
            logger.info(f"Read current offset info from db: {result}")
            context["task_instance"].xcom_push(key="offset", value=offset)
            return offset
        finally:
            cursor.close()
            conn.close()
        

    def get_trips_records_task(**context):
        try:
            # Get offset from previous task
            offset = context["task_instance"].xcom_pull(
                task_ids='read_current_offset',  # specify task id
                key="offset"
            )
            
            # Get config
            api_config = get_api_config()
            
            # Use sync client
            with httpx.Client(timeout=api_config['timeout']) as client:
                params = {
                    "$limit": api_config['limit'],
                    "$offset": offset,
                    "$order": ":id",
                    "$select": ":*,*"
                }
                
                logger.info(f"Call parameters: {params}")
                response = client.get(api_config['base_url'], params=params)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Extracted {len(data)} taxi trips records")
                
                # Push to XCom for next task
                context["task_instance"].xcom_push(key="records", value=data)
                context["task_instance"].xcom_push(key="record_count", value=len(data))
                
                return len(data)
                
        except Exception as e:
            logger.error(f"Failed to get taxi trips: {e}")
            raise
    

    def load_trip_records_task(**context):
    
        try:
            #get trip records data
            records = context["task_instance"].xcom_pull(
                task_ids='get_trips_records',  # specify task id
                key="records"
            )

            conn = psycopg2.connect(**get_db_config())
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
            

            #Push to Xcom
            context["task_instance"].xcom_push(key="inserted", value=inserted)
            context["task_instance"].xcom_push(key="skipped", value=skipped)


        except Exception as e:
            logger.error(f"Failed to bulk insert trips: {e}")
            raise e

        finally:
            cursor.close()
            conn.close()
    

    def update_current_offset_task(**context):
        offset = context["task_instance"].xcom_pull(
            task_ids='read_current_offset',
            key="offset"
        )
        limit = get_api_config()["limit"]
        new_offset = offset + limit
        
        conn = psycopg2.connect(**get_db_config())
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE data_load_info SET current_offset=%s, updated_at=%s", 
                        (new_offset, datetime.now()))
            conn.commit()
            logger.info(f"Updated offset in DB to: {new_offset}")
            return new_offset
        finally:
            cursor.close()
            conn.close()


    #=====SET PIPELINE=====

    read_offset = PythonOperator(
        task_id="read_current_offset",
        python_callable=read_current_offset_task
    )

    get_trips_records = PythonOperator(
        task_id="get_trips_records",
        python_callable=get_trips_records_task
    )

    load_trip_records = PythonOperator(
        task_id="load_trip_records",
        python_callable=load_trip_records_task
    )

    update_current_offset = PythonOperator(
        task_id="update_current_offset",
        python_callable=update_current_offset_task
    )

    read_offset >> get_trips_records >> load_trip_records >> update_current_offset