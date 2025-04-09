import json
import requests
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
import configparser

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException
from airflow.api.common.experimental.trigger_dag import trigger_dag

# ============ CONFIG ============ #
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.conf"
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "tmp_airvisual.json"
CITY = "Salaya"
STATE = "Nakhon Pathom"
COUNTRY = "Thailand"


from enum import Enum

class StateFlow(Enum):
    RESET = 0
    NORMAL = 1
    ONLY_WEATHER = 2
    ONLY_POLLUTION = 3
    ERROR = 4

# ============ FUNCTIONS ============ #
def get_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()

def is_data_in_db(date_time_str):
    conn, cursor = get_cursor()
    try:
        cursor.execute("""
            SELECT 1 FROM factairvisualtable
            WHERE date_time = %s
            LIMIT 1
        """, (date_time_str,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()

def trigger_backup_dag(context):
    dag_id = 'airvisual_pipeline_lat_long_v1'
    run_id = f"manual__backup__{datetime.now().isoformat()}"
    try:
        trigger_dag(dag_id=dag_id, run_id=run_id, conf={})
        logging.info(f"Triggered backup DAG: {dag_id}")
    except Exception as e:
        logging.error(f"Failed to trigger backup DAG: {e}")

def get_dt_str_ts_bangkok(ts_str: str):
    try:
        # ตรวจสอบ datetime ของข้อมูลล่าสุด
        # ts_str = data["data"]["current"]["pollution"]["ts"]
        ts_obj = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        ts_bangkok = ts_obj + timedelta(hours=7)
        current_dt_str = ts_bangkok.strftime("%Y-%m-%d %H:%M")
        return current_dt_str
    except:
        return None

def get_data_from_airvisual(ti=None):
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    api_key = config.get("api", "airvisual_key")

    URL = (
        "http://api.airvisual.com/v2/city"
        f"?city={CITY}&state={STATE}&country={COUNTRY}&key={api_key}"
    )

    logging.info(f"Requesting data from: {URL}")
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # ตรวจสอบ datetime ของข้อมูลล่าสุด
        ts_str = data["data"]["current"]["pollution"]["ts"]
        w_ts_str = data["data"]["current"]["weather"]["ts"]
        current_dt_str = get_dt_str_ts_bangkok(ts_str)
        w_current_dt_str = get_dt_str_ts_bangkok(w_ts_str)

        # เชื่อมต่อ Database แล้วตรวจสอบว่า datetime นี้มีอยู่แล้วหรือไม่
        data_time_now_obj = datetime.now()+ timedelta(hours=7)
        data_time_now_str = data_time_now_obj.strftime("%Y-%m-%d %H:00") 
        if is_data_in_db(data_time_now_str):
            raise AirflowSkipException(f"Data for {data_time_now_str} already exists in DB. Skipping DAG.")

        # โหลด datetime ปัจจุบันจากไฟล์ JSON ที่มีอยู่แล้ว
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f_check:
                prev_data = json.load(f_check)
            prev_dt_str = prev_data["data"]["current"]["pollution"]["ts"]
            w_prev_dt_str = prev_data["data"]["current"]["weather"]["ts"]
            ti.xcom_push(key='prev_dt_str', value=prev_dt_str)
            ti.xcom_push(key='w_prev_dt_str', value=w_prev_dt_str)

            prev_dt_str = get_dt_str_ts_bangkok(prev_dt_str) 
            w_prev_dt_str = get_dt_str_ts_bangkok(w_prev_dt_str)
            if current_dt_str == prev_dt_str \
                and w_current_dt_str == w_prev_dt_str  \
                and is_data_in_db(prev_dt_str) \
                and is_data_in_db(w_prev_dt_str):
                raise Exception(f"Data has not been updated yet: {current_dt_str} == {prev_dt_str}")
            
        if not OUTPUT_FILE.exists():
            ti.xcom_push(key='prev_dt_str', value=None)
            ti.xcom_push(key='w_prev_dt_str', value=None)

        # ใช้หลักการ "Atomic File Write":
        tmp_path = Path(str(OUTPUT_FILE) + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        tmp_path.rename(OUTPUT_FILE)
        logging.info("Data saved successfully.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

def read_json_data(ti=None):
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info("Read JSON file successfully.")
        # Optional: Validate or transform data
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        raise

def load_airvisual_to_postgres(ti=None):
    def parse_utc_to_bangkok(ts_str):
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(hours=7)
    conn, cursor = get_cursor()
    state_flow = StateFlow.RESET  # 0: reset, 1: Normal, 2: Only weather, 3: Only pollution, 4: error
    try:
        # Load mapping files
        with open('/opt/airflow/config/mapping_main_pollution.json', 'r', encoding='utf-8') as mf:
            pollution_mapping = json.load(mf)

        with open('/opt/airflow/config/mapping_weather_code.json', 'r', encoding='utf-8') as wf:
            weather_mapping = json.load(wf)

        # Load raw data
        with open('/opt/airflow/data/tmp_airvisual.json', 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        contents_data = raw_data["data"]
        pollution_ts = contents_data["current"]["pollution"]["ts"]
        weather_ts = contents_data["current"]["weather"]["ts"]

        # ดึงข้อมูล previous
        prev_dt_str = ti.xcom_pull(task_ids='get_data_from_airvisual', key='prev_dt_str')
        w_prev_dt_str = ti.xcom_pull(task_ids='get_data_from_airvisual', key='w_prev_dt_str')
        is_first_run = not prev_dt_str or not w_prev_dt_str

        # ISO datetime ปัจจุบันในรูปแบบ UTC
        iso_format_str = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00:00.000Z")
        logging.info(f"current datetime iso format: {iso_format_str}")
        logging.info(f"weather_ts: {weather_ts} | pollution_ts: {pollution_ts}")
        logging.info(f"prev_dt_str: {prev_dt_str} | w_prev_dt_str: {w_prev_dt_str}")

        date_time_obj = None
        if weather_ts == pollution_ts and pollution_ts == iso_format_str:
            date_time_obj = parse_utc_to_bangkok(pollution_ts)
            state_flow = StateFlow.NORMAL
            logging.info("flow state1: both timestamps are the same")
        elif not is_first_run:
            weather_updated = weather_ts != w_prev_dt_str
            pollution_updated = pollution_ts != prev_dt_str

            if weather_updated and not pollution_updated:
                date_time_obj = parse_utc_to_bangkok(weather_ts)
                state_flow = StateFlow.ONLY_WEATHER
                logging.info("flow state2: only weather updated")
            elif not weather_updated and pollution_updated:
                date_time_obj = parse_utc_to_bangkok(pollution_ts)
                state_flow = StateFlow.ONLY_POLLUTION
                logging.info("flow state3: only pollution updated")
            else:
                state_flow = StateFlow.ERROR
                logging.warning("flow error: unrecognized pattern (non-first run)")
        else: # First RUN
            if weather_ts == iso_format_str:
                date_time_obj = parse_utc_to_bangkok(weather_ts)
                state_flow = StateFlow.ONLY_WEATHER
                logging.info("flow state2: first run, only weather matches current time")
            elif pollution_ts == iso_format_str:
                date_time_obj = parse_utc_to_bangkok(pollution_ts)
                state_flow = StateFlow.ONLY_POLLUTION
                logging.info("flow state3: first run, only pollution matches current time")
            else:
                state_flow = StateFlow.ERROR
                logging.warning("flow error: unrecognized pattern (first run)")

        if date_time_obj:
            date_time_obj_bangkok = date_time_obj
            date_str = date_time_obj_bangkok.strftime("%Y-%m-%d")
            time_str = date_time_obj_bangkok.strftime("%H:%M")

        # Insert dimDateTimeTable
        cursor.execute("""
            INSERT INTO dimDateTimeTable (date_time, date, time, day, month, year, hour)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_time) DO NOTHING;
        """, (
            date_time_obj_bangkok, date_str, time_str,
            date_time_obj_bangkok.day, date_time_obj_bangkok.month,
            date_time_obj_bangkok.year, date_time_obj_bangkok.hour
        ))

        # Insert dimLocationTable
        latitude = round(float(contents_data["location"]["coordinates"][1]), 6)
        longitude = round(float(contents_data["location"]["coordinates"][0]), 6)
        country = contents_data["country"]
        state = contents_data["state"]
        city = contents_data["city"]
        description = f"{city} {state} {country}"

        cursor.execute("""
            SELECT location_id FROM dimLocationTable WHERE description = %s;
        """, (description,))
        result = cursor.fetchone()
        location_id = result[0] if result else None

        if not location_id:
            cursor.execute("""
                INSERT INTO dimLocationTable (latitude, longitude, description, country, state, city)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING location_id;
            """, (latitude, longitude, description, country, state, city))
            location_id = cursor.fetchone()[0]

        # Insert dimMainPollutionTable
        main_code = contents_data["current"]["pollution"]["mainus"]
        mapping = pollution_mapping.get(main_code, {"unit": "unknown", "name_pollution": main_code})
        if main_code not in pollution_mapping:
            logging.warning(f"Pollution code '{main_code}' not found in mapping.")
        cursor.execute("""
            INSERT INTO dimMainPollutionTable (main_pollution_code, unit, name_pollution)
            VALUES (%s, %s, %s)
            ON CONFLICT (main_pollution_code) DO NOTHING;
        """, (main_code, mapping['unit'], mapping['name_pollution']))

        # Insert factAirVisualTable
        weather_data:dict = contents_data["current"]["weather"]
        weather_code = weather_data.get("ic")
        wmapping = weather_mapping.get(weather_code, {})
        weather_name = wmapping.get("name")

        if weather_code not in weather_mapping:
            logging.warning(f"Weather code '{weather_code}' not found in mapping.")

        cursor.execute("""
            INSERT INTO factairvisualtable (
                date_time, location_id, main_pollution_code,
                aqi_us, temperature_c, pressure,
                humidity, wind_speed, wind_direction, weather_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_time, location_id, main_pollution_code) DO NOTHING;
        """, (
            date_time_obj_bangkok,
            location_id,
            main_code if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_POLLUTION) else "PM2.5",
            contents_data["current"]["pollution"]["aqius"] if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_POLLUTION) else None,
            weather_data.get("tp") if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_WEATHER) else None,
            weather_data.get("pr") if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_WEATHER) else None,
            weather_data.get("hu") if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_WEATHER) else None,
            weather_data.get("ws") if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_WEATHER) else None,
            weather_data.get("wd") if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_WEATHER) else None,
            weather_name if state_flow in (StateFlow.NORMAL, StateFlow.ONLY_WEATHER) else None
        ))

        conn.commit()
        logging.info("Finished load_airvisual_to_postgres")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during ETL: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
        logging.info("ETL to PostgreSQL Completed")

# ============ DAG ============ #
default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='airvisual_pipeline_v1_23',
    schedule_interval='30 * * * *',
    default_args=default_args,
    description='A simple data pipeline for airvisual API',
    catchup=False,
    on_failure_callback=trigger_backup_dag,
) as dag:

    t1 = PythonOperator(
        task_id='get_data_from_airvisual',
        python_callable=get_data_from_airvisual,
        retries= 2,
        retry_delay=timedelta(minutes=5)
    )

    t2 = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_json_data
    )

    t3 = PythonOperator(
        task_id='load_data_airvisual_to_postgresql',
        python_callable=load_airvisual_to_postgres
    )

    t1 >> t2 >> t3
