import json
import requests
import logging
from pathlib import Path
from datetime import datetime, timedelta
import configparser

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException

# ============ CONFIG ============ #
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.conf"
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "tmp_airvisual.json"

CITY = "Salaya"
STATE = "Nakhon Pathom"
COUNTRY = "Thailand"
LATITUDE = 13.79059242
LONGITUDE = 100.32622308

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

def get_data_from_airvisual():
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    api_key = config.get("api", "airvisual_key")

    # URL = (
    #     "http://api.airvisual.com/v2/city"
    #     f"?city={CITY}&state={STATE}&country={COUNTRY}&key={api_key}"
    # )
    URL = (
        "http://api.airvisual.com/v2/nearest_city"
        f"?lat={LATITUDE}&lon={LONGITUDE}&key={api_key}"
    )

    logging.info(f"Requesting data from: {URL}")
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # ตรวจสอบ datetime ของข้อมูลล่าสุด
        ts_str = data["data"]["current"]["pollution"]["ts"]
        ts_obj = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        ts_bangkok = ts_obj + timedelta(hours=7)
        current_dt_str = ts_bangkok.strftime("%Y-%m-%d %H:%M")

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
            prev_dt_obj = datetime.strptime(prev_dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            prev_dt_obj = prev_dt_obj + timedelta(hours=7)
            prev_dt_str = prev_dt_obj.strftime("%Y-%m-%d %H:%M")
            if current_dt_str == prev_dt_str and is_data_in_db(prev_dt_str):
                raise Exception(f"Data has not been updated yet: {current_dt_str} == {prev_dt_str}")

        # ใช้หลักการ "Atomic File Write":
        # เขียนลงไฟล์ชั่วคราว (.tmp) แล้วค่อย rename เป็นไฟล์จริง
        # เพื่อป้องกันปัญaหาการอ่านไฟล์ไม่สมบูรณ์ใน Task ถัดไป
        tmp_path = Path(str(OUTPUT_FILE) + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        tmp_path.rename(OUTPUT_FILE)
        logging.info("Data saved successfully.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

def read_json_data():
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info("Extracted data: %s", data)
        logging.info("Read JSON file successfully.")
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        raise

def load_airvisual_to_postgres():
    
    conn ,cursor = get_cursor() 
    try:
        # Load pollution mapping from config
        with open('/opt/airflow/config/mapping_main_pollution.json', 'r', encoding='utf-8') as mf:
            pollution_mapping = json.load(mf)

        # Load Weather code data
        with open('/opt/airflow/config/mapping_weather_code.json', 'r', encoding='utf-8') as wf:
            weather_mapping = json.load(wf)

        # Load raw JSON data
        with open('/opt/airflow/data/tmp_airvisual.json', 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        date_time_str = raw_data["data"]["current"]["pollution"]["ts"]
        date_time_obj = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        date_time_obj_bangkok = date_time_obj + timedelta(hours=7)
        date_str = date_time_obj_bangkok.strftime("%Y-%m-%d")
        time_str = date_time_obj_bangkok.strftime("%H:%M")

        # --- Step 1: Insert into dimDateTimeTable (if not exists) ---
        cursor.execute("""
            INSERT INTO dimDateTimeTable (date_time, date, time, day, month, year, hour)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_time) DO NOTHING;
        """, (
            date_time_obj_bangkok, 
            date_str,
            time_str,
            date_time_obj_bangkok.day,
            date_time_obj_bangkok.month,
            date_time_obj_bangkok.year,
            date_time_obj_bangkok.hour
        ))

        # --- Step 2: Insert into dimLocationTable (if not exists) ---
        contents_data: dict = raw_data["data"]
        latitude = round(float(contents_data["location"]["coordinates"][1]), 6)
        longitude = round(float(contents_data["location"]["coordinates"][0]), 6)
        country = contents_data["country"]
        state = contents_data["state"]
        city = contents_data["city"]
        description = f"{city} {state} {country}"

        cursor.execute("""
            SELECT description, location_id FROM dimLocationTable
            WHERE description = %s;
        """, (description,))
        result = cursor.fetchone()
        print(f"fecth cursor result find location id: {result}")
        if result:
            location_id = result[1]
        else:
            cursor.execute(""" 
                INSERT INTO dimLocationTable (latitude, longitude, description, country, state, city)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING location_id;
            """, (latitude, longitude, description, country, state, city))
            location_id = cursor.fetchone()[0]

         # --- Step 3: Insert into dimMainPollutionTable (if not exists) ---
        main_code = raw_data["data"]["current"]["pollution"]["mainus"]
        if main_code not in pollution_mapping:
            logging.warning(f"Pollution code '{main_code}' not found in mapping file.")
        mapping = pollution_mapping.get(main_code, {"unit": "unknown", "name_pollution": main_code})
        cursor.execute("""
            INSERT INTO dimMainPollutionTable (main_pollution_code, unit, name_pollution)
            VALUES (%s, %s, %s)
            ON CONFLICT (main_pollution_code) DO NOTHING;
        """, (main_code, mapping['unit'], mapping['name_pollution']))

        # --- Step 4: Insert into factAirVisualTable ---
        weather_data: dict = contents_data.get("current").get("weather")
        weather_code = weather_data.get("ic")
        if weather_code not in weather_mapping:
            logging.warning(f"weather code '{weather_code}' not found in mapping file.")
        wmapping: dict = weather_mapping.get(weather_code)
        weather_name = wmapping.get("name") if wmapping else None
        
        cursor.execute("""
            INSERT INTO factairvisualtable (
                date_time, location_id, main_pollution_code,
                aqi_us, temperature_c, pressure,
                humidity, wind_speed, wind_direction, weather_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (date_time, location_id, main_pollution_code) DO NOTHING;
        """,(
            date_time_obj_bangkok,
            location_id,
            main_code,
            contents_data["current"]["pollution"]["aqius"],
            weather_data.get("tp"),
            weather_data.get("pr"),
            weather_data.get("hu"),
            weather_data.get("ws"),
            weather_data.get("wd"),
            weather_name
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
    dag_id='airvisual_pipeline_lat_long_v1',
    schedule_interval=None,
    default_args=default_args,
    description='A simple data pipeline for airvisual API by lat long',
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='get_airvisual_data_hourly',
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
