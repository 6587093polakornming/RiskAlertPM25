import json
import requests
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException

# ================= CONFIG ===================
AIR4THAI_URL = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php?stationID=81t"
OUTPUT_DIR = Path("/opt/airflow/data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "tmp_air4thai.json"

# ================= FUNCTION =================
def get_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()

def is_data_in_db(date_time_str):
    conn, cursor = get_cursor()
    try:
        cursor.execute("""
            SELECT 1 FROM factAir4thaiTable
            WHERE date_time = %s
            LIMIT 1
        """, (date_time_str,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()

def get_data_from_air4thai():
    logging.info("Sending request to AIR4THAI API...")
    try:
        response = requests.get(AIR4THAI_URL, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        # ตรวจสอบ datetime ของข้อมูลล่าสุด
        date_str = json_data["AQILast"]["date"]
        time_str = json_data["AQILast"]["time"]
        current_dt_str = f"{date_str} {time_str}"

        # เชื่อมต่อ Database แล้วตรวจสอบว่า datetime นี้มีอยู่แล้วหรือไม่
        # data_time_now_obj = datetime.now()+ timedelta(hours=7) # UTC +7 but schedule every XX:30 
        # data_time_now_str = data_time_now_obj.strftime("%Y-%m-%d %H:00") 
        if is_data_in_db(current_dt_str):
            raise AirflowSkipException(f"Data for {current_dt_str} already exists in DB. Skipping DAG.")

        # โหลด datetime ปัจจุบันจากไฟล์ JSON ที่มีอยู่แล้ว
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f_check:
                prev_data = json.load(f_check)
            prev_date = prev_data["AQILast"]["date"]
            prev_time = prev_data["AQILast"]["time"]
            prev_dt_str = f"{prev_date} {prev_time}"

            #เช็คว่าไฟล์ JSON ล่าสุด ตรวจสอบว่า datetime นี้มีอยู่แล้วหรือไม่ใน DB
            if current_dt_str == prev_dt_str and is_data_in_db(prev_dt_str):
                raise Exception(f"Data has not been updated yet: {current_dt_str} == {prev_dt_str}")

        # ใช้หลักการ "Atomic File Write":
        # เขียนลงไฟล์ชั่วคราว (.tmp) แล้วค่อย rename เป็นไฟล์จริง
        # เพื่อป้องกันปัญหาการอ่านไฟล์ไม่สมบูรณ์ใน Task ถัดไป
        tmp_path = Path(str(OUTPUT_FILE) + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        tmp_path.rename(OUTPUT_FILE)
        logging.info("Response saved successfully.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from AIR4THAI: {e}")
        raise

def read_json_data():
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info("Read JSON data successfully.")
        # Optional: validate or transform data here
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        raise

def load_air4thai_to_postgres():
    logging.info("Starting load_air4thai_to_postgres")
    conn, cursor = get_cursor()

    try:
        # Load pollution mapping from config
        with open('/opt/airflow/config/mapping_main_pollution.json', 'r', encoding='utf-8') as mf:
            pollution_mapping = json.load(mf)

        # Load raw JSON data
        with open('/opt/airflow/data/tmp_air4thai.json', 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        aqi = raw_data['AQILast']
        date_str = aqi['date']
        time_str = aqi['time']
        date_time_str = f"{date_str} {time_str}"
        date_time_obj = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")

        # --- Step 1: Insert into dimDateTimeTable (if not exists) ---
        cursor.execute("""
            INSERT INTO dimDateTimeTable (date_time, date, time, day, month, year, hour)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_time) DO NOTHING;
        """, (
            date_time_obj,
            date_str,
            time_str,
            date_time_obj.day,
            date_time_obj.month,
            date_time_obj.year,
            date_time_obj.hour
        ))

        # --- Step 2: Insert into dimLocationTable (if not exists) ---
        latitude = float(raw_data['lat'])
        longitude = float(raw_data['long'])
        description = raw_data.get('nameEN')
        country = "Thailand"
        area_parts = raw_data.get('areaEN', '').split(",")
        city = area_parts[0].strip() if len(area_parts) > 0 else None
        state = area_parts[1].strip() if len(area_parts) > 1 else None

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
        main_code = aqi['AQI']['param']
        if main_code not in pollution_mapping:
            logging.warning(f"Pollution code '{main_code}' not found in mapping file.")
        mapping = pollution_mapping.get(main_code, {"unit": "unknown", "name_pollution": main_code})
        cursor.execute("""
            INSERT INTO dimMainPollutionTable (main_pollution_code, unit, name_pollution)
            VALUES (%s, %s, %s)
            ON CONFLICT (main_pollution_code) DO NOTHING;
        """, (main_code, mapping['unit'], mapping['name_pollution']))

        # --- Step 4: Insert into factAir4thaiTable ---
        def clean_float(val):
            try:
                fval = float(val)
                return fval if fval >= 0 else None
            except:
                return None

        cursor.execute("""
            INSERT INTO factAir4thaiTable (
                date_time, location_id, main_pollution_code,
                AQI, PM25_value, PM10_value, O3_value,
                CO_value, NO2_value, SO2_value
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_time, location_id, main_pollution_code) DO NOTHING;
        """, (
            date_time_obj,
            location_id,
            main_code,
            clean_float(aqi['AQI']['aqi']),
            clean_float(aqi['PM25']['value']),
            clean_float(aqi['PM10']['value']),
            clean_float(aqi['O3']['value']),
            clean_float(aqi['CO']['value']),
            clean_float(aqi['NO2']['value']),
            clean_float(aqi['SO2']['value']),
        ))

        conn.commit()
        logging.info("Finished load_air4thai_to_postgres")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during ETL: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
        logging.info("ETL to PostgreSQL Completed")

# ================= DAG ======================
default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 20), # 'start_date': get_start_date(),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='air4thai_pipeline_v1_19_5',
    schedule_interval='30 * * * *',
    default_args=default_args,
    catchup=False,
    description='A simple data pipeline for air4thai API',
) as dag:

    t1 = PythonOperator(
        task_id='get_air4thai_data_hourly',
        python_callable=get_data_from_air4thai
    )

    t2 = PythonOperator(
        task_id='read_data_air4thai',
        python_callable=read_json_data
    )

    t3 = PythonOperator(
        task_id='load_data_air4thai_to_postgresql',
        python_callable=load_air4thai_to_postgres
    )

    t1 >> t2 >> t3
