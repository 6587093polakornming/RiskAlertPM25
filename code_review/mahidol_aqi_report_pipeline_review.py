import json
import requests
import logging
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException

# ============ CONFIG ============ #
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_FILE = DATA_DIR / "mahidol_aqi.html"
OUTPUT_FILE = DATA_DIR / "tmp_mahidol.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LATITUDE = 13.794606
LONGITUDE = 100.327256
DESCRIPTION = "คณะสิ่งแวดล้อมและทรัพยากรศาสตร์ มหาวิทยาลัยมหิดล"
COUNTRY = "Thailand"
STATE = "Nakhon Pathom"
CITY = "Salaya"

# ============ FUNCTIONS ============ #
def get_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()

def is_data_in_db(date_time_str):
    conn, cursor = get_cursor()
    try:
        cursor.execute("""
            SELECT 1 FROM factmahidolaqitable
            WHERE date_time = %s
            LIMIT 1
        """, (date_time_str,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()

# --- ====== TASK 1 ====== ---
def get_data_mahidol_aqi_report():
    URL = "https://mahidol.ac.th/aqireport/"
    try:
        res = requests.get(URL, timeout=10)
        res.raise_for_status()
        tmp_path = Path(str(HTML_FILE) + ".tmp")
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(res.text)
        tmp_path.rename(HTML_FILE)
        logging.info("Saved Mahidol AQI HTML successfully.")

    except Exception as e:
        logging.error(f"Failed to fetch Mahidol AQI report: {e}")
        raise

def get_clean_text(element_id, soup: BeautifulSoup):
    element = soup.find(id=element_id)
    if element:
        return element.get_text(strip=True, separator=" ").split()[0].strip()
    return None

def convert_to_datetime(datetime_str):
    if datetime_str:
        try:
            return datetime.strptime(datetime_str, "%d %B %Y, %H:%M hrs.")
        except ValueError as e:
            logging.error(f"Error parsing datetime: {e}")
    return None

def get_main_pollution_text(soup: BeautifulSoup):
    try:
        aqi_container = soup.find("div", class_="fa-10x")
        if aqi_container:
            h4_element = aqi_container.find("h4")
            if h4_element:
                return h4_element.get_text(strip=True).replace("(", "").replace(")", "").strip()
    except Exception as e:
        logging.error(f"Error in get_main_pollution_text: {e}")
    return None

# --- ====== TASK 2 ====== ---
def create_json_object():
    try:
        with open(HTML_FILE, "r", encoding="utf-8") as file:
            soup = BeautifulSoup(file, "html.parser")
    except Exception as e:
        logging.error(f"Error reading HTML file: {e}")
        raise

    try:
        date_en = soup.find(id="ContentPlaceHolder1_lblDateTimeEN")
        datetime_value = f" {date_en.get_text(strip=True)}".strip() if date_en else None
        datetime_value_obj = convert_to_datetime(datetime_value)

        air_quality_div = soup.find("div", class_="alert")
        air_quality_text = air_quality_div.find("h4").get_text(strip=True) if air_quality_div else None
        main_pollution = get_main_pollution_text(soup)

        data = {
            "Datetime": datetime_value_obj.isoformat() if datetime_value_obj else None,
            "Air Quality": air_quality_text,
            "Main Pollution": main_pollution,
            "AQI": get_clean_text("ContentPlaceHolder1_lblAQI", soup),
            "PM25": get_clean_text("ContentPlaceHolder1_lblHourlyPM25", soup),
            "PM10": get_clean_text("ContentPlaceHolder1_lblHourlyPM10", soup),
            "O3": get_clean_text("ContentPlaceHolder1_lblHourlyO3", soup),
            "CO": get_clean_text("ContentPlaceHolder1_lblHourlyCO", soup),
            "NO2": get_clean_text("ContentPlaceHolder1_lblHourlyNO2", soup),
            "SO2": get_clean_text("ContentPlaceHolder1_lblHourlySO2", soup),
            "Temperature": get_clean_text("ContentPlaceHolder1_lblTemperature", soup),
            "Humidity": get_clean_text("ContentPlaceHolder1_lblHumidity", soup),
            "Wind Speed": get_clean_text("ContentPlaceHolder1_lblWindSpeed", soup),
            "Wind Direction": get_clean_text("ContentPlaceHolder1_lblWindDirection", soup),
            "Rainfall": get_clean_text("ContentPlaceHolder1_lblRainfall", soup),
            "Solar Radiation": get_clean_text("ContentPlaceHolder1_lblSolar", soup),
        }
        logging.info("Extracted data: %s", data)

        # ตรวจสอบ datetime ของข้อมูลล่าสุด
        iso_string = data["Datetime"] #FORMAT eg. "2025-04-02T21:00:00"
        if iso_string:
            dt_obj = datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%S")
            current_dt_str = dt_obj.strftime("%Y-%m-%d %H:%M")
        else:
            raise Exception(f"Data DateTime has Error value of Error: {current_dt_str}")
        
        # เชื่อมต่อ Database แล้วตรวจสอบว่า datetime นี้มีอยู่แล้วหรือไม่
        data_time_now_obj = datetime.now()+ timedelta(hours=7)
        data_time_now_str = data_time_now_obj.strftime("%Y-%m-%d %H:00") 
        if is_data_in_db(data_time_now_str):
            raise AirflowSkipException(f"Data for {data_time_now_str} already exists in DB. Skipping DAG.")

        # โหลด datetime ปัจจุบันจากไฟล์ JSON ที่มีอยู่แล้ว ตรวจสอบว่าข้อมูลต้นทางอัปเดตหรือยัง
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f_check:
                prev_data = json.load(f_check)
            prev_dt_str = prev_data["Datetime"]
            prev_dt_obj = datetime.strptime(prev_dt_str, "%Y-%m-%dT%H:%M:%S")
            prev_dt_str = prev_dt_obj.strftime("%Y-%m-%d %H:%M")
            if current_dt_str == prev_dt_str and is_data_in_db(prev_dt_str):
                raise Exception(f"Data has not been updated yet: {current_dt_str} == {prev_dt_str}")

        # ใช้หลักการ "Atomic File Write":
        # เขียนลงไฟล์ชั่วคราว (.tmp) แล้วค่อย rename เป็นไฟล์จริง
        # เพื่อป้องกันปัญหาการอ่านไฟล์ไม่สมบูรณ์ใน Task ถัดไป
        tmp_path = Path(str(OUTPUT_FILE) + ".tmp")
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        tmp_path.rename(OUTPUT_FILE)
        logging.info("Response saved successfully.")

    except Exception as e:
        logging.error(f"Error create_json_object: {e}")
        raise

# --- ====== TASK 3 ====== ---
def load_mahidol_aqi_to_postgres():
    logging.info("Starting load_mahidolAQI_to_postgres")
    conn, cursor = get_cursor()
    try:
        # Load pollution mapping from config
        with open('/opt/airflow/config/mapping_main_pollution.json', 'r', encoding='utf-8') as mf:
            pollution_mapping = json.load(mf)

        # Load raw JSON data
        with open('/opt/airflow/data/tmp_mahidol.json', 'r', encoding='utf-8') as f:
            raw_data: dict = json.load(f)

        date_time_str = raw_data["Datetime"]
        date_time_obj = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S")
        date_str = date_time_obj.strftime("%Y-%m-%d")
        time_str = date_time_obj.strftime("%H:%M:%S")

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
        cursor.execute("""
            SELECT description, location_id FROM dimLocationTable
            WHERE description = %s;
        """, (DESCRIPTION,))
        result = cursor.fetchone()
        print(f"fecth cursor result find location id: {result}")
        if result:
            location_id = result[1]
        else:
            cursor.execute("""
                INSERT INTO dimLocationTable (latitude, longitude, description, country, state, city)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING location_id;
            """, (LATITUDE, LONGITUDE, DESCRIPTION, COUNTRY, STATE, CITY))
            location_id = cursor.fetchone()[0]

        # --- Step 3: Insert into dimMainPollutionTable (if not exists) ---
        main_code = raw_data["Main Pollution"]
        if main_code not in pollution_mapping:
            logging.warning(f"Pollution code '{main_code}' not found in mapping file.")
        mapping = pollution_mapping.get(main_code, {"unit": "unknown", "name_pollution": main_code})
        cursor.execute("""
            INSERT INTO dimMainPollutionTable (main_pollution_code, unit, name_pollution)
            VALUES (%s, %s, %s)
            ON CONFLICT (main_pollution_code) DO NOTHING;
        """, (main_code, mapping['unit'], mapping['name_pollution']))

        # --- Step 4: Insert into factMahidolAqiTable ---
        def clean_value(val):
            try:
                return round(float(val), 2)
            except:
                return None

        cursor.execute("""
            INSERT INTO factmahidolaqitable (
                date_time, location_id, main_pollution_code,
                air_quality_text, aqi,
                pm25_value, pm10_value, o3_value,
                co_value, no2_value, so2_value,
                temperature_c, humidity, wind_speed,
                wind_direction, rainfall, solar_radiation
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_time, location_id, main_pollution_code) DO NOTHING;
        """, (
            date_time_obj,
            location_id,
            main_code,
            raw_data.get("Air Quality"),
            clean_value(raw_data.get("AQI")),
            clean_value(raw_data.get("PM25")),
            clean_value(raw_data.get("PM10")),
            clean_value(raw_data.get("O3")),
            clean_value(raw_data.get("CO")),
            clean_value(raw_data.get("NO2")),
            clean_value(raw_data.get("SO2")),
            clean_value(raw_data.get("Temperature")),
            clean_value(raw_data.get("Humidity")),
            clean_value(raw_data.get("Wind Speed")),
            clean_value(raw_data.get("Wind Direction")),
            clean_value(raw_data.get("Rainfall")),
            clean_value(raw_data.get("Solar Radiation"))
        ))
        conn.commit()
        logging.info("Finished load_mahidolAQI_to_postgres")

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
    dag_id='mahidol_aqi_pipeline_v1_7',
    schedule_interval='30 * * * *',
    default_args=default_args,
    description='A simple data pipeline for Mahidol AQI report',
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='scraping_mahidol_aqi_report',
        python_callable=get_data_mahidol_aqi_report
    )

    t2 = PythonOperator(
        task_id='create_json_mahidol_aqi',
        python_callable=create_json_object
    )

    t3 = PythonOperator(
        task_id='load_data_mahidolAQI_to_postgresql',
        python_callable=load_mahidol_aqi_to_postgres
    )

    t1 >> t2 >> t3
