import json
import requests
import logging
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator

# ============ CONFIG ============ #
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_FILE = DATA_DIR / "mahidol_aqi.html"
JSON_FILE = DATA_DIR / "tmp_mahidol.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ============ FUNCTIONS ============ #
def get_data_mahidol_aqi_report():
    URL = "https://mahidol.ac.th/aqireport/"
    try:
        res = requests.get(URL, timeout=10)
        res.raise_for_status()
        with open(HTML_FILE, 'w', encoding='utf-8') as f:
            f.write(res.text)
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

def create_json_object():
    try:
        with open(HTML_FILE, "r", encoding="utf-8") as file:
            soup = BeautifulSoup(file, "html.parser")
    except Exception as e:
        logging.error(f"Error reading HTML file: {e}")
        raise

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
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ============ DAG ============ #
default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 26),
}

with DAG(
    dag_id='mahidol_aqi_pipeline_v1_2',
    schedule_interval='@daily',
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

    t1 >> t2
