import json
import requests
import logging
from pathlib import Path
from datetime import datetime
import configparser

from airflow import DAG
from airflow.operators.python import PythonOperator

# ============ CONFIG ============ #
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.conf"
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_FILE = OUTPUT_DIR / "tmp_airvisual.json"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CITY = "Salaya"
STATE = "Nakhon Pathom"
COUNTRY = "Thailand"

# ============ FUNCTIONS ============ #
def get_data_from_airvisual():
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    api_key = config.get("api", "airvisual_key")

    url = (
        "http://api.airvisual.com/v2/city"
        f"?city={CITY}&state={STATE}&country={COUNTRY}&key={api_key}"
    )

    logging.info(f"Requesting data from: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logging.info("Data saved successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

def read_json_data():
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info("Read JSON file successfully.")
        # Optional: Validate or transform data
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        raise

# ============ DAG ============ #
default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 26),
}

with DAG(
    dag_id='airvisual_pipeline_v1_3',
    schedule_interval='@daily',
    default_args=default_args,
    description='A simple data pipeline for testing',
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='get_airvisual_data_hourly',
        python_callable=get_data_from_airvisual
    )

    t2 = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_json_data
    )

    t1 >> t2
