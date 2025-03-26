import json
import requests
import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ================= CONFIG ===================
AIR4THAI_URL = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php?stationID=81t"
OUTPUT_DIR = Path("/opt/airflow/data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "tmp_air4thai.json"

# ================= FUNCTION =================
def get_data_from_air4thai():
    logging.info("Sending request to AIR4THAI API...")
    try:
        response = requests.get(AIR4THAI_URL, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

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

# ================= DAG ======================
default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 26),
}

with DAG(
    dag_id='air4thai_pipeline_v1_3',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='A simple data pipeline for testing',
) as dag:

    t1 = PythonOperator(
        task_id='get_air4thai_data_hourly',
        python_callable=get_data_from_air4thai
    )

    t2 = PythonOperator(
        task_id='read_data_air4thai',
        python_callable=read_json_data
    )

    t1 >> t2
