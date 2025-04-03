# Gmail .edu

"""
import json, requests, http.client, configparser, os
from datetime import datetime, date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def get_data_from_airvisual():
    config = configparser.ConfigParser()
    # config.read("../config/config.conf")
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "config", "config.conf")
    config.read(config_path)

    host = "api.airvisual.com"
    YOUR_API_KEY = config.get("api", "airvisual_key")
    input_city = "Salaya"
    input_state = "Nakhon%20Pathom" 
    input_country = "Thailand"
    endpoint = f"/v2/city?city={input_city}&state={input_state}&country={input_country}&key={YOUR_API_KEY}"
    print("Sending Request Please Wait...")
    try:
        # สร้าง HTTP connection
        conn = http.client.HTTPConnection(host)
        conn.request("GET", endpoint)
        res = conn.getresponse()
        if res.status == 200:
            data = res.read().decode("utf-8")  # อ่านและแปลงเป็น string
            try:
                json_data = json.loads(data)  # แปลงเป็น dict
                print("Response received successfully:")
                with open('data/tmp_airvisual.json', 'w') as f:
                    json.dump(json_data, f)
                print("Content ",json_data)

            except json.JSONDecodeError:
                print("Error: Response is not in JSON format.")
        else:
            print(f"Error: Unable to fetch data, status code {res.status}")
            print(res.read().decode("utf-8"))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        conn.close()

def read_json_data():
    with open('data/tmp_airvisual.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    print("Content ",data)

#===================== DAG =============================================
default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 26),
}
with DAG('airvisual_pipeline_v1-3',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for testing',
         catchup=False) as dag:
    
    t1 = PythonOperator(
        task_id='get_airvisual_data_hourly',
        python_callable=get_data_from_airvisual
    )

    t2 = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_json_data
    )


    t1 >> t2

"""

"""
import http.client

conn = http.client.HTTPSConnection("api.airvisual.com")
payload = ''
headers = {}
conn.request("GET", "/v2/city?city=Los%20Angeles&state=California&country=USA&key={{YOUR_API_KEY}}", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))

"""

import http.client, json, configparser
from datetime import datetime

def get_data_from_airvisual():
    config = configparser.ConfigParser()
    config.read("config.conf")

    host = "api.airvisual.com"
    YOUR_API_KEY = config.get("api", "airvisual_key")
    input_city = "Salaya"
    input_state = "Nakhon%20Pathom" 
    input_country = "Thailand"
    endpoint = f"/v2/city?city={input_city}&state={input_state}&country={input_country}&key={YOUR_API_KEY}"
    print("Sending Request Please Wait...")
    try:
        # สร้าง HTTP connection
        conn = http.client.HTTPConnection(host)
        conn.request("GET", endpoint)
        res = conn.getresponse()
        if res.status == 200:
            data = res.read().decode("utf-8")  # อ่านและแปลงเป็น string
            try:
                json_data = json.loads(data)  # แปลงเป็น dict
                print("Response received successfully:")
                print(json_data)

            except json.JSONDecodeError:
                print("Error: Response is not in JSON format.")
        else:
            print(f"Error: Unable to fetch data, status code {res.status}")
            print(res.read().decode("utf-8"))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        conn.close()


data = {
    'status': 'success', 
    'data': {'city': 'Salaya', 
             'state': 'Nakhon Pathom', 
             'country': 'Thailand', 
             'location': {
                 'type': 'Point', 
                 'coordinates': [100.32622308, 13.79059242]}, ''
                 'current': {
                     'pollution': {
                         'ts': '2025-03-17T15:00:00.000Z', 
                         'aqius': 69, 
                         'mainus': 'p2', 
                         'aqicn': 27, 
                         'maincn': 'p2'}, 
                         'weather': {
                             'ts': '2025-03-17T15:00:00.000Z', 
                             'tp': 31, 
                             'pr': 1012, 
                             'hu': 63, 
                             'ws': 4.55, 
                             'wd': 189, ''
                             'ic': '01n'}
                            }
                }
        }

if __name__ == "__main__":
    from datetime import datetime, timedelta, timezone
    # print(data)
    # get_data_from_airvisual()

    # ตรวจสอบ datetime ของข้อมูลล่าสุด
    # ts_str = data["data"]["current"]["pollution"]["ts"]
    # ts_str = "2025-03-30T12:00:00.000Z"
    # prev_dt_obj = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    # prev_dt_obj = prev_dt_obj + timedelta(hours=7)
    # prev_dt_str = prev_dt_obj.strftime("%Y-%m-%d %H:%M")
    # print(prev_dt_str)
    # print(data["data"]["current"]["pollution"]["ts"])
    # ts_obj = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")

    # current_dt_str = ts_obj.strftime("%Y-%m-%d %H:%M")
    # print(ts_obj)
    # prev_dt_obj = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    # print(prev_dt_obj.strftime("%Y-%m-%d %H:%M"))


    # =============
    # data = {
    # "status": "success",
    # "data": {
    #     "city": "Salaya",
    #     "state": "Nakhon Pathom",
    #     "country": "Thailand",
    #     "location": {
    #     "type": "Point",
    #     "coordinates": [
    #         100.32622308,
    #         13.79059242
    #     ]
    #     },
    #     "current": {
    #     "pollution": {
    #         "ts": "2025-04-01T07:00:00.000Z",
    #         "aqius": 143,
    #         "mainus": "p2",
    #         "aqicn": 72,
    #         "maincn": "p2"
    #     },
    #     "weather": {
    #         "ts": "2025-04-01T06:00:00.000Z",
    #         "tp": 35,
    #         "pr": 1008,
    #         "hu": 47,
    #         "ws": 3.33,
    #         "wd": 164,
    #         "ic": "04d"
    #     }
    #     }
    # }
    # }
    # def parse_utc_to_bangkok(ts_str):
    #     return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(hours=7)

    # pollution_ts = data["data"]["current"]["pollution"]["ts"]
    # weather_ts = data["data"]["current"]["weather"]["ts"]

    # w_prev_dt_str ="2025-04-01T06:00:00.000Z"
    # prev_dt_str = "2025-04-01T06:00:00.000Z"

    # # Flow Conditions
    # if weather_ts == pollution_ts:
    #     date_time_obj = parse_utc_to_bangkok(pollution_ts)
    #     state_flow = 1
    #     print("flow state1")
    # elif weather_ts != pollution_ts and weather_ts != w_prev_dt_str and pollution_ts == prev_dt_str:
    #     date_time_obj = parse_utc_to_bangkok(weather_ts)
    #     state_flow = 2
    #     print("flow state2")
    # elif weather_ts != pollution_ts and weather_ts == w_prev_dt_str and pollution_ts != prev_dt_str:
    #     date_time_obj = parse_utc_to_bangkok(pollution_ts)
    #     state_flow = 3
    #     print("flow state3")
    # else:
    #     state_flow = 4
    #     print("Unrecognized data flow state.")


    # original_dt = datetime.now()  # หรือ datetime ที่คุณได้มาแบบ datetime object โดยตรง
    # dt_floor_hour = original_dt.replace(minute=0, second=0, microsecond=0)  # ปัดลงให้เป็นต้นชั่วโมง
    # iso_format_str = dt_floor_hour.isoformat() + ".000Z" # แปลงเป็น ISO format + .000Z
    # print(iso_format_str)
    iso_format_str = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00:00.000Z")
    print(iso_format_str)
