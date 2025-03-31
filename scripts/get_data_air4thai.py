import http.client
import json

def get_data_from_air4thai():
    host = "air4thai.pcd.go.th"
    endpoint = "/services/getNewAQI_JSON.php?stationID=81t"
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
    'stationID': '81t', 
    'nameTH': 'อ่างเก็บน้ำประปา', 
    'nameEN': 'Water reservoir', 
    'areaTH': 'ต.นครปฐม อ.เมือง, นครปฐม', 
    'areaEN': 'Mueang, Nakhon Pathom', 
    'stationType': 'GROUND', 
    'lat': '13.832076', 
    'long': '100.057961', 
    'forecast': [], 
    'AQILast': {
        'date': '2025-03-17', 
        'time': '20:00', 
        'PM25': {'color_id': '1', 'aqi': '21', 'value': '12.8'}, 
        'PM10': {'color_id': '0', 'aqi': '-1', 'value': '-1'}, 
        'O3': {'color_id': '0', 'aqi': '-1', 'value': '-1'}, 
        'CO': {'color_id': '0', 'aqi': '-1', 'value': '-1'}, 
        'NO2': {'color_id': '0', 'aqi': '-1', 'value': '-1'}, 
        'SO2': {'color_id': '0', 'aqi': '-1', 'value': '-1'}, 
        'AQI': {'color_id': '1', 'aqi': '21', 'param': 'PM25'}
    }
}


# import json, requests, http.client
# from datetime import datetime, date

# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator

# def get_data_from_air4thai():
#     host = "air4thai.pcd.go.th"
#     endpoint = "/services/getNewAQI_JSON.php?stationID=81t"
#     print("Sending Request Please Wait...")
#     try:
#         # สร้าง HTTP connection
#         conn = http.client.HTTPConnection(host)
#         conn.request("GET", endpoint)
#         res = conn.getresponse()
#         if res.status == 200:
#             data = res.read().decode("utf-8")  # อ่านและแปลงเป็น string
#             try:
#                 json_data = json.loads(data)  # แปลงเป็น dict
#                 print("Response received successfully:")
#                 with open('data/tmp_air4thai.json', 'w') as f:
#                     json.dump(json_data, f)
#                 print("Content ",json_data)

#             except json.JSONDecodeError:
#                 print("Error: Response is not in JSON format.")
#         else:
#             print(f"Error: Unable to fetch data, status code {res.status}")
#             print(res.read().decode("utf-8"))

#     except Exception as e:
#         print(f"Error: {e}")

#     finally:
#         conn.close()

# def read_json_data():
#     with open('data/tmp_air4thai.json', 'r', encoding='utf-8') as f:
#         data = json.load(f)
#     print("Content ",data)


# #===================== DAG =============================================
# default_args = {
#     'owner': 'Polakorn Anantapakorn Ming',
#     'start_date': datetime(2025, 3, 26),
# }
# with DAG('air4thai_pipeline_v1-3',
#          schedule_interval='@daily',
#          default_args=default_args,
#          description='A simple data pipeline for testing',
#          catchup=False) as dag:

#     t1 = PythonOperator(
#         task_id='get_air4thai_data_hourly',
#         python_callable=get_data_from_air4thai
#     )

#     t2 = PythonOperator(
#         task_id='read_data_air4thai',
#         python_callable=read_json_data
#     )


#     t1 >> t2



if __name__ == "__main__":
    pass
    # print(data)
    # get_data_from_air4thai()






