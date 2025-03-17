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



if __name__ == "__main__":
    pass
    # print(data)
    # get_data_from_air4thai()