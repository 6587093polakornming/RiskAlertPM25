# Gmail .edu

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
    pass
    # print(data)
    # get_data_from_airvisual()