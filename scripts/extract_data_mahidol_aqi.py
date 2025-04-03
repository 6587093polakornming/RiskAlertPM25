from bs4 import BeautifulSoup
from datetime import datetime

# ฟังก์ชันดึงค่าเฉพาะตัวเลขหลัก (เลขจาก `span` โดยไม่เอา `small`, `sup`, หรือ tag อื่นๆ)
def get_clean_text(element_id, soup:BeautifulSoup):
    element = soup.find(id=element_id)
    if element:
        return element.get_text(strip=True, separator=" ").split()[0]  # ดึงเฉพาะตัวเลขหลัก
    return None

def convert_to_datetime(datetime_str):
    # แปลง string datetime เป็น datetime object
    datetime_obj = None
    if datetime_str:
        try:
            datetime_obj = datetime.strptime(datetime_str, "%d %B %Y, %H:%M hrs.")
            return datetime_obj
        except ValueError as e:
            print(f"Error parsing datetime: {e}")
    return None


def get_main_pollution_text(soup: BeautifulSoup):
    """
    ดึงข้อมูลมลพิษที่มีผลกระทบมากที่สุดจาก HTML
    โดยค้นหา <h4> ภายใน <div class="fa-10x">
    
    :param soup: BeautifulSoup object ของ HTML
    :return: ชื่อมลพิษหลัก เช่น "PM2.5" หรือ None หากไม่พบหรือเกิดข้อผิดพลาด
    """
    try:
        # ค้นหา <div class="fa-10x">
        aqi_container = soup.find("div", class_="fa-10x")
        if aqi_container:
            # ค้นหา <h4> ที่อยู่ใน <div class="fa-10x">
            h4_element = aqi_container.find("h4")

            if h4_element:
                return h4_element.get_text(strip=True).replace("(", "").replace(")", "").strip()

        return None  # คืนค่า None หากไม่พบข้อมูล

    except Exception as e:
        print(f"Error in get_main_pollution_text: {e}")
        return None  # คืนค่า None หากเกิดข้อผิดพลาด

"""
import json, requests, http.client
from datetime import datetime, date
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def get_data_mahido_aqi_report():
# URL ของเว็บที่ต้องการดึงข้อมูล
    URL = "https://mahidol.ac.th/aqireport/"
    res = requests.get(URL)
    if res.status_code == 200:
        # Parsing HTML
        contents = BeautifulSoup(res.content, 'html.parser')

        # Note: อาจจะไม่ได้บันทึก HTML ไม่ต้องใช้
        formatted_html = contents.prettify()
        with open('data/mahidol_aqi.html', 'w', encoding='utf-8') as f:
            f.write(formatted_html)
        print("Save mahidol_aqi.html สำเร็จ")
    else:
        print(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {res.status_code}")

# ฟังก์ชันดึงค่าเฉพาะตัวเลขหลัก (เลขจาก `span` โดยไม่เอา `small`, `sup`, หรือ tag อื่นๆ)
def get_clean_text(element_id, soup:BeautifulSoup):
    element = soup.find(id=element_id)
    if element:
        return element.get_text(strip=True, separator=" ").split()[0].strip()  # ดึงเฉพาะตัวเลขหลัก
    return None

def convert_to_datetime(datetime_str):
    # แปลง string datetime เป็น datetime object
    datetime_obj = None
    if datetime_str:
        try:
            datetime_obj = datetime.strptime(datetime_str, "%d %B %Y, %H:%M hrs.")
            return datetime_obj
        except ValueError as e:
            print(f"Error parsing datetime: {e}")
    return None


def get_main_pollution_text(soup: BeautifulSoup):
    # ดึงข้อมูลมลพิษที่มีผลกระทบมากที่สุดจาก HTML
    # โดยค้นหา <h4> ภายใน <div class="fa-10x">
    
    # :param soup: BeautifulSoup object ของ HTML
    # :return: ชื่อมลพิษหลัก เช่น "PM2.5" หรือ None หากไม่พบหรือเกิดข้อผิดพลาด

    try:
        # ค้นหา <div class="fa-10x">
        aqi_container = soup.find("div", class_="fa-10x")
        if aqi_container:
            # ค้นหา <h4> ที่อยู่ใน <div class="fa-10x">
            h4_element = aqi_container.find("h4")

            if h4_element:
                return h4_element.get_text(strip=True).replace("(", "").replace(")", "").strip()

        return None  # คืนค่า None หากไม่พบข้อมูล

    except Exception as e:
        print(f"Error in get_main_pollution_text: {e}")
        return None  # คืนค่า None หากเกิดข้อผิดพลาด

def create_json_object():
    with open("data/mahidol_aqi.html", "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")

    # ดึงข้อมูล datetime
    date_en = soup.find(id="ContentPlaceHolder1_lblDateTimeEN")
    datetime_value = f" {date_en.get_text(strip=True)}".strip() if  date_en else None
    datetime_value_obj = convert_to_datetime(datetime_value)

    # ดึงข้อความ Air Quality (อยู่ใน <div class="alert"> -> <h4>)
    air_quality_div = soup.find("div", class_="alert")
    air_quality_text = air_quality_div.find("h4").get_text(strip=True) if air_quality_div else None

    # ดึงดึงข้อมูลมลพิษที่มีผลกระทบมาก
    main_pollution = get_main_pollution_text(soup)

    # ดึงค่าข้อมูลสภาพอากาศ
    aqi = get_clean_text("ContentPlaceHolder1_lblAQI", soup)
    pm25 = get_clean_text("ContentPlaceHolder1_lblHourlyPM25", soup)
    pm10 = get_clean_text("ContentPlaceHolder1_lblHourlyPM10", soup)
    o3 = get_clean_text("ContentPlaceHolder1_lblHourlyO3", soup)
    co = get_clean_text("ContentPlaceHolder1_lblHourlyCO", soup)
    no2 = get_clean_text("ContentPlaceHolder1_lblHourlyNO2", soup)
    so2 = get_clean_text("ContentPlaceHolder1_lblHourlySO2", soup)
    temperature = get_clean_text("ContentPlaceHolder1_lblTemperature", soup)
    humidity = get_clean_text("ContentPlaceHolder1_lblHumidity", soup)
    wind_speed = get_clean_text("ContentPlaceHolder1_lblWindSpeed", soup)
    wind_direction = get_clean_text("ContentPlaceHolder1_lblWindDirection", soup)
    rainfall = get_clean_text("ContentPlaceHolder1_lblRainfall", soup)
    solar_radiation = get_clean_text("ContentPlaceHolder1_lblSolar", soup)

    # แสดงผล
    data = {
        "Datetime":  datetime_value_obj.isoformat() if datetime_value_obj else None,
        "Air Quality": air_quality_text,
        "Main Pollution": main_pollution,
        "AQI": aqi,
        "PM25": pm25, # "PM2.5 (µg/m³)"
        "PM10": pm10,  # "PM10 (µg/m³)"
        "O3": o3,        # "O3 (ppb)"
        "CO": co,        # "CO (ppm)"
        "NO2": no2,      # "NO2 (ppb)"
        "SO2": so2,      # "SO2 (ppb)"
        "Temperature": temperature,            # Temperature (°C)
        "Humidity": humidity,                   # Humidity (%)
        "Wind Speed": wind_speed,             # Wind Speed (m/s)
        "Wind Direction": wind_direction,       # Wind Direction (°)
        "Rainfall": rainfall,                  # Rainfall (mm)
        "Solar Radiation": solar_radiation,  # Solar Radiation (W/m²)
    }

    print("Content ",data)
    with open('data/tmp_mahidol.json', 'w') as f:
        json.dump(data, f)

#===================== DAG =============================================
default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 26),
}
with DAG('mahidol_aqi_pipeline_v1-2',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for testing',
         catchup=False) as dag:
    
    t1 = PythonOperator(
        task_id='scraping_mahidol_aqi_report',
        python_callable=get_data_mahido_aqi_report
    )

    t2 = PythonOperator(
        task_id='create_json_mahdidol_aqi',
        python_callable=create_json_object
    )

    t1 >> t2

"""


if __name__ == "__main__":
    # โหลดไฟล์ HTML
    with open("data/mahidol_aqi_main_o3_detech.html", "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")

    # ดึงข้อมูล datetime
    date_en = soup.find(id="ContentPlaceHolder1_lblDateTimeEN")
    datetime_value = f" {date_en.get_text(strip=True)}".strip() if  date_en else None
    datetime_value_obj = convert_to_datetime(datetime_value)

    # ดึงข้อความ Air Quality (อยู่ใน <div class="alert"> -> <h4>)
    air_quality_div = soup.find("div", class_="alert")
    air_quality_text = air_quality_div.find("h4").get_text(strip=True) if air_quality_div else None

    # ดึงดึงข้อมูลมลพิษที่มีผลกระทบมาก
    main_pollution = get_main_pollution_text(soup)

    # ดึงค่าข้อมูลสภาพอากาศ
    aqi = get_clean_text("ContentPlaceHolder1_lblAQI", soup)
    pm25 = get_clean_text("ContentPlaceHolder1_lblHourlyPM25", soup)
    pm10 = get_clean_text("ContentPlaceHolder1_lblHourlyPM10", soup)
    o3 = get_clean_text("ContentPlaceHolder1_lblHourlyO3", soup)
    co = get_clean_text("ContentPlaceHolder1_lblHourlyCO", soup)
    no2 = get_clean_text("ContentPlaceHolder1_lblHourlyNO2", soup)
    so2 = get_clean_text("ContentPlaceHolder1_lblHourlySO2", soup)
    temperature = get_clean_text("ContentPlaceHolder1_lblTemperature", soup)
    humidity = get_clean_text("ContentPlaceHolder1_lblHumidity", soup)
    wind_speed = get_clean_text("ContentPlaceHolder1_lblWindSpeed", soup)
    wind_direction = get_clean_text("ContentPlaceHolder1_lblWindDirection", soup)
    rainfall = get_clean_text("ContentPlaceHolder1_lblRainfall", soup)
    solar_radiation = get_clean_text("ContentPlaceHolder1_lblSolar", soup)

    # แสดงผล
    data = {
        "Datetime": datetime_value_obj,
        "Air Quality": air_quality_text,
        "Main Pollution": main_pollution,
        "AQI": aqi,
        "PM25": pm25, # "PM2.5 (µg/m³)"
        "PM10": pm10,  # "PM10 (µg/m³)"
        "O3": o3,        # "O3 (ppb)"
        "CO": co,        # "CO (ppm)"
        "NO2": no2,      # "NO2 (ppb)"
        "SO2": so2,      # "SO2 (ppb)"
        "Temperature": temperature,            # Temperature (°C)
        "Humidity": humidity,                   # Humidity (%)
        "Wind Speed": wind_speed,             # Wind Speed (m/s)
        "Wind Direction": wind_direction,       # Wind Direction (°)
        "Rainfall": rainfall,                  # Rainfall (mm)
        "Solar Radiation": solar_radiation,  # Solar Radiation (W/m²)
    }

    for key, value in data.items():
        print(f"{key}: {value}")
