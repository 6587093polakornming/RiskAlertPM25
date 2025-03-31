import requests
from bs4 import BeautifulSoup

# URL ของเว็บที่ต้องการดึงข้อมูล
URL = "https://mahidol.ac.th/aqireport/"
res = requests.get(URL)

if res.status_code == 200:
    # Parsing HTML
    contents = BeautifulSoup(res.content, 'html.parser')

    # Note: อาจจะไม่ได้บันทึก HTML ไม่ต้องใช้
    formatted_html = contents.prettify()
    with open('mahidol_aqi.html', 'w', encoding='utf-8') as f:
        f.write(formatted_html)
    
    print("Save mahidol_aqi.html สำเร็จ")
else:
    print(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {res.status_code}")
