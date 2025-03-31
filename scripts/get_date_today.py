from datetime import datetime, timedelta
from pathlib import Path
import json

# Get current datetime
datetime_today = datetime.now()
datetime_today_obj = {
    "year": datetime_today.year,
    "month": datetime_today.month,
    "day": datetime_today.day,
    "hour": (datetime_today - timedelta(hours=7)).hour
}

# Resolve output directory relative to script location
BASE_DIR = Path(__file__).resolve().parent.parent  # from /script to /Project_Dir
OUTPUT_DIR = BASE_DIR / "config"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Ensure directory exists

print(f"Saving to: {OUTPUT_DIR}")

OUTPUT_FILE = OUTPUT_DIR / "tmp_date_today.json"
with open(OUTPUT_FILE, "w") as f:
    json.dump(datetime_today_obj, f, indent=2)

print(f"Date written to: {OUTPUT_FILE}")

"""

def get_start_date() -> datetime:
    # Load pollution mapping from config
    with open('/opt/airflow/config/tmp_date_today.json', 'r', encoding='utf-8') as mf:
        datetime_today = json.load(mf)
    return datetime(datetime_today.get("year"), 
                    datetime_today.get("month"), 
                    datetime_today.get("day"),
                    datetime_today.get("hour"))
"""