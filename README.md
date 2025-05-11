# Risk Alert PM2.5 – Mahidol University  
เตือนภัยคุณภาพอากาศ **PM2.5** และสภาพอากาศแบบ Near‑Real‑Time

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apache-airflow&logoColor=white)](#airflow-dags)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue?logo=postgresql&logoColor=white)](#database)

> A Prototype data‑engineering stack that **ingests** hourly air‑quality & weather data from  
> 1) **AirVisual API**, 
> 2) **Air4Thai API**, and 
> 3) **Mahidol AQI** web‑scrape;  
> **loads** them into a dimensional PostgreSQL warehouse; and **powers** e‑mail alerts & BI dashboards for Mahidol University (Salaya campus).

---

## Table of Contents
1. [Features](#features)  
2. [Architecture](#architecture)  
3. [Data Sources](#data-sources)  
4. [Airflow DAGs](#airflow-dags)  
5. [Quick Start (Docker Compose)](#quick-start-docker-compose)  
6. [Configuration & Secrets](#configuration--secrets)  
7. [Database Schema](#database)  
8. [Project Structure](#project-structure)  
9. [Roadmap](#roadmap)  
10. [Contributing](#contributing)  

---

## Features
| Capability | Detail |
|------------|--------|
| **Multi‑source ingestion** | `airvisual_pipeline.py`, `air4thai_pipeline.py`, `mahidol_aqi_report_pipeline.py` |
| **Hourly schedule** | All DAGs run at `30 * * * *` (Bangkok time, UTC+7) |
| **Idempotent loads** | Skip if the current `date_time` already exists in the target fact table |
| **Atomic writes** | Save as `*.tmp` then atomic‑rename to prevent partial reads |
| **Dimensional warehouse** | `dimDateTimeTable`, `dimLocationTable`, `dimMainPollutionTable` + 3 fact tables |
| **Containerised stack** | One‑command launch with `docker-compose.yaml` |
| **Alert hook** | Pluggable e‑mail alert (`alert_email`) when AQI exceeds a threshold |
| **BI ready** | Any SQL/BI tool (e.g. Power BI, Superset) can connect to PostgreSQL |

---

## Architecture
```text
┌──────────────┐     ┌──────────────┐
│ AirVisual API│     │  Air4Thai API│
└──────┬───────┘     └──────┬───────┘
       │   JSON             │  JSON
       ▼                    ▼
┌─────────────────────────────────────┐
│          Airflow DAGs              │
│ (airvisual / air4thai / mahidol)   │
└──────┬─────────────────────────────┘
       │  load (Postgres hook)
       ▼
┌───────────────────────────┐
│  PostgreSQL Data Warehouse│
└──────┬────────────────────┘
       │  SQL / ODBC
       ▼
   Power BI · Alerts · APIs
```

---

## Data Sources
| Source | Endpoint / Page | Granularity |
|--------|-----------------|-------------|
| **AirVisual** | `/v2/city?city=Salaya&state=Nakhon%20Pathom&country=Thailand&key=…` | Hourly |
| **Air4Thai** | `/getNewAQI_JSON.php?stationID=81t` | Hourly |
| **Mahidol AQI** | `https://mahidol.ac.th/aqireport/` (HTML scrape) | Hourly |

---

## Airflow DAGs
| DAG ID | Pipeline File | Main Tasks | Purpose |
|--------|---------------|-----------|---------|
| `airvisual_pipeline_v1_23` | `dags/airvisual_pipeline.py` | get → parse → load | Primary AirVisual city feed |
| `airvisual_pipeline_lat_long_v1` | `dags/airvisual_pipeline.py` | fallback | Lat/Long‑based feed (backup) |
| `air4thai_pipeline_v1_20` | `dags/air4thai_pipeline.py` | get → parse → load | PCD Station 81t |
| `mahidol_aqi_pipeline_v1_7` | `dags/mahidol_aqi_report_pipeline.py` | scrape → parse → load | Official MU AQI page |

All DAGs set `catchup=False` and log to `./logs/`.

---

## Quick Start (Docker Compose)

```bash
# 1 – Clone
git clone https://github.com/6587093polakornming/RiskAlertPM25.git
cd RiskAlertPM25

# 2 – Secrets (AirVisual API key)
cp config/config.conf.example config/config.conf
#   → edit [api] airvisual_key=YOUR_KEY

# 3 – Launch stack
docker compose up -d           # Airflow 2 + Postgres 13
open http://localhost:8080     # default creds: airflow / airflow

# 4 – Enable DAGs in Airflow UI
```

> **Tip:** Use `docker compose stop` / `start` to pause & resume quickly.

---

## Configuration & Secrets
| File | Purpose |
|------|---------|
| `config/config.conf` | Stores API keys & thresholds |
| `config/mapping_main_pollution.json` | Pollutant code → unit / human name |
| `config/mapping_weather_code.json` | Weather code → label / icon |

**Never** commit real keys; `.gitignore` already excludes them.

---

## Local Configuration — `config/config.conf`

Create a plain‑text file named **`config.conf`** in the project’s `config/` folder with the following INI format:

```ini
[api]
airvisual_key = <YOUR_API_KEY>

[email]
password = <YOUR_DEVICE_PASSWORD>
```

| Section / Key | Purpose | Notes |
|---------------|---------|-------|
| `[api]` | Holds credentials for external APIs. | Currently only **AirVisual** is needed; additional sources can be added later. |
| `airvisual_key` | Your personal or organisational AirVisual API key. | **Keep this private** – never commit the real key to Git. |
| `[email]` | Credentials for the alert‑notification sender account. | Used by `alert_email()` in the Airflow DAGs. |
| `password` | An **app‑specific** or **device** password for the e‑mail account. | For Gmail, generate an App Password rather than using your normal login. |

> **Security tip:** `config/config.conf` is already listed in `.gitignore`, so real secrets won’t be pushed to GitHub.


## Database

### Dimensions
| Table | Primary Key | Columns (abridged) |
|-------|-------------|--------------------|
| `dimDateTimeTable` | `date_time` (timestamp) | date, hour, weekday, month, year |
| `dimLocationTable` | `location_id` (serial) | lat, long, state, city |
| `dimMainPollutionTable` | `main_pollution_code` | pollutant name, unit |

### Facts
| Table | Source DAG | Foreign Keys |
|-------|-----------|--------------|
| `factAirvisualTable` | AirVisual | `date_time`, `location_id`, `main_pollution_code` |
| `factAir4thaiTable` | Air4Thai | same |
| `factMahidolAqiTable` | Mahidol AQI | same |

---

## Project Structure
```
.
├── dags/                    # Airflow DAG definitions
├── data/                    # Cached JSON / HTML (atomic writes)
├── config/                  # Keys & mapping tables
├── logs/                    # Local Airflow logs
├── plugins/                 # (optional) custom operators / hooks
├── Dockerfile               # Extends apache/airflow:2.x-python3.11
├── docker-compose.yaml      # Local single-node Airflow + Postgres
└── README.md
```
---

## Roadmap
- [/] **Streaming** – migrate hourly batch → Apache Kafka for near‑realtime  
- [/] **Alert channels** – add Email Alert
- [ ] **CI/CD** – GitHub Actions for tests & image publish  
- [ ] **Public dashboard** – lightweight Apache Superset demo  

---

## Contributing
Pull requests are welcome! For major changes, open an Issue first.  
Run `pre-commit` (black, flake8, isort) before pushing.

---

## License
Released under the **MIT License** – see [`LICENSE`](LICENSE).

---

### Authors
| Name | Role |
|------|------|
| **Polakorn Anantapakorn (Ming)** | Data Engineer & Project Owner |

Special thanks to the Faculty of Environment and Resource Studies and the Faculty of ICT, Mahidol University.
