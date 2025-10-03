# BEES Data Engineering - Breweries Case
## Solution proposed by Artur Lemes Moretti

This repository contains the solution proposed for the technical case 

# External files 
- .gitignore template from [GitHub Python gitignore](https://github.com/github/gitignore/blob/main/Python.gitignore)
- Apache Airflow docker compose file [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)


# Fetching data

Data is being fetched by requests function and saved in json format

# Orchestration tool

Orchestration is being made by Apache Airflow

```
┌─────────────────────────────────────────────────────────────────┐
│                  BREWERY ETL PIPELINE                           │
│                Bronze → Silver → Gold                           │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  OpenBrewery API │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│ 1. BRONZE LAYER                                              │
│    - Save raw JSON files                                     │
│    - Output: ./data/bronze/ingested_at_YYYYMMDD/*.json       │
└────────┬─────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│ 2. SILVER LAYER                                              │
│    - Read Bronze JSON                                        │
│    - Filter null/empty values                                │
│    - Remove duplicates                                       │
│    - Partition by location                                   │
│    - Output: ./data/silver/location_partition=*/*.parquet    │
└────────┬─────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│ 3. TRANSFORM GOLD LAYER                                      │
│    - Read Silver Parquet                                     │
│    - Aggregate by type/country/state                         │
│    - Output: ./data/gold/*.parquet                           │
└────────┬─────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│ 4. VALIDATE DATA QUALITY                                     │
│    - Check record counts                                     │
│    - Check if there are partitions                           │
└────────┬─────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│ 6. SUCCESS NOTIFICATION                                      │
└──────────────────────────────────────────────────────────────┘
```

# Data Lake Archetecture

## Bronze layer
  - Data is being extracted using endpoint (()) and saved in the json format. Metada endpoint is used to make all requests 
- Silver layer data is saved in Parquet, using Pyspark. Data is cleaned in order to get non null values of id, name, country, state 
- Golde Layer is saved in 

# External services
 Get a SendGrid Key to send emails

# How to start

- create a env file

```env
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW__WEBSERVER__AUTHENTICATE=True
AIRFLOW__WEBSERVER__AUTH_BACKEND=airflow.providers.password.auth_backend.auth_backend

# Add username and password
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

```

- Run docker compose 

```bash
docker compose up airflow-init
docker compose up
```

- Run o bash: 

```bash
sudo ./start.sh
```

- open [local server](http://localhost:8080/)

- Login 

```
user: admin
password: admin
```

