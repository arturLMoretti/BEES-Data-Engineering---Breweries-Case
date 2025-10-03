# BEES Data Engineering - Breweries Case

This repository contains my proposed for the Data Engineer techinical case at Bees. It uses Apache Airflow for orchestration and PySpark for transformations, following the Medallion Archtecture. The solution is fully containerized with Docker Compose, requiring no external cloud services.

# External files 
- .gitignore template from [GitHub Python gitignore](https://github.com/github/gitignore/blob/main/Python.gitignore)
- Apache Airflow docker compose file [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)


# Fetching data

Data ingestion is performed via the [List Breweries](https://www.openbrewerydb.org/documentation#list-breweries), orchestrated with Apache Airflow
- Up to 3 retries
- Retray delay: 5 minutes
- Timeout: 1 hour

# Orchestration tool

Orchestration is being made by Apache Airflow. Data is being saved into medalion architecture
- In case of errors, 3 retries are used with a retray delay of 5 minutes. A timeout of 1 hour was used

```
┌─────────────────────────────────────────────────────────────────┐
│                  BREWERY ETL PIPELINE                           |
└────────┬───────────────────────────────────────────────────────┘
         │
         ▼
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
  - Extracts data [List Breweries](https://www.openbrewerydb.org/documentation#list-breweries)
  - Saves raw json files
  - Uses metadata endpoint to calculate total requests
  - Implements exponential backof with 3 retries

## Silver layer
  - Reads Bronze JSON files with PySpark
  - Cleans data (removes nulls/duplicates in id, name, country, state)
- Saves partitioned Parquet files by location

## Golden layer
- Agregates breweries by type and location
- Saves final datasets in Parquet

## Data Validation
- Check records counts
- Check partiion presence
- Final success notification

## External services
 - No external cloud services are required to run this project

# How to start

- Create an env file:

```env
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW__WEBSERVER__AUTHENTICATE=True
AIRFLOW__WEBSERVER__AUTH_BACKEND=airflow.providers.password.auth_backend.auth_backend

# Add username and password
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

```

- Star Airflow with docker compose:

```bash
docker compose down -v
docker compose build
docker compose up airflow-init
docker compose up
```

- Access the Airflow UI [local server](http://localhost:8080/)

```
user: admin
password: admin
```

- Trigger the pipeline!





