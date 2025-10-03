from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os
from pathlib import Path

# Handle local and Docker environments
if os.path.exists("BEES Data Engineering - Breweries Case"):
    project_root = Path("BEES Data Engineering - Breweries Case")
else:
    project_root = Path("/opt/airflow")

src_path = str(project_root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from get_brewery_data import BreweryAPIClient, get_all_breweries
from transform_brewery_data import BreweryDataTransformer


# ==================== DAG Configuration ======================

default_args = {
    'owner': 'artur-lemes-moretti',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'brewery_etl_bronze_silver_gold',
    default_args=default_args,
    description='ETL pipeline: Extract brewery data and persist it into the following medallion architecture: Bronze (raw JSON), Silver (Parquet), Gold (aggregated analytics)',
    schedule='@daily',
    max_active_runs=1,
    catchup=False,
    tags=['brewery', 'etl', 'bronze', 'silver', 'gold', 'pyspark', 'medallion']
)


# ==================== Task Functions =====================

def extract_bronze_layer(**context):
    print("=" * 60)
    print("BRONZE LAYER - Starting Extraction")
    print("=" * 60)
    
    try:
        api_client = BreweryAPIClient()
        
        total_breweries = api_client.get_total_data_count()
        
        if total_breweries == 0:
            raise ValueError("No data returned from API endpoint")
        
        brewery_per_page = 200
        total_pages = (total_breweries // brewery_per_page) + (1 if total_breweries % brewery_per_page > 0 else 0)
        
        print(f"Total pages to fetch: {total_pages}")
        print(f"Starting extraction...")
        
        get_all_breweries(per_page=brewery_per_page, total_breweries=total_breweries)
        
        print("=" * 60)
        print(f"BRONZE LAYER - Extraction completed successfully")
        print(f" Total records: {total_breweries}")
        print(f" Pages fetched: {total_pages}")
        print("=" * 60)
        
        # Push metadata to XCom for Silver layer
        context['task_instance'].xcom_push(key='total_breweries', value=total_breweries)
        context['task_instance'].xcom_push(key='total_pages', value=total_pages)
        
        return {
            'status': 'success',
            'total_breweries': total_breweries,
            'total_pages': total_pages
        }
        
    except Exception as e:
        print(f"Error in data extraction: {str(e)}")
        raise


def transform_silver_layer(**context):
    print("=" * 60)
    print("Starting SILVER LAYER")
    print("=" * 60)
    
    try:
        transformer = BreweryDataTransformer(
            app_name="BreweryETL_Silver",
            bronze_path="./data/bronze"
        )

        transformer.transform_data_to_silver()
        
        transformer.stop_spark_session()
        
        print("=" * 60)
        print("SILVER LAYER - Transformation completed successfully")
        print("=" * 60)
        
        return {
            'status': 'success',
            'layer': 'silver',
            'format': 'parquet',
            'partitioned_by': 'country_state'
        }
        
    except Exception as e:
        print(f"Error in Silver layer: {str(e)}")
        raise


def transform_gold_layer(**context):
    print("=" * 60)
    print("GOLD LAYER - Analytics Creation Starting")
    print("=" * 60)
    
    try:
        transformer = BreweryDataTransformer(
            app_name="BreweryETL_Gold",
            bronze_path="./data/bronze"
        )

        transformer.transform_data_to_gold()
        
        transformer.stop_spark_session()
        
        print("=" * 60)
        print("GOLD LAYER - Aggregations created successfully")
        print("=" * 60)
        
        return {
            'status': 'success',
            'layer': 'gold',
            'aggregations': ['brewery_type_country_state']
        }
        
    except Exception as e:
        print(f"Error in Gold layer: {str(e)}")
        raise


def validate_data_quality(**context):

    print("=" * 60)
    print("🔍 DATA QUALITY VALIDATION")
    print("=" * 60)
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.appName("DataQualityValidation").getOrCreate()
        
        print("\n Bronze Layer Validation:")
        try:
            bronze_df = spark.read.option("recursiveFileLookup", "true").json("./data/bronze")
            bronze_count = bronze_df.count()
            bronze_columns = len(bronze_df.columns)
            print(f"Records: {bronze_count}")
            print(f"Columns: {bronze_columns}")
        except Exception as e:
            print(f"Bronze validation failed: {e}")
            bronze_count = 0
        
        print("\n Silver Layer Validation:")
        try:
            silver_df = spark.read.parquet("./data/silver")
            silver_count = silver_df.count()
            silver_partitions = silver_df.select("location_partition").distinct().count()
            print(f"Records: {silver_count}")
            print(f"Partitions: {silver_partitions}")
            print(f"Data quality: {(silver_count/bronze_count*100):.2f}% retained")
        except Exception as e:
            print(f"Silver validation failed: {e}")
            silver_count = 0
            silver_partitions = 0
        
        print("\n Gold Layer Validation:")
        try:
            gold_df = spark.read.parquet("./data/gold")
            gold_count = gold_df.count()
            print(f"Aggregated records: {gold_count}")
        except Exception as e:
            print(f"Gold validation failed: {e}")
            gold_count = 0
        
        spark.stop()
        
        print("\n📊 Validation Summary:")
        validation_passed = True
        
        if bronze_count == 0:
            print("FAIL: No data in Bronze layer")
            validation_passed = False
        else:
            print(f"PASS: Bronze layer has {bronze_count} records")
        
        if silver_count == 0:
            print("FAIL: No data in Silver layer")
            validation_passed = False
        else:
            print(f"PASS: Silver layer has {silver_count} records")

        if gold_count == 0:
            print("FAIL: No data in Gold layer")
            validation_passed = False
        else:
            print(f"PASS: Gold layer has {gold_count} records")
        
        if not validation_passed:
            raise ValueError("Data quality validation failed")
        
        print("\n" + "=" * 60)
        print("DATA QUALITY VALIDATION PASSED")
        print("=" * 60)
        
        return {
            'bronze_records': bronze_count,
            'silver_records': silver_count,
            'silver_partitions': silver_partitions,
            'gold_records': gold_count,
            'validation_passed': validation_passed
        }
        
    except Exception as e:
        print(f"\n Data quality validation failed: {str(e)}")
        raise


# ==================== Task Definitions ====================

extract_bronze_task = PythonOperator(
    task_id='extract_bronze_layer',
    python_callable=extract_bronze_layer,
    dag=dag,
    doc_md="""### Extract to Bronze Layer"""
)

transform_silver_task = PythonOperator(
    task_id='transform_silver_layer',
    python_callable=transform_silver_layer,
    dag=dag,
    doc_md="""### Transform to Silver Layer"""
)

transform_gold_task = PythonOperator(
    task_id='transform_gold_layer',
    python_callable=transform_gold_layer,
    dag=dag,
    doc_md="""### Transform to Gold Layer"""
)

validate_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
    doc_md="""### Data Quality Validation"""
)

success_notification = BashOperator(
    task_id='success_notification',
    bash_command="""
    echo "============================================"
    echo "BREWERY ETL PIPELINE COMPLETED SUCCESSFULLY!"
    echo "============================================"
    echo "Date: $(date)"
    echo "============================================"
    """,
    dag=dag,
)


# ==================== Task execution order ====================

extract_bronze_task >> transform_silver_task >> transform_gold_task >> validate_quality_task >> success_notification
