from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "pustaka",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def extract_to_staging():
    logging.info("Extract data dari OLTP / CSV ke staging area")

def transform_dimensions():
    logging.info("Transform dimensi: category, location, date")

def load_fact():
    logging.info("Load data ke tabel fact_sales (PostgreSQL DWH)")

def quality_check():
    logging.info("Quality check: NULL, duplikat, validasi data")

def enrich_api():
    logging.info("Data enrichment menggunakan Google Books API")

def export_ml():
    logging.info("Export dataset untuk Machine Learning")

with DAG(
    dag_id="bookstore_etl_integrated",
    description="ETL Pipeline to Data Warehouse",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "data-warehouse"],
) as dag:

    t1 = PythonOperator(
        task_id="1_extract_to_staging",
        python_callable=extract_to_staging,
    )

    t2 = PythonOperator(
        task_id="2_transform_dimensions",
        python_callable=transform_dimensions,
    )

    t3 = PythonOperator(
        task_id="3_load_fact",
        python_callable=load_fact,
    )

    t4 = PythonOperator(
        task_id="4_quality_check",
        python_callable=quality_check,
    )

    t5 = PythonOperator(
        task_id="5_enrich_api",
        python_callable=enrich_api,
    )

    t6 = PythonOperator(
        task_id="6_export_ml",
        python_callable=export_ml,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
