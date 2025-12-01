from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
import sys
import os

# Add your source folder so Airflow can find your scripts
sys.path.insert(0, "/opt/airflow/projects/src")

# Import your actual scripts
import ingest_xml
import pre_filter_data
import process_nlp_groq
import run_analytics

# Default arguments for the DAG
default_args = {
    'owner': 'Data Engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'personal_finance_pipeline',
    default_args=default_args,
    description='End-to-end SMS to Power BI pipeline',
    schedule_interval='@daily', # Runs once a day
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Ingest XML Data
    t1_ingest = PythonOperator(
        task_id='ingest_xml_backup',
        python_callable=ingest_xml.ingest_xml_data
    )

    # Task 2: Pre-filter Junk (Regex)
    t2_filter = PythonOperator(
        task_id='pre_filter_sms',
        python_callable=pre_filter_data.main
    )

    # Task 3: AI Enrichment (Groq)
    t3_enrich = PythonOperator(
        task_id='enrich_transactions_groq',
        python_callable=process_nlp_groq.main
    )

    # Task 4: BigQuery Data Modeling (SQL)
   
    t4_model = BigQueryExecuteQueryOperator(
        task_id='run_bigquery_models',
        sql="CALL --",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default'
    )

    # Task 5: Anomaly Detection
    t5_analyze = PythonOperator(
        task_id='detect_anomalies',
        python_callable=run_analytics.run_anomaly_detection
    )

    # Define the workflow order
    t1_ingest >> t2_filter >> t3_enrich >> t4_model >> t5_analyze