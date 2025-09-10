import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


current_directory = os.path.dirname(os.path.abspath(__file__))
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='datalake_part2_pipeline',
    default_args=default_args,
    description='Landing->Bronze->Silver->Gold Spark jobs',
    schedule_interval=None,
    start_date=datetime(2025, 9, 10),
    catchup=False,
    max_active_runs=1
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(current_directory, 'landing_to_bronze.py'),
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1
    )

    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(current_directory, 'bronze_to_silver.py'),
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1
    )

    landing_to_bronze >> bronze_to_silver
