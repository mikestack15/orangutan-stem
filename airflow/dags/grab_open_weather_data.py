from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from scripts.extract_open_weather_api_to_lake import extract_open_weather_to_lake
import requests
import boto3

with DAG(
    'weather-data-api-extraction',
    default_args={
        'depends_on_past': False,
        'retries': 0,
    },
    description='run extraction for weather api data from open weather map',
    schedule_interval=None,
    start_date=datetime(2023, 3, 31),
    catchup=False,
    tags=['weather-api-pipeline'],
) as dag:

    extract_data = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_open_weather_to_lake,
    )