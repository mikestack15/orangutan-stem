import datetime
import json
import uuid

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from operators.custom_operators import S3ToGCSAndBigQueryOperator
import requests


"""
Activity 1: Open Weather Map API Airflow DAG

Author: Michael Stack
Last Updated: 7/11/2023

This DAG should work both locally and server-side if you utilize the docker setup in the airflow directory,
have the open weather map api key set as an airflow variable, and connections for 'aws_default', 'bigquery_default', 
'google_cloud_default'
Follow the instructions provided in the wiki,
https://github.com/mikestack15/orangutan-stem/wiki/Activity-1:-Open-Weather-Map-API-Data-Pipeline

Goal: Extract Open Weather Map API data into a date-partitioned S3 bucket/key path, transfer to gcs, and load into bigquery

This is a good use-case of the Airflow Taskflow API. The activity also ensures that you have your local machine 
set up with the necessary AWS keys for S3, and you are able to successfully load data into GCP BigQuery.

Be sure to follow along on the wiki if you need guidance on getting this DAG to work on your desired machine
"""

# set date variables for f-string in S3 bucket load
dt = datetime.datetime.now()
current_year, current_month, current_day, current_hour = dt.strftime('%Y %m %d %H').split()

# retrieve airflow variable for open weather map API key
# note, this takes a few hours upon sign-up to become active, so be patient
open_weather_api_key = Variable.get('open_weather_api_key')

# lake bucket and key_path
lake_dest_bucket = "orangutan-orchard"
lake_dest_path = f'raw/open_weather_map/bukit_lawang/{current_year}/{current_month}/{current_day}/{current_hour}/'

# BigQuery project.dataset.table and schema
bq_dest = 'orangutan-orchard.bukit_lawang_weather.raw_ingested_main_weather_content'

# Bukit Lawang lat and long coordinates - feel free to update to yours!
lat_long = {'lat': 3.5553, 'long': 98.1448}


@dag(start_date=days_ago(0),
     schedule_interval='@hourly',
     tags=['extract', 'weather-data'],
     description='Run extraction for weather API data from Open Weather Map',
     catchup=False)
def extract_open_weather_data_to_lake():
    @task()
    def extract():
        """
        Retrieve OpenWeatherAPI data via the requests library.

        Returns:
            main_weather_content (dict): Main weather content.
        """
        response = requests.get(
            url=f"https://api.openweathermap.org/data/2.5/weather?lat={lat_long['lat']}&lon={lat_long['long']}&appid={open_weather_api_key}"
        )
        weather_data_content = response.json()
        resp_main_weather_content = weather_data_content['main']

        # Generate a UUID4
        generated_uuid = uuid.uuid4()

        # Get the current date and time
        now = datetime.datetime.now()
        # Convert the current date and time to a string in the format of a timestamp
        resp_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

        # Create a new dictionary with 'uuid' and 'timestamp' at the front
        ingested_main_weather_content = {'uuid': str(generated_uuid), 'resp_ingested_timestamp': resp_timestamp}
        # Update the new dictionary with the old dictionary
        ingested_main_weather_content.update(resp_main_weather_content)

        return ingested_main_weather_content

    @task()
    def load(ingested_main_weather_content: dict):
        """
        Writes OpenWeatherAPI response (dict/json) into S3 bucket/folder and loads data into appropriate GCS/BigQuery endpoint

        Args:
            main_weather_content (dict): Dictionary response from main_weather_content.
        """
        data = json.dumps(ingested_main_weather_content)

        # Load data into AWS S3
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            data,
            lake_dest_path,
            bucket_name=lake_dest_bucket,
            replace=True
        )

        # Load data from S3 bucket into BigQuery
        gcs_bucket = 'orangutan-orchard'
        gcs_key = f'raw/open_weather_map/bukit_lawang/{current_year}/{current_month}/{current_day}/{current_hour}/*.json'
        s3_to_bigquery_operator = S3ToGCSAndBigQueryOperator(
                task_id='s3_to_bigquery',
                s3_bucket=lake_dest_bucket,
                s3_key=f'raw/',
                gcs_bucket=gcs_bucket,
                gcs_key=gcs_key,
                bigquery_table=bq_dest,
                bigquery_schema_fields=[
                    {"name": "uuid", "mode": "REQUIRED", "type": "STRING"},
                    {"name": "resp_ingested_timestamp", "mode": "NULLABLE", "type": "DATETIME"},
                    {"name": "temp", "mode": "NULLABLE", "type": "FLOAT"},
                    {"name": "feels_like", "mode": "NULLABLE", "type": "FLOAT"},
                    {"name": "temp_min", "mode": "NULLABLE", "type": "FLOAT"},
                    {"name": "temp_max", "mode": "NULLABLE", "type": "FLOAT"},
                    {"name": "pressure", "mode": "NULLABLE", "type": "FLOAT"},
                    {"name": "humidity", "mode": "NULLABLE", "type": "FLOAT"},
                    {"name": "sea_level", "mode": "NULLABLE", "type": "FLOAT"},
                    {"name": "grnd_level", "mode": "NULLABLE", "type": "FLOAT"}
                ],
                s3_conn_id='aws_default',
                gcs_conn_id='google_cloud_default',
                bigquery_conn_id='bigquery_default'
            )

        # Execute the BigQuery load operator
        # Execute the BigQuery load operator
        context = {"execution_date": datetime.datetime.now()}
        s3_to_bigquery_operator.execute(context)

    main_weather_data = extract()
    load(main_weather_data)
    

weather_api_dag = extract_open_weather_data_to_lake()
