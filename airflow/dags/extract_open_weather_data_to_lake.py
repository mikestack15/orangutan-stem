import datetime
import json
import uuid

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import requests
from operators.custom_operators import S3toBigQueryOperator, DogNamedMike

"""
Activity 1: Open Weather Map API Airflow DAG

Author: Michael Stack
Last Updated: 7/7/2023

This DAG should work both locally and server-side if you utilize the docker-compose file in the airflow directory,
and have the open weather map api key set as an airflow variable. Follow the instructions provied in the wiki,
https://github.com/mikestack15/orangutan-stem/wiki/Activity-1:-Open-Weather-Map-API-Data-Pipeline

Goal: Extract Open Weather Map API data into a date partitioned s3 bucket/key path

This is a good use-case of the Airflow Taskflow API, ensuring you have your local machine set up with the necessary aws
keys for s3, and you are able to successfully load data into gcp bigquery. 

Be sure to follow along on the wiki if you need guidance on getting this dag to work on your desired machine
"""

# set date variables for f-string in s3 bucket load
dt = datetime.datetime.now()
current_year, current_month, current_day, current_hour = dt.strftime('%Y %m %d %H').split()

# retrieve airflow variable for open weather map api key
# note, this takes a few hours upon sign-up to become active, so be patient
open_weather_api_key = Variable.get('open_weather_api_key')

# aws bucket and path
aws_dest_bucket = 'orangutan-orchard'
aws_dest_path = 'raw/open_weather_map/bukit_lawang/{current_year}/{current_month}/{current_day}/{current_hour}/ingested_main_weather_content.json'


# bigquery project.dataset.table and schema




# bukit lawang lat and long coordinates-feel free to update to yours!
lat_long = {'lat': 3.5553, 'long': 98.1448}


@dag(start_date=days_ago(0),
     schedule='@hourly',
     tags=['extract','weather-data'],
     description='run extraction for weather api data from open weather map',
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
        Writes OpenWeatherAPI response (dict/json) into S3 bucket/folder, and loads data into appropriate gcs/bigquery endpoint
        
        Args:
            main_weather_content (dict): Dictionary response from main_weather_content.
        """
        data = json.dumps(ingested_main_weather_content)

        # load data into aws s3
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            data,
            aws_dest_path,
            bucket_name=aws_dest_bucket,
            replace=True
        )

        # load data from s3 bucket into bigquery

        # S3ToBigQueryOperator(
        #     task_id='s3_to_bigquery',
        #     bucket='your-s3-bucket-name',
        #     source_objects=[aws_dest_path],
        #     destination_project_dataset_table='orangutan-orchard.bukit_lawang_weather.raw_ingested_main_weather_content',
        #     schema_fields=[
        #         {'name': 'field1', 'type': 'STRING', 'mode': 'REQUIRED'},
        #         {'name': 'field2', 'type': 'INT64', 'mode': 'NULLABLE'},
        #         # add more fields according to your schema
        #     ],
        #     source_format='CSV',
        #     create_disposition='CREATE_IF_NEEDED',
        #     write_disposition='WRITE_TRUNCATE',
        #     bigquery_conn_id='gcp_bigquery_conn',  # the connection id we defined earlier
        #     google_cloud_storage_conn_id='aws_s3_conn',  # the connection id we defined earlier
        # )

    main_weather_data = extract()
    load(main_weather_data)


weather_api_dag = extract_open_weather_data_to_lake()