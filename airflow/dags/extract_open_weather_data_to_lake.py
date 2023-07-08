import datetime
import json

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import requests


"""
Activity 1: Open Weather Map API Airflow DAG

Author: Michael Stack
Last Updated: 7/7/2023

This DAG should work both locally and server-side if you utilize the docker-compose file in the airflow directory,
and have the open weather map api key set as an airflow variable.

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
        main_weather_content = weather_data_content['main']
        
        return main_weather_content


    @task()
    def load(main_weather_content: dict):
        """
        Loads OpenWeatherAPI response (dict/json) into S3 bucket/folder.
        
        Args:
            main_weather_content (dict): Dictionary response from main_weather_content.
        """
        data = json.dumps(main_weather_content)

        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            data,
            f'raw/open_weather_map/bukit_lawang/{current_year}/{current_month}/{current_day}/{current_hour}/main_weather_content.json',
            bucket_name='orangutan-orchard',
            replace=True
        )

    main_weather_data = extract()
    load(main_weather_data)


weather_api_dag = extract_open_weather_data_to_lake()