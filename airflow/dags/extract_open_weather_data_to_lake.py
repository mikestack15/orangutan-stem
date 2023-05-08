from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json
import datetime
import os


# set date variables for f string in data lake load
dt = datetime.datetime.now()
current_year, current_month, current_day, current_hour = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d'), dt.strftime('%H')

# set environment variables for open weather map api
open_weather_api_key = os.environ['open_weather_api_key']

"""
Activity 1: Open Weather Map API Airflow DAG

Author: Michael Stack
Last Updated: 5/5/2023

This DAG should work both locally and server-side if you utilize the docker-compose file in the airflow directory,
and have the open weather map api key set as an environment variable.

Goal: Extract Open Weather Map API data into a date partitioned s3 bucket/key path

This is a good use-case of the Airflow Taskflow API
Be sure to follow along on the wiki if you need guidance on getting this dag to work on your desired machine

"""


@dag(start_date=days_ago(1),
     schedule='@hourly',
     tags=['extract','weather-data'],
     description='run extraction for weather api data from open weather map',
     catchup=False)

def extract_open_weather_data_to_lake():

    @task()
    def extract():
        """
        retrieve open weather api data via requests library
        Returns:
        main_weather_content: dict
        """

        response = requests.get(url=f"https://api.openweathermap.org/data/2.5/weather?lat=3.5553&lon=98.1448&appid={open_weather_api_key}")
        weather_data_content = response.json()
        main_weather_content = weather_data_content['main']

        return main_weather_content

    @task()
    def load(main_weather_content: dict):
        """
        loads open weather api response (dict/json) into s3 bucket/folder
        Params:
        main_weather_content: dict
        dictionary response from main_weather_content
        """
        data = json.dumps(main_weather_content)

        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(data,f'raw/open_weather_map/bukit_lawang/{current_year}/{current_month}/{current_day}/{current_hour}/ \
                            main_weather_content.json',bucket_name='orangutan-orchard',replace=True)


    main_weather_data = extract()
    load(main_weather_data)


weather_api_dag = extract_open_weather_data_to_lake()