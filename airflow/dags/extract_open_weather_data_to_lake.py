from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json


@dag(start_date=days_ago(2),
     schedule='@daily',
     tags=['extract','weather-data'],
     description='run extraction for weather api data from open weather map',
     catchup=False)

def extract_open_weather_data_to_lake():

    @task()
    def extract():
        """
        retrieve open weather api data via requests library and writes to correct s3 data lake endpoint
        """

        url = "https://api.openweathermap.org/data/2.5/weather?lat=3.5553&lon=98.1448&appid=726baba800d0a2c82084d5bb77daa499"
        response = requests.get(url=url)
        weather_data_content = response.json()
        main_weather_content = weather_data_content['main']

        return main_weather_content

    @task()
    def load(main_weather_content: dict):

        data = json.dumps(main_weather_content)

        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(data,'main_weather_content.json',bucket_name='orangutan-orchard',replace=True)



    main_weather_data = extract()
    load(main_weather_data)


first_dag = extract_open_weather_data_to_lake()