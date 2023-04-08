from datetime import date, datetime
import requests
import json
import pprint
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def extract_open_weather_to_lake():
    """
    retrieve open weather api data via requests library and writes to correct s3 data lake endpoint
    """

    url = "https://api.openweathermap.org/data/2.5/weather?lat=3.5553&lon=98.1448&appid=726baba800d0a2c82084d5bb77daa499"
    response = requests.get(url=url)
    s3_hook = S3Hook(aws_conn_id='aws_default')
    weather_data_content = response.json()

    main_weather_content = weather_data_content['main']
    data = json.dumps(main_weather_content)
    s3_hook.load_string(data,'main_weather_content.json',bucket_name='orangutan-orchard',replace=True)
