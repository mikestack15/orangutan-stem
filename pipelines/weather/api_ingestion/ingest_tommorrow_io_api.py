import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import NoCredentialsError
from io import BytesIO  # Import BytesIO for in-memory operations

class WeatherDataFetcher:
    """
    A class to fetch weather data using the Tomorrow.io API
    and upload it to an S3 bucket.

    IMPORTANT
    ***Be sure to create a .env file in the orangutan-stem/pipelines/weather/api_ingestion/ directory in the repo***
    the .gitignore file should ignore all .env files safely, but be sure to double-check.


    """

    def __init__(self, api_key, location, s3_output_path, s3_bucket):
        """
        Initialize the WeatherDataFetcher object.

        :param api_key: API key for Tomorrow.io API.
        :param location: Dictionary containing latitude and longitude coordinates.
        :param s3_output_path: Path to the S3 bucket where data will be uploaded.
        :param s3_bucket: Name of the S3 bucket.
        """
        self.api_key = api_key
        self.location = location
        self.s3_output_path = s3_output_path or "raw/tomorrow_io_weather_data"
        self.s3_bucket = s3_bucket

    def upload_to_s3(self, df):
        """
        Upload DataFrame to the specified S3 bucket.

        :param df: Pandas DataFrame containing weather data.
        """
        s3 = boto3.client('s3')
        current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M")
        file_name = f'{current_datetime}_tomorrow_io_weather_resp.parquet'
        try:
            with BytesIO() as buffer:
                df.to_parquet(buffer, index=False)
                buffer.seek(0)
                s3.upload_fileobj(buffer, self.s3_bucket, f"{self.s3_output_path}/{file_name}")
            print(f"Data successfully uploaded to S3 bucket '{self.s3_bucket}' at '{self.s3_output_path}'.")
        except NoCredentialsError:
            print("Credentials not available. Please check your AWS credentials.")

    def fetch_weather_data(self):
        """
        Fetch weather data from Tomorrow.io API and process it.
        """
        url = f'https://api.tomorrow.io/v4/weather/forecast?location={self.location["lat"]},{self.location["long"]}&apikey={self.api_key}'
        response = requests.get(url)
        data = response.json()

        minutely_data = data['timelines']['minutely']
        parsed_data = []

        for entry in minutely_data:
            time = entry['time']
            values = entry['values']

            parsed_entry = {
                'time': time,
                'cloudBase': values['cloudBase'],
                'cloudCeiling': values['cloudCeiling'],
                'cloudCover': values['cloudCover'],
                'dewPoint': values['dewPoint'],
                'humidity': values['humidity'],
                'precipitationProbability': values['precipitationProbability'],
                'pressureSurfaceLevel': values['pressureSurfaceLevel'],
                'rainIntensity': values['rainIntensity'],
                'sleetIntensity': values['sleetIntensity'],
                'snowIntensity': values['snowIntensity'],
                'temperature': values['temperature'],
                'temperatureApparent': values['temperatureApparent'],
                'uvHealthConcern': values['uvHealthConcern'],
                'uvIndex': values['uvIndex'],
                'visibility': values['visibility'],
                'weatherCode': values['weatherCode'],
                'windDirection': values['windDirection'],
                'windGust': values['windGust'],
                'windSpeed': values['windSpeed']
            }

            parsed_data.append(parsed_entry)

        df = pd.DataFrame(parsed_data)

        # Upload DataFrame to S3
        self.upload_to_s3(df)

if __name__ == "__main__":
    load_dotenv()
    api_key = os.environ.get('TOMORROW_IO_API_KEY')
    location = {'lat': 3.5553, 'long': 98.1448}
    s3_output_path = 'raw/tomorrow_io_weather_data'
    s3_bucket = 'orangutan-orchard'  # Replace with your actual S3 bucket name

    weather_fetcher = WeatherDataFetcher(api_key, location, s3_output_path, s3_bucket)
    weather_fetcher.fetch_weather_data()


