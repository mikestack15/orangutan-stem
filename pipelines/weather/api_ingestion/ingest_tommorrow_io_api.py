import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

class WeatherDataFetcher:
    def __init__(self, api_key, location, output_path):
        """
        Initialize the WeatherDataFetcher.

        Parameters:
        - api_key (str): The API key for accessing the tomorrow.io API.
        - location (dict): Dictionary containing latitude and longitude.
        - output_path (str): The directory path to save the weather data.
        """
        self.api_key = api_key
        self.location = location
        self.output_path = output_path or os.path.expanduser("/sandbox/tomorrow_io_weather_data/")
        self.verify_directory()

    def verify_directory(self):
        """
        Verify if the output directory exists; if not, create it.
        """
        os.chdir('/Users/')
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
            print(f"Directory '{self.output_path}' created.")

    def fetch_weather_data(self):
        """
        Fetch weather data from tomorrow.io API and save it as a Parquet file.
        """
        # API endpoint URL
        url = f'https://api.tomorrow.io/v4/weather/forecast?location={self.location["lat"]},{self.location["long"]}&apikey={self.api_key}'

        # Fetching data from the API
        response = requests.get(url)
        data = response.json()

        # Extracting relevant information
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
                # Add more fields as needed
            }

            parsed_data.append(parsed_entry)

        # Creating a DataFrame for tabular representation
        df = pd.DataFrame(parsed_data)

        # Save DataFrame as a Parquet file with a unique name
        current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M")
        file_name = f'{current_datetime}_tomorrow_io_weather_resp.parquet'
        df.to_parquet(os.path.join(self.output_path, file_name))
        print(f'Data saved successfully as {file_name} in {self.output_path}')

if __name__ == "__main__":
    load_dotenv()
    api_key = os.environ.get('TOMORROW_IO_API_KEY')
    location = {'lat': 3.5553, 'long': 98.1448}
    output_path = '/Users/michaelstack/sandbox/tomorrow_io_weather_data/'

    # Example usage
    weather_fetcher = WeatherDataFetcher(api_key, location, output_path)
    weather_fetcher.fetch_weather_data()

