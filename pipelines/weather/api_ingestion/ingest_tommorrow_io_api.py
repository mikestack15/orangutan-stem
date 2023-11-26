import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

def fetch_weather_data(api_key, location, output_path=None):
    """
    fetch_weather_data: retrieves minutely weather api data for precise coordinates at the time of 
    the tomorrow.io API request. *Note, a tomorrow.io user account, API key, and enviornment varaible 
    of the key is required for this code to run successfully

    params:
    api_key (str): the string-value provided by the API provider
    location (dict): a dictionary of {'lat': lat_value, 'long': long_value}
    output_path (str): [OPTIONAL]
    """
    directory_path = os.path.expanduser("~/Desktop/sandbox/tomorrow_io_weather_data/")

    # Check if the directory exists
    if not os.path.exists(directory_path):
        # If it doesn't exist, create the directory and its parent directories
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")

    output_path = directory_path

    # API endpoint URL
    url = f'https://api.tomorrow.io/v4/weather/forecast?location={location["lat"]},{location["long"]}&apikey={api_key}'

    # Fetching data from the API
    response = requests.get(url)
    data = response.json()

    # TODO Verify the minute frequency, make this a pytest

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
        }

        parsed_data.append(parsed_entry)

    # Creating a DataFrame for tabular representation
    df = pd.DataFrame(parsed_data)

    # Save DataFrame as a Parquet file with a unique name
    current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M")
    file_name = f'{current_datetime}_tomorrow_io_weather_resp.parquet'
    df.to_parquet(os.path.join(output_path, file_name))
    print(f'Data saved successfully as {file_name} in {output_path}')

if __name__ == "__main__":
    load_dotenv()
    api_key = os.environ.get('TOMORROW_IO_API_KEY')
    location = {'lat': 3.5553, 'long': 98.1448}
    output_path = '/home/Desktop/sandbox/tomorrow_io_weather_data'

    fetch_weather_data(api_key, location, output_path)

