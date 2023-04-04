import os
from datetime import date, datetime
import requests
import json


def extract_open_weather_api_to_lake():
    # retrieve open weather api data via curl command

    params = {'Content'}
    url = "https://api.openweathermap.org/data/2.5/weather?lat=3.5553&lon=98.1448&appid=726baba800d0a2c82084d5bb77daa499"
    response = requests.get(url=url)

    x = response.json()

    try:
        z = x['rain']
    except:
        pass


    current_weather_summary = x['weather']

    main = x['main']

    current_temp_kelvin = main['temp']

    current_wind = x['wind']

    visibility = x['visibility']

    print(current_weather_summary)
    print(main)
    print(current_temp_kelvin)
    print(current_wind)
    print(visibility)

    return main

# {'temp': 296.46, 'feels_like': 297.38, 'temp_min': 296.46, 'temp_max': 296.46, 'pressure': 1012, 'humidity': 97, 'sea_level': 1012, 'grnd_level': 992}


if __name__ == "__main__":
    extract_open_weather_api_to_lake()
