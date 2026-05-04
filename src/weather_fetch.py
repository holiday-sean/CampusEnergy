import openmeteo_requests
import pandas as pd
import requests_cache
import requests

from datetime import date 
from retry_requests import retry
from pathlib import Path

class WeatherCollector():
    def __init__(self, export_path="data/processed/weather"):
        self.export_path = Path(export_path)

    def fetch_api_response(self, startDate: str, endDate: str) -> requests.Response:
        """
        Setup and create API request to OpenMeteo for weather data 
        """
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)
        
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 49.26,
            "longitude": -123.52,
            "start_date": startDate,
            "end_date": endDate,
            "hourly": ["temperature_2m", "wind_speed_10m", "snow_depth", "is_day", "precipitation", "relative_humidity_2m", "cloud_cover", "apparent_temperature"],
        }
        
        responses = openmeteo.weather_api(url, params = params)
        
        return responses[0]

    def process_hourly_data(self, response) -> pd.DataFrame:
        """
        Receive the requested weather features and compact it into a dataframe
        """
        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(1).ValuesAsNumpy()
        hourly_snow_depth = hourly.Variables(2).ValuesAsNumpy()
        hourly_is_day = hourly.Variables(3).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(5).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(6).ValuesAsNumpy()
        hourly_apparent_temperature = hourly.Variables(7).ValuesAsNumpy()
    
        hourly_data = {"date": pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = hourly.Interval()),
                inclusive = "left"
        )}
    
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["snow_depth"] = hourly_snow_depth
        hourly_data["is_day"] = hourly_is_day
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["apparent_temperature"] = hourly_apparent_temperature
        hourly_dataframe = pd.DataFrame(data = hourly_data)

        return hourly_dataframe

    def export_file(self, df: pd.DataFrame, curr_year: int):
        """
        Export weather data them to processed directory.
        """
        path = self.export_path / f"weather_{curr_year}.csv"
        df.to_csv(path, index = False)

    def run(self):
        """
        Collect annual weather data and export them to processed directory.
        """
        curr_year = date.today().year

        for curr_year in range(2021, curr_year, 1):
            response = self.fetch_api_response(startDate = f"{curr_year}-01-01", endDate = f"{curr_year}-12-31")
            hourly_df = self.process_hourly_data(response)
            self.export_file(hourly_df, curr_year)

if __name__ == "__main__":
    collector = WeatherCollector()
    collector.run()