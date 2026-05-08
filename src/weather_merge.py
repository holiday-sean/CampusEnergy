import pandas as pd
from datetime import date

def get_weather_data() -> pd.DataFrame:
    """
    Returns merged dataframe from yearly data of weather data at UBC
    """
    curr_year = date.today().year
    dfs = []

    for year in range(2021, curr_year):
        df = pd.read_csv(f"data/processed/weather/weather_{year}.csv")
        dfs.append(df)

    return pd.concat(dfs, ignore_index = True)