import pandas as pd

from config import config_db
from skyspark_merge import get_building_data
from sqlalchemy import create_engine, text
from weather_merge import get_weather_data


class db_upload():
    def __init__(self, df, table_name):
        self.df = df
        self.table_name = table_name

    def upload(self):
        params = config_db()
        engine = create_engine(
            f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
        )
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {self.table_name}"))
            self.df.to_sql(self.table_name, conn, if_exists='append', index=False)
            print(f"Uploaded {len(self.df)} rows to {self.table_name}")
            conn.commit()
        
if __name__ == "__main__":
    weather_df = get_weather_data()
    building_energy_df = get_building_data()

    weather_loader = db_upload(weather_df, "weather_data")
    building_loader = db_upload(building_energy_df, "building_data")

    weather_loader.upload()
    building_loader.upload()
