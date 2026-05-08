CREATE TABLE weather_data (
    ts TIMESTAMPTZ PRIMARY KEY,
    temperature_2m FLOAT NOT NULL,
    wind_speed_10m FLOAT NOT NULL,
    snow_depth FLOAT NOT NULL,
    is_day BOOLEAN NOT NULL,
    precipitation FLOAT NOT NULL,
    relative_humidity_2m INTEGER NOT NULL,
    cloud_cover INTEGER NOT NULL,
    apparent_temperature FLOAT NOT NULL
);