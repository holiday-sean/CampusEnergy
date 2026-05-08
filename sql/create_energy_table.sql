CREATE TABLE building_data (
    building_name VARCHAR(255) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    electrical_energy FLOAT NOT NULL,
    hot_water_energy FLOAT NOT NULL,
    PRIMARY KEY (building_name, ts),
    FOREIGN KEY (ts) REFERENCES weather_data(ts)
);