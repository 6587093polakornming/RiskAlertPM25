-- DROP TABLES IF EXISTED (for safety in re-creation order)
DROP TABLE IF EXISTS factMahidolAqiTable CASCADE;
DROP TABLE IF EXISTS factAirVisualTable CASCADE;
DROP TABLE IF EXISTS factAir4thaiTable CASCADE;
DROP TABLE IF EXISTS dimMainPollutionTable CASCADE;
DROP TABLE IF EXISTS dimLocationTable CASCADE;
DROP TABLE IF EXISTS dimDateTimeTable CASCADE;

-- Dimension: DateTime
CREATE TABLE dimDateTimeTable (
    date_time TIMESTAMP PRIMARY KEY,
    date DATE,
    time TIME,
    day INT,
    month INT,
    year INT,
    hour INT
);

-- Dimension: Location
CREATE TABLE dimLocationTable (
    location_id SERIAL PRIMARY KEY,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    description VARCHAR(255),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100)
);

-- Dimension: Main Pollution
CREATE TABLE dimMainPollutionTable (
    main_pollution_code VARCHAR(50) PRIMARY KEY,
    unit VARCHAR(20),
    name_pollution VARCHAR(100)
);

-- Fact: Air4Thai
CREATE TABLE factAir4thaiTable (
    date_time TIMESTAMP NOT NULL REFERENCES dimDateTimeTable(date_time),
    location_id INT NOT NULL REFERENCES dimLocationTable(location_id),
    main_pollution_code VARCHAR(50) NOT NULL REFERENCES dimMainPollutionTable(main_pollution_code),
    AQI NUMERIC(6,2),
    PM25_value NUMERIC(6,2),
    PM10_value NUMERIC(6,2),
    O3_value NUMERIC(6,2),
    CO_value NUMERIC(6,2),
    NO2_value NUMERIC(6,2),
    SO2_value NUMERIC(6,2),
    PRIMARY KEY (date_time, location_id, main_pollution_code)
);

-- Fact: AirVisual
CREATE TABLE factAirVisualTable (
    date_time TIMESTAMP NOT NULL REFERENCES dimDateTimeTable(date_time),
    location_id INT NOT NULL REFERENCES dimLocationTable(location_id),
    main_pollution_code VARCHAR(50) NOT NULL REFERENCES dimMainPollutionTable(main_pollution_code),
    AQI_US NUMERIC(6,2),
    temperature_c NUMERIC(6,2),
    pressure NUMERIC(6,2),
    humidity NUMERIC(6,2),
    wind_speed NUMERIC(6,2),
    wind_direction NUMERIC(6,2),
    weather_text VARCHAR(100),
    PRIMARY KEY (date_time, location_id, main_pollution_code)
);

-- Fact: Mahidol AQI
CREATE TABLE factMahidolAqiTable (
    date_time TIMESTAMP NOT NULL REFERENCES dimDateTimeTable(date_time),
    location_id INT NOT NULL REFERENCES dimLocationTable(location_id),
    main_pollution_code VARCHAR(50) NOT NULL REFERENCES dimMainPollutionTable(main_pollution_code),
    air_quality_text VARCHAR(100),
    AQI NUMERIC(6,2),
    PM25_value NUMERIC(6,2),
    PM10_value NUMERIC(6,2),
    O3_value NUMERIC(6,2),
    CO_value NUMERIC(6,2),
    NO2_value NUMERIC(6,2),
    SO2_value NUMERIC(6,2),
    temperature_c NUMERIC(6,2),
    humidity NUMERIC(6,2),
    wind_speed NUMERIC(6,2),
    wind_direction NUMERIC(6,2),
    rainfall NUMERIC(6,2),
    solar_radiation NUMERIC(6,2),
    PRIMARY KEY (date_time, location_id, main_pollution_code)
);
