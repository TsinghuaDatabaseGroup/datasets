CREATE TABLE building_metadata (site_id INT, building_id INT, primary_use VARCHAR, square_feet DOUBLE PRECISION, year_built INT, floor_count DOUBLE PRECISION);
CREATE TABLE test (row_id INT, building_id INT, meter INT, timestamp TIMESTAMP);
CREATE TABLE train (building_id INT, meter INT, timestamp TIMESTAMP, meter_reading DOUBLE PRECISION);
CREATE TABLE weather_test (site_id INT, timestamp TIMESTAMP, air_temperature DOUBLE PRECISION, cloud_coverage DOUBLE PRECISION, dew_temperature DOUBLE PRECISION, precip_depth_1_hr DOUBLE PRECISION, sea_level_pressure DOUBLE PRECISION, wind_direction DOUBLE PRECISION, wind_speed DOUBLE PRECISION);
CREATE TABLE weather_train (site_id INT, timestamp TIMESTAMP, air_temperature DOUBLE PRECISION, cloud_coverage DOUBLE PRECISION, dew_temperature DOUBLE PRECISION, precip_depth_1_hr DOUBLE PRECISION, sea_level_pressure DOUBLE PRECISION, wind_direction DOUBLE PRECISION, wind_speed DOUBLE PRECISION);
