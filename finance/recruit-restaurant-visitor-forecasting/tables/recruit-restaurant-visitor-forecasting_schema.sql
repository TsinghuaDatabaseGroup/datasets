CREATE TABLE air_reserve (air_store_id VARCHAR, visit_datetime TIMESTAMP, reserve_datetime TIMESTAMP, reserve_visitors INT);
CREATE TABLE air_store_info (air_store_id VARCHAR, air_genre_name VARCHAR, air_area_name VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);
CREATE TABLE air_visit_data (air_store_id VARCHAR, visit_date TIMESTAMP, visitors INT);
CREATE TABLE date_info (calendar_date TIMESTAMP, day_of_week VARCHAR, holiday_flg INT);
CREATE TABLE hpg_reserve (hpg_store_id VARCHAR, visit_datetime TIMESTAMP, reserve_datetime TIMESTAMP, reserve_visitors INT);
CREATE TABLE hpg_store_info (hpg_store_id VARCHAR, hpg_genre_name VARCHAR, hpg_area_name VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);
CREATE TABLE store_id_relation (air_store_id VARCHAR, hpg_store_id VARCHAR);
