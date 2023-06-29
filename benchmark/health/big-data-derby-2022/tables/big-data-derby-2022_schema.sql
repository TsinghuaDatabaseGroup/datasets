CREATE TABLE nyra_race_table (track_id VARCHAR, race_date TIMESTAMP, race_number INT, distance_id INT, course_type VARCHAR, track_condition VARCHAR, run_up_distance INT, race_type VARCHAR, purse INT, post_time INT);
CREATE TABLE nyra_start_table (track_id VARCHAR, race_date TIMESTAMP, race_number INT, program_number VARCHAR, weight_carried INT, jockey VARCHAR, odds INT);
CREATE TABLE nyra_tracking_table (track_id VARCHAR, race_date TIMESTAMP, race_number INT, program_number VARCHAR, trakus_index INT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);
