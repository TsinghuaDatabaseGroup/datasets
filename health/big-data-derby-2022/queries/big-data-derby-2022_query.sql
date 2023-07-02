# start sql code
# output table name: sql_table
select * from 
(
select
    `track_id` as track_id_1,
    `race_date` as nyra_start_table_race_date_original_0,
    `track_id` as nyra_start_table_track_id_original_1,
    `odds` as nyra_start_table_odds_original_2,
    `jockey` as nyra_start_table_jockey_original_30,
    `program_number` as nyra_start_table_program_number_original_31,
    `race_number` as nyra_start_table_race_number_original_32,
    `weight_carried` as nyra_start_table_weight_carried_original_33,
    fz_top1_ratio(`program_number`) over nyra_start_table_jockey_race_date_0s_14d_100 as nyra_start_table_program_number_window_top1_ratio_34,
    `race_number` as nyra_start_table_race_number_combine_35,
    fz_top1_ratio(`weight_carried`) over nyra_start_table_jockey_race_date_0s_7d_200 as nyra_start_table_weight_carried_window_top1_ratio_36,
    fz_top1_ratio(`program_number`) over nyra_start_table_jockey_race_date_0s_7d_100 as nyra_start_table_program_number_window_top1_ratio_37,
    fz_top1_ratio(`race_number`) over nyra_start_table_jockey_race_date_0s_14d_100 as nyra_start_table_race_number_window_top1_ratio_38,
    distinct_count(`jockey`) over nyra_start_table_program_number_race_date_0s_2d_100 as nyra_start_table_jockey_window_unique_count_40,
    fz_top1_ratio(`race_number`) over nyra_start_table_jockey_race_date_0s_7d_200 as nyra_start_table_race_number_window_top1_ratio_41,
    distinct_count(`race_number`) over nyra_start_table_program_number_race_date_0s_14d_100 as nyra_start_table_race_number_window_unique_count_43,
    distinct_count(`race_number`) over nyra_start_table_program_number_race_date_0_10_ as nyra_start_table_race_number_window_unique_count_49
from
    `nyra_start_table`
    window nyra_start_table_jockey_race_date_0s_14d_100 as (partition by `jockey` order by `race_date` rows_range between 14d open preceding and 0s preceding MAXSIZE 100),
    nyra_start_table_jockey_race_date_0s_7d_200 as (partition by `jockey` order by `race_date` rows_range between 7d open preceding and 0s preceding MAXSIZE 200),
    nyra_start_table_jockey_race_date_0s_7d_100 as (partition by `jockey` order by `race_date` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    nyra_start_table_program_number_race_date_0s_2d_100 as (partition by `program_number` order by `race_date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    nyra_start_table_program_number_race_date_0s_14d_100 as (partition by `program_number` order by `race_date` rows_range between 14d open preceding and 0s preceding MAXSIZE 100),
    nyra_start_table_program_number_race_date_0_10_ as (partition by `program_number` order by `race_date` rows between 10 open preceding and 0 preceding))
as out0
last join
(
select
    `track_id_4paradigm` as track_id_4,
    avg(`latitude`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_latitude_multi_avg_3,
    avg(`longitude`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_longitude_multi_avg_4,
    max(`latitude`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_latitude_multi_max_5,
    max(`longitude`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_longitude_multi_max_6,
    min(`latitude`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_latitude_multi_min_7,
    min(`longitude`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_longitude_multi_min_8,
    fz_topn_frequency(`race_number`, 3) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_race_number_multi_top3frequency_17,
    fz_topn_frequency(`track_id`, 3) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_track_id_multi_top3frequency_18,
    fz_topn_frequency(`trakus_index`, 3) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_trakus_index_multi_top3frequency_19,
    distinct_count(`race_number`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_race_number_multi_unique_count_28,
    distinct_count(`track_id`) over nyra_tracking_table_program_number_race_date_0s_64d_100 as nyra_tracking_table_track_id_multi_unique_count_29
from
    (select '' as `track_id`, `race_date` as `race_date`, int(0) as `race_number`, `program_number` as `program_number`, int(0) as `trakus_index`, double(0) as `latitude`, double(0) as `longitude`, track_id as track_id_4paradigm from `nyra_start_table`)
    window nyra_tracking_table_program_number_race_date_0s_64d_100 as (
UNION (select `track_id`, `race_date`, `race_number`, `program_number`, `trakus_index`, `latitude`, `longitude`, '' as track_id_4paradigm from `nyra_tracking_table`) partition by `program_number` order by `race_date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out1
on out0.track_id_1 = out1.track_id_4
last join
(
select
    `track_id_4paradigm` as track_id_10,
    fz_topn_frequency(`course_type`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_course_type_multi_top3frequency_9,
    fz_topn_frequency(`distance_id`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_distance_id_multi_top3frequency_10,
    fz_topn_frequency(`post_time`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_post_time_multi_top3frequency_11,
    fz_topn_frequency(`purse`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_purse_multi_top3frequency_12,
    fz_topn_frequency(`race_type`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_race_type_multi_top3frequency_13,
    fz_topn_frequency(`run_up_distance`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_run_up_distance_multi_top3frequency_14,
    fz_topn_frequency(`track_condition`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_track_condition_multi_top3frequency_15,
    fz_topn_frequency(`track_id`, 3) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_track_id_multi_top3frequency_16,
    distinct_count(`course_type`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_course_type_multi_unique_count_20,
    distinct_count(`distance_id`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_distance_id_multi_unique_count_21,
    distinct_count(`post_time`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_post_time_multi_unique_count_22,
    distinct_count(`purse`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_purse_multi_unique_count_23,
    distinct_count(`race_type`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_race_type_multi_unique_count_24,
    distinct_count(`run_up_distance`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_run_up_distance_multi_unique_count_25,
    distinct_count(`track_condition`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_track_condition_multi_unique_count_26,
    distinct_count(`track_id`) over nyra_race_table_race_number_race_date_0s_64d_100 as nyra_race_table_track_id_multi_unique_count_27
from
    (select '' as `track_id`, `race_date` as `race_date`, `race_number` as `race_number`, int(0) as `distance_id`, '' as `course_type`, '' as `track_condition`, int(0) as `run_up_distance`, '' as `race_type`, int(0) as `purse`, int(0) as `post_time`, track_id as track_id_4paradigm from `nyra_start_table`)
    window nyra_race_table_race_number_race_date_0s_64d_100 as (
UNION (select `track_id`, `race_date`, `race_number`, `distance_id`, `course_type`, `track_condition`, `run_up_distance`, `race_type`, `purse`, `post_time`, '' as track_id_4paradigm from `nyra_race_table`) partition by `race_number` order by `race_date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out2
on out0.track_id_1 = out2.track_id_10
;