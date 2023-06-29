# start sql code
# output table name: sql_table
select * from 
(
select
    `row_id` as row_id_1,
    `timestamp` as test_timestamp_original_0,
    `row_id` as test_row_id_original_1,
    `meter` as test_meter_original_2,
    `building_id` as test_building_id_original_10,
    case when 1 < dayofweek(timestamp(`timestamp`)) and dayofweek(timestamp(`timestamp`)) < 7 then 1 else 0 end as test_timestamp_isweekday_13,
    dayofweek(timestamp(`timestamp`)) as test_timestamp_dayofweek_16,
    hour(timestamp(`timestamp`)) as test_timestamp_hourofday_18,
    `building_id` as test_building_id_combine_19,
    `building_id` as test_building_id_combine_20
from
    `test`
    )
as out0
last join
(
select
    `row_id` as row_id_4,
    avg(`meter_reading`) over train_building_id_timestamp_0s_64d_100 as train_meter_reading_multi_avg_3,
    distinct_count(`meter`) over train_building_id_timestamp_0s_64d_100 as train_meter_multi_unique_count_9
from
    (select `building_id` as `building_id`, int(0) as `meter`, `timestamp` as `timestamp`, double(0) as `meter_reading`, row_id from `test`)
    window train_building_id_timestamp_0s_64d_100 as (
UNION (select `building_id`, `meter`, `timestamp`, `meter_reading`, int(0) as row_id from `train`) partition by `building_id` order by `timestamp` rows_range between 64d open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out1
on out0.row_id_1 = out1.row_id_4
last join
(
select
    `test`.`row_id` as row_id_5,
    `building_metadata_building_id`.`floor_count` as building_metadata_floor_count_multi_direct_4,
    `building_metadata_building_id`.`primary_use` as building_metadata_primary_use_multi_direct_5,
    `building_metadata_building_id`.`site_id` as building_metadata_site_id_multi_direct_6,
    `building_metadata_building_id`.`square_feet` as building_metadata_square_feet_multi_direct_7,
    `building_metadata_building_id`.`year_built` as building_metadata_year_built_multi_direct_8
from
    `test`
    last join `building_metadata` as `building_metadata_building_id` on `test`.`building_id` = `building_metadata_building_id`.`building_id`)
as out2
on out0.row_id_1 = out2.row_id_5
;