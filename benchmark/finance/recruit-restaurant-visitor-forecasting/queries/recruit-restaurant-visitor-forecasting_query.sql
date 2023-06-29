# start sql code
# output table name: sql_table
select * from 
(
select
    `air_store_id` as air_store_id_1,
    `visit_date` as air_visit_data_visit_date_original_0,
    `air_store_id` as air_visit_data_air_store_id_original_1,
    `visitors` as air_visit_data_visitors_original_2,
    dayofweek(timestamp(`visit_date`)) as air_visit_data_visit_date_dayofweek_9,
    case when 1 < dayofweek(timestamp(`visit_date`)) and dayofweek(timestamp(`visit_date`)) < 7 then 1 else 0 end as air_visit_data_visit_date_isweekday_10,
    hour(timestamp(`visit_date`)) as air_visit_data_visit_date_hourofday_14
from
    `air_visit_data`
    )
as out0
last join
(
select
    `air_visit_data`.`air_store_id` as air_store_id_4,
    `air_store_info_air_store_id`.`air_area_name` as air_store_info_air_area_name_multi_direct_3,
    `air_store_info_air_store_id`.`air_genre_name` as air_store_info_air_genre_name_multi_direct_4,
    `air_store_info_air_store_id`.`latitude` as air_store_info_latitude_multi_direct_5,
    `store_id_relation_air_store_id`.`hpg_store_id` as store_id_relation_hpg_store_id_multi_direct_6
from
    `air_visit_data`
    last join `air_store_info` as `air_store_info_air_store_id` on `air_visit_data`.`air_store_id` = `air_store_info_air_store_id`.`air_store_id`
    last join `store_id_relation` as `store_id_relation_air_store_id` on `air_visit_data`.`air_store_id` = `store_id_relation_air_store_id`.`air_store_id`)
as out1
on out0.air_store_id_1 = out1.air_store_id_4
last join
(
select
    `air_store_id` as air_store_id_8,
    fz_topn_frequency(`reserve_visitors`, 3) over air_reserve_air_store_id_visit_datetime_0s_64d_100 as air_reserve_reserve_visitors_multi_top3frequency_7,
    distinct_count(`reserve_visitors`) over air_reserve_air_store_id_visit_datetime_0s_64d_100 as air_reserve_reserve_visitors_multi_unique_count_8
from
    (select `air_store_id` as `air_store_id`, `visit_date` as `visit_datetime`, timestamp('2019-07-18 09:20:20') as `reserve_datetime`, int(0) as `reserve_visitors` from `air_visit_data`)
    window air_reserve_air_store_id_visit_datetime_0s_64d_100 as (
UNION `air_reserve` partition by `air_store_id` order by `visit_datetime` rows_range between 64d open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out2
on out0.air_store_id_1 = out2.air_store_id_8
;