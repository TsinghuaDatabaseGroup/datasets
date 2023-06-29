# start sql code
# output table name: sql_table
select * from 
(
select
    `date` as date_1,
    `date` as train_date_original_0,
    `units` as train_units_original_2,
    `item_nbr` as train_item_nbr_original_19,
    `store_nbr` as train_store_nbr_original_20,
    `item_nbr` as train_item_nbr_combine_21,
    `store_nbr` as train_store_nbr_combine_21,
    `item_nbr` as train_item_nbr_combine_22,
    `store_nbr` as train_store_nbr_combine_22,
    `item_nbr` as train_item_nbr_combine_23,
    `store_nbr` as train_store_nbr_combine_23,
    `item_nbr` as train_item_nbr_combine_24,
    `store_nbr` as train_store_nbr_combine_24
from
    `train`
    )
as out0
last join
(
select
    `train`.`date` as date_3,
    `key_store_nbr`.`station_nbr` as key_station_nbr_multi_direct_3,
    `weather_date`.`avgspeed` as weather_avgspeed_multi_direct_4,
    `weather_date`.`codesum` as weather_codesum_multi_direct_5,
    `weather_date`.`cool` as weather_cool_multi_direct_6,
    `weather_date`.`depart` as weather_depart_multi_direct_7,
    `weather_date`.`dewpoint` as weather_dewpoint_multi_direct_8,
    `weather_date`.`heat` as weather_heat_multi_direct_9,
    `weather_date`.`preciptotal` as weather_preciptotal_multi_direct_10,
    `weather_date`.`resultdir` as weather_resultdir_multi_direct_11,
    `weather_date`.`resultspeed` as weather_resultspeed_multi_direct_12,
    `weather_date`.`sealevel` as weather_sealevel_multi_direct_13,
    `weather_date`.`stnpressure` as weather_stnpressure_multi_direct_14,
    `weather_date`.`tavg` as weather_tavg_multi_direct_15,
    `weather_date`.`tmax` as weather_tmax_multi_direct_16,
    `weather_date`.`tmin` as weather_tmin_multi_direct_17,
    `weather_date`.`wetbulb` as weather_wetbulb_multi_direct_18
from
    `train`
    last join `key` as `key_store_nbr` on `train`.`store_nbr` = `key_store_nbr`.`store_nbr`
    last join `weather` as `weather_date` on `train`.`date` = `weather_date`.`date`)
as out1
on out0.date_1 = out1.date_3
;