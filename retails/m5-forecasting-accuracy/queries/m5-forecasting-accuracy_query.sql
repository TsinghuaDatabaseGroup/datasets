# start sql code
# output table name: sql_table
select * from 
(
select
    `d` as d_1,
    `date` as calendar_date_original_0,
    `d` as calendar_d_original_1,
    `event_name_1` as calendar_event_name_1_original_5,
    `event_name_2` as calendar_event_name_2_original_6,
    `event_type_1` as calendar_event_type_1_original_7,
    `event_type_2` as calendar_event_type_2_original_8,
    `month` as calendar_month_original_9,
    `snap_CA` as calendar_snap_CA_original_10,
    `snap_TX` as calendar_snap_TX_original_11,
    `snap_WI` as calendar_snap_WI_original_12,
    `wday` as calendar_wday_original_13,
    `wm_yr_wk` as calendar_wm_yr_wk_original_14,
    `year` as calendar_year_original_15,
    `snap_CA` as calendar_snap_CA_combine_16,
    `month` as calendar_month_combine_16,
    `event_type_2` as calendar_event_type_2_combine_17,
    `month` as calendar_month_combine_17,
    `month` as calendar_month_combine_18,
    `snap_CA` as calendar_snap_CA_combine_19,
    case when !isnull(at(`month`, 0)) over calendar_wday_date_0s_32d_200 then count_where(`month`, `month` = at(`month`, 0)) over calendar_wday_date_0s_32d_200 else null end as calendar_month_window_count_20,
    fz_top1_ratio(`snap_TX`) over calendar_event_name_2_date_0s_64d_100 as calendar_snap_TX_window_top1_ratio_21,
    fz_top1_ratio(`snap_TX`) over calendar_event_name_2_date_0s_14d_100 as calendar_snap_TX_window_top1_ratio_22,
    fz_top1_ratio(`snap_WI`) over calendar_event_name_2_date_0s_64d_100 as calendar_snap_WI_window_top1_ratio_23,
    fz_top1_ratio(`event_type_1`) over calendar_event_name_1_date_0s_64d_100 as calendar_event_type_1_window_top1_ratio_24,
    `snap_CA` as calendar_snap_CA_combine_25,
    `month` as calendar_month_combine_25,
    distinct_count(`event_type_1`) over calendar_snap_CA_date_0s_64d_200 as calendar_event_type_1_window_unique_count_26,
    fz_top1_ratio(`event_type_1`) over calendar_snap_CA_date_0s_64d_100 as calendar_event_type_1_window_top1_ratio_27,
    distinct_count(`event_type_1`) over calendar_snap_CA_date_0s_14d_100 as calendar_event_type_1_window_unique_count_28,
    fz_top1_ratio(`event_type_1`) over calendar_snap_CA_date_0s_14d_100 as calendar_event_type_1_window_top1_ratio_29,
    `snap_CA` as calendar_snap_CA_combine_30,
    `snap_TX` as calendar_snap_TX_combine_30,
    `wday` as calendar_wday_combine_30,
    case when !isnull(at(`month`, 0)) over calendar_event_name_2_date_0s_64d_100 then count_where(`month`, `month` = at(`month`, 0)) over calendar_event_name_2_date_0s_64d_100 else null end as calendar_month_window_count_31,
    case when !isnull(at(`month`, 0)) over calendar_event_name_2_date_0s_14d_100 then count_where(`month`, `month` = at(`month`, 0)) over calendar_event_name_2_date_0s_14d_100 else null end as calendar_month_window_count_32,
    fz_top1_ratio(`event_type_1`) over calendar_event_name_2_date_0s_64d_100 as calendar_event_type_1_window_top1_ratio_33
from
    `calendar`
    window calendar_wday_date_0s_32d_200 as (partition by `wday` order by `date` rows_range between 32d open preceding and 0s preceding MAXSIZE 200),
    calendar_event_name_2_date_0s_64d_100 as (partition by `event_name_2` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    calendar_event_name_2_date_0s_14d_100 as (partition by `event_name_2` order by `date` rows_range between 14d open preceding and 0s preceding MAXSIZE 100),
    calendar_event_name_1_date_0s_64d_100 as (partition by `event_name_1` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    calendar_snap_CA_date_0s_64d_200 as (partition by `snap_CA` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    calendar_snap_CA_date_0s_64d_100 as (partition by `snap_CA` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    calendar_snap_CA_date_0s_14d_100 as (partition by `snap_CA` order by `date` rows_range between 14d open preceding and 0s preceding MAXSIZE 100))
as out0
last join
(
select
    `calendar`.`d` as d_3,
    `sell_prices_wm_yr_wk`.`sell_price` as sell_prices_sell_price_multi_direct_2,
    `sell_prices_wm_yr_wk`.`item_id` as sell_prices_item_id_multi_direct_3,
    `sell_prices_wm_yr_wk`.`store_id` as sell_prices_store_id_multi_direct_4
from
    `calendar`
    last join `sell_prices` as `sell_prices_wm_yr_wk` on `calendar`.`wm_yr_wk` = `sell_prices_wm_yr_wk`.`wm_yr_wk`)
as out1
on out0.d_1 = out1.d_3
;