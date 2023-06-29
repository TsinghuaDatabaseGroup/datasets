# start sql code
# output table name: sql_table
select * from 
(
select
    `store_nbr` as store_nbr_1,
    `date` as main_date_original_0,
    `store_nbr` as main_store_nbr_original_1,
    `sales` as main_sales_original_2,
    `family` as main_family_original_9,
    `onpromotion` as main_onpromotion_original_10,
    `family` as main_family_combine_11,
    `store_nbr` as main_store_nbr_combine_11,
    `family` as main_family_combine_12,
    `family` as main_family_combine_13,
    `family` as main_family_combine_14,
    dayofweek(timestamp(`date`)) as main_date_dayofweek_15,
    case when 1 < dayofweek(timestamp(`date`)) and dayofweek(timestamp(`date`)) < 7 then 1 else 0 end as main_date_isweekday_16,
    fz_top1_ratio(`onpromotion`) over main_store_nbr_date_0s_2d_200 as main_onpromotion_window_top1_ratio_17,
    distinct_count(`onpromotion`) over main_store_nbr_date_0s_64d_100 as main_onpromotion_window_unique_count_18,
    distinct_count(`onpromotion`) over main_store_nbr_date_0s_2d_200 as main_onpromotion_window_unique_count_19,
    distinct_count(`store_nbr`) over main_onpromotion_date_0_10_ as main_store_nbr_window_unique_count_20,
    fz_top1_ratio(`store_nbr`) over main_onpromotion_date_0_10_ as main_store_nbr_window_top1_ratio_21,
    distinct_count(`family`) over main_onpromotion_date_0s_64d_200 as main_family_window_unique_count_22,
    distinct_count(`family`) over main_onpromotion_date_0s_14d_200 as main_family_window_unique_count_23,
    distinct_count(`store_nbr`) over main_onpromotion_date_0s_64d_100 as main_store_nbr_window_unique_count_24,
    fz_top1_ratio(`store_nbr`) over main_onpromotion_date_0s_64d_200 as main_store_nbr_window_top1_ratio_25,
    fz_top1_ratio(`family`) over main_onpromotion_date_0s_7d_200 as main_family_window_top1_ratio_26,
    fz_top1_ratio(`family`) over main_onpromotion_date_0s_64d_200 as main_family_window_top1_ratio_27,
    case when !isnull(at(`onpromotion`, 0)) over main_store_nbr_date_0s_7d_200 then count_where(`onpromotion`, `onpromotion` = at(`onpromotion`, 0)) over main_store_nbr_date_0s_7d_200 else null end as main_onpromotion_window_count_28
from
    `main`
    window main_store_nbr_date_0s_2d_200 as (partition by `store_nbr` order by `date` rows_range between 2d open preceding and 0s preceding MAXSIZE 200),
    main_store_nbr_date_0s_64d_100 as (partition by `store_nbr` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    main_onpromotion_date_0_10_ as (partition by `onpromotion` order by `date` rows between 10 open preceding and 0 preceding),
    main_onpromotion_date_0s_64d_200 as (partition by `onpromotion` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    main_onpromotion_date_0s_14d_200 as (partition by `onpromotion` order by `date` rows_range between 14d open preceding and 0s preceding MAXSIZE 200),
    main_onpromotion_date_0s_64d_100 as (partition by `onpromotion` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    main_onpromotion_date_0s_7d_200 as (partition by `onpromotion` order by `date` rows_range between 7d open preceding and 0s preceding MAXSIZE 200),
    main_store_nbr_date_0s_7d_200 as (partition by `store_nbr` order by `date` rows_range between 7d open preceding and 0s preceding MAXSIZE 200))
as out0
last join
(
select
    `main`.`store_nbr` as store_nbr_4,
    `stores_store_nbr`.`city` as stores_city_multi_direct_3,
    `stores_store_nbr`.`cluster` as stores_cluster_multi_direct_4,
    `stores_store_nbr`.`state` as stores_state_multi_direct_5,
    `stores_store_nbr`.`type` as stores_type_multi_direct_6
from
    `main`
    last join `stores` as `stores_store_nbr` on `main`.`store_nbr` = `stores_store_nbr`.`store_nbr`)
as out1
on out0.store_nbr_1 = out1.store_nbr_4
last join
(
select
    `store_nbr` as store_nbr_8,
    fz_topn_frequency(`transactions`, 3) over transactions_store_nbr_date_0s_64d_100 as transactions_transactions_multi_top3frequency_7,
    distinct_count(`transactions`) over transactions_store_nbr_date_0s_64d_100 as transactions_transactions_multi_unique_count_8
from
    (select `date` as `date`, `store_nbr` as `store_nbr`, int(0) as `transactions` from `main`)
    window transactions_store_nbr_date_0s_64d_100 as (
UNION `transactions` partition by `store_nbr` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out2
on out0.store_nbr_1 = out2.store_nbr_8
;