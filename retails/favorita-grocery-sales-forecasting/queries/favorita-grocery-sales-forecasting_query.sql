# start sql code
# output table name: sql_table
select * from 
(
select
    `id` as id_1,
    `date` as train_date_original_0,
    `id` as train_id_original_1,
    `unit_sales` as train_unit_sales_original_2,
    `item_nbr` as train_item_nbr_original_12,
    `store_nbr` as train_store_nbr_original_13,
    `item_nbr` as train_item_nbr_combine_14,
    `store_nbr` as train_store_nbr_combine_14,
    `item_nbr` as train_item_nbr_combine_15,
    `item_nbr` as train_item_nbr_combine_16,
    `item_nbr` as train_item_nbr_combine_17,
    `store_nbr` as train_store_nbr_combine_18,
    `store_nbr` as train_store_nbr_combine_21,
    `store_nbr` as train_store_nbr_combine_24,
    case when !isnull(at(`store_nbr`, 0)) over train_item_nbr_date_0s_2d_100 then count_where(`store_nbr`, `store_nbr` = at(`store_nbr`, 0)) over train_item_nbr_date_0s_2d_100 else null end as train_store_nbr_window_count_25,
    case when !isnull(at(`store_nbr`, 0)) over train_item_nbr_date_0s_7d_100 then count_where(`store_nbr`, `store_nbr` = at(`store_nbr`, 0)) over train_item_nbr_date_0s_7d_100 else null end as train_store_nbr_window_count_26,
    distinct_count(`item_nbr`) over train_store_nbr_date_0_10_ as train_item_nbr_window_unique_count_28,
    distinct_count(`store_nbr`) over train_item_nbr_date_0_10_ as train_store_nbr_window_unique_count_29,
    case when !isnull(at(`item_nbr`, 0)) over train_store_nbr_date_0s_14d_100 then count_where(`item_nbr`, `item_nbr` = at(`item_nbr`, 0)) over train_store_nbr_date_0s_14d_100 else null end as train_item_nbr_window_count_30,
    fz_top1_ratio(`item_nbr`) over train_store_nbr_date_0_10_ as train_item_nbr_window_top1_ratio_31
from
    `train`
    window train_item_nbr_date_0s_2d_100 as (partition by `item_nbr` order by `date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    train_item_nbr_date_0s_7d_100 as (partition by `item_nbr` order by `date` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    train_store_nbr_date_0_10_ as (partition by `store_nbr` order by `date` rows between 10 open preceding and 0 preceding),
    train_item_nbr_date_0_10_ as (partition by `item_nbr` order by `date` rows between 10 open preceding and 0 preceding),
    train_store_nbr_date_0s_14d_100 as (partition by `store_nbr` order by `date` rows_range between 14d open preceding and 0s preceding MAXSIZE 100))
as out0
last join
(
select
    `train`.`id` as id_4,
    `items_item_nbr`.`class` as items_class_multi_direct_3,
    `items_item_nbr`.`family` as items_family_multi_direct_4,
    `items_item_nbr`.`perishable` as items_perishable_multi_direct_5,
    `stores_store_nbr`.`city` as stores_city_multi_direct_6,
    `stores_store_nbr`.`cluster` as stores_cluster_multi_direct_7,
    `stores_store_nbr`.`state` as stores_state_multi_direct_8,
    `stores_store_nbr`.`type` as stores_type_multi_direct_9
from
    `train`
    last join `items` as `items_item_nbr` on `train`.`item_nbr` = `items_item_nbr`.`item_nbr`
    last join `stores` as `stores_store_nbr` on `train`.`store_nbr` = `stores_store_nbr`.`store_nbr`)
as out1
on out0.id_1 = out1.id_4
last join
(
select
    `id` as id_11,
    fz_topn_frequency(`transactions`, 3) over transactions_store_nbr_date_5s_64d_100 as transactions_transactions_multi_top3frequency_10,
    distinct_count(`transactions`) over transactions_store_nbr_date_5s_64d_100 as transactions_transactions_multi_unique_count_11
from
    (select `date` as `date`, `store_nbr` as `store_nbr`, int(0) as `transactions`, id from `train`)
    window transactions_store_nbr_date_5s_64d_100 as (
UNION (select `date`, `store_nbr`, `transactions`, int(0) as id from `transactions`) partition by `store_nbr` order by `date` rows_range between 64d open preceding and 5s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out2
on out0.id_1 = out2.id_11
;