# start sql code
# output table name: sql_table
select * from 
(
select
    `date` as date_1,
    `date` as sales_train_date_original_0,
    `item_cnt_day` as sales_train_item_cnt_day_original_2,
    `date_block_num` as sales_train_date_block_num_original_8,
    `item_id` as sales_train_item_id_original_9,
    `item_price` as sales_train_item_price_original_10,
    case when !isnull(at(`date_block_num`, 0)) over sales_train_item_id_date_0s_2d_100 then count_where(`date_block_num`, `date_block_num` = at(`date_block_num`, 0)) over sales_train_item_id_date_0s_2d_100 else null end as sales_train_date_block_num_window_count_11,
    case when !isnull(at(`date_block_num`, 0)) over sales_train_item_id_date_0s_5d_200 then count_where(`date_block_num`, `date_block_num` = at(`date_block_num`, 0)) over sales_train_item_id_date_0s_5d_200 else null end as sales_train_date_block_num_window_count_12,
    case when !isnull(at(`item_id`, 0)) over sales_train_date_block_num_date_0s_64d_200 then count_where(`item_id`, `item_id` = at(`item_id`, 0)) over sales_train_date_block_num_date_0s_64d_200 else null end as sales_train_item_id_window_count_13,
    sum(`item_price`) over sales_train_item_id_date_0s_2d_200 as sales_train_item_price_window_sum_14,
    `item_id` as sales_train_item_id_combine_15,
    `item_id` as sales_train_item_id_combine_16,
    sum(`item_price`) over sales_train_item_id_date_0s_5d_200 as sales_train_item_price_window_sum_17,
    min(`item_price`) over sales_train_item_id_date_0s_2d_200 as sales_train_item_price_window_min_18,
    min(`item_price`) over sales_train_item_id_date_0s_5d_200 as sales_train_item_price_window_min_19,
    min(`item_price`) over sales_train_date_block_num_date_0s_64d_200 as sales_train_item_price_window_min_20,
    avg(`item_price`) over sales_train_date_block_num_date_0s_32d_100 as sales_train_item_price_window_avg_21,
    avg(`item_price`) over sales_train_item_id_date_0s_32d_100 as sales_train_item_price_window_avg_22,
    distinct_count(`date_block_num`) over sales_train_item_id_date_0s_64d_200 as sales_train_date_block_num_window_unique_count_23,
    distinct_count(`date_block_num`) over sales_train_item_id_date_0_10_ as sales_train_date_block_num_window_unique_count_24,
    case when 1 < dayofweek(timestamp(`date`)) and dayofweek(timestamp(`date`)) < 7 then 1 else 0 end as sales_train_date_isweekday_25,
    hour(timestamp(`date`)) as sales_train_date_hourofday_26,
    fz_top1_ratio(`date_block_num`) over sales_train_item_id_date_0s_32d_200 as sales_train_date_block_num_window_top1_ratio_27,
    fz_top1_ratio(`item_id`) over sales_train_date_block_num_date_0s_32d_200 as sales_train_item_id_window_top1_ratio_28
from
    `sales_train`
    window sales_train_item_id_date_0s_2d_100 as (partition by `item_id` order by `date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    sales_train_item_id_date_0s_5d_200 as (partition by `item_id` order by `date` rows_range between 5d open preceding and 0s preceding MAXSIZE 200),
    sales_train_date_block_num_date_0s_64d_200 as (partition by `date_block_num` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    sales_train_item_id_date_0s_2d_200 as (partition by `item_id` order by `date` rows_range between 2d open preceding and 0s preceding MAXSIZE 200),
    sales_train_date_block_num_date_0s_32d_100 as (partition by `date_block_num` order by `date` rows_range between 32d open preceding and 0s preceding MAXSIZE 100),
    sales_train_item_id_date_0s_32d_100 as (partition by `item_id` order by `date` rows_range between 32d open preceding and 0s preceding MAXSIZE 100),
    sales_train_item_id_date_0s_64d_200 as (partition by `item_id` order by `date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    sales_train_item_id_date_0_10_ as (partition by `item_id` order by `date` rows between 10 open preceding and 0 preceding),
    sales_train_item_id_date_0s_32d_200 as (partition by `item_id` order by `date` rows_range between 32d open preceding and 0s preceding MAXSIZE 200),
    sales_train_date_block_num_date_0s_32d_200 as (partition by `date_block_num` order by `date` rows_range between 32d open preceding and 0s preceding MAXSIZE 200))
as out0
last join
(
select
    `sales_train`.`date` as date_3,
    `items_item_id`.`item_category_id` as items_item_category_id_multi_direct_3,
    `items_item_id`.`item_name` as items_item_name_multi_direct_4,
    `shops_shop_id`.`shop_name` as shops_shop_name_multi_direct_5,
    `test_item_id`.`ID` as test_ID_multi_direct_6,
    `test_item_id`.`shop_id` as test_shop_id_multi_direct_7
from
    `sales_train`
    last join `items` as `items_item_id` on `sales_train`.`item_id` = `items_item_id`.`item_id`
    last join `shops` as `shops_shop_id` on `sales_train`.`shop_id` = `shops_shop_id`.`shop_id`
    last join `test` as `test_item_id` on `sales_train`.`item_id` = `test_item_id`.`item_id`)
as out1
on out0.date_1 = out1.date_3
;