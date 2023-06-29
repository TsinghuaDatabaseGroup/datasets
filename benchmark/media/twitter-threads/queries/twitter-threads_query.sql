# start sql code
# output table name: sql_table

select
    `id` as id_1,
    `timestamp` as five_ten_timestamp_original_0,
    `id` as five_ten_id_original_1,
    `likes` as five_ten_likes_original_2,
    `replies` as five_ten_replies_original_3,
    `retweets` as five_ten_retweets_original_4,
    `text` as five_ten_text_original_5,
    `thread_number` as five_ten_thread_number_original_6,
    case when !isnull(at(`retweets`, 0)) over five_ten_replies_timestamp_0s_64d_200 then count_where(`retweets`, `retweets` = at(`retweets`, 0)) over five_ten_replies_timestamp_0s_64d_200 else null end as five_ten_retweets_window_count_7,
    case when !isnull(at(`retweets`, 0)) over five_ten_replies_timestamp_0s_14d_200 then count_where(`retweets`, `retweets` = at(`retweets`, 0)) over five_ten_replies_timestamp_0s_14d_200 else null end as five_ten_retweets_window_count_8,
    case when !isnull(at(`replies`, 0)) over five_ten_retweets_timestamp_0s_32d_200 then count_where(`replies`, `replies` = at(`replies`, 0)) over five_ten_retweets_timestamp_0s_32d_200 else null end as five_ten_replies_window_count_9,
    case when !isnull(at(`replies`, 0)) over five_ten_retweets_timestamp_0_10_ then count_where(`replies`, `replies` = at(`replies`, 0)) over five_ten_retweets_timestamp_0_10_ else null end as five_ten_replies_window_count_10,
    case when !isnull(at(`replies`, 0)) over five_ten_thread_number_timestamp_0s_1h_200 then count_where(`replies`, `replies` = at(`replies`, 0)) over five_ten_thread_number_timestamp_0s_1h_200 else null end as five_ten_replies_window_count_11,
    case when !isnull(at(`replies`, 0)) over five_ten_thread_number_timestamp_0s_2h_100 then count_where(`replies`, `replies` = at(`replies`, 0)) over five_ten_thread_number_timestamp_0s_2h_100 else null end as five_ten_replies_window_count_12,
    fz_top1_ratio(`text`) over five_ten_retweets_timestamp_0s_64d_200 as five_ten_text_window_top1_ratio_13,
    fz_top1_ratio(`text`) over five_ten_retweets_timestamp_0s_14d_200 as five_ten_text_window_top1_ratio_14,
    case when !isnull(at(`retweets`, 0)) over five_ten_thread_number_timestamp_0s_2h_100 then count_where(`retweets`, `retweets` = at(`retweets`, 0)) over five_ten_thread_number_timestamp_0s_2h_100 else null end as five_ten_retweets_window_count_15,
    fz_top1_ratio(`retweets`) over five_ten_replies_timestamp_0_10_ as five_ten_retweets_window_top1_ratio_16,
    fz_top1_ratio(`replies`) over five_ten_retweets_timestamp_0s_32d_200 as five_ten_replies_window_top1_ratio_17,
    fz_top1_ratio(`text`) over five_ten_replies_timestamp_0_10_ as five_ten_text_window_top1_ratio_18,
    fz_top1_ratio(`replies`) over five_ten_thread_number_timestamp_0s_5h_100 as five_ten_replies_window_top1_ratio_19,
    distinct_count(`id`) over five_ten_replies_timestamp_0_10_ as five_ten_id_window_unique_count_20,
    distinct_count(`thread_number`) over five_ten_replies_timestamp_0s_14d_200 as five_ten_thread_number_window_unique_count_21,
    case when !isnull(at(`id`, 0)) over five_ten_retweets_timestamp_0s_32d_100 then count_where(`id`, `id` = at(`id`, 0)) over five_ten_retweets_timestamp_0s_32d_100 else null end as five_ten_id_window_count_22,
    fz_top1_ratio(`thread_number`) over five_ten_retweets_timestamp_0s_32d_200 as five_ten_thread_number_window_top1_ratio_23,
    fz_top1_ratio(`replies`) over five_ten_thread_number_timestamp_0s_10h_200 as five_ten_replies_window_top1_ratio_24
from
    `five_ten`
    window five_ten_replies_timestamp_0s_64d_200 as (partition by `replies` order by `timestamp` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    five_ten_replies_timestamp_0s_14d_200 as (partition by `replies` order by `timestamp` rows_range between 14d open preceding and 0s preceding MAXSIZE 200),
    five_ten_retweets_timestamp_0s_32d_200 as (partition by `retweets` order by `timestamp` rows_range between 32d open preceding and 0s preceding MAXSIZE 200),
    five_ten_retweets_timestamp_0_10_ as (partition by `retweets` order by `timestamp` rows between 10 open preceding and 0 preceding),
    five_ten_thread_number_timestamp_0s_1h_200 as (partition by `thread_number` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 200),
    five_ten_thread_number_timestamp_0s_2h_100 as (partition by `thread_number` order by `timestamp` rows_range between 2h open preceding and 0s preceding MAXSIZE 100),
    five_ten_retweets_timestamp_0s_64d_200 as (partition by `retweets` order by `timestamp` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    five_ten_retweets_timestamp_0s_14d_200 as (partition by `retweets` order by `timestamp` rows_range between 14d open preceding and 0s preceding MAXSIZE 200),
    five_ten_replies_timestamp_0_10_ as (partition by `replies` order by `timestamp` rows between 10 open preceding and 0 preceding),
    five_ten_thread_number_timestamp_0s_5h_100 as (partition by `thread_number` order by `timestamp` rows_range between 5h open preceding and 0s preceding MAXSIZE 100),
    five_ten_retweets_timestamp_0s_32d_100 as (partition by `retweets` order by `timestamp` rows_range between 32d open preceding and 0s preceding MAXSIZE 100),
    five_ten_thread_number_timestamp_0s_10h_200 as (partition by `thread_number` order by `timestamp` rows_range between 10h open preceding and 0s preceding MAXSIZE 200);