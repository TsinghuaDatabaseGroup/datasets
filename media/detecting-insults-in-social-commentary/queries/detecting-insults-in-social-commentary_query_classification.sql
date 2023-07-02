# start sql code
# output table name: sql_table

select
    `id` as id_1,
    `Date` as impermium_verification_labels_Date_original_0,
    `id` as impermium_verification_labels_id_original_1,
    `Insult` as impermium_verification_labels_Insult_original_2,
    `Comment` as impermium_verification_labels_Comment_original_3,
    `Usage` as impermium_verification_labels_Usage_original_4,
    distinct_count(`Comment`) over impermium_verification_labels_Usage_Date_0s_5h_200 as impermium_verification_labels_Comment_window_unique_count_5,
    fz_top1_ratio(`Comment`) over impermium_verification_labels_Usage_Date_0s_1h_100 as impermium_verification_labels_Comment_window_top1_ratio_6,
    distinct_count(`Comment`) over impermium_verification_labels_Usage_Date_0s_2h_200 as impermium_verification_labels_Comment_window_unique_count_7,
    `Usage` as impermium_verification_labels_Usage_combine_8,
    `Comment` as impermium_verification_labels_Comment_combine_8,
    fz_top1_ratio(`Comment`) over impermium_verification_labels_Usage_Date_0s_2d_100 as impermium_verification_labels_Comment_window_top1_ratio_9,
    fz_top1_ratio(`Usage`) over impermium_verification_labels_Comment_Date_0s_64d_200 as impermium_verification_labels_Usage_window_top1_ratio_10,
    fz_top1_ratio(`Usage`) over impermium_verification_labels_Comment_Date_0s_7d_200 as impermium_verification_labels_Usage_window_top1_ratio_11,
    dayofweek(timestamp(`Date`)) as impermium_verification_labels_Date_dayofweek_12,
    distinct_count(`Usage`) over impermium_verification_labels_Comment_Date_0s_64d_200 as impermium_verification_labels_Usage_window_unique_count_13
from
    `impermium_verification_labels`
    window impermium_verification_labels_Usage_Date_0s_5h_200 as (partition by `Usage` order by `Date` rows_range between 5h open preceding and 0s preceding MAXSIZE 200),
    impermium_verification_labels_Usage_Date_0s_1h_100 as (partition by `Usage` order by `Date` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    impermium_verification_labels_Usage_Date_0s_2h_200 as (partition by `Usage` order by `Date` rows_range between 2h open preceding and 0s preceding MAXSIZE 200),
    impermium_verification_labels_Usage_Date_0s_2d_100 as (partition by `Usage` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    impermium_verification_labels_Comment_Date_0s_64d_200 as (partition by `Comment` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    impermium_verification_labels_Comment_Date_0s_7d_200 as (partition by `Comment` order by `Date` rows_range between 7d open preceding and 0s preceding MAXSIZE 200);