# start sql code
# output table name: sql_table

select
    `id` as id_1,
    `Date` as impermium_verification_labels_Date_original_0,
    `id` as impermium_verification_labels_id_original_1,
    `Insult` as impermium_verification_labels_Insult_original_2,
    `Comment` as impermium_verification_labels_Comment_original_3,
    `Usage` as impermium_verification_labels_Usage_original_4,
    distinct_count(`Comment`) over impermium_verification_labels_Usage_Date_0s_10h_200 as impermium_verification_labels_Comment_window_unique_count_5,
    fz_top1_ratio(`Comment`) over impermium_verification_labels_Usage_Date_0s_10h_200 as impermium_verification_labels_Comment_window_top1_ratio_6,
    fz_top1_ratio(`Comment`) over impermium_verification_labels_Usage_Date_0s_2d_200 as impermium_verification_labels_Comment_window_top1_ratio_7,
    distinct_count(`Comment`) over impermium_verification_labels_Usage_Date_0s_2d_200 as impermium_verification_labels_Comment_window_unique_count_8,
    `Comment` as impermium_verification_labels_Comment_combine_9,
    `Usage` as impermium_verification_labels_Usage_combine_9,
    case when !isnull(at(`Comment`, 0)) over impermium_verification_labels_Usage_Date_0s_32d_100 then count_where(`Comment`, `Comment` = at(`Comment`, 0)) over impermium_verification_labels_Usage_Date_0s_32d_100 else null end as impermium_verification_labels_Comment_window_count_10,
    dayofweek(timestamp(`Date`)) as impermium_verification_labels_Date_dayofweek_11,
    case when 1 < dayofweek(timestamp(`Date`)) and dayofweek(timestamp(`Date`)) < 7 then 1 else 0 end as impermium_verification_labels_Date_isweekday_12,
    hour(timestamp(`Date`)) as impermium_verification_labels_Date_hourofday_13,
    case when !isnull(at(`Usage`, 0)) over impermium_verification_labels_Comment_Date_0s_32d_200 then count_where(`Usage`, `Usage` = at(`Usage`, 0)) over impermium_verification_labels_Comment_Date_0s_32d_200 else null end as impermium_verification_labels_Usage_window_count_14,
    distinct_count(`Usage`) over impermium_verification_labels_Comment_Date_0s_5d_100 as impermium_verification_labels_Usage_window_unique_count_15,
    distinct_count(`Usage`) over impermium_verification_labels_Comment_Date_0s_64d_200 as impermium_verification_labels_Usage_window_unique_count_16,
    case when !isnull(at(`Usage`, 0)) over impermium_verification_labels_Comment_Date_0s_10h_100 then count_where(`Usage`, `Usage` = at(`Usage`, 0)) over impermium_verification_labels_Comment_Date_0s_10h_100 else null end as impermium_verification_labels_Usage_window_count_17,
    fz_top1_ratio(`Usage`) over impermium_verification_labels_Comment_Date_0s_32d_200 as impermium_verification_labels_Usage_window_top1_ratio_18,
    fz_top1_ratio(`Usage`) over impermium_verification_labels_Comment_Date_0s_5d_100 as impermium_verification_labels_Usage_window_top1_ratio_19
from
    `impermium_verification_labels`
    window impermium_verification_labels_Usage_Date_0s_10h_200 as (partition by `Usage` order by `Date` rows_range between 10h open preceding and 0s preceding MAXSIZE 200),
    impermium_verification_labels_Usage_Date_0s_2d_200 as (partition by `Usage` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 200),
    impermium_verification_labels_Usage_Date_0s_32d_100 as (partition by `Usage` order by `Date` rows_range between 32d open preceding and 0s preceding MAXSIZE 100),
    impermium_verification_labels_Comment_Date_0s_32d_200 as (partition by `Comment` order by `Date` rows_range between 32d open preceding and 0s preceding MAXSIZE 200),
    impermium_verification_labels_Comment_Date_0s_5d_100 as (partition by `Comment` order by `Date` rows_range between 5d open preceding and 0s preceding MAXSIZE 100),
    impermium_verification_labels_Comment_Date_0s_64d_200 as (partition by `Comment` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    impermium_verification_labels_Comment_Date_0s_10h_100 as (partition by `Comment` order by `Date` rows_range between 10h open preceding and 0s preceding MAXSIZE 100);