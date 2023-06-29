# start sql code
# output table name: sql_table

select
    `Id` as Id_1,
    `Date` as train_Date_original_0,
    `Id` as train_Id_original_1,
    `ConfirmedCases` as train_ConfirmedCases_original_2,
    `Country_Region` as train_Country_Region_original_3,
    `Fatalities` as train_Fatalities_original_4,
    `Province_State` as train_Province_State_original_5,
    log(`Fatalities`) as train_Fatalities_log_6,
    min(`Fatalities`) over train_Country_Region_Date_0s_2d_200 as train_Fatalities_window_min_7,
    min(`Fatalities`) over train_Province_State_Date_0s_2d_100 as train_Fatalities_window_min_8,
    sum(`Fatalities`) over train_Country_Region_Date_0s_2d_100 as train_Fatalities_window_sum_9,
    min(`Fatalities`) over train_Country_Region_Date_0s_7d_100 as train_Fatalities_window_min_10,
    min(`Fatalities`) over train_Province_State_Date_0s_5d_200 as train_Fatalities_window_min_11,
    sum(`Fatalities`) over train_Country_Region_Date_0s_7d_100 as train_Fatalities_window_sum_12,
    avg(`Fatalities`) over train_Country_Region_Date_0s_5d_100 as train_Fatalities_window_avg_13,
    avg(`Fatalities`) over train_Country_Region_Date_0s_2d_200 as train_Fatalities_window_avg_14,
    fz_top1_ratio(`Country_Region`) over train_Province_State_Date_0s_64d_100 as train_Country_Region_window_top1_ratio_15,
    max(`Fatalities`) over train_Country_Region_Date_0_10_ as train_Fatalities_window_max_16,
    case when !isnull(at(`Country_Region`, 0)) over train_Province_State_Date_0s_64d_200 then count_where(`Country_Region`, `Country_Region` = at(`Country_Region`, 0)) over train_Province_State_Date_0s_64d_200 else null end as train_Country_Region_window_count_17,
    case when !isnull(at(`Country_Region`, 0)) over train_Province_State_Date_0s_14d_200 then count_where(`Country_Region`, `Country_Region` = at(`Country_Region`, 0)) over train_Province_State_Date_0s_14d_200 else null end as train_Country_Region_window_count_18,
    case when !isnull(at(`Province_State`, 0)) over train_Country_Region_Date_0_10_ then count_where(`Province_State`, `Province_State` = at(`Province_State`, 0)) over train_Country_Region_Date_0_10_ else null end as train_Province_State_window_count_19,
    case when !isnull(at(`Province_State`, 0)) over train_Country_Region_Date_0s_2d_200 then count_where(`Province_State`, `Province_State` = at(`Province_State`, 0)) over train_Country_Region_Date_0s_2d_200 else null end as train_Province_State_window_count_20,
    fz_top1_ratio(`Province_State`) over train_Country_Region_Date_0_10_ as train_Province_State_window_top1_ratio_21,
    distinct_count(`Province_State`) over train_Country_Region_Date_0_10_ as train_Province_State_window_unique_count_22,
    max(`Fatalities`) over train_Country_Region_Date_0s_32d_200 as train_Fatalities_window_max_23
from
    `train`
    window train_Country_Region_Date_0s_2d_200 as (partition by `Country_Region` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 200),
    train_Province_State_Date_0s_2d_100 as (partition by `Province_State` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    train_Country_Region_Date_0s_2d_100 as (partition by `Country_Region` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    train_Country_Region_Date_0s_7d_100 as (partition by `Country_Region` order by `Date` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    train_Province_State_Date_0s_5d_200 as (partition by `Province_State` order by `Date` rows_range between 5d open preceding and 0s preceding MAXSIZE 200),
    train_Country_Region_Date_0s_5d_100 as (partition by `Country_Region` order by `Date` rows_range between 5d open preceding and 0s preceding MAXSIZE 100),
    train_Province_State_Date_0s_64d_100 as (partition by `Province_State` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    train_Country_Region_Date_0_10_ as (partition by `Country_Region` order by `Date` rows between 10 open preceding and 0 preceding),
    train_Province_State_Date_0s_64d_200 as (partition by `Province_State` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    train_Province_State_Date_0s_14d_200 as (partition by `Province_State` order by `Date` rows_range between 14d open preceding and 0s preceding MAXSIZE 200),
    train_Country_Region_Date_0s_32d_200 as (partition by `Country_Region` order by `Date` rows_range between 32d open preceding and 0s preceding MAXSIZE 200);