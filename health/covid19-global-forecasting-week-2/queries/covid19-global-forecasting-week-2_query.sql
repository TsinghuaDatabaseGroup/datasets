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
    `Province_State` as train_Province_State_combine_6,
    `Country_Region` as train_Country_Region_combine_6,
    min(`Fatalities`) over train_Country_Region_Date_0s_2d_200 as train_Fatalities_window_min_7,
    log(`Fatalities`) as train_Fatalities_log_8,
    min(`Fatalities`) over train_Country_Region_Date_0s_7d_100 as train_Fatalities_window_min_9,
    avg(`Fatalities`) over train_Province_State_Date_0s_2d_200 as train_Fatalities_window_avg_10,
    avg(`Fatalities`) over train_Province_State_Date_0s_5d_200 as train_Fatalities_window_avg_11,
    sum(`Fatalities`) over train_Province_State_Date_0s_2d_100 as train_Fatalities_window_sum_12,
    max(`Fatalities`) over train_Province_State_Date_0_10_ as train_Fatalities_window_max_13,
    sum(`Fatalities`) over train_Province_State_Date_0s_7d_100 as train_Fatalities_window_sum_14,
    sum(`Fatalities`) over train_Country_Region_Date_0s_5d_200 as train_Fatalities_window_sum_15,
    max(`Fatalities`) over train_Country_Region_Date_0_10_ as train_Fatalities_window_max_16,
    max(`Fatalities`) over train_Country_Region_Date_0s_2d_200 as train_Fatalities_window_max_17,
    sum(`Fatalities`) over train_Country_Region_Date_0s_2d_200 as train_Fatalities_window_sum_18,
    min(`Fatalities`) over train_Province_State_Date_0s_32d_200 as train_Fatalities_window_min_19,
    avg(`Fatalities`) over train_Country_Region_Date_0s_32d_200 as train_Fatalities_window_avg_20,
    avg(`Fatalities`) over train_Country_Region_Date_0_10_ as train_Fatalities_window_avg_21,
    fz_top1_ratio(`Province_State`) over train_Country_Region_Date_0s_2d_100 as train_Province_State_window_top1_ratio_22
from
    `train`
    window train_Country_Region_Date_0s_2d_200 as (partition by `Country_Region` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 200),
    train_Country_Region_Date_0s_7d_100 as (partition by `Country_Region` order by `Date` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    train_Province_State_Date_0s_2d_200 as (partition by `Province_State` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 200),
    train_Province_State_Date_0s_5d_200 as (partition by `Province_State` order by `Date` rows_range between 5d open preceding and 0s preceding MAXSIZE 200),
    train_Province_State_Date_0s_2d_100 as (partition by `Province_State` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    train_Province_State_Date_0_10_ as (partition by `Province_State` order by `Date` rows between 10 open preceding and 0 preceding),
    train_Province_State_Date_0s_7d_100 as (partition by `Province_State` order by `Date` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    train_Country_Region_Date_0s_5d_200 as (partition by `Country_Region` order by `Date` rows_range between 5d open preceding and 0s preceding MAXSIZE 200),
    train_Country_Region_Date_0_10_ as (partition by `Country_Region` order by `Date` rows between 10 open preceding and 0 preceding),
    train_Province_State_Date_0s_32d_200 as (partition by `Province_State` order by `Date` rows_range between 32d open preceding and 0s preceding MAXSIZE 200),
    train_Country_Region_Date_0s_32d_200 as (partition by `Country_Region` order by `Date` rows_range between 32d open preceding and 0s preceding MAXSIZE 200),
    train_Country_Region_Date_0s_2d_100 as (partition by `Country_Region` order by `Date` rows_range between 2d open preceding and 0s preceding MAXSIZE 100);