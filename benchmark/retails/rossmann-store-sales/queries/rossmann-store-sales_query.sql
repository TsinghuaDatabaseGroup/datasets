# start sql code
# output table name: sql_table
select * from 
(
select
    `Id` as Id_1,
    `Date` as train_Date_original_0,
    `Id` as train_Id_original_1,
    `Sales` as train_Sales_original_2,
    `Customers` as train_Customers_original_12,
    `DayOfWeek` as train_DayOfWeek_original_13,
    `Open` as train_Open_original_14,
    `Promo` as train_Promo_original_15,
    `SchoolHoliday` as train_SchoolHoliday_original_16,
    `StateHoliday` as train_StateHoliday_original_17,
    `Store` as train_Store_original_18,
    `Open` as train_Open_combine_19,
    `Open` as train_Open_combine_20,
    `Open` as train_Open_combine_21,
    `Open` as train_Open_combine_22,
    case when !isnull(at(`Open`, 0)) over train_Customers_Date_0s_64d_100 then count_where(`Open`, `Open` = at(`Open`, 0)) over train_Customers_Date_0s_64d_100 else null end as train_Open_window_count_23,
    case when !isnull(at(`Open`, 0)) over train_Customers_Date_0s_14d_100 then count_where(`Open`, `Open` = at(`Open`, 0)) over train_Customers_Date_0s_14d_100 else null end as train_Open_window_count_24,
    case when !isnull(at(`StateHoliday`, 0)) over train_Customers_Date_0s_14d_100 then count_where(`StateHoliday`, `StateHoliday` = at(`StateHoliday`, 0)) over train_Customers_Date_0s_14d_100 else null end as train_StateHoliday_window_count_25,
    case when !isnull(at(`StateHoliday`, 0)) over train_Customers_Date_0s_64d_100 then count_where(`StateHoliday`, `StateHoliday` = at(`StateHoliday`, 0)) over train_Customers_Date_0s_64d_100 else null end as train_StateHoliday_window_count_26,
    case when !isnull(at(`Customers`, 0)) over train_DayOfWeek_Date_0s_64d_200 then count_where(`Customers`, `Customers` = at(`Customers`, 0)) over train_DayOfWeek_Date_0s_64d_200 else null end as train_Customers_window_count_27,
    case when !isnull(at(`Customers`, 0)) over train_Open_Date_0s_64d_200 then count_where(`Customers`, `Customers` = at(`Customers`, 0)) over train_Open_Date_0s_64d_200 else null end as train_Customers_window_count_28,
    fz_top1_ratio(`Customers`) over train_Open_Date_0s_64d_100 as train_Customers_window_top1_ratio_29,
    distinct_count(`Customers`) over train_Open_Date_0_10_ as train_Customers_window_unique_count_30,
    distinct_count(`Customers`) over train_Open_Date_0s_64d_100 as train_Customers_window_unique_count_31,
    case when !isnull(at(`Customers`, 0)) over train_Open_Date_0_10_ then count_where(`Customers`, `Customers` = at(`Customers`, 0)) over train_Open_Date_0_10_ else null end as train_Customers_window_count_32,
    fz_top1_ratio(`Store`) over train_Customers_Date_0s_14d_100 as train_Store_window_top1_ratio_33,
    case when !isnull(at(`Promo`, 0)) over train_Customers_Date_0s_64d_100 then count_where(`Promo`, `Promo` = at(`Promo`, 0)) over train_Customers_Date_0s_64d_100 else null end as train_Promo_window_count_34,
    case when !isnull(at(`Promo`, 0)) over train_Customers_Date_0s_14d_100 then count_where(`Promo`, `Promo` = at(`Promo`, 0)) over train_Customers_Date_0s_14d_100 else null end as train_Promo_window_count_35,
    fz_top1_ratio(`Customers`) over train_Open_Date_0_10_ as train_Customers_window_top1_ratio_36
from
    `train`
    window train_Customers_Date_0s_64d_100 as (partition by `Customers` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    train_Customers_Date_0s_14d_100 as (partition by `Customers` order by `Date` rows_range between 14d open preceding and 0s preceding MAXSIZE 100),
    train_DayOfWeek_Date_0s_64d_200 as (partition by `DayOfWeek` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    train_Open_Date_0s_64d_200 as (partition by `Open` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 200),
    train_Open_Date_0s_64d_100 as (partition by `Open` order by `Date` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    train_Open_Date_0_10_ as (partition by `Open` order by `Date` rows between 10 open preceding and 0 preceding))
as out0
last join
(
select
    `train`.`Id` as Id_4,
    `store_Store`.`Assortment` as store_Assortment_multi_direct_3,
    `store_Store`.`CompetitionDistance` as store_CompetitionDistance_multi_direct_4,
    `store_Store`.`CompetitionOpenSinceMonth` as store_CompetitionOpenSinceMonth_multi_direct_5,
    `store_Store`.`CompetitionOpenSinceYear` as store_CompetitionOpenSinceYear_multi_direct_6,
    `store_Store`.`Promo2` as store_Promo2_multi_direct_7,
    `store_Store`.`Promo2SinceWeek` as store_Promo2SinceWeek_multi_direct_8,
    `store_Store`.`Promo2SinceYear` as store_Promo2SinceYear_multi_direct_9,
    `store_Store`.`PromoInterval` as store_PromoInterval_multi_direct_10,
    `store_Store`.`StoreType` as store_StoreType_multi_direct_11
from
    `train`
    last join `store` as `store_Store` on `train`.`Store` = `store_Store`.`Store`)
as out1
on out0.Id_1 = out1.Id_4
;