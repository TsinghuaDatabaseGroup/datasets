# start sql code
# output table name: sql_table
select * from 
(
select
    `PURCHASEID_hash` as PURCHASEID_hash_1,
    `I_DATE` as coupon_detail_train_I_DATE_original_0,
    `PURCHASEID_hash` as coupon_detail_train_PURCHASEID_hash_original_1,
    `ITEM_COUNT` as coupon_detail_train_ITEM_COUNT_original_2,
    `COUPON_ID_hash` as coupon_detail_train_COUPON_ID_hash_original_36,
    `SMALL_AREA_NAME` as coupon_detail_train_SMALL_AREA_NAME_original_37,
    `USER_ID_hash` as coupon_detail_train_USER_ID_hash_original_38
from
    `coupon_detail_train`
    )
as out0
last join
(
select
    `coupon_detail_train`.`PURCHASEID_hash` as PURCHASEID_hash_4,
    `coupon_area_train_COUPON_ID_hash`.`PREF_NAME` as coupon_area_train_PREF_NAME_multi_direct_3,
    `coupon_area_train_COUPON_ID_hash`.`SMALL_AREA_NAME` as coupon_area_train_SMALL_AREA_NAME_multi_direct_4,
    `coupon_list_train_COUPON_ID_hash`.`CAPSULE_TEXT` as coupon_list_train_CAPSULE_TEXT_multi_direct_5,
    `coupon_list_train_COUPON_ID_hash`.`CATALOG_PRICE` as coupon_list_train_CATALOG_PRICE_multi_direct_6,
    `coupon_list_train_COUPON_ID_hash`.`DISCOUNT_PRICE` as coupon_list_train_DISCOUNT_PRICE_multi_direct_7,
    `coupon_list_train_COUPON_ID_hash`.`DISPEND` as coupon_list_train_DISPEND_multi_direct_8,
    `coupon_list_train_COUPON_ID_hash`.`DISPFROM` as coupon_list_train_DISPFROM_multi_direct_9,
    `coupon_list_train_COUPON_ID_hash`.`DISPPERIOD` as coupon_list_train_DISPPERIOD_multi_direct_10,
    `coupon_list_train_COUPON_ID_hash`.`GENRE_NAME` as coupon_list_train_GENRE_NAME_multi_direct_11,
    `coupon_list_train_COUPON_ID_hash`.`PRICE_RATE` as coupon_list_train_PRICE_RATE_multi_direct_12,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_BEFORE_HOLIDAY` as coupon_list_train_USABLE_DATE_BEFORE_HOLIDAY_multi_direct_13,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_FRI` as coupon_list_train_USABLE_DATE_FRI_multi_direct_14,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_HOLIDAY` as coupon_list_train_USABLE_DATE_HOLIDAY_multi_direct_15,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_MON` as coupon_list_train_USABLE_DATE_MON_multi_direct_16,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_SAT` as coupon_list_train_USABLE_DATE_SAT_multi_direct_17,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_SUN` as coupon_list_train_USABLE_DATE_SUN_multi_direct_18,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_THU` as coupon_list_train_USABLE_DATE_THU_multi_direct_19,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_TUE` as coupon_list_train_USABLE_DATE_TUE_multi_direct_20,
    `coupon_list_train_COUPON_ID_hash`.`USABLE_DATE_WED` as coupon_list_train_USABLE_DATE_WED_multi_direct_21,
    `coupon_list_train_COUPON_ID_hash`.`VALIDEND` as coupon_list_train_VALIDEND_multi_direct_22,
    `coupon_list_train_COUPON_ID_hash`.`VALIDFROM` as coupon_list_train_VALIDFROM_multi_direct_23,
    `coupon_list_train_COUPON_ID_hash`.`VALIDPERIOD` as coupon_list_train_VALIDPERIOD_multi_direct_24,
    `coupon_list_train_COUPON_ID_hash`.`ken_name` as coupon_list_train_ken_name_multi_direct_25,
    `coupon_list_train_COUPON_ID_hash`.`large_area_name` as coupon_list_train_large_area_name_multi_direct_26,
    `coupon_list_train_COUPON_ID_hash`.`small_area_name` as coupon_list_train_small_area_name_multi_direct_27,
    `user_list_USER_ID_hash`.`AGE` as user_list_AGE_multi_direct_28,
    `user_list_USER_ID_hash`.`PREF_NAME` as user_list_PREF_NAME_multi_direct_29,
    `user_list_USER_ID_hash`.`REG_DATE` as user_list_REG_DATE_multi_direct_30,
    `user_list_USER_ID_hash`.`SEX_ID` as user_list_SEX_ID_multi_direct_31,
    `user_list_USER_ID_hash`.`WITHDRAW_DATE` as user_list_WITHDRAW_DATE_multi_direct_32
from
    `coupon_detail_train`
    last join `coupon_area_train` as `coupon_area_train_COUPON_ID_hash` on `coupon_detail_train`.`COUPON_ID_hash` = `coupon_area_train_COUPON_ID_hash`.`COUPON_ID_hash`
    last join `coupon_list_train` as `coupon_list_train_COUPON_ID_hash` on `coupon_detail_train`.`COUPON_ID_hash` = `coupon_list_train_COUPON_ID_hash`.`COUPON_ID_hash`
    last join `user_list` as `user_list_USER_ID_hash` on `coupon_detail_train`.`USER_ID_hash` = `user_list_USER_ID_hash`.`USER_ID_hash`)
as out1
on out0.PURCHASEID_hash_1 = out1.PURCHASEID_hash_4
last join
(
select
    `PURCHASEID_hash` as PURCHASEID_hash_34,
    fz_topn_frequency(`PAGE_SERIAL`, 3) over coupon_visit_train_PURCHASEID_hash_I_DATE_0s_32d_100 as coupon_visit_train_PAGE_SERIAL_multi_top3frequency_33,
    fz_topn_frequency(`PAGE_SERIAL`, 3) over coupon_visit_train_PURCHASEID_hash_I_DATE_0s_64d_100 as coupon_visit_train_PAGE_SERIAL_multi_top3frequency_34,
    distinct_count(`PAGE_SERIAL`) over coupon_visit_train_PURCHASEID_hash_I_DATE_0s_1h_100 as coupon_visit_train_PAGE_SERIAL_multi_unique_count_35
from
    (select int(0) as `PURCHASE_FLG`, `I_DATE` as `I_DATE`, int(0) as `PAGE_SERIAL`, '' as `REFERRER_hash`, '' as `VIEW_COUPON_ID_hash`, '' as `USER_ID_hash`, '' as `SESSION_ID_hash`, `PURCHASEID_hash` as `PURCHASEID_hash` from `coupon_detail_train`)
    window coupon_visit_train_PURCHASEID_hash_I_DATE_0s_32d_100 as (
UNION `coupon_visit_train` partition by `PURCHASEID_hash` order by `I_DATE` rows_range between 32d open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    coupon_visit_train_PURCHASEID_hash_I_DATE_0s_64d_100 as (
UNION `coupon_visit_train` partition by `PURCHASEID_hash` order by `I_DATE` rows_range between 64d open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    coupon_visit_train_PURCHASEID_hash_I_DATE_0s_1h_100 as (
UNION `coupon_visit_train` partition by `PURCHASEID_hash` order by `I_DATE` rows_range between 1h open preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out2
on out0.PURCHASEID_hash_1 = out2.PURCHASEID_hash_34
;