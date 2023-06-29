# start sql code
# output table name: sql_table
select * from 
(
select
    `id` as id_1,
    `timestamp_first_active` as train_users_2_timestamp_first_active_original_0,
    `id` as train_users_2_id_original_1,
    `affiliate_channel` as train_users_2_affiliate_channel_original_7,
    `affiliate_provider` as train_users_2_affiliate_provider_original_8,
    `age` as train_users_2_age_original_9,
    `country_destination` as train_users_2_country_destination_original_10,
    `date_account_created` as train_users_2_date_account_created_original_11,
    `date_first_booking` as train_users_2_date_first_booking_original_12,
    `first_affiliate_tracked` as train_users_2_first_affiliate_tracked_original_13,
    `first_browser` as train_users_2_first_browser_original_14,
    `first_device_type` as train_users_2_first_device_type_original_15,
    `gender` as train_users_2_gender_original_16,
    `language` as train_users_2_language_original_17,
    `signup_app` as train_users_2_signup_app_original_18,
    `signup_flow` as train_users_2_signup_flow_original_19,
    `signup_method` as train_users_2_signup_method_original_20,
    `country_destination` as train_users_2_country_destination_combine_21,
    `gender` as train_users_2_gender_combine_23,
    `country_destination` as train_users_2_country_destination_combine_24,
    `gender` as train_users_2_gender_combine_24,
    `country_destination` as train_users_2_country_destination_combine_25,
    `affiliate_provider` as train_users_2_affiliate_provider_combine_25,
    `country_destination` as train_users_2_country_destination_combine_26,
    `affiliate_provider` as train_users_2_affiliate_provider_combine_26,
    `signup_app` as train_users_2_signup_app_combine_27,
    `first_device_type` as train_users_2_first_device_type_combine_27,
    `first_affiliate_tracked` as train_users_2_first_affiliate_tracked_combine_27,
    `signup_flow` as train_users_2_signup_flow_combine_28,
    `first_device_type` as train_users_2_first_device_type_combine_28,
    `first_affiliate_tracked` as train_users_2_first_affiliate_tracked_combine_28,
    `signup_app` as train_users_2_signup_app_combine_29,
    `signup_method` as train_users_2_signup_method_combine_29,
    `first_affiliate_tracked` as train_users_2_first_affiliate_tracked_combine_29,
    `date_account_created` as train_users_2_date_account_created_combine_30,
    `date_first_booking` as train_users_2_date_first_booking_combine_30,
    `first_affiliate_tracked` as train_users_2_first_affiliate_tracked_combine_30,
    `date_account_created` as train_users_2_date_account_created_combine_31,
    `date_first_booking` as train_users_2_date_first_booking_combine_31,
    `first_browser` as train_users_2_first_browser_combine_31,
    `date_account_created` as train_users_2_date_account_created_combine_32,
    `date_first_booking` as train_users_2_date_first_booking_combine_32,
    `signup_method` as train_users_2_signup_method_combine_32,
    `date_account_created` as train_users_2_date_account_created_combine_33,
    `gender` as train_users_2_gender_combine_33,
    `date_first_booking` as train_users_2_date_first_booking_combine_33,
    min(`age`) over train_users_2_country_destination_timestamp_first_active_0s_2d_100 as train_users_2_age_window_min_34,
    max(`age`) over train_users_2_country_destination_timestamp_first_active_0s_2d_100 as train_users_2_age_window_max_35,
    sum(`age`) over train_users_2_country_destination_timestamp_first_active_0s_2d_200 as train_users_2_age_window_sum_36,
    avg(`age`) over train_users_2_country_destination_timestamp_first_active_0s_2d_200 as train_users_2_age_window_avg_37,
    avg(`age`) over train_users_2_country_destination_timestamp_first_active_0s_10h_200 as train_users_2_age_window_avg_38
from
    `train_users_2`
    window train_users_2_country_destination_timestamp_first_active_0s_2d_100 as (partition by `country_destination` order by `timestamp_first_active` rows_range between 2d open preceding and 0s preceding MAXSIZE 100),
    train_users_2_country_destination_timestamp_first_active_0s_2d_200 as (partition by `country_destination` order by `timestamp_first_active` rows_range between 2d open preceding and 0s preceding MAXSIZE 200),
    train_users_2_country_destination_timestamp_first_active_0s_10h_200 as (partition by `country_destination` order by `timestamp_first_active` rows_range between 10h open preceding and 0s preceding MAXSIZE 200))
as out0
last join
(
select
    `train_users_2`.`id` as id_3,
    `countries_country_destination`.`distance_km` as countries_distance_km_multi_direct_2,
    `age_gender_bkts_country_destination`.`age_bucket` as age_gender_bkts_age_bucket_multi_direct_3,
    `age_gender_bkts_country_destination`.`gender` as age_gender_bkts_gender_multi_direct_4,
    `age_gender_bkts_country_destination`.`year` as age_gender_bkts_year_multi_direct_5,
    `countries_country_destination`.`destination_language` as countries_destination_language_multi_direct_6
from
    `train_users_2`
    last join `countries` as `countries_country_destination` on `train_users_2`.`country_destination` = `countries_country_destination`.`country_destination`
    last join `age_gender_bkts` as `age_gender_bkts_country_destination` on `train_users_2`.`country_destination` = `age_gender_bkts_country_destination`.`country_destination`)
as out1
on out0.id_1 = out1.id_3
;