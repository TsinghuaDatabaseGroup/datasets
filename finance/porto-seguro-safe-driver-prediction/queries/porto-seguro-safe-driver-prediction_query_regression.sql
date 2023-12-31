# start sql code
# output table name: sql_table

select
    `id` as id_1,
    `timestamp` as train_timestamp_original_0,
    `id` as train_id_original_1,
    `target` as train_target_original_2,
    `ps_calc_01` as train_ps_calc_01_original_3,
    `ps_calc_02` as train_ps_calc_02_original_4,
    `ps_calc_03` as train_ps_calc_03_original_5,
    `ps_calc_04` as train_ps_calc_04_original_6,
    `ps_calc_05` as train_ps_calc_05_original_7,
    `ps_calc_06` as train_ps_calc_06_original_8,
    `ps_calc_07` as train_ps_calc_07_original_9,
    `ps_calc_08` as train_ps_calc_08_original_10,
    `ps_calc_09` as train_ps_calc_09_original_11,
    `ps_calc_10` as train_ps_calc_10_original_12,
    `ps_calc_11` as train_ps_calc_11_original_13,
    `ps_calc_12` as train_ps_calc_12_original_14,
    `ps_calc_13` as train_ps_calc_13_original_15,
    `ps_calc_14` as train_ps_calc_14_original_16,
    `ps_calc_15_bin` as train_ps_calc_15_bin_original_17,
    `ps_calc_16_bin` as train_ps_calc_16_bin_original_18,
    `ps_calc_17_bin` as train_ps_calc_17_bin_original_19,
    `ps_calc_18_bin` as train_ps_calc_18_bin_original_20,
    `ps_calc_19_bin` as train_ps_calc_19_bin_original_21,
    `ps_calc_20_bin` as train_ps_calc_20_bin_original_22,
    `ps_car_01_cat` as train_ps_car_01_cat_original_23,
    `ps_car_02_cat` as train_ps_car_02_cat_original_24,
    `ps_car_03_cat` as train_ps_car_03_cat_original_25,
    `ps_car_04_cat` as train_ps_car_04_cat_original_26,
    `ps_car_05_cat` as train_ps_car_05_cat_original_27,
    `ps_car_06_cat` as train_ps_car_06_cat_original_28,
    `ps_car_07_cat` as train_ps_car_07_cat_original_29,
    `ps_car_08_cat` as train_ps_car_08_cat_original_30,
    `ps_car_09_cat` as train_ps_car_09_cat_original_31,
    `ps_car_10_cat` as train_ps_car_10_cat_original_32,
    `ps_car_11` as train_ps_car_11_original_33,
    `ps_car_11_cat` as train_ps_car_11_cat_original_34,
    `ps_car_12` as train_ps_car_12_original_35,
    `ps_car_13` as train_ps_car_13_original_36,
    `ps_car_14` as train_ps_car_14_original_37,
    `ps_car_15` as train_ps_car_15_original_38,
    `ps_ind_01` as train_ps_ind_01_original_39,
    `ps_ind_02_cat` as train_ps_ind_02_cat_original_40,
    `ps_ind_03` as train_ps_ind_03_original_41,
    `ps_ind_04_cat` as train_ps_ind_04_cat_original_42,
    `ps_ind_05_cat` as train_ps_ind_05_cat_original_43,
    `ps_ind_06_bin` as train_ps_ind_06_bin_original_44,
    `ps_ind_07_bin` as train_ps_ind_07_bin_original_45,
    `ps_ind_08_bin` as train_ps_ind_08_bin_original_46,
    `ps_ind_09_bin` as train_ps_ind_09_bin_original_47,
    `ps_ind_10_bin` as train_ps_ind_10_bin_original_48,
    `ps_ind_11_bin` as train_ps_ind_11_bin_original_49,
    `ps_ind_12_bin` as train_ps_ind_12_bin_original_50,
    `ps_ind_13_bin` as train_ps_ind_13_bin_original_51,
    `ps_ind_14` as train_ps_ind_14_original_52,
    `ps_ind_15` as train_ps_ind_15_original_53,
    `ps_ind_16_bin` as train_ps_ind_16_bin_original_54,
    `ps_ind_17_bin` as train_ps_ind_17_bin_original_55,
    `ps_ind_18_bin` as train_ps_ind_18_bin_original_56,
    `ps_reg_01` as train_ps_reg_01_original_57,
    `ps_reg_02` as train_ps_reg_02_original_58,
    `ps_reg_03` as train_ps_reg_03_original_59,
    max(`ps_car_12`) over train_ps_ind_06_bin_timestamp_0s_1h_100 as train_ps_car_12_window_max_60,
    max(`ps_car_13`) over train_ps_ind_12_bin_timestamp_0s_1h_100 as train_ps_car_13_window_max_61,
    max(`ps_car_13`) over train_ps_car_08_cat_timestamp_0s_1h_100 as train_ps_car_13_window_max_62,
    min(`ps_car_13`) over train_ps_calc_18_bin_timestamp_0s_1h_100 as train_ps_car_13_window_min_63,
    max(`ps_reg_03`) over train_ps_ind_09_bin_timestamp_0s_1h_100 as train_ps_reg_03_window_max_64,
    max(`ps_car_12`) over train_ps_car_02_cat_timestamp_0s_1h_100 as train_ps_car_12_window_max_65,
    max(`ps_reg_03`) over train_ps_ind_13_bin_timestamp_0s_1h_100 as train_ps_reg_03_window_max_66,
    max(`ps_car_13`) over train_ps_ind_08_bin_timestamp_0s_1h_100 as train_ps_car_13_window_max_67,
    min(`ps_car_13`) over train_ps_ind_02_cat_timestamp_0s_1h_100 as train_ps_car_13_window_min_68,
    max(`ps_car_14`) over train_ps_ind_09_bin_timestamp_0s_1h_200 as train_ps_car_14_window_max_69,
    max(`ps_car_12`) over train_ps_calc_16_bin_timestamp_0s_1h_100 as train_ps_car_12_window_max_70,
    max(`ps_car_14`) over train_ps_ind_04_cat_timestamp_0s_1h_200 as train_ps_car_14_window_max_71,
    max(`ps_car_12`) over train_ps_car_11_cat_timestamp_0s_1h_200 as train_ps_car_12_window_max_72,
    max(`ps_car_12`) over train_ps_car_08_cat_timestamp_0s_1h_200 as train_ps_car_12_window_max_73,
    max(`ps_car_14`) over train_ps_calc_17_bin_timestamp_0s_1h_200 as train_ps_car_14_window_max_74,
    sum(`ps_reg_01`) over train_ps_calc_15_bin_timestamp_0_10_ as train_ps_reg_01_window_sum_75,
    sum(`ps_reg_01`) over train_ps_ind_16_bin_timestamp_0s_1h_100 as train_ps_reg_01_window_sum_76,
    avg(`ps_reg_01`) over train_ps_ind_16_bin_timestamp_0s_1h_100 as train_ps_reg_01_window_avg_77
from
    `train`
    window train_ps_ind_06_bin_timestamp_0s_1h_100 as (partition by `ps_ind_06_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_ind_12_bin_timestamp_0s_1h_100 as (partition by `ps_ind_12_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_car_08_cat_timestamp_0s_1h_100 as (partition by `ps_car_08_cat` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_calc_18_bin_timestamp_0s_1h_100 as (partition by `ps_calc_18_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_ind_09_bin_timestamp_0s_1h_100 as (partition by `ps_ind_09_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_car_02_cat_timestamp_0s_1h_100 as (partition by `ps_car_02_cat` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_ind_13_bin_timestamp_0s_1h_100 as (partition by `ps_ind_13_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_ind_08_bin_timestamp_0s_1h_100 as (partition by `ps_ind_08_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_ind_02_cat_timestamp_0s_1h_100 as (partition by `ps_ind_02_cat` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_ind_09_bin_timestamp_0s_1h_200 as (partition by `ps_ind_09_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 200),
    train_ps_calc_16_bin_timestamp_0s_1h_100 as (partition by `ps_calc_16_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_ps_ind_04_cat_timestamp_0s_1h_200 as (partition by `ps_ind_04_cat` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 200),
    train_ps_car_11_cat_timestamp_0s_1h_200 as (partition by `ps_car_11_cat` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 200),
    train_ps_car_08_cat_timestamp_0s_1h_200 as (partition by `ps_car_08_cat` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 200),
    train_ps_calc_17_bin_timestamp_0s_1h_200 as (partition by `ps_calc_17_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 200),
    train_ps_calc_15_bin_timestamp_0_10_ as (partition by `ps_calc_15_bin` order by `timestamp` rows between 10 open preceding and 0 preceding),
    train_ps_ind_16_bin_timestamp_0s_1h_100 as (partition by `ps_ind_16_bin` order by `timestamp` rows_range between 1h open preceding and 0s preceding MAXSIZE 100);