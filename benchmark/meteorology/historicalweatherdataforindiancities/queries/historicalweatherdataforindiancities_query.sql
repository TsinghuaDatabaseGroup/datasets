# start sql code
# output table name: sql_table
select * from 
(
select
    `time` as time_1,
    `time` as Bangalore_1990_2022_BangaloreCity_time_original_0,
    `prcp` as Bangalore_1990_2022_BangaloreCity_prcp_original_18,
    `tavg` as Bangalore_1990_2022_BangaloreCity_tavg_original_19,
    `tmax` as Bangalore_1990_2022_BangaloreCity_tmax_original_20,
    `tmin` as Bangalore_1990_2022_BangaloreCity_tmin_original_21,
    `tmax` as Bangalore_1990_2022_BangaloreCity_tmax_multiply_26,
    `tavg` as Bangalore_1990_2022_BangaloreCity_tavg_multiply_27,
    `tmax` as Bangalore_1990_2022_BangaloreCity_tmax_multiply_29,
    `tavg` as Bangalore_1990_2022_BangaloreCity_tavg_multiply_32,
    `tavg` as Bangalore_1990_2022_BangaloreCity_tavg_multiply_34,
    `tavg` as Bangalore_1990_2022_BangaloreCity_tavg_multiply_37,
    `tmax` as Bangalore_1990_2022_BangaloreCity_tmax_multiply_38,
    `tmax` as Bangalore_1990_2022_BangaloreCity_tmax_multiply_39
from
    `Bangalore_1990_2022_BangaloreCity`
    )
as out0
last join
(
select
    `Bangalore_1990_2022_BangaloreCity`.`time` as time_2,
    `Delhi_NCR_1990_2022_Safdarjung_time`.`tavg` as Delhi_NCR_1990_2022_Safdarjung_tavg_multi_direct_2,
    `Chennai_1990_2022_Madras_time`.`prcp` as Chennai_1990_2022_Madras_prcp_multi_direct_3,
    `Chennai_1990_2022_Madras_time`.`tavg` as Chennai_1990_2022_Madras_tavg_multi_direct_4,
    `Chennai_1990_2022_Madras_time`.`tmax` as Chennai_1990_2022_Madras_tmax_multi_direct_5,
    `Chennai_1990_2022_Madras_time`.`tmin` as Chennai_1990_2022_Madras_tmin_multi_direct_6,
    `Delhi_NCR_1990_2022_Safdarjung_time`.`prcp` as Delhi_NCR_1990_2022_Safdarjung_prcp_multi_direct_7,
    `Delhi_NCR_1990_2022_Safdarjung_time`.`tmax` as Delhi_NCR_1990_2022_Safdarjung_tmax_multi_direct_8,
    `Delhi_NCR_1990_2022_Safdarjung_time`.`tmin` as Delhi_NCR_1990_2022_Safdarjung_tmin_multi_direct_9,
    `Lucknow_1990_2022_time`.`prcp` as Lucknow_1990_2022_prcp_multi_direct_10,
    `Lucknow_1990_2022_time`.`tavg` as Lucknow_1990_2022_tavg_multi_direct_11,
    `Lucknow_1990_2022_time`.`tmax` as Lucknow_1990_2022_tmax_multi_direct_12,
    `Lucknow_1990_2022_time`.`tmin` as Lucknow_1990_2022_tmin_multi_direct_13,
    `Mumbai_1990_2022_Santacruz_time`.`prcp` as Mumbai_1990_2022_Santacruz_prcp_multi_direct_14,
    `Mumbai_1990_2022_Santacruz_time`.`tavg` as Mumbai_1990_2022_Santacruz_tavg_multi_direct_15,
    `Mumbai_1990_2022_Santacruz_time`.`tmax` as Mumbai_1990_2022_Santacruz_tmax_multi_direct_16,
    `Mumbai_1990_2022_Santacruz_time`.`tmin` as Mumbai_1990_2022_Santacruz_tmin_multi_direct_17
from
    `Bangalore_1990_2022_BangaloreCity`
    last join `Delhi_NCR_1990_2022_Safdarjung` as `Delhi_NCR_1990_2022_Safdarjung_time` on `Bangalore_1990_2022_BangaloreCity`.`time` = `Delhi_NCR_1990_2022_Safdarjung_time`.`time`
    last join `Chennai_1990_2022_Madras` as `Chennai_1990_2022_Madras_time` on `Bangalore_1990_2022_BangaloreCity`.`time` = `Chennai_1990_2022_Madras_time`.`time`
    last join `Lucknow_1990_2022` as `Lucknow_1990_2022_time` on `Bangalore_1990_2022_BangaloreCity`.`time` = `Lucknow_1990_2022_time`.`time`
    last join `Mumbai_1990_2022_Santacruz` as `Mumbai_1990_2022_Santacruz_time` on `Bangalore_1990_2022_BangaloreCity`.`time` = `Mumbai_1990_2022_Santacruz_time`.`time`)
as out1
on out0.time_1 = out1.time_2
;