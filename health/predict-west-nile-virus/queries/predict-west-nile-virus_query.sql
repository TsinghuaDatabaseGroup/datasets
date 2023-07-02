# start sql code
# output table name: sql_table
select * from 
(
select
    `Id` as Id_1,
    `Date` as train_Date_original_0,
    `Id` as train_Id_original_1,
    `WnvPresent` as train_WnvPresent_original_2,
    `Address` as train_Address_original_23,
    `AddressAccuracy` as train_AddressAccuracy_original_24,
    `Block` as train_Block_original_25,
    `Latitude` as train_Latitude_original_26,
    `Longitude` as train_Longitude_original_27,
    `NumMosquitos` as train_NumMosquitos_original_28,
    `Species` as train_Species_original_29,
    `Street` as train_Street_original_30,
    `Trap` as train_Trap_original_31
from
    `train`
    )
as out0
last join
(
select
    `train`.`Id` as Id_4,
    `spray_Date`.`Latitude` as spray_Latitude_multi_direct_3,
    `weather_Date`.`AvgSpeed` as weather_AvgSpeed_multi_direct_4,
    `weather_Date`.`CodeSum` as weather_CodeSum_multi_direct_5,
    `weather_Date`.`Cool` as weather_Cool_multi_direct_6,
    `weather_Date`.`Depart` as weather_Depart_multi_direct_7,
    `weather_Date`.`Depth` as weather_Depth_multi_direct_8,
    `weather_Date`.`DewPoint` as weather_DewPoint_multi_direct_9,
    `weather_Date`.`Heat` as weather_Heat_multi_direct_10,
    `weather_Date`.`PrecipTotal` as weather_PrecipTotal_multi_direct_11,
    `weather_Date`.`ResultDir` as weather_ResultDir_multi_direct_12,
    `weather_Date`.`ResultSpeed` as weather_ResultSpeed_multi_direct_13,
    `weather_Date`.`SeaLevel` as weather_SeaLevel_multi_direct_14,
    `weather_Date`.`SnowFall` as weather_SnowFall_multi_direct_15,
    `weather_Date`.`StnPressure` as weather_StnPressure_multi_direct_16,
    `weather_Date`.`Sunrise` as weather_Sunrise_multi_direct_17,
    `weather_Date`.`Sunset` as weather_Sunset_multi_direct_18,
    `weather_Date`.`Tavg` as weather_Tavg_multi_direct_19,
    `weather_Date`.`Tmax` as weather_Tmax_multi_direct_20,
    `weather_Date`.`Tmin` as weather_Tmin_multi_direct_21,
    `weather_Date`.`WetBulb` as weather_WetBulb_multi_direct_22
from
    `train`
    last join `spray` as `spray_Date` on `train`.`Date` = `spray_Date`.`Date`
    last join `weather` as `weather_Date` on `train`.`Date` = `weather_Date`.`Date`)
as out1
on out0.Id_1 = out1.Id_4
;