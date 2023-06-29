# start sql code
# output table name: sql_table

select
    `id` as id_1,
    `Posted_On` as House_Rent_Dataset_Posted_On_original_0,
    `id` as House_Rent_Dataset_id_original_1,
    `Rent` as House_Rent_Dataset_Rent_original_2,
    `Area_Locality` as House_Rent_Dataset_Area_Locality_original_3,
    `Area_Type` as House_Rent_Dataset_Area_Type_original_4,
    `BHK` as House_Rent_Dataset_BHK_original_5,
    `Bathroom` as House_Rent_Dataset_Bathroom_original_6,
    `City` as House_Rent_Dataset_City_original_7,
    `Floor` as House_Rent_Dataset_Floor_original_8,
    `Furnishing_Status` as House_Rent_Dataset_Furnishing_Status_original_9,
    `Point_of_Contact` as House_Rent_Dataset_Point_of_Contact_original_10,
    `Size` as House_Rent_Dataset_Size_original_11,
    `Tenant_Preferred` as House_Rent_Dataset_Tenant_Preferred_original_12,
    `Bathroom` as House_Rent_Dataset_Bathroom_combine_13,
    `Point_of_Contact` as House_Rent_Dataset_Point_of_Contact_combine_13,
    `Bathroom` as House_Rent_Dataset_Bathroom_combine_14,
    `Point_of_Contact` as House_Rent_Dataset_Point_of_Contact_combine_14,
    `Furnishing_Status` as House_Rent_Dataset_Furnishing_Status_combine_14,
    `Bathroom` as House_Rent_Dataset_Bathroom_combine_15,
    `Area_Type` as House_Rent_Dataset_Area_Type_combine_15,
    `Point_of_Contact` as House_Rent_Dataset_Point_of_Contact_combine_15,
    `Bathroom` as House_Rent_Dataset_Bathroom_combine_16,
    `BHK` as House_Rent_Dataset_BHK_combine_16,
    `Point_of_Contact` as House_Rent_Dataset_Point_of_Contact_combine_16,
    `BHK` as House_Rent_Dataset_BHK_combine_17,
    `City` as House_Rent_Dataset_City_combine_17,
    `BHK` as House_Rent_Dataset_BHK_combine_18,
    `Area_Type` as House_Rent_Dataset_Area_Type_combine_18,
    `City` as House_Rent_Dataset_City_combine_18,
    `BHK` as House_Rent_Dataset_BHK_combine_19,
    `City` as House_Rent_Dataset_City_combine_19,
    `Furnishing_Status` as House_Rent_Dataset_Furnishing_Status_combine_19,
    fz_top1_ratio(`Tenant_Preferred`) over House_Rent_Dataset_Point_of_Contact_Posted_On_0s_7d_100 as House_Rent_Dataset_Tenant_Preferred_window_top1_ratio_20,
    fz_top1_ratio(`Floor`) over House_Rent_Dataset_Point_of_Contact_Posted_On_0s_64d_100 as House_Rent_Dataset_Floor_window_top1_ratio_21,
    case when !isnull(at(`Area_Type`, 0)) over House_Rent_Dataset_Floor_Posted_On_0s_14d_200 then count_where(`Area_Type`, `Area_Type` = at(`Area_Type`, 0)) over House_Rent_Dataset_Floor_Posted_On_0s_14d_200 else null end as House_Rent_Dataset_Area_Type_window_count_22,
    case when !isnull(at(`Area_Locality`, 0)) over House_Rent_Dataset_Floor_Posted_On_0s_32d_200 then count_where(`Area_Locality`, `Area_Locality` = at(`Area_Locality`, 0)) over House_Rent_Dataset_Floor_Posted_On_0s_32d_200 else null end as House_Rent_Dataset_Area_Locality_window_count_23,
    case when !isnull(at(`Floor`, 0)) over House_Rent_Dataset_Point_of_Contact_Posted_On_0s_7d_200 then count_where(`Floor`, `Floor` = at(`Floor`, 0)) over House_Rent_Dataset_Point_of_Contact_Posted_On_0s_7d_200 else null end as House_Rent_Dataset_Floor_window_count_24,
    fz_top1_ratio(`Size`) over House_Rent_Dataset_Floor_Posted_On_0_10_ as House_Rent_Dataset_Size_window_top1_ratio_25,
    case when !isnull(at(`Floor`, 0)) over House_Rent_Dataset_Tenant_Preferred_Posted_On_0s_7d_100 then count_where(`Floor`, `Floor` = at(`Floor`, 0)) over House_Rent_Dataset_Tenant_Preferred_Posted_On_0s_7d_100 else null end as House_Rent_Dataset_Floor_window_count_26,
    case when !isnull(at(`Floor`, 0)) over House_Rent_Dataset_Point_of_Contact_Posted_On_0s_14d_200 then count_where(`Floor`, `Floor` = at(`Floor`, 0)) over House_Rent_Dataset_Point_of_Contact_Posted_On_0s_14d_200 else null end as House_Rent_Dataset_Floor_window_count_27,
    case when !isnull(at(`Floor`, 0)) over House_Rent_Dataset_Tenant_Preferred_Posted_On_0s_14d_100 then count_where(`Floor`, `Floor` = at(`Floor`, 0)) over House_Rent_Dataset_Tenant_Preferred_Posted_On_0s_14d_100 else null end as House_Rent_Dataset_Floor_window_count_28,
    fz_top1_ratio(`Size`) over House_Rent_Dataset_Floor_Posted_On_0s_7d_100 as House_Rent_Dataset_Size_window_top1_ratio_29,
    case when !isnull(at(`Floor`, 0)) over House_Rent_Dataset_Area_Type_Posted_On_0s_5d_100 then count_where(`Floor`, `Floor` = at(`Floor`, 0)) over House_Rent_Dataset_Area_Type_Posted_On_0s_5d_100 else null end as House_Rent_Dataset_Floor_window_count_30
from
    `House_Rent_Dataset`
    window House_Rent_Dataset_Point_of_Contact_Posted_On_0s_7d_100 as (partition by `Point_of_Contact` order by `Posted_On` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    House_Rent_Dataset_Point_of_Contact_Posted_On_0s_64d_100 as (partition by `Point_of_Contact` order by `Posted_On` rows_range between 64d open preceding and 0s preceding MAXSIZE 100),
    House_Rent_Dataset_Floor_Posted_On_0s_14d_200 as (partition by `Floor` order by `Posted_On` rows_range between 14d open preceding and 0s preceding MAXSIZE 200),
    House_Rent_Dataset_Floor_Posted_On_0s_32d_200 as (partition by `Floor` order by `Posted_On` rows_range between 32d open preceding and 0s preceding MAXSIZE 200),
    House_Rent_Dataset_Point_of_Contact_Posted_On_0s_7d_200 as (partition by `Point_of_Contact` order by `Posted_On` rows_range between 7d open preceding and 0s preceding MAXSIZE 200),
    House_Rent_Dataset_Floor_Posted_On_0_10_ as (partition by `Floor` order by `Posted_On` rows between 10 open preceding and 0 preceding),
    House_Rent_Dataset_Tenant_Preferred_Posted_On_0s_7d_100 as (partition by `Tenant_Preferred` order by `Posted_On` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    House_Rent_Dataset_Point_of_Contact_Posted_On_0s_14d_200 as (partition by `Point_of_Contact` order by `Posted_On` rows_range between 14d open preceding and 0s preceding MAXSIZE 200),
    House_Rent_Dataset_Tenant_Preferred_Posted_On_0s_14d_100 as (partition by `Tenant_Preferred` order by `Posted_On` rows_range between 14d open preceding and 0s preceding MAXSIZE 100),
    House_Rent_Dataset_Floor_Posted_On_0s_7d_100 as (partition by `Floor` order by `Posted_On` rows_range between 7d open preceding and 0s preceding MAXSIZE 100),
    House_Rent_Dataset_Area_Type_Posted_On_0s_5d_100 as (partition by `Area_Type` order by `Posted_On` rows_range between 5d open preceding and 0s preceding MAXSIZE 100);