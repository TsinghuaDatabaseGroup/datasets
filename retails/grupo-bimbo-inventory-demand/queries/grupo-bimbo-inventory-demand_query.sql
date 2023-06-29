# start sql code
# output table name: sql_table
select * from 
(
select
    `id` as id_1,
    `Semana` as train_Semana_original_0,
    `id` as train_id_original_1,
    `Demanda_uni_equil` as train_Demanda_uni_equil_original_2,
    `Agencia_ID` as train_Agencia_ID_original_7,
    `Canal_ID` as train_Canal_ID_original_8,
    `Cliente_ID` as train_Cliente_ID_original_9,
    `Dev_proxima` as train_Dev_proxima_original_10,
    `Dev_uni_proxima` as train_Dev_uni_proxima_original_11,
    `Ruta_SAK` as train_Ruta_SAK_original_12,
    `Venta_hoy` as train_Venta_hoy_original_13,
    `Venta_uni_hoy` as train_Venta_uni_hoy_original_14,
    min(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0s_1h_200 as train_Venta_hoy_window_min_15,
    max(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0s_1h_200 as train_Venta_hoy_window_max_16,
    avg(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0s_1h_200 as train_Venta_hoy_window_avg_17,
    min(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0_10_ as train_Venta_hoy_window_min_18,
    max(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0_10_ as train_Venta_hoy_window_max_19,
    avg(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0_10_ as train_Venta_hoy_window_avg_20,
    distinct_count(`Ruta_SAK`) over train_Venta_uni_hoy_Semana_0s_1h_200 as train_Ruta_SAK_window_unique_count_21,
    distinct_count(`Agencia_ID`) over train_Venta_uni_hoy_Semana_0s_1h_200 as train_Agencia_ID_window_unique_count_22,
    distinct_count(`Cliente_ID`) over train_Venta_uni_hoy_Semana_0s_1h_200 as train_Cliente_ID_window_unique_count_23,
    `Dev_proxima` as train_Dev_proxima_divide_24,
    `Venta_hoy` as train_Venta_hoy_divide_24,
    `Venta_hoy` as train_Venta_hoy_divide_25,
    `Dev_proxima` as train_Dev_proxima_divide_25,
    `Canal_ID` as train_Canal_ID_combine_26,
    `Dev_uni_proxima` as train_Dev_uni_proxima_combine_26,
    sum(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0s_1h_100 as train_Venta_hoy_window_sum_27,
    `Venta_hoy` as train_Venta_hoy_multiply_28,
    `Dev_proxima` as train_Dev_proxima_multiply_28,
    sum(`Venta_hoy`) over train_Venta_uni_hoy_Semana_0_10_ as train_Venta_hoy_window_sum_29,
    case when !isnull(at(`Dev_uni_proxima`, 0)) over train_Canal_ID_Semana_0s_1h_100 then count_where(`Dev_uni_proxima`, `Dev_uni_proxima` = at(`Dev_uni_proxima`, 0)) over train_Canal_ID_Semana_0s_1h_100 else null end as train_Dev_uni_proxima_window_count_30,
    log(`Venta_hoy`) as train_Venta_hoy_log_31,
    case when !isnull(at(`Venta_uni_hoy`, 0)) over train_Dev_uni_proxima_Semana_0s_1h_200 then count_where(`Venta_uni_hoy`, `Venta_uni_hoy` = at(`Venta_uni_hoy`, 0)) over train_Dev_uni_proxima_Semana_0s_1h_200 else null end as train_Venta_uni_hoy_window_count_32
from
    `train`
    window train_Venta_uni_hoy_Semana_0s_1h_200 as (partition by `Venta_uni_hoy` order by `Semana` rows_range between 1h open preceding and 0s preceding MAXSIZE 200),
    train_Venta_uni_hoy_Semana_0_10_ as (partition by `Venta_uni_hoy` order by `Semana` rows between 10 open preceding and 0 preceding),
    train_Venta_uni_hoy_Semana_0s_1h_100 as (partition by `Venta_uni_hoy` order by `Semana` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_Canal_ID_Semana_0s_1h_100 as (partition by `Canal_ID` order by `Semana` rows_range between 1h open preceding and 0s preceding MAXSIZE 100),
    train_Dev_uni_proxima_Semana_0s_1h_200 as (partition by `Dev_uni_proxima` order by `Semana` rows_range between 1h open preceding and 0s preceding MAXSIZE 200))
as out0
last join
(
select
    `train`.`id` as id_4,
    `cliente_tabla_Cliente_ID`.`NombreCliente` as cliente_tabla_NombreCliente_multi_direct_3,
    `producto_tabla_Producto_ID`.`NombreProducto` as producto_tabla_NombreProducto_multi_direct_4,
    `town_state_Agencia_ID`.`State` as town_state_State_multi_direct_5,
    `town_state_Agencia_ID`.`Town` as town_state_Town_multi_direct_6
from
    `train`
    last join `cliente_tabla` as `cliente_tabla_Cliente_ID` on `train`.`Cliente_ID` = `cliente_tabla_Cliente_ID`.`Cliente_ID`
    last join `producto_tabla` as `producto_tabla_Producto_ID` on `train`.`Producto_ID` = `producto_tabla_Producto_ID`.`Producto_ID`
    last join `town_state` as `town_state_Agencia_ID` on `train`.`Agencia_ID` = `town_state_Agencia_ID`.`Agencia_ID`)
as out1
on out0.id_1 = out1.id_4
;