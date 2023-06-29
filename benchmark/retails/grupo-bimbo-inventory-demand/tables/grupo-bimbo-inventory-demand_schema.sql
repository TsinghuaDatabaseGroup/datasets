CREATE TABLE cliente_tabla
(
    Cliente_ID    INT,
    NombreCliente VARCHAR
);
CREATE TABLE producto_tabla
(
    Producto_ID    INT,
    NombreProducto VARCHAR
);
CREATE TABLE town_state
(
    Agencia_ID INT,
    Town       VARCHAR,
    State      VARCHAR
);
CREATE TABLE train
(
    id                INT,
    Semana            VARCHAR,
    Agencia_ID        INT,
    Canal_ID          INT,
    Ruta_SAK          INT,
    Cliente_ID        INT,
    Producto_ID       INT,
    Venta_uni_hoy     INT,
    Venta_hoy         DOUBLE PRECISION,
    Dev_uni_proxima   INT,
    Dev_proxima       DOUBLE PRECISION,
    Demanda_uni_equil INT
);
