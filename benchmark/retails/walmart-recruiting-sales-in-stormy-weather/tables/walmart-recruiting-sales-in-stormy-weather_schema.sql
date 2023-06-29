CREATE TABLE key
(
    store_nbr   INT,
    station_nbr INT
);
CREATE TABLE train
(
    date      TIMESTAMP,
    store_nbr INT,
    item_nbr  INT,
    units     INT
);
CREATE TABLE weather
(
    station_nbr INT,
    date        TIMESTAMP,
    tmax        VARCHAR,
    tmin        VARCHAR,
    tavg        VARCHAR,
    depart      VARCHAR,
    dewpoint    VARCHAR,
    wetbulb     VARCHAR,
    heat        VARCHAR,
    cool        VARCHAR,
    sunrise     VARCHAR,
    sunset      VARCHAR,
    codesum     VARCHAR,
    snowfall    VARCHAR,
    preciptotal VARCHAR,
    stnpressure VARCHAR,
    sealevel    VARCHAR,
    resultspeed VARCHAR,
    resultdir   VARCHAR,
    avgspeed    VARCHAR
);
