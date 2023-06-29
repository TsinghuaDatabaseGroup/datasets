CREATE TABLE item_categories
(
    item_category_name VARCHAR,
    item_category_id   INT
);
CREATE TABLE items
(
    item_name        VARCHAR,
    item_id          VARCHAR,
    item_category_id VARCHAR
);
CREATE TABLE sales_train
(
    date           VARCHAR,
    date_block_num INT,
    shop_id        INT,
    item_id        INT,
    item_price     DOUBLE PRECISION,
    item_cnt_day   DOUBLE PRECISION
);
CREATE TABLE shops
(
    shop_name VARCHAR,
    shop_id   INT
);
CREATE TABLE test
(
    ID      INT,
    shop_id INT,
    item_id INT
);
