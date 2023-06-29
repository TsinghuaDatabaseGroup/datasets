CREATE TABLE app_events
(
    event_id     INT,
    app_id       BIGINT,
    is_installed INT,
    is_active    INT
);
CREATE TABLE app_labels
(
    app_id   BIGINT,
    label_id INT
);
CREATE TABLE events
(
    event_id  INT,
    device_id BIGINT,
    timestamp TIMESTAMP,
    longitude DOUBLE PRECISION,
    latitude  DOUBLE PRECISION
);
CREATE TABLE gender_age_train
(
    device_id BIGINT,
    gender    VARCHAR,
    age       INT,
    group0     VARCHAR
);
CREATE TABLE label_categories
(
    label_id INT,
    category VARCHAR
);
CREATE TABLE phone_brand_device_model
(
    device_id    BIGINT,
    phone_brand  VARCHAR,
    device_model VARCHAR
);
