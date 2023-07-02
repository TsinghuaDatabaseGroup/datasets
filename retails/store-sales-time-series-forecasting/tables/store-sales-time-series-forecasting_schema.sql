CREATE TABLE holidays_events (date TIMESTAMP, type VARCHAR, locale VARCHAR, locale_name VARCHAR, description VARCHAR, transferred VARCHAR);
CREATE TABLE main (id INT, date TIMESTAMP, store_nbr INT, family VARCHAR, sales DOUBLE PRECISION, onpromotion INT);
CREATE TABLE oil (date TIMESTAMP, dcoilwtico DOUBLE PRECISION);
CREATE TABLE stores (store_nbr INT, city VARCHAR, state VARCHAR, type VARCHAR, cluster INT);
CREATE TABLE transactions (date TIMESTAMP, store_nbr INT, transactions INT);
