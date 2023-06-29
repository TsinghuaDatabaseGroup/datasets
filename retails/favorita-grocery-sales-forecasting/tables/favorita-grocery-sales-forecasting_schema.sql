CREATE TABLE holidays_events (date TIMESTAMP, type VARCHAR, locale VARCHAR, locale_name VARCHAR, description VARCHAR, transferred VARCHAR);
CREATE TABLE items (item_nbr INT, family VARCHAR, class INT, perishable INT);
CREATE TABLE oil (date TIMESTAMP, dcoilwtico DOUBLE PRECISION);
CREATE TABLE stores (store_nbr INT, city VARCHAR, state VARCHAR, type VARCHAR, cluster INT);
CREATE TABLE train (id INT, date TIMESTAMP, store_nbr INT, item_nbr INT, unit_sales DOUBLE PRECISION, onpromotion VARCHAR);
CREATE TABLE transactions (date TIMESTAMP, store_nbr INT, transactions INT);
