CREATE TABLE store (Store INT, StoreType VARCHAR, Assortment VARCHAR, CompetitionDistance INT, CompetitionOpenSinceMonth INT, CompetitionOpenSinceYear INT, Promo2 INT, Promo2SinceWeek INT, Promo2SinceYear INT, PromoInterval VARCHAR);
CREATE TABLE train (Store INT, DayOfWeek INT, Date TIMESTAMP, Sales INT, Customers INT, Open INT, Promo INT, StateHoliday VARCHAR, SchoolHoliday INT);
