DROP DATABASE IF EXISTS playerdata_dev;
CREATE DATABASE playerdata_dev;
GRANT SELECT, INSERT, UPDATE, DELETE, EVENT, TRIGGER ON `playerdata\_dev`.* TO 'event_admin'@'localhost'; ALTER USER 'event_admin'@'localhost' ;
