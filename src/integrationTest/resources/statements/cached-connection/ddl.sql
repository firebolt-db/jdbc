DROP TABLE IF EXISTS statement_test;
CREATE
FACT TABLE IF NOT EXISTS statement_test (
id              LONG
)
PRIMARY INDEX id;
CREATE ENGINE IF NOT EXISTS cached_test_second_engine;
CREATE DATABASE IF NOT EXISTS cached_test_second_db;


