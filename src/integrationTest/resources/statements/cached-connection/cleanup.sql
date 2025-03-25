DROP TABLE IF EXISTS statement_test CASCADE;
STOP ENGINE cached_test_second_engine WITH TERMINATE=true;
DROP ENGINE IF EXISTS cached_test_second_engine;
DROP DATABASE IF EXISTS cached_test_second_db;