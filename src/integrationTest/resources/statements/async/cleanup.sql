DROP TABLE IF EXISTS "async_table_test" CASCADE;
STOP ENGINE async_test_second_engine WITH TERMINATE=true;
DROP ENGINE IF EXISTS async_test_second_engine;