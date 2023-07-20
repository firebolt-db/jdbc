DROP TABLE IF EXISTS prepared_statement_test_array;
CREATE
FACT TABLE IF NOT EXISTS prepared_statement_test_array (
intarray           array(int null) null,
textarray           array(text null) null
)
