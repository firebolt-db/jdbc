DROP TABLE IF EXISTS prepared_statement_test;
CREATE
FACT TABLE IF NOT EXISTS prepared_statement_test (
intarray           array(int null) null,
textarray           array(text null) null
)
