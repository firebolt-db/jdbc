DROP TABLE IF EXISTS first_statement_cancel_test;
DROP TABLE IF EXISTS second_statement_cancel_test;
CREATE
FACT TABLE IF NOT EXISTS first_statement_cancel_test (
id              LONG
)
PRIMARY INDEX id;
CREATE
FACT TABLE IF NOT EXISTS second_statement_cancel_test (
id              LONG
)
PRIMARY INDEX id;