DROP TABLE IF EXISTS prepared_statement_test;
CREATE
FACT TABLE IF NOT EXISTS prepared_statement_test (
make            STRING,
sales           bigint,
signature       bytea null
)
PRIMARY INDEX make;