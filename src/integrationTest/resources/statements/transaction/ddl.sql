DROP TABLE IF EXISTS transaction_test;
CREATE FACT TABLE IF NOT EXISTS transaction_test (
id              LONG,
name            VARCHAR(100)
)