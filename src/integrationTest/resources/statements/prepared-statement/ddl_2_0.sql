DROP TABLE IF EXISTS prepared_statement_2_0_test;
CREATE
FACT TABLE IF NOT EXISTS prepared_statement_2_0_test (
    make            STRING not null,
    location        GEOGRAPHY null
)
PRIMARY INDEX make;