DROP TABLE IF EXISTS prepared_statement_test;
CREATE
FACT TABLE IF NOT EXISTS prepared_statement_test (
    make            STRING null,
    sales           bigint not null,
	ts              timestamp NULL,
	d               date NULL,
    signature       bytea null
)
PRIMARY INDEX make;