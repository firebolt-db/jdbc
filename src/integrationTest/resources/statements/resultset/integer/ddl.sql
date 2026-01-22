DROP TABLE IF EXISTS "integer_type_test" CASCADE;
CREATE
FACT TABLE IF NOT EXISTS "integer_type_test" (
    id INTEGER,
	my_int INTEGER NULL,
    my_int_not_null INTEGER NOT NULL,
    my_boolean BOOLEAN NULL,
    my_boolean_not_null BOOLEAN NOT NULL,
    my_bigint BIGINT NULL,
    my_bigint_not_null BIGINT NOT NULL,
    my_real REAL NULL,
    my_real_not_null REAL NOT NULL,
    my_double DOUBLE NULL,
    my_double_not_null DOUBLE NOT NULL,
    my_text TEXT NULL,
    my_text_not_null TEXT NOT NULL,
    my_numeric NUMERIC NULL,
    my_numeric_not_null NUMERIC NOT NULL
    )
primary index id;
INSERT INTO integer_type_test values (1, null, 1, null, false, null, 9223372036854775807, null, 12147483647.402823, null, 12147483647.4028231234, null, '12345', null, 123456789.123456789);
INSERT INTO integer_type_test values (2, 101, 1, true, false, -9223372036854775808, 9223372036854775807, -12147483647.99, 12147483647.123456, -12147483647.991234567, 12147483647.4028231234, '-123', 'not an int', -123456789.123456789, 123456789.123456789);