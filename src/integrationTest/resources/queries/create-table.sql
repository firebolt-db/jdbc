CREATE FACT TABLE IF NOT EXISTS "integration_test" (
    id BIGINT,
	ts timestamp NULL,
    content text NULL,
	success BOOLEAN NULL,
    year int
)
primary index ts