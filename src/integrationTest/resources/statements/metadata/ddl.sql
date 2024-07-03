DROP TABLE IF EXISTS "integration_test" CASCADE;
CREATE
FACT TABLE IF NOT EXISTS "integration_test" (
    id BIGINT NOT NULL,
	ts timestamp NULL,
	tstz timestamptz NULL,
	tsntz timestampntz NULL,
    content text NULL,
	success BOOLEAN NOT NULL,
    year int NOT NULL
)
primary index id