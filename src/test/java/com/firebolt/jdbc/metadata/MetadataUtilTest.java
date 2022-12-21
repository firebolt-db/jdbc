package com.firebolt.jdbc.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class MetadataUtilTest {

	/*
	 * @Test void shouldGetSchemasQueryWhenGettingQueryWithoutArguments() {
	 * assertEquals("SELECT 'public' AS TABLE_SCHEM, 'default' AS TABLE_CATALOG FROM information_schema.databases"
	 * , MetadataUtil.getSchemasQuery(null, null)); }
	 * 
	 * @Test
	 * 
	 * @Disabled // To enable once schemas are supported void
	 * shouldGetSchemasQueryWhenGettingQueryWithArguments() { assertEquals(
	 * "SELECT 'public' AS TABLE_SCHEM, 'default' AS TABLE_CATALOG FROM information_schema.databases WHERE catalog_name = 'catalog'"
	 * , MetadataUtil.getSchemasQuery("catalog", "schema%")); }
	 */

	@Test
	void shouldGetTablesQueryWhenGettingQueryWithArguments() {
		assertEquals(
				"SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema LIKE 'db' AND table_name LIKE 'tableName' AND table_type NOT LIKE 'EXTERNAL' order by table_schema, table_name",
				MetadataUtil.getTablesQuery("catalog", "db", "tableName"));
	}

	@Test
	void shouldGetTablesQueryWhenGettingQueryWithoutArguments() {
		assertEquals(
				"SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_type NOT LIKE 'EXTERNAL' order by table_schema, table_name",
				MetadataUtil.getTablesQuery(null, null, null));
	}

	@Test
	void shouldGetViewsQueryWhenGettingQueryWithArguments() {
		assertEquals(
				"SELECT table_schema, table_name FROM information_schema.views WHERE table_schema LIKE 'schem' AND table_name LIKE 'tableName' order by table_schema, table_name",
				MetadataUtil.getViewsQuery("catalog", "schem", "tableName"));
	}

	@Test
	void shouldGetViewsQueryWhenGettingQueryWithoutArguments() {
		assertEquals("SELECT table_schema, table_name FROM information_schema.views order by table_schema, table_name",
				MetadataUtil.getViewsQuery(null, null, null));
	}

	@Test
	void shouldGetColumnsQueryWhenGettingQueryWithArguments() {
		assertEquals(
				"SELECT table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position FROM information_schema.columns WHERE table_name LIKE 'tableName' AND column_name LIKE 'col%' AND table_schema LIKE 'schema'",
				MetadataUtil.getColumnsQuery("schema", "tableName", "col%"));
	}

	@Test
	void shouldGetColumnsQueryWhenGettingQueryWithoutArguments() {
		assertEquals(
				"SELECT table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position FROM information_schema.columns",
				MetadataUtil.getColumnsQuery(null, null, null));
	}

	@Test
	void shouldGetVersionQuery() {
		assertEquals("SELECT version FROM information_schema.engines WHERE engine_name iLIKE 'test%'",
				MetadataUtil.getDatabaseVersionQuery("test"));
	}
}
