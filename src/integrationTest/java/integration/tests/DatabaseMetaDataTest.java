package integration.tests;

import integration.ConnectionInfo;
import integration.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.firebolt.jdbc.metadata.MetadataColumns.BUFFER_LENGTH;
import static com.firebolt.jdbc.metadata.MetadataColumns.CHAR_OCTET_LENGTH;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_DEF;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_SIZE;
import static com.firebolt.jdbc.metadata.MetadataColumns.DATA_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.DECIMAL_DIGITS;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_AUTOINCREMENT;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_GENERATEDCOLUMN;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_NULLABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NULLABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NUM_PREC_RADIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.ORDINAL_POSITION;
import static com.firebolt.jdbc.metadata.MetadataColumns.REF_GENERATION;
import static com.firebolt.jdbc.metadata.MetadataColumns.REMARKS;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCOPE_CATALOG;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCOPE_SCHEMA;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCOPE_TABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SELF_REFERENCING_COL_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.SOURCE_DATA_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SQL_DATA_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SQL_DATETIME_SUB;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_SCHEM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DatabaseMetaDataTest extends IntegrationTest {

	@BeforeEach
	void beforeAll() {
		executeStatementFromFile("/statements/metadata/ddl.sql");
	}

	@AfterEach
	void afterEach() {
		executeStatementFromFile("/statements/metadata/cleanup.sql");
	}

	@Test
	void shouldReturnSchema() throws SQLException {
		List<String> schemas = new ArrayList<>();
		List<String> catalogs = new ArrayList<>();
		try (Connection connection = createConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			try (ResultSet resultSet = databaseMetaData.getSchemas()) {
				while (resultSet.next()) {
					schemas.add(resultSet.getString("TABLE_SCHEM"));
					catalogs.add(resultSet.getString("TABLE_CATALOG"));
				}
			}
		}
		assertThat(schemas, containsInAnyOrder("public", "catalog", "information_schema"));
		String dbName = ConnectionInfo.getInstance().getDatabase();
		assertThat(catalogs, contains(dbName, dbName, dbName));
	}

	@Test
	void shouldReturnTable() throws SQLException {
		Map<String, List<String>> result = new HashMap<>();
		try (Connection connection = createConnection()) {
			try (ResultSet rs = connection.getMetaData().getTables(connection.getCatalog(), "public",
					"integration_test", null)) {
				ResultSetMetaData metadata = rs.getMetaData();
				while (rs.next()) {
					for (int i = 1; i <= metadata.getColumnCount(); i++) {
						result.computeIfAbsent(metadata.getColumnName(i), k -> new ArrayList<>()).add(rs.getString(i));
					}
				}
			}
		}

		result.forEach((k, v) -> assertEquals(1, v.size()));
		assertEquals(ConnectionInfo.getInstance().getDatabase(), result.get(TABLE_CAT).get(0));
		assertEquals("integration_test", result.get(TABLE_NAME).get(0));
		assertEquals("public", result.get(TABLE_SCHEM).get(0));
		assertEquals("TABLE", result.get(TABLE_TYPE).get(0));
		assertNull(result.get(SELF_REFERENCING_COL_NAME).get(0));
		assertNull(result.get(TYPE_SCHEM).get(0));
		assertNull(result.get(TYPE_CAT).get(0));
		assertNull(result.get(REMARKS).get(0));
		assertNull(result.get(REF_GENERATION).get(0));
		assertNull(result.get(TYPE_NAME).get(0));
	}

	@Test
	void shouldReturnColumns() throws SQLException {
		Map<String, List<String>> result = new HashMap<>();
		try (Connection connection = createConnection()) {
			try (ResultSet rs = connection.getMetaData().getColumns(connection.getCatalog(), "public",
					"integration_test", null)) {
				ResultSetMetaData metadata = rs.getMetaData();
				while (rs.next()) {
					for (int i = 1; i <= metadata.getColumnCount(); i++) {
						result.computeIfAbsent(metadata.getColumnName(i), k -> new ArrayList<>()).add(rs.getString(i));
					}
				}
			}
		}
		String database = ConnectionInfo.getInstance().getDatabase();
		String tableName = "integration_test";
		String schemaName = "public";
		assertThat(result.get(SCOPE_TABLE), contains(null, null, null, null, null, null, null));
		assertThat(result.get(IS_NULLABLE), contains("NO", "YES", "YES", "YES", "YES", "YES", "NO"));
		assertThat(result.get(BUFFER_LENGTH), contains(null, null, null, null, null, null, null));
		assertThat(result.get(TABLE_CAT), contains(database, database, database, database, database, database, database));
		assertThat(result.get(SCOPE_CATALOG), contains(null, null, null, null, null, null, null));
		assertThat(result.get(COLUMN_DEF), contains(null, null, null, null, null, null, null));
		assertThat(result.get(TABLE_NAME), contains(tableName, tableName, tableName, tableName, tableName, tableName, tableName));
		assertThat(result.get(COLUMN_NAME), contains("id", "ts", "tstz", "tsntz", "content", "success", "year"));
		assertThat(result.get(TABLE_SCHEM), contains(schemaName, schemaName, schemaName, schemaName, schemaName, schemaName, schemaName));
		assertThat(result.get(REMARKS), contains(null, null, null, null, null, null, null));
		assertThat(result.get(NULLABLE), contains("0", "1", "1", "1", "1", "1", "0"));
		assertThat(result.get(DECIMAL_DIGITS), contains("0", "0", "0", "0", "0", "0", "0"));
		assertThat(result.get(SQL_DATETIME_SUB), contains(null, null, null, null, null, null, null));
		assertThat(result.get(NUM_PREC_RADIX), contains("10", "10", "10", "10", "10", "10", "10"));
		assertThat(result.get(IS_GENERATEDCOLUMN), contains("NO", "NO", "NO", "NO", "NO", "NO", "NO"));
		assertThat(result.get(IS_AUTOINCREMENT), contains("NO", "NO", "NO", "NO", "NO", "NO", "NO"));
		assertThat(result.get(SQL_DATA_TYPE), contains(null, null, null, null, null, null, null));
		assertThat(result.get(CHAR_OCTET_LENGTH), contains(null, null, null, null, null, null, null));
		assertThat(result.get(SOURCE_DATA_TYPE), contains(null, null, null, null, null, null, null));
		assertThat(result.get(SCOPE_SCHEMA), contains(null, null, null, null, null, null, null));
		assertThat(result.get(ORDINAL_POSITION), contains("1", "2", "3", "4", "5", "6", "7"));
		assertThat(result.get(TYPE_NAME), contains("bigint", "timestamp", "timestamptz", "timestamp", "text", "boolean", "integer"));
		assertThat(result.get(DATA_TYPE), contains("-5", "93", "2014", "93", "12", "16", "4"));
		assertThat(result.get(COLUMN_SIZE), contains("0", "6", "6", "6", "0", "1", "0"));
	}

	@Test
	void shouldReturnColumnsFromSelect() throws SQLException {
		Map<Integer, Map<String, Object>> result = new TreeMap<>();
		try (Connection connection = createConnection()) {
			try (ResultSet rs = connection.createStatement().executeQuery("select * from integration_test")) {
				ResultSetMetaData metadata = rs.getMetaData();
				for (int i = 1; i <= metadata.getColumnCount(); i++) {
					result.put(i,
							new TreeMap<>(Map.of(
									"type", metadata.getColumnType(i),
									"typeName", metadata.getColumnTypeName(i),
									"className", metadata.getColumnClassName(i),
									"label", metadata.getColumnLabel(i),
									"name", metadata.getColumnName(i),
									"displaySize", metadata.getColumnDisplaySize(i)
							)));
				}
			}
		}

		Map<Integer, Map<String, Object>> expected = new TreeMap<>(Map.of(
				1, new TreeMap<>(Map.of("type", Types.BIGINT, "typeName", "bigint", "className", Long.class.getName(), "label", "id", "name", "id",  "displaySize", 80)),
				2, new TreeMap<>(Map.of("type", Types.TIMESTAMP, "typeName", "timestamp", "className", Timestamp.class.getName(), "label", "ts", "name", "ts",  "displaySize", 80)),
				3, new TreeMap<>(Map.of("type", Types.TIMESTAMP_WITH_TIMEZONE, "typeName", "timestamptz", "className", Timestamp.class.getName(), "label", "tstz", "name", "tstz",  "displaySize", 80)),
				4, new TreeMap<>(Map.of("type", Types.TIMESTAMP, "typeName", "timestamp", "className", Timestamp.class.getName(), "label", "tsntz", "name", "tsntz",  "displaySize", 80)),
				5, new TreeMap<>(Map.of("type", Types.VARCHAR, "typeName", "text", "className", String.class.getName(), "label", "content", "name", "content",  "displaySize", 80)),
				6, new TreeMap<>(Map.of("type", Types.BOOLEAN, "typeName", "boolean", "className", Boolean.class.getName(), "label", "success", "name", "success",  "displaySize", 80)),
				7, new TreeMap<>(Map.of("type", Types.INTEGER, "typeName", "integer", "className", Integer.class.getName(), "label", "year", "name", "year",  "displaySize", 80))
		));

		assertEquals(expected, result);

	}
}
