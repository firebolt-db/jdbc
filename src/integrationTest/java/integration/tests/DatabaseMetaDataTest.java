package integration.tests;

import static com.firebolt.jdbc.metadata.MetadataColumns.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import integration.ConnectionInfo;
import integration.IntegrationTest;

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
		assertThat(schemas, Matchers.containsInAnyOrder("public", "catalog", "information_schema"));
		String dbName = ConnectionInfo.getInstance().getDatabase();
		assertThat(catalogs, Matchers.containsInAnyOrder(dbName, dbName, dbName));
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
		assertThat(result.get(SCOPE_TABLE), contains(null, null, null, null, null));
		assertThat(result.get(IS_NULLABLE), Matchers.containsInAnyOrder("NO", "YES", "YES", "YES", "NO"));
		assertThat(result.get(BUFFER_LENGTH), contains(null, null, null, null, null));
		assertThat(result.get(TABLE_CAT),
				Matchers.containsInAnyOrder(database, database, database, database, database));
		assertThat(result.get(SCOPE_CATALOG), contains(null, null, null, null, null));
		assertThat(result.get(COLUMN_DEF), contains(null, null, null, null, null));
		assertThat(result.get(TABLE_NAME),
				Matchers.containsInAnyOrder(tableName, tableName, tableName, tableName, tableName));
		assertThat(result.get(COLUMN_NAME), Matchers.containsInAnyOrder("id", "ts", "content", "success", "year"));
		assertThat(result.get(TABLE_SCHEM),
				Matchers.containsInAnyOrder(schemaName, schemaName, schemaName, schemaName, schemaName));
		assertThat(result.get(REMARKS), contains(null, null, null, null, null));
		assertThat(result.get(NULLABLE), Matchers.containsInAnyOrder("0", "1", "1", "1", "0"));
		assertThat(result.get(DECIMAL_DIGITS), Matchers.containsInAnyOrder("0", "0", "0", "0", "0"));
		assertThat(result.get(SQL_DATETIME_SUB), contains(null, null, null, null, null));
		assertThat(result.get(NUM_PREC_RADIX), Matchers.containsInAnyOrder("10", "10", "10", "10", "10"));
		assertThat(result.get(IS_GENERATEDCOLUMN), Matchers.containsInAnyOrder("NO", "NO", "NO", "NO", "NO"));
		assertThat(result.get(IS_AUTOINCREMENT), Matchers.containsInAnyOrder("NO", "NO", "NO", "NO", "NO"));
		assertThat(result.get(SQL_DATA_TYPE), contains(null, null, null, null, null));
		assertThat(result.get(CHAR_OCTET_LENGTH), contains(null, null, null, null, null));
		assertThat(result.get(SOURCE_DATA_TYPE), contains(null, null, null, null, null));
		assertThat(result.get(SCOPE_SCHEMA), contains(null, null, null, null, null));
		assertThat(result.get(ORDINAL_POSITION), Matchers.containsInAnyOrder("1", "2", "3", "4", "5"));
		assertThat(result.get(TYPE_NAME),
				Matchers.containsInAnyOrder("BIGINT", "TIMESTAMP", "STRING", "INTEGER", "INTEGER"));
		assertThat(result.get(DATA_TYPE), Matchers.containsInAnyOrder("-5", "93", "12", "-6", "4"));
		assertThat(result.get(COLUMN_SIZE), Matchers.containsInAnyOrder("20", "19", "0", "3", "11"));
	}

}
