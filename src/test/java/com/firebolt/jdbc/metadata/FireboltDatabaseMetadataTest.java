package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.QueryResult.Column;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.testutils.AssertionUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.firebolt.jdbc.metadata.MetadataColumns.*;
import static com.firebolt.jdbc.type.FireboltDataType.INTEGER;
import static com.firebolt.jdbc.type.FireboltDataType.TEXT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltDatabaseMetadataTest {

	@Mock
	private FireboltConnection fireboltConnection;

	@Mock
	private FireboltStatement statement;

	@InjectMocks
	private FireboltDatabaseMetadata fireboltDatabaseMetadata;

	@BeforeEach
	void init() throws SQLException {
		lenient().when(fireboltConnection.createStatement(any())).thenReturn(statement);
		lenient().when(fireboltConnection.createStatement()).thenReturn(statement);
		lenient().when(fireboltConnection.getCatalog()).thenReturn("db_name");
		lenient().when(fireboltConnection.getSessionProperties()).thenReturn(FireboltProperties.builder().build());
		lenient().when(statement.executeQuery(anyString())).thenReturn(FireboltResultSet.empty());
	}

	@Test
	void shouldReturnTableTypes() throws SQLException {
		ResultSet expectedResultSet = FireboltResultSet.of(QueryResult.builder()
				.columns(Collections.singletonList(QueryResult.Column.builder().name(TABLE_TYPE).type(TEXT).build()))
				.rows(Arrays.asList(Collections.singletonList("TABLE"), Collections.singletonList("VIEW"))).build());

		ResultSet actualResultSet = fireboltDatabaseMetadata.getTableTypes();

		AssertionUtil.assertResultSetEquality(expectedResultSet, actualResultSet);
	}

	@Test
	void shouldReturnCatalogs() throws SQLException {
		ResultSet expectedResultSet = FireboltResultSet.of(QueryResult.builder()
				.columns(Collections.singletonList(Column.builder().name(TABLE_CAT).type(TEXT).build()))
				.rows(Collections.singletonList(Collections.singletonList("db_name"))).build());

		ResultSet actualResultSet = fireboltDatabaseMetadata.getCatalogs();

		AssertionUtil.assertResultSetEquality(expectedResultSet, actualResultSet);
	}

	@Test
	void shouldNotReturnAnyProcedureColumns() throws SQLException {
		AssertionUtil.assertResultSetEquality(FireboltResultSet.empty(),
				fireboltDatabaseMetadata.getProcedureColumns(null, null, null, null));
	}

	@Test
	void shouldReturnDatabaseProduct() throws SQLException {
		assertEquals("Firebolt", fireboltDatabaseMetadata.getDatabaseProductName());
	}

	@Test
	void shouldReturnDriverName() throws SQLException {
		assertEquals("Firebolt JDBC Driver", fireboltDatabaseMetadata.getDriverName());
	}

	@Test
	void shouldReturnTrueWhenCorrectTransactionIsolationLevelIsSpecified() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsTransactionIsolationLevel(0));
	}

	@Test
	void shouldReturnFalseWhenIncorrectTransactionIsolationLevelIsSpecified() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsTransactionIsolationLevel(1));
	}

//	@Test
//	@Disabled // To enable once schemas are supported
//	void shouldGetSchemas() throws SQLException {
//		String expectedSql = "SELECT 'public' AS TABLE_SCHEM, 'default' AS TABLE_CATALOG FROM information_schema.databases";
//		when(statement.executeQuery(expectedSql)).thenReturn(new FireboltResultSet(getInputStreamForGetSchemas()));
//		ResultSet resultSet = fireboltDatabaseMetadata.getSchemas();
//		verify(statement).executeQuery(expectedSql);
//
//		ResultSet expectedResultSet = FireboltDatabaseMetadataResult.builder()
//				.columns(Arrays.asList(Column.builder().name(TABLE_SCHEM).type(STRING).build(),
//						Column.builder().name(TABLE_CATALOG).type(STRING).build()))
//				.rows(Arrays.asList(Arrays.asList("Tutorial_11_04", "default"), Arrays.asList("system", "default")))
//				.build().toResultSet();
//
//		verifyResultSetEquality(expectedResultSet, resultSet);
//	}

	@Test
	void shouldGetColumns() throws SQLException {
		String expectedQuery = "SELECT table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position FROM information_schema.columns WHERE table_name LIKE 'c' AND column_name LIKE 'd' AND table_schema LIKE 'b'";

		ResultSet expectedResultSet = FireboltResultSet.of(QueryResult.builder()
				.columns(Arrays.asList(Column.builder().name(TABLE_CAT).type(TEXT).build(),
						Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
						Column.builder().name(TABLE_NAME).type(TEXT).build(),
						Column.builder().name(COLUMN_NAME).type(TEXT).build(),
						Column.builder().name(DATA_TYPE).type(INTEGER).build(),
						Column.builder().name(TYPE_NAME).type(TEXT).build(),
						Column.builder().name(COLUMN_SIZE).type(INTEGER).build(),
						Column.builder().name(BUFFER_LENGTH).type(INTEGER).build(),
						Column.builder().name(DECIMAL_DIGITS).type(INTEGER).build(),
						Column.builder().name(NUM_PREC_RADIX).type(INTEGER).build(),
						Column.builder().name(NULLABLE).type(INTEGER).build(),
						Column.builder().name(REMARKS).type(TEXT).build(),
						Column.builder().name(COLUMN_DEF).type(TEXT).build(),
						Column.builder().name(SQL_DATA_TYPE).type(INTEGER).build(),
						Column.builder().name(SQL_DATETIME_SUB).type(INTEGER).build(),
						Column.builder().name(CHAR_OCTET_LENGTH).type(INTEGER).build(),
						Column.builder().name(ORDINAL_POSITION).type(INTEGER).build(),
						Column.builder().name(IS_NULLABLE).type(TEXT).build(),
						Column.builder().name(SCOPE_CATALOG).type(TEXT).build(),
						Column.builder().name(SCOPE_SCHEMA).type(TEXT).build(),
						Column.builder().name(SCOPE_TABLE).type(TEXT).build(),
						Column.builder().name(SOURCE_DATA_TYPE).type(INTEGER).build(),
						Column.builder().name(IS_AUTOINCREMENT).type(TEXT).build(),
						Column.builder().name(IS_GENERATEDCOLUMN).type(TEXT).build()))
				.rows(Collections.singletonList(Arrays.asList("db_name", "Tutorial_11_04", // schema
						"D2_TIMESTAMP", // table name
						"id", // column name
						Types.INTEGER, // sql data type
						"integer", // shorter type name
						0, // Precision of INT
						null, // buffer length (not used, see Javadoc)
						0, 10, // base of a number system / radix
						0, null, // description of the column
						null, null, // SQL_DATA_TYPE - reserved for future use (see javadoc)
						null, // SQL_DATETIME_SUB - reserved for future use (see javadoc)
						null, // CHAR_OCTET_LENGTH - The maximum length of binary and character
						// based
						// columns (null for others)
						1, // The ordinal position
						"NO", null, // "SCOPE_CATALOG - Unused
						null, // "SCOPE_SCHEMA" - Unused
						null, // "SCOPE_TABLE" - Unused
						null, // "SOURCE_DATA_TYPE" - Unused
						"NO", // IS_AUTOINCREMENT - Not supported
						"NO")))
				.build());

		when(statement.executeQuery(expectedQuery))
				.thenReturn(new FireboltResultSet(this.getInputStreamForGetColumns()));

		ResultSet resultSet = fireboltDatabaseMetadata.getColumns("a", "b", "c", "d");
		verify(statement).executeQuery(expectedQuery);
		AssertionUtil.assertResultSetEquality(expectedResultSet, resultSet);
	}

	@Test
	void shouldGetTypeInfo() throws SQLException {
		ResultSet resultSet = fireboltDatabaseMetadata.getTypeInfo();
		ResultSet expectedTypeInfo = new FireboltResultSet(this.getExpectedTypeInfo());
		AssertionUtil.assertResultSetEquality(expectedTypeInfo, resultSet);
	}

	@Test
	void shouldGetTables() throws SQLException {
		String expectedSqlForTables = "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema LIKE 'def%' AND table_name LIKE 'tab%' AND table_type NOT LIKE 'EXTERNAL' order by table_schema, table_name";

		String expectedSqlForViews = "SELECT table_schema, table_name FROM information_schema.views WHERE table_schema LIKE 'def%' AND table_name LIKE 'tab%' order by table_schema, table_name";

		when(statement.executeQuery(expectedSqlForTables))
				.thenReturn(new FireboltResultSet(getInputStreamForGetTables()));
		when(statement.executeQuery(expectedSqlForViews)).thenReturn(FireboltResultSet.empty());

		ResultSet resultSet = fireboltDatabaseMetadata.getTables("catalog", "def%", "tab%", null);

		verify(statement).executeQuery(expectedSqlForTables);
		verify(statement).executeQuery(expectedSqlForViews);

		List<List<?>> expectedRows = new ArrayList<>();
		expectedRows.add(Arrays.asList("db_name", "public", "ex_lineitem", "TABLE", null, null, null, null, null, null,
				null, null, null));
		expectedRows.add(Arrays.asList("db_name", "public", "test_1", "TABLE", null, null, null, null, null, null, null,
				null, null));

		ResultSet expectedResultSet = FireboltResultSet.of(QueryResult.builder()
				.columns(Arrays.asList(Column.builder().name(TABLE_CAT).type(TEXT).build(),
						Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
						Column.builder().name(TABLE_NAME).type(TEXT).build(),
						Column.builder().name(TABLE_TYPE).type(TEXT).build(),
						Column.builder().name(REMARKS).type(TEXT).build(),
						Column.builder().name(TYPE_CAT).type(TEXT).build(),
						Column.builder().name(TYPE_SCHEM).type(TEXT).build(),
						Column.builder().name(TYPE_NAME).type(TEXT).build(),
						Column.builder().name(SELF_REFERENCING_COL_NAME).type(TEXT).build(),
						Column.builder().name(REF_GENERATION).type(TEXT).build()))
				.rows(expectedRows).build());

		AssertionUtil.assertResultSetEquality(expectedResultSet, resultSet);
	}

	@Test
	void shouldGetDatabaseProductVersion() throws SQLException {
		Statement statement = mock(FireboltStatement.class);
		when(fireboltConnection.createStatement()).thenReturn(statement);
		when(fireboltConnection.getEngine()).thenReturn("test");
		when(statement.executeQuery("SELECT version FROM information_schema.engines WHERE engine_name iLIKE 'test%'"))
				.thenReturn(new FireboltResultSet(getInputStreamForGetVersion()));
		assertEquals("abcd_xxx_123", fireboltDatabaseMetadata.getDatabaseProductVersion());
	}

	@Test
	void shouldGetDatabaseMajorVersion() throws SQLException {
		Statement statement = mock(FireboltStatement.class);
		when(fireboltConnection.createStatement()).thenReturn(statement);
		when(fireboltConnection.getEngine()).thenReturn("test");
		when(statement.executeQuery("SELECT version FROM information_schema.engines WHERE engine_name iLIKE 'test%'"))
				.thenReturn(new FireboltResultSet(getInputStreamForGetVersion()));
		assertEquals(0, fireboltDatabaseMetadata.getDatabaseMajorVersion());
	}

	@Test
	void shouldGetDatabaseMinorVersion() throws SQLException {
		Statement statement = mock(FireboltStatement.class);
		when(fireboltConnection.createStatement()).thenReturn(statement);
		when(fireboltConnection.getEngine()).thenReturn("test");
		when(statement.executeQuery("SELECT version FROM information_schema.engines WHERE engine_name iLIKE 'test%'"))
				.thenReturn(new FireboltResultSet(getInputStreamForGetVersion()));
		assertEquals(0, fireboltDatabaseMetadata.getDatabaseMinorVersion());
	}

	@Test
	void getStringFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getStringFunctions);
	}

	@Test
	void getNumericFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getNumericFunctions);
	}

	@Test
	void getSystemFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getSystemFunctions);
	}

	@Test
	void getTimeDateFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getTimeDateFunctions);
	}

	@ParameterizedTest
	@CsvSource(value = {",true", "'',true", "abs,true", "SIN,true", "ThisFunctionDoesNotExist,false"}, delimiter = ',')
	void getFunctions(String functionNamePattern, boolean filled) throws SQLException {
		String previousFunction = null;
		int count = 0;
		for (ResultSet rs = fireboltDatabaseMetadata.getFunctions(null, null, functionNamePattern); rs.next();) {
			count++;
			String functionName = rs.getString("FUNCTION_NAME");
			System.out.println(functionName);
			String specificName = rs.getString("SPECIFIC_NAME");
			assertNotNull(functionName);
			assertNotNull(specificName);
			assertEquals(functionName, specificName);
			if (functionNamePattern != null) {
				assertTrue(StringUtils.containsIgnoreCase(functionName, functionNamePattern));
			}
			if (previousFunction != null) {
				assertTrue(previousFunction.compareToIgnoreCase(functionName) < 0);
			}
			previousFunction = functionName;
		}
		assertEquals(filled, count > 0);
	}

	private InputStream getInputStreamForGetColumns() {
		return FireboltDatabaseMetadata.class
				.getResourceAsStream("/responses/metadata/firebolt-response-get-columns-example");
	}

	private InputStream getInputStreamForGetTables() {
		return FireboltDatabaseMetadata.class
				.getResourceAsStream("/responses/metadata/firebolt-response-get-tables-example");
	}

	private InputStream getInputStreamForGetVersion() {
		return FireboltDatabaseMetadata.class
				.getResourceAsStream("/responses/metadata/firebolt-response-get-version-example");
	}

	private InputStream getExpectedTypeInfo() {
		InputStream is = FireboltDatabaseMetadata.class.getResourceAsStream("/responses/metadata/expected-types.csv");
		String typesWithTabs = new BufferedReader(
				new InputStreamReader(is, StandardCharsets.UTF_8))
				.lines()
				.collect(Collectors.joining("\n")).replaceAll(",","\t");
		return new ByteArrayInputStream(typesWithTabs.getBytes());

	}

	private void getFunctions(CheckedFunction<DatabaseMetaData, String> getter) throws SQLException {
		String functions = getter.apply(fireboltDatabaseMetadata);
		assertNotNull(functions);
		assertTrue(functions.length() > 0);
	}
}
