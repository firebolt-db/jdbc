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
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_CATALOG;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_SCHEM;
import static com.firebolt.jdbc.type.FireboltDataType.INTEGER;
import static com.firebolt.jdbc.type.FireboltDataType.TEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltDatabaseMetadataTest {

	@Mock
	private FireboltConnection fireboltConnection;

	@Mock
	private FireboltStatement statement;

//	@InjectMocks
	private FireboltDatabaseMetadata fireboltDatabaseMetadata;

	@BeforeEach
	void init() throws SQLException {
		lenient().when(fireboltConnection.createStatement(any())).thenReturn(statement);
		lenient().when(fireboltConnection.createStatement()).thenReturn(statement);
		lenient().when(fireboltConnection.getCatalog()).thenReturn("db_name");
		lenient().when(fireboltConnection.getSessionProperties()).thenReturn(FireboltProperties.builder().database("my-db").build());
		lenient().when(statement.executeQuery(anyString())).thenReturn(FireboltResultSet.empty());
		fireboltDatabaseMetadata = new FireboltDatabaseMetadata("jdbc:firebolt:host", fireboltConnection);
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
	void shouldReturnSchemas() throws SQLException {
		ResultSet expectedResultSet = FireboltResultSet.of(QueryResult.builder()
				.columns(List.of(
						Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
						Column.builder().name(TABLE_CATALOG).type(TEXT).build()))
				.rows(List.of(List.of("public", "my-db"), List.of("information_schema", "my-db"), List.of("catalog", "my-db")))
				.build());

		ResultSet actualResultSet = fireboltDatabaseMetadata.getSchemas();

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
	void shouldGetConnection() throws SQLException {
		assertSame(fireboltConnection, fireboltDatabaseMetadata.getConnection());
	}

	@Test
	void shouldGetUrl() throws SQLException {
		assertEquals("jdbc:firebolt:host", fireboltDatabaseMetadata.getURL());
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
//				.rows(Arrays.asList(Arrays.asList("Tutorial_11_04", "default"), Arrays.asList(SYSTEM_ENGINE_NAME, "default")))
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
	void shouldGetDriverMajorVersion() {
		assertEquals(3, fireboltDatabaseMetadata.getDriverMajorVersion());
	}

	@Test
	void shouldGetDriverMinorVersion() {
		assertEquals(0, fireboltDatabaseMetadata.getDriverMinorVersion());
	}

	@Test
	void shouldGetDriverVersion() throws SQLException {
		assertEquals("3.0.0", fireboltDatabaseMetadata.getDriverVersion());
	}

	@Test
	void shouldGetJdbcMajorVersion() throws SQLException {
		assertEquals(4, fireboltDatabaseMetadata.getJDBCMajorVersion());
	}

	@Test
	void shouldGetJdbcManorVersion() throws SQLException {
		assertEquals(3, fireboltDatabaseMetadata.getJDBCMinorVersion());
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

	@ParameterizedTest
	@CsvSource({
			"false,,false",
			"true,false,false",
			"true,true,true"
	})
	void isReadOnly(boolean next, Boolean value, boolean expectedReadOnly) throws SQLException {
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs  = mock(ResultSet.class);
		when(fireboltConnection.prepareStatement(anyString())).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.next()).thenReturn(next);
		if (value != null) {
			when(rs.getBoolean(1)).thenReturn(value);
		}
		assertEquals(expectedReadOnly, fireboltDatabaseMetadata.isReadOnly());
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
		getFunctions(md -> md.getFunctions(null, null, functionNamePattern), functionNamePattern, filled, false);
	}

	@ParameterizedTest
	@CsvSource(value = {",true", "'',true", "abs,true", "SIN,true", "ThisFunctionDoesNotExist,false"}, delimiter = ',')
	void getFunctionColumns(String functionNamePattern, boolean filled) throws SQLException {
		getFunctions(md -> md.getFunctionColumns(null, null, functionNamePattern, null), functionNamePattern, filled, true);
	}

	@ParameterizedTest
	@ValueSource(classes = {DatabaseMetaData.class, FireboltDatabaseMetadata.class, Wrapper.class})
	void successfulUnwrap(Class<?> clazz) throws SQLException {
		assertSame(fireboltDatabaseMetadata, fireboltDatabaseMetadata.unwrap(clazz));
	}

	@ParameterizedTest
	@ValueSource(classes = {Closeable.class, String.class})
	void failingUnwrap(Class<?> clazz) {
		assertThrows(SQLException.class, () -> fireboltDatabaseMetadata.unwrap(clazz));
	}

	@ParameterizedTest
	@ValueSource(classes = {DatabaseMetaData.class, FireboltDatabaseMetadata.class, Wrapper.class})
	void isWrapperFor(Class<?> clazz) throws SQLException {
		assertTrue(fireboltDatabaseMetadata.isWrapperFor(clazz));
	}

	@ParameterizedTest
	@ValueSource(classes = {Driver.class, Connection.class, String.class})
	void isNotWrapperFor(Class<?> clazz) throws SQLException {
		assertFalse(fireboltDatabaseMetadata.isWrapperFor(clazz));
	}

	void getFunctions(CheckedFunction<DatabaseMetaData, ResultSet> getter, String functionNamePattern, boolean filled, boolean allowDuplicates) throws SQLException {
		String previousFunction = null;
		int count = 0;
		for (ResultSet rs = getter.apply(fireboltDatabaseMetadata); rs.next();) {
			count++;
			String functionName = rs.getString("FUNCTION_NAME");
			String specificName = rs.getString("SPECIFIC_NAME");
			assertNotNull(functionName);
			assertNotNull(specificName);
			assertEquals(functionName, specificName);
			if (functionNamePattern != null) {
				assertTrue(StringUtils.containsIgnoreCase(functionName, functionNamePattern));
			}
			if (previousFunction != null) {
				int functionNameComparison = previousFunction.compareToIgnoreCase(functionName);
				if (allowDuplicates) {
					assertTrue(functionNameComparison <= 0);
				} else {
					assertTrue(functionNameComparison < 0);
				}
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
