package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.QueryResult.Column;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.testutils.AssertionUtil;
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
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.Wrapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.firebolt.jdbc.metadata.MetadataColumns.BUFFER_LENGTH;
import static com.firebolt.jdbc.metadata.MetadataColumns.CHAR_OCTET_LENGTH;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_DEF;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_SIZE;
import static com.firebolt.jdbc.metadata.MetadataColumns.DATA_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.DECIMAL_DIGITS;
import static com.firebolt.jdbc.metadata.MetadataColumns.GRANTEE;
import static com.firebolt.jdbc.metadata.MetadataColumns.GRANTOR;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_AUTOINCREMENT;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_GENERATEDCOLUMN;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_GRANTABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_NULLABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NULLABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NUM_PREC_RADIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.ORDINAL_POSITION;
import static com.firebolt.jdbc.metadata.MetadataColumns.PRIVILEGE;
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
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.DatabaseMetaData.sqlStateSQL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

	private DatabaseMetaData fireboltDatabaseMetadata;

	@BeforeEach
	void init() throws SQLException {
		fireboltDatabaseMetadata = new FireboltDatabaseMetadata("jdbc:firebolt:host", fireboltConnection);
		lenient().when(fireboltConnection.createStatement()).thenReturn(statement);
		lenient().when(fireboltConnection.getCatalog()).thenReturn("db_name");
		lenient().when(fireboltConnection.getSessionProperties()).thenReturn(FireboltProperties.builder().database("my-db").principal("the-user").build());
		lenient().when(statement.executeQuery(anyString())).thenReturn(createResultSet(new ByteArrayInputStream(new byte[0])));
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
				.rows(List.of(List.of("public", "my-db"), List.of("information_schema", "my-db")))
				.build());

		when(statement.executeQuery(anyString())).thenReturn(createResultSet(getInputStreamForGetSchemas()));
		ResultSet actualResultSet = fireboltDatabaseMetadata.getSchemas();

		AssertionUtil.assertResultSetEquality(expectedResultSet, actualResultSet);
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
		String expectedQuery = "SELECT table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position " +
				"FROM information_schema.columns WHERE table_name LIKE 'c' AND column_name LIKE 'd' " +
				"AND table_schema LIKE 'b' AND table_catalog LIKE 'a'";

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

		when(statement.executeQuery(expectedQuery)).thenReturn(createResultSet(getInputStreamForGetColumns()));

		ResultSet resultSet = fireboltDatabaseMetadata.getColumns("a", "b", "c", "d");
		verify(statement).executeQuery(expectedQuery);
		AssertionUtil.assertResultSetEquality(expectedResultSet, resultSet);
	}

	@Test
	void shouldGetColumnPrivileges() throws SQLException {
		String expectedQuery = "SELECT table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position " +
				"FROM information_schema.columns WHERE table_name LIKE 'c' AND column_name LIKE 'd' " +
				"AND table_schema LIKE 'b' AND table_catalog LIKE 'a'";

		ResultSet expectedResultSet = FireboltResultSet.of(QueryResult.builder()
				.columns(Arrays.asList(Column.builder().name(TABLE_CAT).type(TEXT).build(),
						Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
						Column.builder().name(TABLE_NAME).type(TEXT).build(),
						Column.builder().name(COLUMN_NAME).type(TEXT).build(),
						Column.builder().name(GRANTOR).type(TEXT).build(),
						Column.builder().name(GRANTEE).type(TEXT).build(),
						Column.builder().name(PRIVILEGE).type(TEXT).build(),
						Column.builder().name(IS_GRANTABLE).type(TEXT).build()))
				.rows(Collections.singletonList(Arrays.asList("db_name", "Tutorial_11_04", // schema
						"D2_TIMESTAMP", // table name
						"id", // column name
						null, // grantor
						null, // grantee
						null, // privilege
						"NO")))
				.build());

		when(statement.executeQuery(expectedQuery)).thenReturn(createResultSet(getInputStreamForGetColumns()));

		ResultSet resultSet = fireboltDatabaseMetadata.getColumnPrivileges("a", "b", "c", "d");
		verify(statement).executeQuery(expectedQuery);
		AssertionUtil.assertResultSetEquality(expectedResultSet, resultSet);
	}

	@Test
	void shouldGetTypeInfo() throws SQLException {
		ResultSet resultSet = fireboltDatabaseMetadata.getTypeInfo();
		ResultSet expectedTypeInfo = createResultSet(getExpectedTypeInfo());
		AssertionUtil.assertResultSetEquality(expectedTypeInfo, resultSet);
	}

	@Test
	void shouldGetTables() throws SQLException {
		String expectedSql = "SELECT table_schema, table_name, table_type FROM information_schema.tables " +
				"WHERE table_type IN ('BASE TABLE', 'DIMENSION', 'FACT', 'VIEW') AND table_catalog LIKE 'catalog' " +
				"AND table_schema LIKE 'def%' AND table_name LIKE 'tab%' order by table_schema, table_name";
		when(statement.executeQuery(expectedSql)).thenReturn(createResultSet(getInputStreamForGetTables()));
		ResultSet resultSet = fireboltDatabaseMetadata.getTables("catalog", "def%", "tab%", null);
		verify(statement).executeQuery(expectedSql);

		List<List<?>> expectedRows = List.of(
				Arrays.asList("db_name", "public", "ex_lineitem", "TABLE", null, null, null, null, null, null, null, null, null),
				Arrays.asList("db_name", "public", "test_1", "TABLE", null, null, null, null, null, null, null, null, null));

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
	void shouldGetTablePrivileges() throws SQLException {
		String expectedSql = "SELECT table_schema, table_name, table_type FROM information_schema.tables " +
				"WHERE table_type IN ('BASE TABLE', 'DIMENSION', 'FACT') AND table_catalog LIKE 'catalog' " +
				"AND table_schema LIKE 'def%' AND table_name LIKE 'tab%' order by table_schema, table_name";
		when(statement.executeQuery(expectedSql)).thenReturn(createResultSet(getInputStreamForGetTables()));
		ResultSet resultSet = fireboltDatabaseMetadata.getTablePrivileges("catalog", "def%", "tab%");
		verify(statement).executeQuery(expectedSql);

		List<List<?>> expectedRows = List.of(
				Arrays.asList("db_name", "public", "ex_lineitem", null, null, null, "NO"),
				Arrays.asList("db_name", "public", "test_1", null, null, null, "NO"));

		ResultSet expectedResultSet = FireboltResultSet.of(QueryResult.builder()
				.columns(Arrays.asList(Column.builder().name(TABLE_CAT).type(TEXT).build(),
						Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
						Column.builder().name(TABLE_NAME).type(TEXT).build(),
						Column.builder().name(GRANTOR).type(TEXT).build(),
						Column.builder().name(GRANTEE).type(TEXT).build(),
						Column.builder().name(PRIVILEGE).type(TEXT).build(),
						Column.builder().name(IS_GRANTABLE).type(TEXT).build()))
				.rows(expectedRows).build());

		AssertionUtil.assertResultSetEquality(expectedResultSet, resultSet);
	}

	@Test
	void shouldGetDriverMajorVersion() {
		assertEquals(3, fireboltDatabaseMetadata.getDriverMajorVersion());
	}

	@Test
	void shouldGetDriverMinorVersion() {
		assertEquals(4, fireboltDatabaseMetadata.getDriverMinorVersion());
	}

	@Test
	void shouldGetDriverVersion() throws SQLException {
		assertEquals("3.4.0", fireboltDatabaseMetadata.getDriverVersion());
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
		mockGetDatabaseVersion();
		assertEquals("abcd_xxx_123", fireboltDatabaseMetadata.getDatabaseProductVersion());
	}

	@Test
	void shouldGetDatabaseMajorVersion() throws SQLException {
		mockGetDatabaseVersion();
		assertEquals(0, fireboltDatabaseMetadata.getDatabaseMajorVersion());
	}

	@Test
	void shouldGetDatabaseMinorVersion() throws SQLException {
		mockGetDatabaseVersion();
		assertEquals(0, fireboltDatabaseMetadata.getDatabaseMinorVersion());
	}

	private void mockGetDatabaseVersion() throws SQLException {
		Statement statement = mock(FireboltStatement.class);
		when(fireboltConnection.createStatement()).thenReturn(statement);
		when(statement.executeQuery("SELECT VERSION()")).thenReturn(createResultSet(getInputStreamForGetVersion()));
	}

	@Test
	void isReadOnly() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.isReadOnly());
	}

	@Test
	void nullSorting() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.nullsAreSortedHigh());
		assertTrue(fireboltDatabaseMetadata.nullsAreSortedLow());
		assertFalse(fireboltDatabaseMetadata.nullsAreSortedAtStart());
		assertTrue(fireboltDatabaseMetadata.nullsAreSortedAtEnd());
	}

	@Test
	void useLocalFiles() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.usesLocalFiles());
		assertFalse(fireboltDatabaseMetadata.usesLocalFilePerTable());
	}

	@Test
	void identifiersCase() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsMixedCaseIdentifiers());
		assertFalse(fireboltDatabaseMetadata.storesMixedCaseIdentifiers());
		assertFalse(fireboltDatabaseMetadata.storesUpperCaseIdentifiers());
		assertTrue(fireboltDatabaseMetadata.storesLowerCaseIdentifiers());
	}

	@Test
	void quotedIdentifiersCase() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsMixedCaseQuotedIdentifiers());
		assertTrue(fireboltDatabaseMetadata.storesMixedCaseQuotedIdentifiers());
		assertFalse(fireboltDatabaseMetadata.storesUpperCaseQuotedIdentifiers());
		assertFalse(fireboltDatabaseMetadata.storesLowerCaseQuotedIdentifiers());
	}

	@Test
	void getIdentifierQuoteString() throws SQLException {
		assertEquals("\"", fireboltDatabaseMetadata.getIdentifierQuoteString());
	}

	@Test
	void getSearchStringEscape() throws SQLException {
		assertEquals("\\", fireboltDatabaseMetadata.getSearchStringEscape());
	}

	@Test
	void getExtraNameCharacters() throws SQLException {
		assertEquals("", fireboltDatabaseMetadata.getExtraNameCharacters());
	}

	@Test
	void supportsAlterTable() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsAlterTableWithAddColumn());
		assertFalse(fireboltDatabaseMetadata.supportsAlterTableWithDropColumn());
	}

	@Test
	void supportsColumnAliasing() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsColumnAliasing());
	}

	@Test
	void nullPlusNonNullIsNull() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.nullPlusNonNullIsNull());
	}

	@Test
	void supportsConvert() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsConvert());
		assertFalse(fireboltDatabaseMetadata.supportsConvert(Types.INTEGER, Types.VARCHAR));
	}


	@Test
	void correlationNames() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsTableCorrelationNames());
		assertFalse(fireboltDatabaseMetadata.supportsDifferentTableCorrelationNames());
	}

	@Test
	void supportsExpressionsInOrderBy() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsExpressionsInOrderBy());
		assertTrue(fireboltDatabaseMetadata.supportsOrderByUnrelated());
	}

	@Test
	void supportsGroupBy() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsGroupBy());
	}

	@Test
	void supportsGroupByUnrelated() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsGroupByUnrelated());
	}

	@Test
	void supportsGroupByBeyondSelect() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsGroupByBeyondSelect());
	}

	@Test
	void supportsLikeEscapeClause() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsLikeEscapeClause());
	}

	@Test
	void supportsMultipleResultSets() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsMultipleResultSets());
	}

	@Test
	void supportsMinimumSQLGrammar() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsMinimumSQLGrammar());
	}

	@Test
	void supportsCoreSQLGrammar() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsCoreSQLGrammar());
	}

	@Test
	void supportsExtendedSQLGrammar() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsExtendedSQLGrammar());
	}

	@Test
	void supportsANSI92EntryLevelSQL() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsANSI92EntryLevelSQL());
	}

	@Test
	void supportsANSI92IntermediateSQL() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsANSI92IntermediateSQL());
	}

	@Test
	void supportsANSI92FullSQL() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsANSI92FullSQL());
	}

	@Test
	void supportsIntegrityEnhancementFacility() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsIntegrityEnhancementFacility());
	}

	@Test
	void supportsTransactions() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsTransactions());
		assertFalse(fireboltDatabaseMetadata.supportsMultipleTransactions());
		assertTrue(fireboltDatabaseMetadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
		assertEquals(Connection.TRANSACTION_NONE, fireboltDatabaseMetadata.getDefaultTransactionIsolation());
		assertFalse(fireboltDatabaseMetadata.supportsDataManipulationTransactionsOnly());
		assertFalse(fireboltDatabaseMetadata.supportsDataDefinitionAndDataManipulationTransactions());
		assertFalse(fireboltDatabaseMetadata.dataDefinitionCausesTransactionCommit());

		for (int level : new int[] {TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE}) {
			assertFalse(fireboltDatabaseMetadata.supportsTransactionIsolationLevel(level));
		}
		assertFalse(fireboltDatabaseMetadata.supportsSavepoints());
		assertFalse(fireboltDatabaseMetadata.autoCommitFailureClosesAllResultSets());

		assertFalse(fireboltDatabaseMetadata.supportsOpenCursorsAcrossCommit());
		assertFalse(fireboltDatabaseMetadata.supportsOpenCursorsAcrossRollback());
		assertFalse(fireboltDatabaseMetadata.supportsOpenStatementsAcrossCommit());
		assertFalse(fireboltDatabaseMetadata.supportsOpenStatementsAcrossRollback());
		assertFalse(fireboltDatabaseMetadata.dataDefinitionIgnoredInTransactions());
	}

	@Test
	void supportsBatchUpdates() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsBatchUpdates());
	}

	@Test
	void supportsResultSetType() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
		assertFalse(fireboltDatabaseMetadata.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
		assertFalse(fireboltDatabaseMetadata.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
	}

	@Test
	void supportJoins() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsOuterJoins());
		assertTrue(fireboltDatabaseMetadata.supportsFullOuterJoins());
		assertTrue(fireboltDatabaseMetadata.supportsLimitedOuterJoins());
	}

	@Test
	void supportTerms() throws SQLException {
		assertEquals("schema", fireboltDatabaseMetadata.getSchemaTerm());
		assertEquals("procedure", fireboltDatabaseMetadata.getProcedureTerm());
		assertEquals("database", fireboltDatabaseMetadata.getCatalogTerm());
	}

	@Test
	void supportsCatalogs() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.isCatalogAtStart());
		assertEquals(".", fireboltDatabaseMetadata.getCatalogSeparator());
		assertFalse(fireboltDatabaseMetadata.supportsCatalogsInDataManipulation());
		assertFalse(fireboltDatabaseMetadata.supportsCatalogsInProcedureCalls());
		assertFalse(fireboltDatabaseMetadata.supportsCatalogsInTableDefinitions());
		assertFalse(fireboltDatabaseMetadata.supportsCatalogsInIndexDefinitions());
		assertFalse(fireboltDatabaseMetadata.supportsCatalogsInPrivilegeDefinitions());
	}

	@Test
	void supportsSchemas() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsSchemasInDataManipulation());
		assertFalse(fireboltDatabaseMetadata.supportsSchemasInProcedureCalls());
		assertFalse(fireboltDatabaseMetadata.supportsSchemasInTableDefinitions());
		assertFalse(fireboltDatabaseMetadata.supportsSchemasInIndexDefinitions());
		assertFalse(fireboltDatabaseMetadata.supportsSchemasInPrivilegeDefinitions());
	}

	@Test
	void supportsUnions() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsUnion());
		assertTrue(fireboltDatabaseMetadata.supportsUnionAll());
	}

	@Test
	void checkLimits() throws SQLException {
		assertEquals(63, fireboltDatabaseMetadata.getMaxColumnNameLength());
		assertEquals(63, fireboltDatabaseMetadata.getMaxSchemaNameLength());
		assertEquals(63, fireboltDatabaseMetadata.getMaxCatalogNameLength());
		assertEquals(63, fireboltDatabaseMetadata.getMaxTableNameLength());
		assertEquals(1000, fireboltDatabaseMetadata.getMaxColumnsInTable());
		assertEquals(0x40000, fireboltDatabaseMetadata.getMaxBinaryLiteralLength());
		assertEquals(0x40000, fireboltDatabaseMetadata.getMaxCharLiteralLength());
		assertEquals(65536, fireboltDatabaseMetadata.getMaxColumnsInGroupBy());
		assertEquals(16384, fireboltDatabaseMetadata.getMaxColumnsInOrderBy());
		assertEquals(8192, fireboltDatabaseMetadata.getMaxColumnsInSelect());
		assertEquals(0, fireboltDatabaseMetadata.getMaxColumnsInIndex());
		assertEquals(0, fireboltDatabaseMetadata.getMaxConnections());
		assertEquals(0, fireboltDatabaseMetadata.getMaxCursorNameLength());
		assertEquals(0, fireboltDatabaseMetadata.getMaxIndexLength());
		assertEquals(0, fireboltDatabaseMetadata.getMaxProcedureNameLength());
		assertEquals(0, fireboltDatabaseMetadata.getMaxStatements());
		assertEquals(63, fireboltDatabaseMetadata.getMaxUserNameLength());
		assertEquals(0, fireboltDatabaseMetadata.getMaxTablesInSelect());
		assertEquals(0, fireboltDatabaseMetadata.getMaxRowSize());
		assertEquals(0, fireboltDatabaseMetadata.getMaxStatementLength());
		assertTrue(fireboltDatabaseMetadata.doesMaxRowSizeIncludeBlobs());
	}

	@Test
	void supportsMultipleOpenResults() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsMultipleOpenResults());
	}

	@Test
	void supportsGetGeneratedKeys() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsGetGeneratedKeys());
	}

	@Test
	void supportsNonNullableColumns() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsNonNullableColumns());
	}

	@Test
	void supportsNamedParameters() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsNamedParameters());
	}

	@Test
	void holdability() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
		assertFalse(fireboltDatabaseMetadata.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
		assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, fireboltDatabaseMetadata.getResultSetHoldability());
	}

	@Test
	void getSQLStateType() throws SQLException {
		assertEquals(sqlStateSQL, fireboltDatabaseMetadata.getSQLStateType());
	}

	@Test
	void locatorsUpdateCopy() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.locatorsUpdateCopy());
	}

	@Test
	void supportsStatementPooling() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsStatementPooling());
	}

	@Test
	void getRowIdLifetime() throws SQLException {
		assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, fireboltDatabaseMetadata.getRowIdLifetime());
	}

	@Test
	void supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsStoredFunctionsUsingCallSyntax());
	}

	@Test
	void positioned() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsPositionedDelete());
		assertFalse(fireboltDatabaseMetadata.supportsPositionedUpdate());
	}

	@Test
	void emptyResultSets() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.getProcedureColumns(null, null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getUDTs(null, null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getUDTs(null, null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getSuperTypes(null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getSuperTables(null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getAttributes(null, null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getProcedures(null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getBestRowIdentifier(null, null, null, 0, false).next());
		assertFalse(fireboltDatabaseMetadata.getVersionColumns(null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getPrimaryKeys(null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getImportedKeys(null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getExportedKeys(null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getCrossReference(null, null, null, null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getClientInfoProperties().next());
		assertFalse(fireboltDatabaseMetadata.getPseudoColumns(null, null, null, null).next());
		assertFalse(fireboltDatabaseMetadata.getIndexInfo(null, null, null, true, true).next());
	}

	@Test
	void getStringFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getStringFunctions, "CONCAT", "SPLIT");
	}

	@Test
	void getSQLKeywords() throws SQLException {
		getFunctions(DatabaseMetaData::getSQLKeywords, "ACCOUNT", "COPY", "ENGINE", "TABLE");
	}

	@Test
	void getNumericFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getNumericFunctions, "ABS",  "RANDOM");
	}

	@Test
	void getSystemFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getSystemFunctions, "VERSION");
	}

	@Test
	void getTimeDateFunctions() throws SQLException {
		getFunctions(DatabaseMetaData::getTimeDateFunctions, "DATE_ADD", "DATE_DIFF");
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

	@Test
	void generatedKeyAlwaysReturned() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.generatedKeyAlwaysReturned());
	}

	@Test
	void allProceduresAreCallable() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.allProceduresAreCallable());
	}

	@Test
	void allTablesAreSelectable() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.allTablesAreSelectable());
	}

	@Test
	void getUserName() throws SQLException {
		assertEquals("the-user", fireboltDatabaseMetadata.getUserName());
	}

	@Test
	void supportsSelectForUpdate() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsSelectForUpdate());
	}

	@Test
	void supportsStoredProcedures() throws SQLException {
		assertFalse(fireboltDatabaseMetadata.supportsStoredProcedures());
	}

	@Test
	void supportsSubqueries() throws SQLException {
		assertTrue(fireboltDatabaseMetadata.supportsSubqueriesInComparisons());
		assertTrue(fireboltDatabaseMetadata.supportsSubqueriesInExists());
		assertTrue(fireboltDatabaseMetadata.supportsSubqueriesInIns());
		assertFalse(fireboltDatabaseMetadata.supportsSubqueriesInQuantifieds());
		assertTrue(fireboltDatabaseMetadata.supportsCorrelatedSubqueries());
	}


	@ParameterizedTest
	@CsvSource(value = {
			ResultSet.TYPE_FORWARD_ONLY + "," + ResultSet.CONCUR_READ_ONLY + ",true",
			ResultSet.TYPE_FORWARD_ONLY + "," + ResultSet.CONCUR_UPDATABLE + ",false",
			ResultSet.TYPE_SCROLL_INSENSITIVE + "," + ResultSet.CONCUR_READ_ONLY + ",false",
			ResultSet.TYPE_SCROLL_INSENSITIVE + "," + ResultSet.CONCUR_UPDATABLE + ",false",
			ResultSet.TYPE_SCROLL_SENSITIVE + "," + ResultSet.CONCUR_READ_ONLY + ",false",
			ResultSet.TYPE_SCROLL_SENSITIVE + "," + ResultSet.CONCUR_UPDATABLE + ",false",
	})
	void supportsResultSetConcurrency(int type, int concurrency, boolean expected) throws SQLException {
		assertEquals(expected, fireboltDatabaseMetadata.supportsResultSetConcurrency(type, concurrency));
	}

	@ParameterizedTest
	@ValueSource(ints = {ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE})
	void writeability(int type) throws SQLException {
		assertFalse(fireboltDatabaseMetadata.ownUpdatesAreVisible(type));
		assertFalse(fireboltDatabaseMetadata.ownDeletesAreVisible(type));
		assertFalse(fireboltDatabaseMetadata.ownInsertsAreVisible(type));
		assertFalse(fireboltDatabaseMetadata.othersUpdatesAreVisible(type));
		assertFalse(fireboltDatabaseMetadata.othersDeletesAreVisible(type));
		assertFalse(fireboltDatabaseMetadata.othersInsertsAreVisible(type));
		assertFalse(fireboltDatabaseMetadata.updatesAreDetected(type));
		assertFalse(fireboltDatabaseMetadata.deletesAreDetected(type));
		assertFalse(fireboltDatabaseMetadata.insertsAreDetected(type));
	}

	private void getFunctions(CheckedFunction<DatabaseMetaData, ResultSet> getter, String functionNamePattern, boolean filled, boolean allowDuplicates) throws SQLException {
		String previousFunction = null;
		int count = 0;
		Pattern pattern = functionNamePattern == null ? null : Pattern.compile(functionNamePattern, Pattern.CASE_INSENSITIVE);
		try (ResultSet rs = getter.apply(fireboltDatabaseMetadata)) {
			while (rs.next()) {
				count++;
				String functionName = rs.getString("FUNCTION_NAME");
				String specificName = rs.getString("SPECIFIC_NAME");
				assertNotNull(functionName);
				assertNotNull(specificName);
				assertEquals(functionName, specificName);
				if (pattern != null) {
					assertTrue(pattern.matcher(functionName).find());
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
		}
		assertEquals(filled, count > 0);
	}

	private InputStream getInputStreamForGetColumns() {
		return FireboltDatabaseMetadata.class.getResourceAsStream("/responses/metadata/firebolt-response-get-columns-example");
	}

	private InputStream getInputStreamForGetTables() {
		return FireboltDatabaseMetadata.class.getResourceAsStream("/responses/metadata/firebolt-response-get-tables-example");
	}

	private InputStream getInputStreamForGetSchemas() {
		return FireboltDatabaseMetadata.class.getResourceAsStream("/responses/metadata/firebolt-response-get-schemas-example");
	}

	private InputStream getInputStreamForGetVersion() {
		return FireboltDatabaseMetadata.class.getResourceAsStream("/responses/metadata/firebolt-response-get-version-example");
	}

	private InputStream getExpectedTypeInfo() {
		InputStream is = Objects.requireNonNull(FireboltDatabaseMetadata.class.getResourceAsStream("/responses/metadata/expected-types.csv"));
		String typesWithTabs = new BufferedReader(
				new InputStreamReader(is, StandardCharsets.UTF_8))
				.lines()
				.collect(Collectors.joining("\n")).replaceAll(",","\t");
		return new ByteArrayInputStream(typesWithTabs.getBytes());

	}

	private void getFunctions(CheckedFunction<DatabaseMetaData, String> getter, String ... examples) throws SQLException {
		String functions = getter.apply(fireboltDatabaseMetadata);
		assertNotNull(functions);
        assertFalse(functions.isEmpty());
		Arrays.stream(examples).forEach(example -> assertTrue(functions.contains(example), example + " is not found in list"));
	}

	private ResultSet createResultSet(InputStream is) throws SQLException {
		return new FireboltResultSet(is, null, null, 65535, false, null, true);
	}
}
