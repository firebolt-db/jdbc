package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.testutils.TestTag;
import integration.ConnectionInfo;
import integration.ConnectionOptions;
import integration.EnvironmentCondition;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import kotlin.collections.ArrayDeque;
import lombok.CustomLog;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static integration.EnvironmentCondition.Attribute.databaseVersion;
import static java.lang.String.format;
import static java.sql.Statement.SUCCESS_NO_INFO;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
class StatementTest extends IntegrationTest {

	private static final long TEN_SECONDS_IN_MILLIS = TimeUnit.SECONDS.toMillis(10);

	// simulate the system engine by passing the engine as null
	private static final String NO_ENGINE = null;

	@BeforeEach
	void beforeEach() {
		executeStatementFromFile("/statements/statement/ddl.sql");
	}

	@AfterEach
	void afterEach() {
		executeStatementFromFile("/statements/statement/cleanup.sql");
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldSelect1() throws SQLException {
		try (Connection connection = createConnection();
			 ResultSet rs = connection.createStatement().executeQuery("SELECT 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

	@Test
	@EnabledIfSystemProperty(named = "engine", matches = ".+")
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	void shouldSelect1WithEngine() throws SQLException {
		try (Connection connection = createConnection(System.getProperty("engine")); Statement statement = connection.createStatement()) {
			statement.executeQuery("SELECT 1;");
			assertNotNull(statement.executeQuery("SELECT 1;"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldSelect1WithQueryTimeout() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.setQueryTimeout(10); // 10 seconds
			statement.executeQuery("SELECT 1;");
			assertNotNull(statement.executeQuery("SELECT 1;"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldReuseStatementWhenNotCloseOnCompletion() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.executeQuery("SELECT 1;");
			assertNotNull(statement.executeQuery("SELECT 1;"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldThrowExceptionWhenTryingToReuseStatementClosedOnCompletion() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.closeOnCompletion();
			statement.executeQuery("SELECT 1;");
			assertTrue(statement.isCloseOnCompletion());
			assertThrows(FireboltException.class, () -> statement.executeQuery("SELECT 1;"));
		}
	}

	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldThrowExceptionWhenExecutingWrongQuery() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			String errorMessage = assertThrows(FireboltException.class, () -> statement.executeQuery("select wrong query")).getMessage();
			assertTrue(errorMessage.contains("Column 'wrong' does not exist."));
		}
	}

	@Tag(TestTag.V1)
	@Test
	void shouldThrowExceptionWhenExecutingWrongQueryV1() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			String errorMessage = assertThrows(FireboltException.class, () -> statement.executeQuery("select wrong query")).getMessage();
			assertTrue(errorMessage.contains("wrong"));
			assertTrue(errorMessage.contains("Line 1"));
			assertTrue(errorMessage.contains("Column 8"));
		}
	}

	@Test
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@EnvironmentCondition(value = "4.2.0", attribute = databaseVersion, comparison = EnvironmentCondition.Comparison.GE)
	void shouldThrowExceptionWhenExecutingWrongQueryWithJsonError() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.execute("set advanced_mode=1");
			statement.execute("set enable_json_error_output_format=true");
			String errorMessage = assertThrows(FireboltException.class, () -> statement.executeQuery("select wrong query")).getMessage();
			assertTrue(errorMessage.contains("Column 'wrong' does not exist."));
			assertTrue(errorMessage.contains("Line 1"));
			assertTrue(errorMessage.contains("Column 8"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldReturnTrueWhenExecutingAStatementThatReturnsAResultSet() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			assertTrue(statement.execute("SELECT 1;"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldReturnTrueWhenExecutingMultiStatementWithFirstStatementReturningAResultSet() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			assertTrue(statement.execute("SELECT 1;"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldReturnFalseWhenExecutingMultiStatementWithFirstStatementNotReturningAResultSet() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			assertFalse(statement.execute("INSERT INTO statement_test(id) values (1); SELECT 1;"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldReturnLimitedNumberOfLines() throws SQLException {
		try (Connection connection = createConnection(); Statement insert = connection.createStatement()) {
			String valuesAsStr = IntStream.rangeClosed(1, 100).mapToObj(n -> format("(%d)", n)).collect(joining(","));
			insert.execute("INSERT INTO statement_test(id) values " + valuesAsStr);
			List<Integer> resultAll = IntStream.rangeClosed(1, 100).boxed().collect(toList());
			assertEquals(resultAll, selectIntValues(connection, 0));
			assertEquals(resultAll, selectIntValues(connection, 100));
			assertEquals(resultAll, selectIntValues(connection, 101));
			assertEquals(resultAll, selectIntValues(connection, 1000));

			List<Integer> result99 = IntStream.rangeClosed(1, 99).boxed().collect(toList());
			assertEquals(result99, selectIntValues(connection, 99));
			assertEquals(List.of(1, 2), selectIntValues(connection, 2));
			assertEquals(List.of(1), selectIntValues(connection, 1));
		}
	}

	private List<Integer> selectIntValues(Connection connection, int limit) throws SQLException {
		Statement select = connection.createStatement();
		select.setMaxRows(limit);
		List<Integer> result = new ArrayDeque<>();
		try  (ResultSet rs = select.executeQuery("select id from statement_test order by id")) {
			while (rs.next()) {
				result.add(rs.getInt(1));
			}
		}
		return result;
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldExecuteBatch() throws SQLException {
		int size = 10;
		try (Connection connection = createConnection(); Statement insert = connection.createStatement()) {
			for (int i = 0; i < size; i++) {
				insert.addBatch(format("INSERT INTO statement_test(id) values (%d)", i));
			}
			assertArrayEquals(IntStream.generate(() -> SUCCESS_NO_INFO).limit(size).toArray(), insert.executeBatch());
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldThrowExceptionWhenTryingToExecuteQueryThatWouldReturnMultipleResultSets() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			assertThrows(FireboltException.class, () -> statement.executeQuery("SELECT 1; SELECT 2;"));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldGetMultipleResultSets() throws SQLException {
		String sql = "  --Getting Multiple RS;\nSELECT 1; /* comment 1 ; ; ; */\n\n --Another comment ; \n  -- ; \n SELECT 2; /* comment 2 */";
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql);
				ResultSet resultSet = statement.getResultSet();
				resultSet.next();
				assertEquals(1, resultSet.getInt(1));
				resultSet.close();
				assertTrue(statement.getMoreResults());
				resultSet = statement.getResultSet();
				resultSet.next();
				assertEquals(2, resultSet.getInt(1));
			}

		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldNotCloseStatementWithCloseOnCompletionIfItHasMoreResults() throws SQLException {
		String sql = "SELECT 1;SELECT 2;";
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.closeOnCompletion();
				statement.execute(sql);
				ResultSet resultSet = statement.getResultSet();
				resultSet.close();
				assertFalse(statement.isClosed());
				statement.getMoreResults();
				resultSet = statement.getResultSet();
				resultSet.close();
				assertTrue(statement.isClosed());
			}

		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldGetBooleans() throws SQLException {
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT true, false, null::boolean;");
				resultSet.next();
				assertEquals(Boolean.TRUE, resultSet.getObject(1));
				assertTrue(resultSet.getBoolean(1));

				assertEquals(Boolean.FALSE, resultSet.getObject(2));
				assertFalse(resultSet.getBoolean(2));

				assertNull(resultSet.getObject(3));
				assertFalse(resultSet.getBoolean(3));
			}
		}
	}

	@Test
	@Tag(TestTag.V1)
	void setWrongParameterOnUserEngineForV1() throws SQLException {
		String errorMessage = format("parameter %s is not allowed", "foo");
		setWrongParameterOnUserEngine("SET foo=bar", Map.of(), errorMessage);
	}

	@Test
	@Tag(TestTag.V2)
	void setWrongParameterOnUserEngineForV2() throws SQLException {
		String errorMessage = format("query param not allowed: %s", "foo");
		setWrongParameterOnUserEngine("SET foo=bar", Map.of(), errorMessage);
	}

	@ParameterizedTest
	@CsvSource({
			"set foo=bar,query param not allowed: foo,0",
			"set foo=bar,Unknown setting foo,1"
	})
	@Tag(TestTag.V2)
	void setWrongParameterOnSystemEngineForV2(String setStatement, String expectedErrorMessage, String advancedMode) throws SQLException {
		try (Connection connection = createConnection(NO_ENGINE, Map.of("advanced_mode", advancedMode)); Statement statement = connection.createStatement()) {
			String message = assertThrows(SQLException.class, () -> statement.execute(setStatement)).getMessage();
			assertTrue(message.contains(expectedErrorMessage),
					format("Unexpected error message: '%s'. Message should contain statement: '%s''", message, expectedErrorMessage));
			assertEquals(Map.of("advanced_mode", advancedMode), ((FireboltConnection)connection).getSessionProperties().getAdditionalProperties());
		}
	}

	@Test
	@Tag(TestTag.CORE)
	void setWrongParameterOnUserEngineForCore() throws SQLException {
		String errorMessage = format("Unknown setting %s", "foo");
		setWrongParameterOnUserEngine("SET foo=bar", Map.of(), errorMessage);
	}

	@Test
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	void setCorrectThenWrongParameterOnUserEngine() throws SQLException {
		setWrongParameterOnUserEngine("SET time_zone = 'EST';SET bar=tar", Map.of("time_zone", "EST"), "bar");
	}

	@Test
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	void setWrongParameterOnSystemEngineOnUserEngine() throws SQLException {
		setWrongParameterOnUserEngine("SET foo=bar", Map.of(), "foo");
	}

	@Test
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	void setCorrectThenWrongParameterOnSystemEngine() throws SQLException {
		setWrongParameterOnUserEngine("SET time_zone = 'EST';SET bar=tar", Map.of("time_zone", "EST"), "bar");
	}

	@Test
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Tag(TestTag.SLOW)
	void canSetQueryLabelMultipleTimes() throws SQLException {
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				String currentTime = getCurrentUTCTime();

				String firstQueryLabel = "first query label " + RandomStringUtils.randomNumeric(4);
				statement.execute(String.format("SET query_label = '%s'", firstQueryLabel));

				String nextQueryAfterFirstLabelSet = "SELECT " + RandomStringUtils.randomNumeric(4)  + ";";
				statement.executeQuery(nextQueryAfterFirstLabelSet);

				// set a new query label
				String secondQueryLabel = "second query label " + RandomStringUtils.randomNumeric(4);
				statement.execute(String.format("SET query_label = '%s'", secondQueryLabel));

				String nextQueryAfterSecondLabelSet = "SELECT " + RandomStringUtils.randomNumeric(4) + ";";
				statement.executeQuery(nextQueryAfterSecondLabelSet);

				// sleep for some time to allow query history to execute
				sleepForMillis(TEN_SECONDS_IN_MILLIS);

				// check query history for both queries
				assertQueryFound(statement, firstQueryLabel, currentTime, nextQueryAfterFirstLabelSet);
				assertQueryFound(statement, secondQueryLabel, currentTime, nextQueryAfterSecondLabelSet);
			}
		}
	}

	@Test
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Tag(TestTag.SLOW)
	void willUseRandomQueryLabelIfNoneExplicitlySet() throws SQLException {
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				String currentTime = getCurrentUTCTime();

				String statementWithoutExplicitQueryLabel = "SELECT " + RandomStringUtils.randomNumeric(3)  + ";";
				statement.executeQuery(statementWithoutExplicitQueryLabel);

				// sleep for some time to allow query history to execute
				sleepForMillis(TEN_SECONDS_IN_MILLIS);

				String implicitQueryLabel = getQueryLabel(statement, currentTime, statementWithoutExplicitQueryLabel);
				assertNotNull(implicitQueryLabel, "Expected the default query label to be non null");
			}
		}
	}

	/**
	 * looks up a query in the query history. Returns the query text
	 * @param statement
	 * @return
	 */
	private void assertQueryFound(Statement statement, String queryLabelValue, String afterTimestamp, String queryText) throws SQLException {
		String queryHistoryQuery =
			"SELECT query_text " +
			"FROM information_schema.engine_query_history " +
			"WHERE query_label = '%s' and submitted_time > '%s' and query_text = '%s'; ";
		ResultSet resultSet = statement.executeQuery(String.format(queryHistoryQuery, queryLabelValue, afterTimestamp, queryText));
		assertTrue(resultSet.next(), "Did not find query with the specified query label");
	}

	private String getQueryLabel(Statement statement, String afterTimestamp, String queryText) throws SQLException {
		String queryHistoryQuery =
				"SELECT query_label " +
				"FROM information_schema.engine_query_history WHERE submitted_time > '%s' and query_text = '%s'; ";
		ResultSet resultSet = statement.executeQuery(String.format(queryHistoryQuery, afterTimestamp, queryText));
		assertTrue(resultSet.next(), "Did not find any query in history");
		return resultSet.getString(1);
	}

	/**
	 * Connect to DB using {@code advanced_mode} sent in JDBC URL and set {@code force_pgdate_timestampntz} that requires advanced mode.
	 * @throws SQLException if connection fails
	 */
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Test
	void successfulSettingOfPropertyThatRequiresAdvancedModeConfiguredWhenConnectionIsCreated() throws SQLException {
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), Map.of("advanced_mode", "true"))) {
			setParam(connection, "force_pgdate_timestampntz", "1");
		}
	}

	@Tag(TestTag.CORE)
	@Test
	void forcePgdateTimestampntzIsNotAKnownSettingForCore() throws SQLException {
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), Map.of("advanced_mode", "true"))) {
			SQLException exception = assertThrows(SQLException.class, () -> setParam(connection, "force_pgdate_timestampntz", "1"));
			assertTrue(exception.getMessage().contains("Unknown setting force_pgdate_timestampntz"));
		}
	}

	/**
	 * Connect to DB without {@code advanced_mode}. Then set {@code advanced_mode=1} and {@code force_pgdate_timestampntz} that requires advanced mode.
	 * @throws SQLException if connection fails
	 */
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Test
	void successfulSettingOfPropertyThatRequiresAdvancedModePreviouslySetAtRuntime() throws SQLException {
		try (Connection connection = createConnection()) {
			setParam(connection, "advanced_mode", "1");
			setParam(connection, "force_pgdate_timestampntz", "1");
		}
	}

	/**
	 * Try to set {@code force_pgdate_timestampntz} that requires advanced mode that was not set. This test will fail.
	 */
	@Test
	@Tag(TestTag.V2)
	void failedSettingPropertyThatRequiresAdvancedModeThatWasNotSet() throws SQLException {
		try (Connection connection = createConnection()) {
			assertFailingSet(connection, "force_pgdate_timestampntz");
		}
	}

	/**
	 * Connect to DB using {@code advanced_mode} sent in JDBC URL. Then set {@code advanced_mode=0} and
	 * try to set {@code force_pgdate_timestampntz} that requires advanced mode and therefore fails.
	 */
	@Test
	@Tag(TestTag.V2)
	void failedSettingPropertyThatRequiresAdvancedModeThatWasUnset() throws SQLException {
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), Map.of("advanced_mode", "1"))) {
			setParam(connection, "advanced_mode", "0");
			assertFailingSet(connection, "force_pgdate_timestampntz");
		}
	}

	private void assertFailingSet(Connection connection, String paramName) {
		FireboltException e = assertThrows(FireboltException.class, () -> setParam(connection, paramName, "1"));
		assertTrue(e.getMessage().contains(paramName) && e.getMessage().contains("not allowed"), format("error message say that parameter %s is not allowed but was %s", paramName, e.getMessage()));
	}

	private void setWrongParameterOnUserEngine(String set, Map<String, String> expectedAdditionalProperties, String expectedErrorMessage) throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			String message = assertThrows(SQLException.class, () -> statement.execute(set)).getMessage();
			assertTrue(message.contains(expectedErrorMessage),
					format("Unexpected error message: '%s'. Message should contain statement: '%s''", message, expectedErrorMessage));
			assertEquals(expectedAdditionalProperties, ((FireboltConnection)connection).getSessionProperties().getAdditionalProperties());
		}
	}

	private void setWrongParameterOnSystemEngine(String set, Map<String, String> expectedAdditionalProperties, String expectedErrorMessage) throws SQLException {
		try (Connection connection = createConnection(NO_ENGINE); Statement statement = connection.createStatement()) {
			String message = assertThrows(SQLException.class, () -> statement.execute(set)).getMessage();
			assertTrue(message.contains(expectedErrorMessage),
					format("Unexpected error message: '%s'. Message should contain statement: '%s''", message, expectedErrorMessage));
			assertEquals(expectedAdditionalProperties, ((FireboltConnection)connection).getSessionProperties().getAdditionalProperties());
		}
	}

	@ParameterizedTest(name = "query:{0};")
	@ValueSource(strings = {
			"",
			"     ",
			"--",
			"-- SELECT 1",
			"/* {\"app\": \"dbt\", \"dbt_version\": \"0.20.0\", \"profile_name\": \"jaffle_shop\", \"target_name\": \"fb_app\", \"connection_name\": \"macro_stage_external_sources\"} */"
	})
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	void empty(String sql) throws SQLException {
		try (Connection connection = createConnection()) {
			assertFalse(connection.createStatement().execute(sql));
		}
	}

	/**
	 * This test validates that null values are sorted last.
	 * @throws SQLException if something is going wrong
	 * see com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#nullSorting
	 */
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void nullSortOrder() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery("select x from (select null as x union all select '' as x union all select 'a' as x) order by x")) {
			List<String> actuals = new ArrayList<>();
			while(rs.next()) {
				actuals.add(rs.getString(1));
			}
			assertEquals(Arrays.asList("", "a", null), actuals);
		}
	}

	/**
	 * This test proves that unquoted query columns become lower case and quoted columns preserve case
	 * @throws SQLException if something is going wrong
	 * see com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#identifiersCase
	 * see com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#quotedIdentifiersCase
	 */
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@ParameterizedTest
	@CsvSource(value = {
			"select 1 as lower, 2 as UPPER, 3 AS MiXeD;lower,upper,mixed",
			"select 1 as \"lower\", 2 as \"UPPER\", 3 AS \"MiXeD\";lower,UPPER,MiXeD"
	}, delimiter = ';')
	void mixedCaseSelect(String query, String expectedColumns) throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery(query)) {
			ResultSetMetaData md = rs.getMetaData();
			int n = md.getColumnCount();
			List<String> names = new ArrayList<>();
			for (int i = 1; i <= n; i++) {
				names.add(md.getColumnName(i));
			}
			assertEquals(Arrays.asList(expectedColumns.split(",")), names);
		}
	}

	/**
	 * This test proves that unquoted table name is stored in lower case and quoted in mixed case
	 * @throws SQLException if something is going wrong
	 * see com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#identifiersCase
	 * see com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#quotedIdentifiersCase
	 */
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@ParameterizedTest
	@CsvSource(value = {
			"CREATE FACT TABLE Case_Test (x long);case_test;Case_Test;case_test",
			"CREATE FACT TABLE \"Case_Test\" (x long);Case_Test;case_test;\"Case_Test\""
	}, delimiter = ';')
	void mixedCaseTable(String createTable, String expectedTableName, String unexpectedTableName, String dropTableName) throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			try {
				statement.executeUpdate(createTable);
				try (ResultSet rs = statement.executeQuery(format("select table_name from information_schema.tables where table_name  = '%s'", expectedTableName))) {
					assertTrue(rs.next());
					assertEquals(expectedTableName, rs.getString(1));
					assertFalse(rs.next());
				}
				try (ResultSet rs = statement.executeQuery(format("select table_name from information_schema.tables where table_name  = '%s'", unexpectedTableName))) {
					assertFalse(rs.next());
				}
			} finally {
				statement.executeUpdate("DROP TABLE IF EXISTS " + dropTableName);
			}
		}
	}

	/**
	 * Validates that specific statement fails because used function is not supported. If specific test fails, i.e.
	 * the query succeeds this means that used function is supported now. In this case corresponding unit test should be
	 * fixed too.
	 * @param query the SQL statement that should fail
	 * @throws SQLException if something is going wrong
	 */
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@ParameterizedTest
	@ValueSource(strings = {
			"SELECT CONVERT(varchar, 3.14)" //com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#supportsConvert
	})
	void failingQuery(String query) throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			assertThrows(SQLException.class, () -> statement.execute(query));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void caseInsensitiveGetter() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery("select table_schema, table_name as NAME from information_schema.tables")) {
			while(rs.next()) {
				assertEquals(rs.getString("table_schema"), rs.getString("TABLE_SCHEMA"));
				assertEquals(rs.getString("table_schema"), rs.getString("Table_Schema"));
				assertEquals(rs.getString("table_schema"), rs.getString("TaBlE_ScHeMa"));
				assertEquals(rs.getString("name"), rs.getString("NAME"));
				assertEquals(rs.getString("name"), rs.getString("NaMe"));
			}

		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@ParameterizedTest
	@ValueSource(ints = {1, 3, 5, 50})
	void maxFieldSize(int maxFieldSize) throws SQLException {
		String query = "select table_name from information_schema.tables order by table_name";
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.setMaxFieldSize(maxFieldSize);
			readValues(statement, query, 1).forEach(table -> assertThat(table.length(), Matchers.lessThanOrEqualTo(maxFieldSize)));
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@ParameterizedTest
	@ValueSource(ints = {0, -1, 100})
	void unlimitedMaxFieldSize(int maxFieldSize) throws SQLException {
		String query = "select table_name from information_schema.tables order by table_name";
		try (Connection connection = createConnection();
			 Statement unlimitedStatement = connection.createStatement();
			 Statement limitedStatement = connection.createStatement()) {
			limitedStatement.setMaxFieldSize(maxFieldSize);
			assertEquals(readValues(unlimitedStatement, query, 1), readValues(limitedStatement, query, 1));
		}
	}

	@ParameterizedTest
	@CsvSource({
			"USE ENGINE \"test_engine\",USE ENGINE is not supported in Firebolt Core"
	})
	@Tag(TestTag.CORE)
	void doNotSupportStatements(String sqlStatement, String expectedErrorMessage) throws SQLException {
		try (Connection connection = createConnectionWithOptions(ConnectionOptions.builder().url("http://localhost:3473").build()); Statement statement = connection.createStatement()) {
			SQLException exception = assertThrows(SQLException.class, () -> statement.executeUpdate(sqlStatement));
			assertTrue(exception.getMessage().contains(expectedErrorMessage));
		}
	}

	@Tag(TestTag.V2)
	@Test
	void cannotChangeToDatabaseThatDoesNotExist() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			SQLException exception = assertThrows(SQLException.class, () -> statement.executeUpdate("USE DATABASE \"a_db_that_does_not_exist\";"));
			assertTrue(exception.getMessage().contains("Database 'a_db_that_does_not_exist' does not exist or not authorized."));
		}
	}

	@Tag(TestTag.CORE)
	@Test
	void cannotChangeToDatabaseThatDoesNotExistForCore() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			// this test will start failing when the backend will fix their issue, just add the Tag(TestTag.CORE) annotation to the method cannotChangeToDatabaseThatDoesNotExist
			statement.executeUpdate("USE DATABASE \"a_db_that_does_not_exist\";");
		}
	}

	private Collection<String> readValues(Statement statement, String query, int columnIndex) throws SQLException {
		List<String> values = new ArrayList<>();
		try (ResultSet rs = statement.executeQuery(query)) {
			while(rs.next()) {
				values.add(rs.getString(columnIndex));
			}
		}
		return values;
	}

}
