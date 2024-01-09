package integration.tests;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.metadata.MetadataUtil;
import integration.IntegrationTest;
import kotlin.collections.ArrayDeque;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
class StatementTest extends IntegrationTest {

	@BeforeEach
	void beforeEach() {
		executeStatementFromFile("/statements/statement/ddl.sql");
	}

	@AfterEach
	void afterEach() {
		executeStatementFromFile("/statements/statement/cleanup.sql");
	}

	@Test
	void shouldSelect1() throws SQLException {
		try (Connection connection = this.createConnection();
			 ResultSet rs = connection.createStatement().executeQuery("SELECT 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

	@Test
	@EnabledIfSystemProperty(named = "engine", matches = ".+")
	void shouldSelect1WithEngine() throws SQLException {
		try (Connection connection = this.createConnection(System.getProperty("engine")); Statement statement = connection.createStatement()) {
			statement.executeQuery("SELECT 1;");
			assertNotNull(statement.executeQuery("SELECT 1;"));
		}
	}

	@Test
	void shouldSelect1WithQueryTimeout() throws SQLException {
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			statement.setQueryTimeout(10); // 10 seconds
			statement.executeQuery("SELECT 1;");
			assertNotNull(statement.executeQuery("SELECT 1;"));
		}
	}

	@Test
	void shouldReuseStatementWhenNotCloseOnCompletion() throws SQLException {
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			statement.executeQuery("SELECT 1;");
			assertNotNull(statement.executeQuery("SELECT 1;"));
		}
	}

	@Test
	void shouldThrowExceptionWhenTryingToReuseStatementClosedOnCompletion() throws SQLException {
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			statement.closeOnCompletion();
			statement.executeQuery("SELECT 1;");
			assertTrue(statement.isCloseOnCompletion());
			assertThrows(FireboltException.class, () -> statement.executeQuery("SELECT 1;"));
		}
	}

	@Test
	void shouldReturnTrueWhenExecutingAStatementThatReturnsAResultSet() throws SQLException {
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			assertTrue(statement.execute("SELECT 1;"));
		}
	}

	@Test
	void shouldReturnTrueWhenExecutingMultiStatementWithFirstStatementReturningAResultSet() throws SQLException {
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			assertTrue(statement.execute("SELECT 1;"));
		}
	}

	@Test
	void shouldReturnFalseWhenExecutingMultiStatementWithFirstStatementNotReturningAResultSet() throws SQLException {
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			assertFalse(statement.execute("INSERT INTO statement_test(id) values (1); SELECT 1;"));
		}
	}

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

	@Test
	void shouldThrowExceptionWhenTryingToExecuteQueryThatWouldReturnMultipleResultSets() throws SQLException {
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			assertThrows(FireboltException.class, () -> statement.executeQuery("SELECT 1; SELECT 2;"));
		}
	}

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

	@Test
	void shouldGetBooleans() throws SQLException {
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("SET bool_output_format=postgres;");
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

	@ParameterizedTest(name = "query:{0};")
	@ValueSource(strings = {
			"",
			"     ",
			"--",
			"-- SELECT 1",
			"/* {\"app\": \"dbt\", \"dbt_version\": \"0.20.0\", \"profile_name\": \"jaffle_shop\", \"target_name\": \"fb_app\", \"connection_name\": \"macro_stage_external_sources\"} */"
	})
	void empty(String sql) throws SQLException {
		try (Connection connection = createConnection()) {
			assertFalse(connection.createStatement().execute(sql));
		}
	}

	@Test
	void specialIntValue() throws SQLException {
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT 1/0");
				resultSet.next();
				assertThrows(IllegalArgumentException.class, () -> resultSet.getShort(1));
				assertThrows(IllegalArgumentException.class, () -> resultSet.getInt(1));
				assertThrows(IllegalArgumentException.class, () -> resultSet.getLong(1));
				assertEquals(Float.POSITIVE_INFINITY, resultSet.getFloat(1));
				assertEquals(Double.POSITIVE_INFINITY, resultSet.getDouble(1));
			}
		}
	}

	/**
	 * This test validates that null values are sorted last.
	 * @throws SQLException if something is going wrong
	 * see com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#nullSorting
	 */
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
	@ParameterizedTest
	@ValueSource(strings = {
			"SELECT CONVERT(varchar, 3.14)" //com.firebolt.jdbc.metadata.FireboltDatabaseMetadataTest#supportsConvert
	})
	void failingQuery(String query) throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			assertThrows(SQLException.class, () -> statement.execute(query));
		}
	}
}
