package integration.tests;

import com.firebolt.jdbc.exception.FireboltException;
import integration.IntegrationTest;
import kotlin.collections.ArrayDeque;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
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
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
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
				statement.execute(
						"SET advanced_mode=1; SET output_format_firebolt_type_names=1; SET bool_output_format=postgres;");
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

}
