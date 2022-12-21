package integration.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.exception.FireboltException;

import integration.IntegrationTest;
import lombok.CustomLog;

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

}
