package integration.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.exception.FireboltException;

import integration.IntegrationTest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Disabled
class StatementTest extends IntegrationTest {

	@BeforeAll
	void beforeAll() {
		executeStatementFromFile("/statements/statement/ddl.sql");
	}

	@AfterAll
	void afterAll() {
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

}
