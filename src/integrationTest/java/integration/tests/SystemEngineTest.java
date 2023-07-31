package integration.tests;

import com.firebolt.jdbc.exception.FireboltException;
import integration.ConnectionInfo;
import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SystemEngineTest extends IntegrationTest {

	private static final String DATABASE_NAME = "jdbc_system_engine_integration_test";
	private static final String ENGINE_NAME = "jdbc_system_engine_integration_test_engine";
	private static final String ENGINE_NEW_NAME = "jdbc_system_engine_integration_test_engine_2";

	@BeforeAll
	void beforeAll() {
		try {
			executeStatementFromFile("/statements/system/ddl.sql", getSystemEngineName());
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@AfterAll
	void afterAll() {
		try {
			executeStatementFromFile("/statements/system/cleanup.sql", getSystemEngineName());
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@Test
	void shouldSelect1() throws SQLException {
		try (Connection connection = createConnection(getSystemEngineName());
			 ResultSet rs = connection.createStatement().executeQuery("SELECT 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

	@Test
	void ddl() throws SQLException {
		Collection<String> expectedErrorMessages = Set.of(
				"Cannot execute a DDL query on the system engine.",
				"std::invalid_argument: URI segment 'N/A' can't contain '/'");

		try (Connection connection = createConnection(getSystemEngineName())) {
			String errorMessage = assertThrows(
					FireboltException.class,
					() -> connection.createStatement().executeUpdate("CREATE DIMENSION TABLE dummy(id INT)"))
					.getErrorMessageFromServer().replaceAll("\r?\n", "");
			assertTrue(expectedErrorMessages.contains(errorMessage));
		}
	}

	@Test
	void shouldFailToSelectFromCustomDbUsingSystemEngine() throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		String systemEngineJdbcUrl = new ConnectionInfo(current.getPrincipal(), current.getSecret(),
				current.getEnv(), current.getDatabase(), current.getAccount(), getSystemEngineName(), current.getApi()).toJdbcUrl();
		String customEngineJdbcUrl = current.toJdbcUrl();
		String principal = current.getPrincipal();
		String secret = current.getSecret();
		Collection<String> expectedErrorMessages = Set.of(
				"Queries against table dummy require a user engine",
				"The system engine doesn't support queries against table dummy. Run this query on a user engine.");

		try (Connection systemConnection = DriverManager.getConnection(systemEngineJdbcUrl, principal, secret);
			 Connection customConnection = DriverManager.getConnection(customEngineJdbcUrl, principal, secret)) {

			try {
				customConnection.createStatement().executeUpdate("CREATE DIMENSION TABLE dummy(id INT)");
				try (ResultSet rs = customConnection.createStatement().executeQuery("select count(*) from dummy")) {
					assertTrue(rs.next());
					assertEquals(0, rs.getInt(1));
					assertFalse(rs.next());
				}
				FireboltException e = assertThrows(FireboltException.class, () -> systemConnection.createStatement().executeQuery("select count(*) from dummy"));
				String actualErrorMessage = e.getErrorMessageFromServer().replaceAll("\r?\n", "");
				assertTrue(expectedErrorMessages.contains(actualErrorMessage));
			} finally {
				try {
					customConnection.createStatement().executeUpdate("DROP TABLE dummy");
				} catch(SQLException e) {
					// ignore the exception here; even if it happens it does not matter.
				}

			}
		}
	}

	@Test
	@Tag("slow")
	void shouldExecuteEngineManagementQueries() throws SQLException {
		try (Connection connection = this.createConnection(getSystemEngineName())) {
			List<String> queries = Arrays.asList(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE_NAME),
					String.format("CREATE ENGINE %s", ENGINE_NAME),
					String.format("ATTACH ENGINE %s TO %s;", ENGINE_NAME, DATABASE_NAME),
					String.format("ALTER DATABASE %s WITH DESCRIPTION = 'JDBC Integration test'", DATABASE_NAME),
					String.format("ALTER ENGINE %s RENAME TO %s", ENGINE_NAME, ENGINE_NEW_NAME),
					String.format("START ENGINE %s", ENGINE_NEW_NAME), String.format("STOP ENGINE %s", ENGINE_NEW_NAME),
					String.format("DROP ENGINE %s", ENGINE_NEW_NAME), String.format("DROP DATABASE %s", DATABASE_NAME));
			for (String query : queries) {
				try (Statement statement = connection.createStatement()) {
					statement.execute(query);
				}
			}
		}
	}
}
