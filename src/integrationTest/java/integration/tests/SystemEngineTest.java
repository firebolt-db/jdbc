package integration.tests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.*;

import com.firebolt.jdbc.exception.FireboltException;

import integration.IntegrationTest;
import lombok.CustomLog;

@CustomLog
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SystemEngineTest extends IntegrationTest {

	private static final String DATABASE_NAME = "jdbc_system_engine_integration_test";
	private static final String ENGINE_NAME = "jdbc_system_engine_integration_test_engine";
	private static final String ENGINE_NEW_NAME = "jdbc_system_engine_integration_test_engine_2";
	private static final String SYSTEM_ENGINE_NAME = "system";

	@BeforeAll
	void beforeAll() {
		try {
			executeStatementFromFile("/statements/system/ddl.sql", SYSTEM_ENGINE_NAME);
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@AfterAll
	void afterAll() {
		try {
			executeStatementFromFile("/statements/system/cleanup.sql", SYSTEM_ENGINE_NAME);
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@Test
	@Order(1)
	void shouldCreateDatabaseWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("CREATE DATABASE %s", DATABASE_NAME));
		}
	}

	@Test
	@Order(2)
	void shouldCreateEngineWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("CREATE ENGINE %s", ENGINE_NAME));
		}
	}

	@Test
	@Order(3)
	void shouldAttachEngineWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("ATTACH ENGINE %s TO %s;", ENGINE_NAME, DATABASE_NAME));
		}
	}

	@Test
	@Order(4)
	void shouldAlterDatabaseWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(
					String.format("ALTER DATABASE %s WITH DESCRIPTION = 'JDBC Integration test'", DATABASE_NAME));
		}
	}

	@Test
	@Order(5)
	void shouldAlterEngineWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("ALTER ENGINE %s RENAME TO %s", ENGINE_NAME, ENGINE_NEW_NAME));
		}
	}

	@Test
	@Order(6)
	void shouldStartEngineWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("START ENGINE %s", ENGINE_NEW_NAME));
		}
	}

	@Test
	@Order(7)
	void shouldStopEngineWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("STOP ENGINE %s", ENGINE_NEW_NAME));
		}
	}

	@Test
	@Order(8)
	void shouldDropEngineWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("DROP ENGINE %s", ENGINE_NEW_NAME));
		}
	}

	@Test
	@Order(9)
	void shouldDropDatabaseWithoutException() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			statement.execute(String.format("DROP DATABASE %s", DATABASE_NAME));
		}
	}

	@Test
	void shouldFailToSelectOnSystemEngine() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME);
				Statement statement = connection.createStatement()) {
			assertThrows(FireboltException.class, () -> statement.executeQuery("SELECT 1;"));
		}
	}

}
